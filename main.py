import os
import json
import time
import asyncio
import logging
from dataclasses import dataclass
from collections import defaultdict, deque
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
import websockets
from dotenv import load_dotenv

# 加载配置
load_dotenv()
BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# ================= 配置增强区 =================
# 建议在 Railway 环境变量中调整这些值
BIG_ORDER_THRESHOLD = float(os.getenv("BIG_ORDER_THRESHOLD", "100000")) # 提高到10万
NET_RATIO_THRESHOLD = float(os.getenv("NET_RATIO_THRESHOLD", "2.0"))    # 优势倍数：买单要是卖单的2倍才报
WINDOW_SIZE_SEC = int(os.getenv("WINDOW_SIZE_SEC", "60"))              # 统计过去60秒的合力
PRICE_MOVE_THRESHOLD = float(os.getenv("PRICE_MOVE_THRESHOLD", "0.0015")) # 推动0.15%才算有效
# =============================================

def bj_now_str() -> str:
    return datetime.now(BEIJING_TZ).strftime("%H:%M:%S")

@dataclass
class TradeRecord:
    ts: float
    side: str  # "buy", "sell"
    amount: float

class SignalBot:
    def __init__(self):
        self.ws_url = "wss://fstream.binance.com/stream"
        # 你的白名单
        whitelist_raw = os.getenv("TOKEN_WHITELIST", "DOGEUSDT,HYPEUSDT,SIGNUSDT,SIRENUSDT,SOLUSDT,BTCUSDT")
        self.whitelist = {x.strip().upper() for x in whitelist_raw.split(",") if x.strip()}
        self.stream_symbols = [s.lower() for s in self.whitelist]
        
        self.tele_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        
        # 运行时数据：每个币种维护一个 60 秒的大单队列
        self.history: dict[str, deque[TradeRecord]] = defaultdict(lambda: deque(maxlen=200))
        self.last_prices: dict[str, float] = {}
        self.last_alert_ts: dict[str, float] = {}
        
        self._stop = False
        self.session = None

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))

    async def send_tg(self, text: str):
        if not self.tele_token or not self.chat_id:
            print(f"DEBUG: {text}")
            return
        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        try:
            async with self.session.post(url, json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}) as r:
                if r.status != 200:
                    logging.error(f"TG发送失败: {await r.text()}")
        except Exception as e:
            logging.error(f"TG异常: {e}")

    def get_net_flow(self, symbol: str):
        """计算窗口内的合力方向"""
        now = time.time()
        q = self.history[symbol]
        
        # 清理过期记录
        while q and now - q[0].ts > WINDOW_SIZE_SEC:
            q.popleft()
            
        buy_vol = sum(t.amount for t in q if t.side == "buy")
        sell_vol = sum(t.amount for t in q if t.side == "sell")
        
        if buy_vol == 0 and sell_vol == 0:
            return None, 0, 0
        
        # 判断优势方向
        if buy_vol > sell_vol * NET_RATIO_THRESHOLD and buy_vol >= BIG_ORDER_THRESHOLD:
            return "BUY", buy_vol, sell_vol
        if sell_vol > buy_vol * NET_RATIO_THRESHOLD and sell_vol >= BIG_ORDER_THRESHOLD:
            return "SELL", sell_vol, buy_vol
            
        return None, buy_vol, sell_vol

    async def check_signal(self, symbol: str, entry_price: float, detected_side: str):
        """价格验证逻辑"""
        await asyncio.sleep(5) # 给市场5秒反应时间
        
        current_price = self.last_prices.get(symbol, entry_price)
        direction, major_vol, minor_vol = self.get_net_flow(symbol)
        
        if not direction or direction != detected_side:
            return # 方向不统一或者合力不够，闭嘴

        # 计算涨跌幅
        move = (current_price - entry_price) / entry_price
        if direction == "BUY" and move >= PRICE_MOVE_THRESHOLD:
            msg = (f"🟢 <b>多头合力突破</b>\n"
                   f"时间: {bj_now_str()}\n"
                   f"币种: # {symbol}\n"
                   f"净买入: {major_vol - minor_vol:,.0f} USDT\n"
                   f"5秒涨幅: +{move*100:.2f}%\n"
                   f"提示: 资金正持续买入，价格撑住。")
            await self.send_tg(msg)
        elif direction == "SELL" and move <= -PRICE_MOVE_THRESHOLD:
            msg = (f"🔴 <b>空头合力砸盘</b>\n"
                   f"时间: {bj_now_str()}\n"
                   f"币种: # {symbol}\n"
                   f"净卖出: {major_vol - minor_vol:,.0f} USDT\n"
                   f"5秒跌幅: {move*100:.2f}%\n"
                   f"提示: 资金持续流出，支撑位破裂。")
            await self.send_tg(msg)

    async def handle_msg(self, msg):
        try:
            data = json.loads(msg).get("data", {})
            symbol = data.get("s")
            if not symbol or symbol not in self.whitelist:
                return
            
            p = float(data.get("p", 0))
            q = float(data.get("q", 0))
            amt = p * q
            self.last_prices[symbol] = p
            
            # 只记录超过 2万 的单子作为统计基数（避免细碎单干扰）
            if amt < 20000:
                return
            
            side = "sell" if data.get("m") else "buy"
            self.history[symbol].append(TradeRecord(ts=time.time(), side=side, amount=amt))
            
            # 只有当单笔超过阈值时，才触发“合力检查”任务
            if amt >= BIG_ORDER_THRESHOLD:
                asyncio.create_task(self.check_signal(symbol, p, side.upper()))
                
        except:
            pass

    async def run(self):
        await self.init_session()
        streams = "/".join([f"{s.lower()}@aggTrade" for s in self.whitelist])
        full_url = f"{self.ws_url}?streams={streams}"
        
        print(f"已启动资金合力监控，当前白名单: {len(self.whitelist)} 个币种")
        
        while not self._stop:
            try:
                async with websockets.connect(full_url) as ws:
                    async for m in ws:
                        await self.handle_msg(m)
            except Exception as e:
                logging.error(f"连接断开: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bot = SignalBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass
