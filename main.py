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

# 环境加载
load_dotenv()
BEIJING_TZ = ZoneInfo("Asia/Shanghai")

# ================= 策略参数建议值 (2026 高胜率版) =================
# 建议在 Railway 或 .env 中根据个人风险偏好调整
GLOBAL_BIG_ORDER = float(os.getenv("BIG_ORDER_THRESHOLD", "80000"))  # 基础门槛 8万 USDT
NET_RATIO_REQ = float(os.getenv("NET_RATIO_THRESHOLD", "2.5"))       # 净流入必须是流出的 2.5 倍
PRICE_CHECK_DELAY = 8                                                # 延迟 8 秒确认，过滤假突破
# ===============================================================

@dataclass
class TradeRecord:
    ts: float
    side: str
    amount: float
    price: float

class WhaleProBot:
    def __init__(self):
        # 币种白名单整合：主流 + 热门Meme + SIGN协议类
        whitelist_raw = os.getenv("TOKEN_WHITELIST", 
            "BTCUSDT,ETHUSDT,SOLUSDT,SIGNUSDT,ENSUSDT,LINKUSDT,DOGEUSDT,PEPEUSDT,WIFUSDT,PNUTUSDT,GOATUSDT,ACTUSDT,NEIROUSDT")
        self.whitelist = {x.strip().upper() for x in whitelist_raw.split(",") if x.strip()}
        
        self.tele_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        
        # 数据存储
        self.history = defaultdict(lambda: deque(maxlen=300))
        self.last_prices = {}
        self._stop = False
        self.session = None

    async def init_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    def get_bj_time(self):
        return datetime.now(BEIJING_TZ).strftime("%H:%M:%S")

    def get_market_sentiment(self, symbol):
        """核心策略：计算 60 秒内的资金合力净值"""
        now = time.time()
        q = self.history[symbol]
        while q and now - q[0].ts > 60:
            q.popleft()
            
        buys = sum(t.amount for t in q if t.side == "buy")
        sells = sum(t.amount for t in q if t.side == "sell")
        
        if buys == 0 and sells == 0: return None, 0, 0, 0
        
        net_flow = buys - sells
        # 计算倍数关系
        ratio = (buys / sells) if sells > 0 else 999 if buys > 0 else 0
        if sells > buys:
            ratio = (sells / buys) if buys > 0 else 999
            
        return ("BUY" if net_flow > 0 else "SELL"), abs(net_flow), ratio, (buys + sells)

    def calculate_tp_sl(self, price, side):
        """基于胜率逻辑的自动止盈止损建议"""
        if side == "BUY":
            tp = price * 1.015  # 1.5% 止盈
            sl = price * 0.994  # 0.6% 止损
        else:
            tp = price * 0.985
            sl = price * 1.006
        return tp, sl

    async def verify_signal(self, symbol, entry_price, detected_side):
        """
        顺势确认逻辑：
        大单发生 8 秒后，价格必须维持在突破方向 0.15% - 0.3% 以上
        """
        await asyncio.sleep(PRICE_CHECK_DELAY)
        
        curr_price = self.last_prices.get(symbol, entry_price)
        side, net_val, ratio, total_vol = self.get_market_sentiment(symbol)
        
        if not side or side != detected_side or ratio < NET_RATIO_REQ:
            return # 合力不足或方向逆转，放弃信号

        # 价格波动率确认
        move = (curr_price - entry_price) / entry_price
        is_valid = False
        
        # 针对不同币种执行不同门槛
        move_req = 0.0015 # 默认 0.15%
        if any(m in symbol for m in ["PEPE", "DOGE", "PNUT", "GOAT"]): move_req = 0.003 # Meme币要求 0.3%
        if "SIGN" in symbol: move_req = 0.001 # SIGN类相对稳健，0.1% 即可

        if side == "BUY" and move >= move_req: is_valid = True
        if side == "SELL" and move <= -move_req: is_valid = True

        if is_valid:
            tp, sl = self.calculate_tp_sl(curr_price, side)
            emoji = "🚀" if side == "BUY" else "⛈"
            msg = (
                f"{emoji} <b>【胜率策略-资金突破】</b>\n\n"
                f"<b>币种:</b> #{symbol}\n"
                f"<b>合力方向:</b> {'多头持续流入' if side == 'BUY' else '空头疯狂砸盘'}\n"
                f"<b>60s净流入:</b> {net_val/10000:.1f}万 USDT\n"
                f"<b>多空合力比:</b> {ratio:.1f} 倍\n"
                f"<b>确认涨跌:</b> {move*100:+.2f}%\n"
                f"------------------------\n"
                f"<b>🎯 建议入场价:</b> {curr_price:.6f}\n"
                f"<b>💰 建议止盈:</b> {tp:.6f}\n"
                f"<b>🛡 建议止损:</b> {sl:.6f}\n"
                f"------------------------\n"
                f"时间: {self.get_bj_time()} (北京)"
            )
            await self.send_tg(msg)

    async def send_tg(self, text):
        if not self.tele_token: 
            print(f"[{self.get_bj_time()}] {text}")
            return
        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        try:
            async with self.session.post(url, json={"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}) as r:
                pass
        except: pass

    async def handle_msg(self, msg):
        try:
            raw = json.loads(msg)
            data = raw.get("data", {})
            symbol = data.get("s")
            if not symbol or symbol not in self.whitelist: return
            
            p = float(data.get("p", 0))
            q = float(data.get("q", 0))
            amt = p * q
            self.last_prices[symbol] = p
            
            # 基础统计门槛：2万以上计入合力，防止散户噪音
            if amt >= 20000:
                side = "sell" if data.get("m") else "buy"
                self.history[symbol].append(TradeRecord(ts=time.time(), side=side, amount=amt, price=p))
                
                # 触发深度验证门槛
                if amt >= GLOBAL_BIG_ORDER:
                    asyncio.create_task(self.verify_signal(symbol, p, side.upper()))
        except: pass

    async def run(self):
        await self.init_session()
        streams = "/".join([f"{s.lower()}@aggTrade" for s in self.whitelist])
        print(f"[{self.get_bj_time()}] 高胜率版启动! 监控币种数: {len(self.whitelist)}")
        
        while not self._stop:
            try:
                async with websockets.connect(f"wss://fstream.binance.com/stream?streams={streams}") as ws:
                    async for m in ws:
                        await self.handle_msg(m)
            except:
                await asyncio.sleep(5)

if __name__ == "__main__":
    bot = WhaleProBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        pass
