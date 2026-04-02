import asyncio
import json
import time
import os
import logging
from collections import defaultdict, deque
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
import websockets
from dotenv import load_dotenv

# 环境与日志配置
load_dotenv()
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ================= 法比奥策略参数 (超短线级别) =================
FABIO_BIG_ORDER = 120000      # 法比奥看重确定性，门槛调高到 12万 USDT
FABIO_SQUEEZE_S = 45          # 统计过去 45 秒的波动收缩
FABIO_MAX_VOLATILITY = 0.0018 # 45秒内波动必须在 0.18% 以内（极度收缩）
FABIO_CONFIRM_S = 3           # 法比奥讲究快，3秒内必须脱离成本区
# ==========================================================

class FabioStrategyBot:
    def __init__(self):
        self.symbols = []
        self.history = defaultdict(lambda: deque(maxlen=200))
        self.prices = defaultdict(lambda: deque(maxlen=120))
        self.last_alert = {}
        self.session = None
        self.tele_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

    async def init_session(self):
        if not self.session: self.session = aiohttp.ClientSession()

    async def get_all_symbols(self):
        """ 自动获取币安所有 USDT 永续合约 """
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        async with self.session.get(url) as r:
            data = await r.json()
            return [s['symbol'] for s in data['symbols'] if s['status'] == 'TRADING' and s['symbol'].endswith('USDT')]

    def is_fabio_squeeze(self, symbol):
        """ 法比奥收缩逻辑：寻找弹簧被压到极致的瞬间 """
        p_list = list(self.prices[symbol])[-FABIO_SQUEEZE_S:]
        if len(p_list) < FABIO_SQUEEZE_S: return False
        
        high, low = max(p_list), min(p_list)
        current_vol = (high - low) / low
        return current_vol <= FABIO_MAX_VOLATILITY

    async def send_tg(self, msg):
        if not self.tele_token: logging.info(msg); return
        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        try:
            await self.session.post(url, json={"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"})
        except: pass

    async def monitor_breakout(self, symbol, entry_p):
        """ 法比奥式的瞬间确认逻辑 """
        # 1. 检查是否处于“法比奥收缩”状态
        if not self.is_fabio_squeeze(symbol): return
        
        # 2. 冷却：同一个币 10 分钟只报一次（法比奥只做首冲）
        if time.time() - self.last_alert.get(symbol, 0) < 600: return

        # 3. 法比奥确认逻辑：3秒内必须保持强势
        await asyncio.sleep(FABIO_CONFIRM_S)
        curr_p = list(self.prices[symbol])[-1] if self.prices[symbol] else entry_p
        
        # 确认：价格维持在突破点 0.05% 以上，且没有大幅回撤
        if curr_p >= entry_p * 1.0005:
            self.last_alert[symbol] = time.time()
            # 法比奥止损：极窄止损，设在收缩区间的底部
            p_list = list(self.prices[symbol])[-FABIO_SQUEEZE_S:]
            sl_price = min(p_list) 
            tp_price = curr_p * 1.012 # 目标 1.2% 左右的快速波动空间

            text = (
                f"🎯 <b>【法比奥·引爆点监控】</b>\n"
                f"<b>标的:</b> #{symbol}\n"
                f"<b>性质:</b> 极窄波动收缩突破\n"
                f"------------------------\n"
                f"<b>⚡ 入场位:</b> {curr_p:.6f}\n"
                f"<b>🛡 止损位:</b> {sl_price:.6f}\n"
                f"<b>💰 止盈参考:</b> {tp_price:.6f}\n"
                f"------------------------\n"
                f"<b>法比奥贴士:</b> 突破不回头。若价格跌破止损位，说明引爆失败，立即离场。"
            )
            await self.send_tg(text)

    async def handle_ws(self, chunk):
        streams = "/".join([f"{s.lower()}@aggTrade" for s in chunk])
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        async with websockets.connect(url) as ws:
            while True:
                raw = await ws.recv()
                data = json.loads(raw)['data']
                symbol, p, q = data['s'], float(data['p']), float(data['q'])
                side = "sell" if data['m'] else "buy"
                
                self.prices[symbol].append(p)
                # 记录大额买单
                if side == "buy" and (p * q) >= FABIO_BIG_ORDER:
                    asyncio.create_task(self.monitor_breakout(symbol, p))

    async def main(self):
        await self.init_session()
        all_symbols = await self.get_all_symbols()
        logging.info(f"法比奥系统启动：监控 {len(all_symbols)} 个合约标的")
        
        # 分片订阅
        chunks = [all_symbols[i:i+100] for i in range(0, len(all_symbols), 100)]
        await asyncio.gather(*[self.handle_ws(c) for c in chunks])

if __name__ == "__main__":
    bot = FabioStrategyBot()
    asyncio.run(bot.main())
