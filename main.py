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

# 环境配置
load_dotenv()
BEIJING_TZ = ZoneInfo("Asia/Shanghai")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ================= 1小时双向策略参数 =================
WHALE_THRESHOLD_1H = 400000   # 1H级别大单门槛：40万 USDT
FLOW_WINDOW = 300            # 5分钟资金聚合
EMA_PERIOD = 20              # EMA 20 趋势生命线
SQUEEZE_LIMIT = 0.012        # 1.2% 以内的窄幅挤压
# ===================================================

class BiDirectionalWhaleBot:
    def __init__(self):
        self.prices = defaultdict(lambda: deque(maxlen=200)) # 存储分钟价
        self.trades = defaultdict(lambda: deque(maxlen=1000))
        self.last_alert = {}
        self.session = None
        self.tele_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

    async def init_session(self):
        if not self.session: self.session = aiohttp.ClientSession()

    def get_ema(self, symbol):
        p_list = list(self.prices[symbol])
        if len(p_list) < EMA_PERIOD: return None
        return sum(p_list[-EMA_PERIOD:]) / EMA_PERIOD

    def analyze_flow(self, symbol):
        now = time.time()
        f_trades = [t for t in self.trades[symbol] if now - t['ts'] < FLOW_WINDOW]
        buys = sum(t['amt'] for t in f_trades if t['side'] == 'buy')
        sells = sum(t['amt'] for t in f_trades if t['side'] == 'sell')
        
        if buys > sells * 2.5: return "LONG", buys - sells
        if sells > buys * 2.5: return "SHORT", sells - buys
        return None, 0

    async def verify_1h_signal(self, symbol, p, trigger_side):
        ema = self.get_ema(symbol)
        if not ema: return

        # 冷却检查
        if time.time() - self.last_alert.get(symbol, 0) < 1800: return

        # 检查 1H 挤压形态（过去 60 分钟价格样本）
        p_history = list(self.prices[symbol])[-60:]
        if not p_history: return
        h, l = max(p_history), min(p_history)
        if (h - l) / l > SQUEEZE_LIMIT: return 

        # 资金流确认
        flow_side, net_val = self.analyze_flow(symbol)
        
        # --- 逻辑判定 ---
        signal = None
        if trigger_side == "BUY" and flow_side == "LONG" and p > ema:
            signal = "多头波段突破 📈"
            tp, sl = p * 1.035, p * 0.988  # 多单止盈 3.5%, 止损 1.2%
        elif trigger_side == "SELL" and flow_side == "SHORT" and p < ema:
            signal = "空头波段破位 📉"
            tp, sl = p * 0.965, p * 1.012  # 空单止盈 3.5%, 止损 1.2%

        if signal:
            self.last_alert[symbol] = time.time()
            text = (
                f"🚨 <b>【1H 双向信号 · {signal}】</b>\n"
                f"<b>标的:</b> #{symbol}\n"
                f"<b>资金净值:</b> {net_val/10000:.1f}万 USDT\n"
                f"<b>趋势位:</b> {'EMA20上方' if p > ema else 'EMA20下方'}\n"
                f"------------------------\n"
                f"<b>🚩 入场参考:</b> {p:.6f}\n"
                f"<b>🎯 目标止盈:</b> {tp:.6f}\n"
                f"<b>🛡 严格止损:</b> {sl:.6f}\n"
                f"------------------------\n"
                f"建议：1小时级别信号，适合带杠杆持仓数小时至一天。"
            )
            await self.send_tg(text)

    async def send_tg(self, msg):
        if not self.tele_token: logging.info(msg); return
        url = f"https://api.telegram.org/bot{self.tele_token}/sendMessage"
        try:
            await self.session.post(url, json={"chat_id": self.chat_id, "text": msg, "parse_mode": "HTML"})
        except: pass

    async def handle_ws(self, symbols):
        streams = "/".join([f"{s.lower()}@aggTrade" for s in symbols])
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        async with websockets.connect(url) as ws:
            while True:
                data = json.loads(await ws.recv())['data']
                symbol, p, q = data['s'], float(data['p']), float(data['q'])
                side = "sell" if data['m'] else "buy"
                amt = p * q
                
                # 记录价格（每分钟存一次用于 EMA）
                if int(time.time()) % 60 == 0:
                    self.prices[symbol].append(p)
                
                self.trades[symbol].append({'ts': time.time(), 'side': side, 'amt': amt})
                
                # 触发条件：大额单出现
                if amt >= WHALE_THRESHOLD_1H:
                    asyncio.create_task(self.verify_1h_signal(symbol, p, side.upper()))

    async def main(self):
        await self.init_session()
        # 精选波动率大的做空/做多标的
        active_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "SIGNUSDT", "DOGEUSDT", "PEPEUSDT", "WIFUSDT", "LINKUSDT", "SUIUSDT", "APTUSDT"]
        logging.info(f"双向 1H 监控启动：{len(active_list)} 个核心品种")
        await self.handle_ws(active_list)

if __name__ == "__main__":
    bot = BiDirectionalWhaleBot()
    asyncio.run(bot.main())
