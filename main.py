import os
import json
import time
import asyncio
import logging
import hashlib
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta

import requests
import websockets

ARKHAM_HTTP_BASE = "https://api.arkm.com"
ARKHAM_WS_BASE = "wss://api.arkm.com/ws/transfers"

API_KEY = os.getenv("ARKHAM_API_KEY", "").strip()

# 固定币种（已去 KRW-）
TOKEN_WHITELIST = [
    "sent","sol","cfg","doge","dood","ankr","bard","tao","elsa","ada",
    "ip","kite","ong","vana","kat","wld","sui","la","steem","virtual",
    "gas","moodeng","xlm","sahara","chz","trump","anime","shib",
    "sei","sign","sonic","trx","skr","bch","pengu","kernel","order",
    "enso","ath","knc","zbt","link","akt","cpool"
]

USD_THRESHOLD = float(os.getenv("USD_THRESHOLD", "150000"))

UPBIT_KEYWORDS = ["upbit", "up-bit"]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

BEIJING_TZ = timezone(timedelta(hours=8))

recent_signals = []

def setup_logger():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def telegram_send(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("Telegram 未配置: token=%s chat=%s", bool(TELEGRAM_BOT_TOKEN), bool(TELEGRAM_CHAT_ID))
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]})

def send_startup_test():
    telegram_send(f"✅ 机器人启动成功\n监控币种数量: {len(TOKEN_WHITELIST)}")

def contains_upbit(text):
    return any(k in text.lower() for k in UPBIT_KEYWORDS)

def get(d, *keys):
    for k in keys:
        if k in d and d[k]:
            return d[k]
    return ""

def classify(item):
    f = get(item, "fromLabel","fromEntity","fromAddress","from")
    t = get(item, "toLabel","toEntity","toAddress","to")

    if contains_upbit(f) and not contains_upbit(t):
        return "流出Upbit"
    if contains_upbit(t) and not contains_upbit(f):
        return "流入Upbit"
    if contains_upbit(f) and contains_upbit(t):
        return "内部"
    return "无关"

def analyze(item):
    direction = classify(item)
    usd = float(get(item,"usd","valueUsd","historicalUSD") or 0)
    symbol = get(item,"symbol","tokenSymbol","tokenId").lower()

    if symbol not in TOKEN_WHITELIST:
        return None

    if usd < USD_THRESHOLD:
        return None

    strength = 0
    reasons = []

    if direction == "流入Upbit":
        strength += 2
        bias = "做空"
        reasons.append("转入交易所")
    elif direction == "流出Upbit":
        strength += 2
        bias = "做多"
        reasons.append("转出交易所")
    else:
        return None

    if usd > USD_THRESHOLD * 2:
        strength += 1
        reasons.append("金额较大")

    same = sum(1 for x in recent_signals if x == direction)
    if same >= 1:
        strength += 1
        reasons.append("连续信号")

    if strength >= 4:
        level = "🔥🔥🔥 强信号"
    elif strength >= 2:
        level = "🔥🔥 中信号"
    else:
        return None   # ❗过滤弱信号

    recent_signals.append(direction)

    return level, bias, reasons

def build_msg(item, signal):
    level, bias, reasons = signal
    usd = float(get(item,"usd","valueUsd","historicalUSD") or 0)
    symbol = get(item,"symbol","tokenSymbol","tokenId").upper()

    return f"""
{level}
币种: {symbol}
方向: {bias}
金额: ${usd:,.0f}

原因:
- {' '.join(reasons)}
"""

async def run():
    if not API_KEY:
        raise ValueError("缺少ARKHAM_API_KEY")

    session = requests.post(f"{ARKHAM_HTTP_BASE}/ws/sessions", headers={"API-Key":API_KEY}).json()
    sid = session.get("sessionId") or session.get("id")

    url = f"{ARKHAM_WS_BASE}?session_id={sid}&usdGte={int(USD_THRESHOLD)}"

    async with websockets.connect(
        url,
        extra_headers={"API-Key": API_KEY}
    ) as ws:

        logging.info("WebSocket 已连接")

        async for raw in ws:
            try:
                data = json.loads(raw)
            except:
                continue

            items = data if isinstance(data,list) else [data]

            for item in items:
                signal = analyze(item)
                if not signal:
                    continue

                msg = build_msg(item, signal)
                logging.info(msg)
                telegram_send(msg)

def main():
    setup_logger()

    logging.info("Upbit监控启动")

    # 🔥 调试环境变量
    logging.info("ENV TELEGRAM_BOT_TOKEN = %s", os.getenv("TELEGRAM_BOT_TOKEN"))
    logging.info("ENV TELEGRAM_CHAT_ID = %s", os.getenv("TELEGRAM_CHAT_ID"))

    send_startup_test()

    asyncio.run(run())

if __name__ == "__main__":
    main()
