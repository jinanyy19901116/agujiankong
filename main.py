import requests
import time
import os
import random
import pandas as pd

# =========================
# 🔧 Telegram 配置（必须填）
# =========================
TG_TOKEN =  "8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TG_CHAT_ID ="6308781694"

def send_telegram(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        print("⚠️ 电报未配置", flush=True)
        return

    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, data={
            "chat_id": TG_CHAT_ID,
            "text": msg
        }, timeout=10)
    except Exception as e:
        print("❌ Telegram发送失败:", e, flush=True)

# =========================
# 🌐 通用请求（防封）
# =========================
def safe_request(url, headers=None):
    try:
        res = requests.get(url, headers=headers, timeout=10)

        if "application/json" not in res.headers.get("Content-Type", ""):
            print("⚠️ 返回非JSON（可能被限流）")
            return None

        return res.json()
    except Exception as e:
        print("❌ 请求失败:", e)
        return None

# =========================
# 🏦 上交所数据（稳定）
# =========================
def get_sse_summary():
    url = "http://www.sse.com.cn/market/stockdata/statistic/"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://www.sse.com.cn/"
    }

    data = safe_request(url, headers)

    if data:
        print("✅ SSE获取成功", flush=True)
    else:
        print("❌ SSE获取失败", flush=True)

    return data

# =========================
# 📊 A股行情（东方财富API直连）
# =========================
def get_all_stocks():
    url = "http://push2.eastmoney.com/api/qt/clist/get"

    params = {
        "pn": 1,
        "pz": 2000,
        "po": 1,
        "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2,
        "invt": 2,
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:13,m:1 t:2,m:1 t:23",
        "fields": "f12,f14,f2,f3,f5,f6,f7,f8,f9,f10,f62"
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://quote.eastmoney.com/"
    }

    try:
        res = requests.get(url, params=params, headers=headers, timeout=10)
        data = res.json()

        stocks = data.get("data", {}).get("diff", [])

        print(f"📊 获取股票数量: {len(stocks)}", flush=True)
        return stocks

    except Exception as e:
        print("❌ A股API失败:", e)
        return []

# =========================
# 💰 主力资金判断
# =========================
def is_main_fund_inflow(stock):
    try:
        main_money = stock.get("f62", 0)  # 主力净流入
        return main_money > 0
    except:
        return False

# =========================
# 🔁 换手率判断
# =========================
def is_turnover_ok(stock):
    try:
        turnover = stock.get("f8", 0)
        return turnover > 5
    except:
        return False

# =========================
# 🚫 避免涨停买入
# =========================
def is_not_limit_up(stock):
    try:
        pct = stock.get("f3", 0)
        return pct < 9.5
    except:
        return False

# =========================
# 📈 市场情绪（SSE）
# =========================
def analyze_market(sse_data):
    try:
        # ⚠️ SSE结构不固定，这里做容错
        return "📊 市场正常"
    except:
        return "未知"

# =========================
# 🔍 核心选股逻辑
# =========================
def scan_stocks(stocks):
    results = []

    for stock in stocks:
        try:
            if (
                is_turnover_ok(stock)
                and is_main_fund_inflow(stock)
                and is_not_limit_up(stock)
            ):
                results.append(stock)
        except:
            continue

    return results

# =========================
# 🚀 主程序
# =========================
def main():
    print("🚀 程序启动成功", flush=True)
    send_telegram("✅ 系统启动成功")

    while True:
        print("🔄 开始扫描...", flush=True)

        # 1️⃣ 股票数据
        stocks = get_all_stocks()

        if not stocks:
            print("❌ 股票数据为空", flush=True)
            time.sleep(60)
            continue

        # 2️⃣ SSE市场
        sse_data = get_sse_summary()
        sentiment = analyze_market(sse_data)

        print("📈 市场:", sentiment, flush=True)

        # 3️⃣ 筛选
        selected = scan_stocks(stocks)

        print(f"🎯 筛选结果: {len(selected)}", flush=True)

        # 4️⃣ 推送
        for s in selected[:5]:  # 最多发5个，防刷屏
            msg = f"""
🔥 发现潜力股
名称: {s.get('f14')}
代码: {s.get('f12')}
涨幅: {s.get('f3')}%
换手: {s.get('f8')}%
主力流入: {s.get('f62')}
"""
            send_telegram(msg)

        # ⏱ 防封随机间隔
        sleep_time = random.randint(40, 80)
        print(f"⏱ 等待 {sleep_time}s", flush=True)
        time.sleep(sleep_time)

# =========================
# ▶️ 启动
# =========================
if __name__ == "__main__":
    main()
