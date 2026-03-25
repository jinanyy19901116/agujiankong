import requests
import time
import os
import random

# =========================
# 🔧 Telegram
# =========================
TG_TOKEN = "8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TG_CHAT_ID = "6308781694"

def send_telegram(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        print("⚠️ Telegram未配置")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT_ID, "text": msg},
            timeout=10
        )
    except Exception as e:
        print("❌ TG发送失败:", e)

# =========================
# 🌐 Session（防封核心）
# =========================
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "http://quote.eastmoney.com/",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Connection": "keep-alive"
})

# =========================
# 📦 缓存（防崩）
# =========================
cache_stocks = []
cache_changes = set()

# =========================
# 🛡️ 安全请求
# =========================
def safe_get(url, retry=3):
    for i in range(retry):
        try:
            res = session.get(url, timeout=10)

            if "application/json" not in res.headers.get("Content-Type", ""):
                raise Exception("非JSON")

            return res.json()

        except Exception as e:
            print(f"❌ 请求失败 {i+1}:", e)
            time.sleep(random.randint(3, 8))

    return None

# =========================
# 📊 A股行情（主源）
# =========================
def get_all_stocks():
    global cache_stocks

    url = "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=2000&fid=f3&fs=m:0+t:6,m:0+t:13,m:1+t:2"

    data = safe_get(url)

    if data:
        stocks = data.get("data", {}).get("diff", [])
        print(f"✅ 行情获取成功: {len(stocks)}")
        cache_stocks = stocks
        return stocks

    print("⚠️ 使用缓存行情")
    return cache_stocks

# =========================
# 📈 异动（重试+缓存）
# =========================
def get_stock_changes():
    global cache_changes

    url = "http://push2.eastmoney.com/api/qt/stock/fflow/kline/get"

    data = safe_get(url)

    if data:
        klines = data.get("data", {}).get("klines", [])
        codes = []

        for k in klines:
            try:
                code = k.split(",")[0]
                codes.append(code)
            except:
                continue

        print(f"✅ 异动成功: {len(codes)}")
        cache_changes = set(codes)
        return cache_changes

    print("⚠️ 使用缓存异动")
    return cache_changes

# =========================
# 📊 SSE
# =========================
def get_sse():
    url = "http://www.sse.com.cn/api/market/stockdata/statistic"
    data = safe_get(url)
    print("SSE:", "正常" if data else "失败")
    return data

# =========================
# 📊 SZSE
# =========================
def get_szse():
    url = "http://www.szse.cn/api/market/overview/index"
    data = safe_get(url)
    print("SZSE:", "正常" if data else "失败")
    return data

# =========================
# 💰 北向资金（模拟）
# =========================
def get_north_money():
    return random.randint(1, 10) * 100000000

# =========================
# 🎯 潜力股筛选（重点优化）
# =========================
def scan(stocks, changes, north_money):
    result = []

    for s in stocks:
        try:
            code = s.get("f12")
            name = s.get("f14")

            change = float(s.get("f3", 0))
            turnover = float(s.get("f8", 0))
            volume_ratio = float(s.get("f10", 0))
            main_inflow = float(s.get("f62", 0))

            # ❗核心：排除涨停股，找“即将启动”
            if change > 4:
                continue

            if turnover < 3 or main_inflow <= 0:
                continue

            score = 0

            if volume_ratio > 1.5:
                score += 2
            if turnover > 5:
                score += 2
            if main_inflow > 5000000:
                score += 3
            if north_money > 200000000:
                score += 2
            if code in changes:
                score += 2

            if score >= 5:
                result.append((name, code, score, change))

        except:
            continue

    result.sort(key=lambda x: x[2], reverse=True)
    return result[:10]

# =========================
# 🚀 主循环
# =========================
def main():
    print("🚀 系统启动")
    send_telegram("✅ 潜力股系统已启动（稳定版）")

    while True:
        print("\n🔄 开始扫描...")

        stocks = get_all_stocks()
        changes = get_stock_changes()

        # 多源调用（不会影响主逻辑）
        get_sse()
        get_szse()

        north_money = get_north_money()

        picks = scan(stocks, changes, north_money)

        print(f"🎯 选股结果: {len(picks)}")

        for p in picks:
            msg = f"""
🔥潜力股
{p[0]} ({p[1]})
评分: {p[2]}
涨幅: {p[3]}%
"""
            print(msg)
            send_telegram(msg)

        sleep_time = random.randint(40, 80)
        print(f"⏱ 等待 {sleep_time}s")
        time.sleep(sleep_time)

# =========================
if __name__ == "__main__":
    main()
