import requests
import time
import os
import threading

# ================= 配置 =================
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

last_inflow = {}
last_push = {}

# ================= 工具函数 =================
def safe_request(url, params=None):
    for _ in range(3):
        try:
            res = requests.get(url, params=params, timeout=5)

            # 防止被限流返回HTML
            if "application/json" not in res.headers.get("Content-Type", ""):
                print("⚠️ 返回非JSON（可能被限流）", flush=True)
                time.sleep(1)
                continue

            return res
        except Exception as e:
            print("请求失败重试:", e, flush=True)
            time.sleep(1)
    return None

# ================= Telegram =================
def send(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        print("⚠️ Telegram未配置", flush=True)
        return

    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": msg}, timeout=5)
    except Exception as e:
        print("TG发送失败:", e, flush=True)

# ================= API检测 =================
def check_apis():
    print("🔍 开始检测API...", flush=True)

    # A股
    res = safe_request("https://push2.eastmoney.com/api/qt/clist/get")
    print("A股API:", "正常" if res else "失败", flush=True)

    # 北向
    res = safe_request("https://push2.eastmoney.com/api/qt/kamt/get")
    print("北向API:", "正常" if res else "失败", flush=True)

# ================= A股数据 =================
def get_stocks():
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 5000,
        "fs": "m:0+t:6,m:1+t:2",
        "fields": "f12,f14,f3,f8,f62,f6,f10,f21"
    }

    res = safe_request(url, params)
    if not res:
        return []

    try:
        data = res.json()

        if not data or "data" not in data:
            print("⚠️ API结构异常", flush=True)
            return []

        diff = data["data"].get("diff")

        if not isinstance(diff, list):
            print("⚠️ diff不是列表", flush=True)
            return []

        return diff

    except Exception as e:
        print("解析股票失败:", e, flush=True)
        return []

# ================= 北向资金 =================
def get_north_money():
    url = "https://push2.eastmoney.com/api/qt/kamt/get"

    res = safe_request(url)
    if not res:
        return {"total": 0}

    try:
        data = res.json().get("data", {})
        return {
            "total": data.get("netBuyAmt", 0)
        }
    except:
        return {"total": 0}

# ================= 市场情绪 =================
def market_emotion(stocks):
    if not stocks:
        return "弱"

    up = sum(1 for s in stocks if isinstance(s, dict) and float(s.get("f3", 0)) > 0)
    down = len(stocks) - up

    ratio = up / max(1, down)

    if ratio > 1.5:
        return "强"
    elif ratio > 1:
        return "中"
    else:
        return "弱"

# ================= 板块强度 =================
def sector_strength(stocks):
    sector_map = {}

    for s in stocks:
        if not isinstance(s, dict):
            continue

        sec = s.get("f21", "")
        if not sec:
            continue

        sector_map.setdefault(sec, []).append(float(s.get("f3", 0)))

    ranked = sorted(
        sector_map.items(),
        key=lambda x: sum(x[1]) / len(x[1]),
        reverse=True
    )

    return [x[0] for x in ranked[:5]]

# ================= 三资模型 =================
def analyze(stocks):
    global last_inflow, last_push

    if not isinstance(stocks, list):
        print("⚠️ stocks不是列表", flush=True)
        return []

    north = get_north_money()
    emotion = market_emotion(stocks)
    hot_sectors = sector_strength(stocks)

    signals = []
    now = time.time()

    for s in stocks:
        try:
            if not isinstance(s, dict):
                continue

            code = s.get("f12")
            name = s.get("f14")

            change = float(s.get("f3", 0))
            turnover = float(s.get("f8", 0))
            inflow = float(s.get("f62", 0))
            volume_ratio = float(s.get("f10", 0))
            sector = s.get("f21", "")

            # ===== 基础过滤 =====
            if sector not in hot_sectors:
                continue

            if not (0 < change < 6):
                continue

            if turnover < 3 or volume_ratio < 1.5:
                continue

            # ===== 资金加速 =====
            delta = inflow - last_inflow.get(code, 0)
            last_inflow[code] = inflow

            if delta < 5_000_000:
                continue

            # ===== 冷却 =====
            if code in last_push and now - last_push[code] < 300:
                continue

            # ===== 评分 =====
            score = 0

            # 主力
            if inflow > 30_000_000:
                score += 3
            if delta > 10_000_000:
                score += 2

            # 游资
            if delta > 20_000_000:
                score += 4
            if volume_ratio > 2:
                score += 3

            # 北向
            if north["total"] > 0:
                score += 2
            if north["total"] > 2_000_000_000:
                score += 3

            # 情绪修正
            if emotion == "弱":
                score -= 2

            # ===== 信号 =====
            if score >= 10:
                tag = "🚀三资共振"
            elif score >= 7:
                tag = "🔥主力+游资"
            elif score >= 5:
                tag = "🟢主力"
            else:
                continue

            last_push[code] = now

            signals.append(
                (score,
                 f"{tag} {name}({code}) 涨:{change:.2f}% 换手:{turnover}% 量比:{volume_ratio}")
            )

        except Exception as e:
            continue

    signals.sort(reverse=True)
    return [x[1] for x in signals[:10]]

# ================= 主循环 =================
def run():
    while True:
        try:
            stocks = get_stocks()

            print(f"📊 stocks类型: {type(stocks)} 数量: {len(stocks) if isinstance(stocks, list) else '异常'}", flush=True)

            if not stocks:
                time.sleep(10)
                continue

            result = analyze(stocks)

            if result:
                print("🚨 信号:", result, flush=True)
                send("\n".join(result))

            time.sleep(10)

        except Exception as e:
            print("❌ 主循环错误:", e, flush=True)
            time.sleep(10)

# ================= 心跳 =================
def heartbeat():
    while True:
        send("💓系统运行正常")
        time.sleep(600)

# ================= 启动 =================
if __name__ == "__main__":
    print("🚀 系统启动", flush=True)

    check_apis()
    send("✅ 三资监控系统已启动")

    threading.Thread(target=heartbeat).start()

    run()
