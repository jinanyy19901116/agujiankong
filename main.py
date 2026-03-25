import requests
import time
import os
import threading

TG_TOKEN = "8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TG_CHAT_ID = "6308781694"

last_inflow = {}
last_push = {}

# ================= TG =================
def send(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            data={"chat_id": TG_CHAT_ID, "text": msg},
            timeout=5
        )
    except:
        pass

# ================= 数据 =================
def get_stocks():
    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1,
        "pz": 5000,
        "fs": "m:0+t:6,m:1+t:2",
        "fields": "f12,f14,f3,f8,f62,f6,f10,f21"
    }
    try:
        return requests.get(url, params=params, timeout=5).json()["data"]["diff"]
    except:
        return []

# ================= 市场情绪 =================
def market_emotion(stocks):
    up = sum(1 for s in stocks if float(s["f3"]) > 0)
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
        sec = s.get("f21", "")
        if not sec:
            continue
        sector_map.setdefault(sec, []).append(float(s["f3"]))

    ranked = sorted(sector_map.items(), key=lambda x: sum(x[1])/len(x[1]), reverse=True)

    return [x[0] for x in ranked[:5]]

# ================= 游资模型 =================
def analyze(stocks):
    global last_inflow, last_push

    emotion = market_emotion(stocks)
    hot_sectors = sector_strength(stocks)

    signals = []
    now = time.time()

    for s in stocks:
        try:
            code = s["f12"]
            name = s["f14"]

            change = float(s["f3"])
            turnover = float(s["f8"])
            inflow = float(s["f62"])
            volume_ratio = float(s.get("f10", 0))
            sector = s.get("f21", "")

            # ---------- 基础过滤 ----------
            if sector not in hot_sectors:
                continue

            if not (0 < change < 6):
                continue

            if turnover < 3 or volume_ratio < 1.5:
                continue

            # ---------- 资金加速 ----------
            delta = inflow - last_inflow.get(code, 0)
            last_inflow[code] = inflow

            if delta < 5_000_000:
                continue

            # ---------- 冷却 ----------
            if code in last_push and now - last_push[code] < 300:
                continue

            # ---------- 评分 ----------
            score = 0

            # 资金
            if delta > 20_000_000:
                score += 4
            elif delta > 10_000_000:
                score += 3

            # 量比
            if volume_ratio > 2:
                score += 3
            elif volume_ratio > 1.5:
                score += 2

            # 换手
            if turnover > 6:
                score += 2

            # 板块
            if sector in hot_sectors[:2]:
                score += 3

            # 涨幅位置
            if 1 < change < 4:
                score += 2

            # 情绪
            if emotion == "强":
                score += 1

            # ---------- 输出 ----------
            if score >= 9:
                tag = "🚀游资主升"
            elif score >= 7:
                tag = "🔥强势股"
            else:
                continue

            last_push[code] = now

            signals.append(
                (score, f"{tag} {name}({code}) 涨:{change:.2f}% 换手:{turnover}% 量比:{volume_ratio} 板块:{sector}")
            )

        except:
            continue

    signals.sort(reverse=True)
    return [x[1] for x in signals[:10]]

# ================= 主循环 =================
def run():
    while True:
        try:
            stocks = get_stocks()
            result = analyze(stocks)

            if result:
                send("\n".join(result))

            time.sleep(10)

        except Exception as e:
            print("错误:", e)
            time.sleep(10)

# ================= 心跳 =================
def heartbeat():
    while True:
        send("💓系统运行正常")
        time.sleep(600)

# ================= 启动 =================
if __name__ == "__main__":
    send("🚀游资监控系统启动")

    threading.Thread(target=heartbeat).start()
    run()
