import time
import os
import threading
import random
import requests
import akshare as ak

# ================= 配置 =================
TG_TOKEN ="8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TG_CHAT_ID = 6308781694

last_push = {}

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

# ================= 数据源（AkShare） =================
def get_stocks():
    try:
        df = ak.stock_zh_a_spot_em()

        stocks = []
        for _, row in df.iterrows():
            try:
                stocks.append({
                    "code": row["代码"],
                    "name": row["名称"],
                    "change": float(row["涨跌幅"]),
                    "turnover": float(row["换手率"]),
                    "volume_ratio": float(row["量比"]),
                    "inflow": float(row["主力净流入"]),
                })
            except:
                continue

        return stocks

    except Exception as e:
        print("❌ AkShare获取失败:", e, flush=True)
        return []

# ================= 北向资金 =================
def get_north_money():
    try:
        df = ak.stock_hsgt_north_net_flow_em()
        value = df.iloc[-1]["当日净流入"]
        return float(value)
    except:
        return 0

# ================= 市场情绪 =================
def market_emotion(stocks):
    if not stocks:
        return "弱"

    up = sum(1 for s in stocks if s["change"] > 0)
    down = len(stocks) - up

    ratio = up / max(1, down)

    if ratio > 1.5:
        return "强"
    elif ratio > 1:
        return "中"
    else:
        return "弱"

# ================= 核心策略 =================
def analyze(stocks):
    global last_push

    north = get_north_money()
    emotion = market_emotion(stocks)

    results = []
    now = time.time()

    for s in stocks:
        try:
            code = s["code"]
            name = s["name"]

            change = s["change"]
            turnover = s["turnover"]
            volume_ratio = s["volume_ratio"]
            inflow = s["inflow"]

            # ===== 提前识别（避免涨停）=====
            if not (0 < change < 7):
                continue

            if turnover < 3:
                continue

            if volume_ratio < 1.5:
                continue

            # ===== 冷却机制 =====
            if code in last_push and now - last_push[code] < 300:
                continue

            # ===== 评分模型 =====
            score = 0

            # 主力资金
            if inflow > 30_000_000:
                score += 3

            # 量能爆发
            if volume_ratio > 2:
                score += 3

            # 换手活跃
            if turnover > 5:
                score += 2

            # 北向资金
            if north > 0:
                score += 2
            if north > 2_000_000_000:
                score += 2

            # 情绪加成
            if emotion == "强":
                score += 2
            elif emotion == "弱":
                score -= 2

            # ===== 信号判断 =====
            if score >= 9:
                tag = "🚀强势启动"
            elif score >= 7:
                tag = "🔥资金介入"
            elif score >= 5:
                tag = "🟢观察"
            else:
                continue

            last_push[code] = now

            results.append(
                (score, f"{tag} {name}({code}) 涨:{change:.2f}% 换手:{turnover}% 量比:{volume_ratio}")
            )

        except:
            continue

    results.sort(reverse=True)
    return [x[1] for x in results[:10]]

# ================= 主循环 =================
def run():
    while True:
        try:
            stocks = get_stocks()

            print(f"📊 股票数量: {len(stocks)}", flush=True)

            if not stocks:
                time.sleep(30)
                continue

            signals = analyze(stocks)

            if signals:
                print("🚨 信号:", signals, flush=True)
                send("\n".join(signals))

            time.sleep(random.randint(20, 40))

        except Exception as e:
            print("❌ 主循环错误:", e, flush=True)
            time.sleep(30)

# ================= 心跳 =================
def heartbeat():
    while True:
        send("💓系统运行正常")
        time.sleep(600)

# ================= 启动 =================
if __name__ == "__main__":
    print("🚀 稳定版系统启动", flush=True)

    send("✅ A股监控系统已启动（稳定版）")

    threading.Thread(target=heartbeat).start()

    run()
