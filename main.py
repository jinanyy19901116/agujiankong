import time
import os
import threading
import random

# 第三方库
import akshare as ak
import tushare as ts
import requests

# ================= 配置 =================
TG_TOKEN ="8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TG_CHAT_ID = 6308781694

TUSHARE_TOKEN = "69c9f84f3ae8dc99b25d0ee2443c218ec28c8c5983859a13c83629dd"

# 初始化 tushare
if TUSHARE_TOKEN:
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
else:
    pro = None

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

# ================= 数据源1：AkShare =================
def get_stocks_akshare():
    try:
        df = ak.stock_zh_a_spot_em()

        stocks = []
        for _, row in df.iterrows():
            stocks.append({
                "code": row["代码"],
                "name": row["名称"],
                "change": float(row["涨跌幅"]),
                "turnover": float(row["换手率"]),
                "volume_ratio": float(row["量比"]),
                "inflow": float(row["主力净流入"]),
            })
        return stocks

    except Exception as e:
        print("❌ AkShare失败:", e, flush=True)
        return []

# ================= 数据源2：Tushare =================
def get_stocks_tushare():
    if not pro:
        return []

    try:
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')

        stocks = []
        for _, row in df.iterrows():
            stocks.append({
                "code": row["ts_code"],
                "name": row["name"],
                "change": 0,
                "turnover": 0,
                "volume_ratio": 1,
                "inflow": 0,
            })
        return stocks

    except Exception as e:
        print("❌ Tushare失败:", e, flush=True)
        return []

# ================= 北向资金 =================
def get_north_money():
    try:
        df = ak.stock_hsgt_north_net_flow_em()
        value = df.iloc[-1]["当日净流入"]
        return float(value)
    except:
        return 0

# ================= 多数据源容灾 =================
def get_all_stocks():
    stocks = get_stocks_akshare()

    if stocks:
        print("✅ 使用 AkShare 数据", flush=True)
        return stocks

    print("⚠️ AkShare失败，切换Tushare", flush=True)

    stocks = get_stocks_tushare()

    if stocks:
        return stocks

    print("❌ 所有数据源失败", flush=True)
    return []

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

            # ===== 提前识别（未涨停）=====
            if not (0 < change < 7):
                continue

            if turnover < 3:
                continue

            if volume_ratio < 1.5:
                continue

            # ===== 冷却 =====
            if code in last_push and now - last_push[code] < 300:
                continue

            # ===== 评分模型 =====
            score = 0

            # 主力资金
            if inflow > 30_000_000:
                score += 3

            # 量能
            if volume_ratio > 2:
                score += 3

            # 换手
            if turnover > 5:
                score += 2

            # 北向
            if north > 0:
                score += 2

            # 情绪
            if emotion == "强":
                score += 2
            elif emotion == "弱":
                score -= 2

            # ===== 信号 =====
            if score >= 8:
                tag = "🚀强势"
            elif score >= 6:
                tag = "🔥启动"
            else:
                continue

            last_push[code] = now

            results.append(
                (score, f"{tag} {name}({code}) 涨:{change:.2f}% 换手:{turnover}% 量比:{volume_ratio}")
            )

        except Exception:
            continue

    results.sort(reverse=True)
    return [x[1] for x in results[:10]]

# ================= 主循环 =================
def run():
    while True:
        try:
            stocks = get_all_stocks()

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
    print("🚀 多数据源监控系统启动", flush=True)

    send("✅ 系统启动成功（AkShare + Tushare）")

    threading.Thread(target=heartbeat).start()

    run()
