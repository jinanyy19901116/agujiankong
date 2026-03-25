import requests
import time
import os
import yfinance as yf
import datetime

# ================= 配置 =================
TG_TOKEN = "8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TG_CHAT_ID = 6308781694

A_STOCK_API = "https://push2.eastmoney.com/api/qt/clist/get"

# 热门板块（可根据热点更新）
HOT_SECTORS = ["科技", "半导体", "AI", "软件", "消费", "新能源", "石油", "有色"]

# ================= Telegram =================
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        print("Telegram发送失败:", e, flush=True)

# ================= A股全市场 =================
def get_all_stocks():
    all_stocks = []
    page = 1

    while True:
        params = {
            "pn": page,
            "pz": 200,
            "fs": "m:0+t:6,m:1+t:2",
            "fields": "f12,f14,f3,f8,f62,f6,f5,f10,f21"  # f10:量比 f21:板块
        }

        try:
            res = requests.get(A_STOCK_API, params=params, timeout=10).json()
            data = res.get("data", {}).get("diff", [])

            if not data:
                break

            all_stocks.extend(data)
            page += 1

        except Exception as e:
            print("获取股票失败:", e, flush=True)
            break

    return all_stocks

# ================= 全球期货 =================
def get_futures():
    symbols = {
        "nasdaq": "NQ=F",
        "sp500": "ES=F",
        "dow": "YM=F",
        "oil": "CL=F",
        "gold": "GC=F",
        "silver": "SI=F",
        "copper": "HG=F",
        "gas": "NG=F"
    }

    data = {}

    for k, v in symbols.items():
        try:
            price = yf.Ticker(v).history(period="1d", interval="1m")['Close'].iloc[-1]
            data[k] = price
        except:
            continue

    return data

# ================= 期货趋势 =================
def futures_trend(now, prev):
    trend = {}
    for k in now:
        if k in prev:
            trend[k] = now[k] > prev[k]
    return trend

# ================= 核心策略 =================
def filter_stocks(stocks, futures_signal, last_inflow):
    result = []

    for s in stocks:
        try:
            name = s["f14"]
            code = s["f12"]
            turnover = float(s["f8"])
            inflow = float(s["f62"])
            amount = float(s["f6"])
            change = float(s["f3"])
            volume_ratio = float(s.get("f10", 0))
            sector = s.get("f21", "")

            # ---------------- 条件1：板块过滤 ----------------
            if sector not in HOT_SECTORS:
                continue

            # ---------------- 条件2：低位启动 ----------------
            if not (0 < change < 5):
                continue

            # ---------------- 条件3：分时资金加速 ----------------
            delta_inflow = inflow - last_inflow.get(code, 0)
            last_inflow[code] = inflow

            if delta_inflow < 5_000_000:
                continue

            # ---------------- 条件4：换手率 + 量比 ----------------
            if turnover < 3 or volume_ratio < 1.5:
                continue

            # ---------------- 标签 ----------------
            tag = "【启动】"
            if delta_inflow > 20_000_000 and volume_ratio > 2:
                tag = "🔥强信号"

            # 期货联动
            for f in futures_signal:
                if futures_signal[f]:
                    tag += f"【{f}↑】"

            result.append(
                f"{tag}{name}({code}) 涨幅:{change:.2f}% 换手:{turnover:.2f}% 成交:{round(amount/1e8,2)}亿 板块:{sector}"
            )

        except Exception as e:
            continue

    # ---------------- 龙头筛选 ----------------
    # 资金前20% or 涨幅前20%
    result = sorted(result, key=lambda x: float(x.split("涨幅:")[1].split("%")[0]), reverse=True)
    result = result[:20]

    return result

# ================= 主程序 =================
def main():
    futures_prev = {}
    last_inflow = {}

    while True:
        try:
            # ---------------- 时间过滤 ----------------
            hour = datetime.datetime.now().hour
            if hour >= 14:
                print("14点后不执行监控", flush=True)
                time.sleep(300)
                continue

            print("开始扫描...", flush=True)
            stocks = get_all_stocks()
            print(f"股票数量: {len(stocks)}", flush=True)

            futures_now = get_futures()
            print(f"期货数据: {futures_now}", flush=True)

            futures_signal = futures_trend(futures_now, futures_prev)

            picks = filter_stocks(stocks, futures_signal, last_inflow)

            if picks:
                print("发现信号:", flush=True)
                send_telegram("\n".join(picks))

            futures_prev = futures_now
            print("扫描完成", flush=True)
            time.sleep(60)

        except Exception as e:
            print("错误:", e, flush=True)
            time.sleep(60)

# ================= Telegram测试 =================
def telegram_test():
    msg = "✅ Telegram连接测试成功！系统已启动"
    send_telegram(msg)
    print("已发送Telegram测试消息", flush=True)

# ================= 启动 =================
if __name__ == "__main__":
    print("程序启动成功", flush=True)
    telegram_test()
    main()
