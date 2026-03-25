import requests
import time
import os
import yfinance as yf

# ================= 配置 =================
TG_TOKEN = os.getenv("TG_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")

A_STOCK_API = "https://push2.eastmoney.com/api/qt/clist/get"

# ================= Telegram =================
def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TG_CHAT_ID, "text": msg}, timeout=10)
    except:
        pass

# ================= A股全市场 =================
def get_all_stocks():
    all_stocks = []
    page = 1

    while True:
        params = {
            "pn": page,
            "pz": 200,
            "fs": "m:0+t:6,m:1+t:2",
            "fields": "f12,f14,f3,f8,f62,f6,f5"
        }

        try:
            res = requests.get(A_STOCK_API, params=params, timeout=10).json()
            data = res.get("data", {}).get("diff", [])

            if not data:
                break

            all_stocks.extend(data)
            page += 1

        except:
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
def filter_stocks(stocks, futures_signal):
    result = []

    for s in stocks:
        try:
            name = s["f14"]
            code = s["f12"]

            turnover = float(s["f8"])
            inflow = float(s["f62"])
            amount = float(s["f6"])
            change = float(s["f3"])

            # 🚀 提前启动信号（避免涨停追高）
            if (
                turnover > 3 and
                inflow > 10_000_000 and
                amount > 300_000_000 and
                0 < change < 5
            ):
                tag = "【启动】"

                # 期货联动
                for f in futures_signal:
                    if futures_signal[f]:
                        tag += f"【{f}↑】"

                result.append(
                    f"{tag}{name}({code}) 涨幅:{change}% 换手:{turnover}% 成交:{round(amount/1e8,2)}亿"
                )

        except:
            continue

    return result

# ================= 主程序 =================
def main():
    futures_prev = {}

    while True:
        try:
            stocks = get_all_stocks()
            futures_now = get_futures()

            futures_signal = futures_trend(futures_now, futures_prev)

            picks = filter_stocks(stocks, futures_signal)

            if picks:
                send_telegram("\n".join(picks[:20]))  # 防止太长

            futures_prev = futures_now

            print("扫描完成")
            time.sleep(60)

        except Exception as e:
            print("错误:", e)
            time.sleep(60)

if __name__ == "__main__":
    main()
