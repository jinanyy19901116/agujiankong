import requests
import time
import os
import yfinance as yf

# ================= 配置 =================
# ================= 配置 =================
TELEGRAM_BOT_TOKEN = "8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TELEGRAM_CHAT_ID = 6308781694

A_STOCK_API = "https://push2.eastmoney.com/api/qt/clist/get"
BINANCE_API = "https://api.binance.com/api/v3/ticker/price"

# ================= 工具 =================

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": msg}, timeout=10)
    except:
        pass


def get_a_stock_data():
    params = {
        "pn": 1,
        "pz": 300,
        "fs": "m:0+t:6,m:1+t:2",
        "fields": "f12,f14,f3,f8,f62,f6,f5"  # 成交量也加入
    }
    try:
        res = requests.get(A_STOCK_API, params=params, timeout=10).json()
        return res.get("data", {}).get("diff", [])
    except:
        return []


def get_btc():
    try:
        res = requests.get(BINANCE_API, timeout=10).json()
        btc = [x for x in res if x['symbol'] == 'BTCUSDT'][0]
        return float(btc['price'])
    except:
        return None


def get_nq():
    try:
        ticker = yf.Ticker("NQ=F")
        data = ticker.history(period="1d", interval="1m")
        return data['Close'].iloc[-1]
    except:
        return None


# ================= 核心策略（提前发现上涨） =================

def filter_stocks(stocks, btc_now, btc_prev, nq_now, nq_prev):
    result = []

    btc_up = btc_now and btc_prev and btc_now > btc_prev
    nq_up = nq_now and nq_prev and nq_now > nq_prev

    for s in stocks:
        try:
            name = s.get("f14")
            code = s.get("f12")

            turnover = float(s.get("f8", 0))
            main_inflow = float(s.get("f62", 0))
            amount = float(s.get("f6", 0))
            change = float(s.get("f3", 0))
            volume = float(s.get("f5", 0))

            # ===== 提前启动信号（核心升级） =====
            early_signal = (
                turnover > 3 and  # 不用等5%，提前发现
                main_inflow > 10_000_000 and  # 提高资金门槛
                amount > 300_000_000 and
                0 < change < 5  # 严格限制涨幅（防止追高）
            )

            # ===== 强势确认信号 =====
            strong_signal = (
                turnover > 5 and
                main_inflow > 50_000_000 and
                amount > 800_000_000 and
                0 < change < 7
            )

            if not (early_signal or strong_signal):
                continue

            tag = ""

            if early_signal:
                tag += "【启动】"
            if strong_signal:
                tag += "【强化】"

            if nq_up:
                tag += "【纳指↑】"
            if btc_up:
                tag += "【情绪↑】"

            result.append(
                f"{tag}{name}({code}) 涨幅:{change}% 换手:{turnover}% 成交:{round(amount/1e8,2)}亿 主力:{round(main_inflow/1e8,2)}亿"
            )

        except:
            continue

    return result


# ================= 主程序 =================

def main():
    btc_prev = None
    nq_prev = None

    while True:
        try:
            stocks = get_a_stock_data()
            btc_now = get_btc()
            nq_now = get_nq()

            picks = filter_stocks(stocks, btc_now, btc_prev, nq_now, nq_prev)

            if picks:
                send_telegram("\n".join(picks))

            btc_prev = btc_now
            nq_prev = nq_now

            print("扫描完成")
            time.sleep(60)

        except Exception as e:
            print("错误:", e)
            time.sleep(60)


def telegram_test():
    msg = "✅ Telegram连接测试成功！系统已启动"
    send_telegram(msg)
    print("已发送Telegram测试消息", flush=True)


if __name__ == "__main__":
    print("程序启动成功", flush=True)
    telegram_test()  # 启动时先测试Telegram
    main()
