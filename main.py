import os
import time
import random
import pandas as pd
import akshare as ak
import tushare as ts
import requests

# =========================
# 🔧 Telegram 配置
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
        print("❌ Telegram发送失败:", e)

# =========================
# 💹 Tushare 初始化
# =========================
TS_TOKEN = os.getenv("TS_TOKEN")  # 你的tushare token
ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# =========================
# 📦 缓存机制
# =========================
cache_stocks = pd.DataFrame()
cache_changes = pd.DataFrame()

# =========================
# 🛡️ 安全获取数据
# =========================
def safe_akshare(func, *args, retry=3, **kwargs):
    global cache_stocks, cache_changes
    for i in range(retry):
        try:
            df = func(*args, **kwargs)
            if df is None or df.empty:
                raise ValueError("空数据")
            return df
        except Exception as e:
            print(f"❌ 数据获取失败 第{i+1}次:", e)
            time.sleep(random.randint(3,7))
    # 返回缓存
    print("⚠️ 使用缓存数据")
    if func == ak.stock_em_yingbi:
        return cache_changes
    else:
        return cache_stocks

# =========================
# 📊 获取沪深A股行情
# =========================
def get_all_stocks():
    global cache_stocks
    try:
        sh = safe_akshare(ak.stock_zh_a_spot_em, symbol="sh")
        sz = safe_akshare(ak.stock_zh_a_spot_em, symbol="sz")
        all_stocks = pd.concat([sh, sz], ignore_index=True)
        cache_stocks = all_stocks
        return all_stocks
    except:
        return cache_stocks

# =========================
# 📈 获取异动数据
# =========================
def get_stock_changes():
    global cache_changes
    df = safe_akshare(ak.stock_em_yingbi)
    cache_changes = df
    return df

# =========================
# 💰 北向资金示例
# =========================
def get_north_money():
    try:
        df = pro.moneyflow_hsgt_daily(ts_code='', start_date='20260101', end_date='20260325')
        total = df['net_mf_amt'].sum()
        return total
    except:
        return 0

# =========================
# 🎯 潜力股筛选
# =========================
def scan_potential_stocks(all_stocks, changes_df, north_money):
    results = []
    changes_codes = set(changes_df['code']) if not changes_df.empty else set()

    for _, row in all_stocks.iterrows():
        try:
            code = row['代码'] if '代码' in row else row['symbol']
            name = row['名称'] if '名称' in row else row['name']
            change = float(row.get('涨跌幅', row.get('changepercent',0)))
            turnover = float(row.get('换手率', row.get('turnoverratio',0)))
            volume_ratio = float(row.get('量比', row.get('amount',0)))
            main_inflow = float(row.get('主力净流入', 0))

            # ❗ 排除已经涨停或大幅上涨
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
            if code in changes_codes:
                score += 2

            if score >= 5:
                results.append({
                    "code": code,
                    "name": name,
                    "change": change,
                    "turnover": turnover,
                    "volume_ratio": volume_ratio,
                    "main_inflow": main_inflow,
                    "score": score
                })
        except:
            continue

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:10]

# =========================
# 🚀 主循环
# =========================
def main():
    print("🚀 潜力股系统启动")
    send_telegram("✅ 潜力股系统已启动（AkShare+Tushare稳定版）")

    while True:
        print("\n🔄 扫描中...")
        all_stocks = get_all_stocks()
        changes_df = get_stock_changes()
        north_money = get_north_money()

        selected = scan_potential_stocks(all_stocks, changes_df, north_money)
        print(f"🎯 潜力股数量: {len(selected)}")

        for s in selected:
            msg = f"""
🔥 潜力股
{name}({code})
涨幅: {s['change']}%
换手率: {s['turnover']}%
量比: {s['volume_ratio']}
主力净流入: {s['main_inflow']}
评分: {s['score']}
"""
            print(msg)
            send_telegram(msg)

        sleep_time = random.randint(40,80)
        print(f"⏱ 等待 {sleep_time}s")
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
