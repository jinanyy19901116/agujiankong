import requests
import time
import os
import random
import pandas as pd

# =========================
# 🔧 Telegram 配置
# =========================
TG_TOKEN = "8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TG_CHAT_ID = "6308781694"

def send_telegram(msg):
    if not TG_TOKEN or not TG_CHAT_ID:
        print("⚠️ Telegram未配置", flush=True)
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
# 🏦 上交所数据（SSE）
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
# 🏦 深交所数据（SZSE）
# =========================
def get_szse_summary():
    url = "http://www.szse.cn/api/market/overview/index?random=" + str(random.random())
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "http://www.szse.cn/market/overview/index.html"
    }
    data = safe_request(url, headers)
    if data:
        print("✅ SZSE获取成功", flush=True)
    else:
        print("❌ SZSE获取失败", flush=True)
    return data

# =========================
# 📊 A股行情（东方财富API）
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
# 💰 北向资金（模拟，可用真实API替换）
# =========================
def get_north_money():
    # 可以替换成真实北向资金接口
    return 500000000  # 示例：5亿流入

# =========================
# 🔍 潜力股筛选
# =========================
def scan_potential_stocks(stocks, north_money=0):
    results = []
    for s in stocks:
        try:
            code = s.get("f12")
            name = s.get("f14")
            change = float(s.get("f3", 0))
            turnover = float(s.get("f8", 0))
            volume_ratio = float(s.get("f10", 0))
            main_inflow = float(s.get("f62", 0))

            # 排除已涨幅较高的
            if change > 3:
                continue
            if turnover < 3:
                continue
            if main_inflow <= 0:
                continue

            # 打分机制
            score = 0
            if volume_ratio > 1.5:
                score += 2
            if turnover > 5:
                score += 2
            if main_inflow > 5000000:
                score += 3
            if north_money > 100000000:
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
# 📈 市场情绪
# =========================
def analyze_market(sse_data, szse_data):
    msg = "📊 市场状态：正常"
    if sse_data:
        msg += " | SSE正常"
    if szse_data:
        msg += " | SZSE正常"
    return msg

# =========================
# 🚀 主程序
# =========================
def main():
    print("🚀 潜力股监控系统启动", flush=True)
    send_telegram("✅ 潜力股监控系统已启动（SSE+SZSE版）")

    while True:
        print("🔄 开始扫描...", flush=True)

        stocks = get_all_stocks()
        if not stocks:
            print("❌ 股票数据为空", flush=True)
            time.sleep(60)
            continue

        sse_data = get_sse_summary()
        szse_data = get_szse_summary()
        sentiment = analyze_market(sse_data, szse_data)
        print("📈 市场:", sentiment, flush=True)

        north_money = get_north_money()
        selected = scan_potential_stocks(stocks, north_money=north_money)
        print(f"🎯 潜力股筛选结果: {len(selected)}", flush=True)

        for s in selected:
            msg = f"""
🔥 潜力股
名称: {s['name']}
代码: {s['code']}
涨幅: {s['change']}%
换手: {s['turnover']}%
量比: {s['volume_ratio']}
主力流入: {s['main_inflow']}
评分: {s['score']}
"""
            send_telegram(msg)

        sleep_time = random.randint(40, 80)
        print(f"⏱ 等待 {sleep_time}s", flush=True)
        time.sleep(sleep_time)

if __name__ == "__main__":
    main()
