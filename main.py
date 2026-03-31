import os
import json
import time
import asyncio
import logging
import hashlib
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from datetime import datetime, timezone, timedelta

import requests
import websockets

ARKHAM_HTTP_BASE = "https://api.arkm.com"
ARKHAM_WS_BASE = "wss://api.arkm.com/ws/transfers"

API_KEY = os.getenv("ARKHAM_API_KEY", "").strip()

# 固定币种（已去 KRW-）
TOKEN_WHITELIST = [
    "sent", "sol", "cfg", "doge", "dood", "ankr", "bard", "tao", "elsa", "ada",
    "ip", "kite", "ong", "vana", "kat", "wld", "sui", "la", "steem", "virtual",
    "gas", "moodeng", "xlm", "sahara", "chz", "trump", "anime", "shib",
    "sei", "sign", "sonic", "trx", "skr", "bch", "pengu", "kernel", "order",
    "enso", "ath", "knc", "zbt", "link", "akt", "cpool"
]

CHAIN_WHITELIST = [
    x.strip().lower()
    for x in os.getenv("CHAIN_WHITELIST", "").split(",")
    if x.strip()
]

USD_THRESHOLD = float(os.getenv("USD_THRESHOLD", "150000"))

EXCHANGE_KEYWORDS = [
    "upbit", "up-bit",
    "mexc",
    "coinex",
    "kucoin",
]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

BEIJING_TZ = timezone(timedelta(hours=8))

RECONNECT_MIN = 3
RECONNECT_MAX = 60
MAX_SEEN_SIZE = 5000


def setup_logger() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def normalize_token_symbol(token: str) -> str:
    token = (token or "").strip().lower()
    if token.startswith("krw-"):
        token = token[4:]
    return token


def arkham_headers() -> Dict[str, str]:
    return {
        "API-Key": API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "exchange-flow-monitor/5.0",
    }


def safe_get(d: Any, *keys: str, default=None):
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def as_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v)


def as_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def get_symbol(item: Dict[str, Any]) -> str:
    return as_str(
        safe_get(item, "symbol", "tokenSymbol", "token", "tokenId", default="")
    ).strip()


def get_token_id(item: Dict[str, Any]) -> str:
    return as_str(safe_get(item, "tokenId", default="")).strip()


def get_token_address(item: Dict[str, Any]) -> str:
    return as_str(safe_get(item, "tokenAddress", default="")).strip()


def get_chain(item: Dict[str, Any]) -> str:
    return as_str(
        safe_get(item, "chain", "chainType", "network", default="")
    ).strip()


def get_amount(item: Dict[str, Any]) -> str:
    return as_str(
        safe_get(item, "amount", "value", "tokenAmount", "quantity", default="")
    ).strip()


def get_usd(item: Dict[str, Any]) -> float:
    return as_float(
        safe_get(
            item,
            "usd",
            "valueUsd",
            "historicalUSD",
            "amountUsd",
            "usdValue",
            default=0,
        ),
        0.0,
    )


def get_tx_hash(item: Dict[str, Any]) -> str:
    return as_str(
        safe_get(item, "txHash", "hash", "transactionHash", default="")
    ).strip()


def get_timestamp(item: Dict[str, Any]) -> Any:
    return safe_get(item, "time", "timestamp", "blockTimestamp", default=None)


def get_from_text(item: Dict[str, Any]) -> str:
    parts = [
        safe_get(item, "fromLabel", default=None),
        safe_get(item, "fromEntity", default=None),
        safe_get(item, "fromName", default=None),
        safe_get(item, "fromAddress", default=None),
        safe_get(item, "from", default=None),
    ]
    return " | ".join([as_str(x).strip() for x in parts if x])


def get_to_text(item: Dict[str, Any]) -> str:
    parts = [
        safe_get(item, "toLabel", default=None),
        safe_get(item, "toEntity", default=None),
        safe_get(item, "toName", default=None),
        safe_get(item, "toAddress", default=None),
        safe_get(item, "to", default=None),
    ]
    return " | ".join([as_str(x).strip() for x in parts if x])


def contains_exchange(text: str) -> bool:
    text = (text or "").lower()
    return any(k in text for k in EXCHANGE_KEYWORDS)


def classify_direction(item: Dict[str, Any]) -> str:
    from_text = get_from_text(item)
    to_text = get_to_text(item)

    from_is_exchange = contains_exchange(from_text)
    to_is_exchange = contains_exchange(to_text)

    if from_is_exchange and not to_is_exchange:
        return "交易所流出"

    if not from_is_exchange and to_is_exchange:
        return "交易所流入"

    if from_is_exchange and to_is_exchange:
        return "交易所互转"

    return "无关"


def format_beijing_time(raw_ts: Any) -> str:
    if raw_ts is None:
        return "-"
    try:
        if isinstance(raw_ts, (int, float)):
            dt = datetime.fromtimestamp(float(raw_ts), tz=timezone.utc).astimezone(BEIJING_TZ)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        s = str(raw_ts).strip()
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(BEIJING_TZ)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(BEIJING_TZ)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(raw_ts)


def make_event_id(item: Dict[str, Any]) -> str:
    raw = json.dumps(
        {
            "tx": get_tx_hash(item),
            "symbol": get_symbol(item),
            "amount": get_amount(item),
            "usd": get_usd(item),
            "time": get_timestamp(item),
            "from": get_from_text(item),
            "to": get_to_text(item),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def token_matches(item: Dict[str, Any]) -> bool:
    symbol = normalize_token_symbol(get_symbol(item))
    token_id = normalize_token_symbol(get_token_id(item))
    token_address = (get_token_address(item) or "").lower()

    wanted = set(TOKEN_WHITELIST)
    return symbol in wanted or token_id in wanted or token_address in wanted


def chain_matches(item: Dict[str, Any]) -> bool:
    if not CHAIN_WHITELIST:
        return True
    return get_chain(item).lower() in set(CHAIN_WHITELIST)


def should_alert(item: Dict[str, Any]) -> bool:
    if not token_matches(item):
        return False

    if not chain_matches(item):
        return False

    if get_usd(item) < USD_THRESHOLD:
        return False

    direction = classify_direction(item)
    if direction not in ("交易所流出", "交易所流入"):
        return False

    return True


def extract_items(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if isinstance(payload, dict):
        for k in ("items", "results", "transfers", "data", "payload"):
            v = payload.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
        if any(k in payload for k in ["txHash", "hash", "fromAddress", "toAddress", "symbol", "tokenSymbol"]):
            return [payload]

    return []


def build_session_payload() -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if CHAIN_WHITELIST:
        payload["chains"] = CHAIN_WHITELIST
    if USD_THRESHOLD > 0:
        payload["usdGte"] = USD_THRESHOLD
    return payload


def create_ws_session() -> str:
    url = f"{ARKHAM_HTTP_BASE}/ws/sessions"

    try:
        resp = requests.post(url, headers=arkham_headers(), json=build_session_payload(), timeout=30)
        if resp.ok:
            data = resp.json()
            session_id = (
                safe_get(data, "sessionId", "session_id", "id", default=None)
                or safe_get(data.get("data", {}), "sessionId", "session_id", "id", default=None)
            )
            if session_id:
                logging.info("创建 session 成功: %s", session_id)
                return str(session_id)
    except Exception as e:
        logging.warning("带过滤条件创建 session 失败: %s", e)

    resp = requests.post(url, headers=arkham_headers(), json={}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    session_id = (
        safe_get(data, "sessionId", "session_id", "id", default=None)
        or safe_get(data.get("data", {}), "sessionId", "session_id", "id", default=None)
    )
    if not session_id:
        raise RuntimeError(f"未拿到 session id: {data}")
    logging.info("创建 session 成功(回退): %s", session_id)
    return str(session_id)


def delete_ws_session(session_id: str) -> None:
    try:
        url = f"{ARKHAM_HTTP_BASE}/ws/sessions/{session_id}"
        requests.delete(url, headers=arkham_headers(), timeout=15)
        logging.info("删除 session: %s", session_id)
    except Exception as e:
        logging.warning("删除 session 失败: %s", e)


def build_ws_url(session_id: str) -> str:
    params = {"session_id": session_id}
    if CHAIN_WHITELIST:
        params["chains"] = ",".join(CHAIN_WHITELIST)
    if USD_THRESHOLD > 0:
        params["usdGte"] = str(int(USD_THRESHOLD))
    return f"{ARKHAM_WS_BASE}?{urlencode(params)}"


def telegram_send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info(
            "Telegram 未配置: token=%s chat=%s",
            bool(TELEGRAM_BOT_TOKEN),
            bool(TELEGRAM_CHAT_ID),
        )
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text[:4000],
            },
            timeout=15,
        )
    except Exception as e:
        logging.warning("Telegram 发送失败: %s", e)


def send_startup_test_message() -> None:
    token_preview = ", ".join(TOKEN_WHITELIST[:12])
    if len(TOKEN_WHITELIST) > 12:
        token_preview += " ..."

    text = (
        "✅ 监控机器人已启动\n"
        f"启动时间(北京): {datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"监控币种数量: {len(TOKEN_WHITELIST)}\n"
        f"监控币种预览: {token_preview}\n"
        f"金额阈值(USD): ${USD_THRESHOLD:,.0f}\n"
        f"链过滤: {', '.join(CHAIN_WHITELIST) if CHAIN_WHITELIST else '全部'}\n"
        f"交易所: {', '.join(EXCHANGE_KEYWORDS)}\n"
        "状态: 启动测试成功，开始监听 WebSocket 实时数据"
    )
    telegram_send(text)
    logging.info("已尝试发送启动测试消息到 Telegram")


def analyze_signal(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    direction = classify_direction(item)
    usd_value = get_usd(item)
    reasons: List[str] = []

    if direction == "交易所流出":
        reasons.append("大额从交易所流出，偏向提币囤币")
        bias = "偏多 / 可考虑买多"
    elif direction == "交易所流入":
        reasons.append("大额流入交易所，存在卖压风险")
        bias = "偏空 / 可考虑买空"
    else:
        return None

    if usd_value >= USD_THRESHOLD:
        reasons.append(f"金额达到阈值 ${USD_THRESHOLD:,.0f}")

    reasons.append("命中重点监控币种列表")

    if CHAIN_WHITELIST:
        reasons.append("命中链白名单")

    return {
        "direction": direction,
        "bias": bias,
        "reasons": reasons,
    }


def build_message(item: Dict[str, Any], signal: Dict[str, Any]) -> str:
    symbol = normalize_token_symbol(get_symbol(item) or get_token_id(item) or "-")
    chain = get_chain(item) or "-"
    amount = get_amount(item) or "-"
    usd_value = get_usd(item)
    ts_bj = format_beijing_time(get_timestamp(item))
    from_text = get_from_text(item) or "-"
    to_text = get_to_text(item) or "-"
    tx = get_tx_hash(item) or "-"

    direction = signal["direction"]
    if direction == "交易所流出":
        emoji = "🟢"
    elif direction == "交易所流入":
        emoji = "🔴"
    else:
        emoji = "⚪"

    reasons_text = "\n".join([f"- {x}" for x in signal["reasons"]])

    return (
        f"{emoji} 币种大额资金异动提示\n"
        f"时间(北京): {ts_bj}\n"
        f"方向: {direction}\n"
        f"建议: {signal['bias']}\n"
        f"币种: {symbol.upper()}\n"
        f"链: {chain}\n"
        f"金额(USD): ${usd_value:,.2f}\n"
        f"数量: {amount}\n"
        f"From: {from_text}\n"
        f"To: {to_text}\n"
        f"Tx: {tx}\n\n"
        f"判定原因:\n{reasons_text}"
    )


async def run_forever() -> None:
    if not API_KEY:
        raise ValueError("缺少 ARKHAM_API_KEY")

    seen: List[str] = []
    seen_set = set()
    backoff = RECONNECT_MIN

    while True:
        session_id: Optional[str] = None
        try:
            session_id = create_ws_session()
            ws_url = build_ws_url(session_id)

            logging.info("连接 WebSocket: %s", ws_url)

            async with websockets.connect(
                ws_url,
                extra_headers={
                    "API-Key": API_KEY,
                    "User-Agent": "exchange-flow-monitor/5.0",
                },
                ping_interval=20,
                ping_timeout=20,
                close_timeout=10,
                max_size=10 * 1024 * 1024,
            ) as ws:
                logging.info("WebSocket 已连接")
                backoff = RECONNECT_MIN

                async for raw in ws:
                    try:
                        payload = json.loads(raw)
                    except Exception:
                        logging.warning("收到非 JSON 数据")
                        continue

                    items = extract_items(payload)
                    if not items:
                        continue

                    for item in items:
                        if not should_alert(item):
                            continue

                        eid = make_event_id(item)
                        if eid in seen_set:
                            continue

                        signal = analyze_signal(item)
                        if not signal:
                            continue

                        seen.append(eid)
                        seen_set.add(eid)

                        if len(seen) > MAX_SEEN_SIZE:
                            old = seen.pop(0)
                            seen_set.discard(old)

                        msg = build_message(item, signal)
                        logging.info("\n%s", msg)
                        telegram_send(msg)

        except Exception as e:
            logging.exception("主循环异常: %s", e)
            if session_id:
                delete_ws_session(session_id)

            logging.info("准备重连，%s秒后继续", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX)


def main():
    setup_logger()

    logging.info("币种大额买多买空提示监控启动")
    logging.info("TOKEN_WHITELIST数量=%s", len(TOKEN_WHITELIST))
    logging.info("TOKEN_WHITELIST=%s", ",".join(TOKEN_WHITELIST))
    logging.info("CHAIN_WHITELIST=%s", CHAIN_WHITELIST if CHAIN_WHITELIST else "全部")
    logging.info("USD_THRESHOLD=%s", USD_THRESHOLD)
    logging.info("EXCHANGE_KEYWORDS=%s", EXCHANGE_KEYWORDS)

    # Telegram 环境变量调试
    logging.info("ENV TELEGRAM_BOT_TOKEN = %s", os.getenv("TELEGRAM_BOT_TOKEN"))
    logging.info("ENV TELEGRAM_CHAT_ID = %s", os.getenv("TELEGRAM_CHAT_ID"))

    send_startup_test_message()

    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
