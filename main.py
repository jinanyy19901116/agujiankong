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

TOKEN_WHITELIST = [
    x.strip().lower()
    for x in os.getenv("TOKEN_WHITELIST", "").split(",")
    if x.strip()
]

CHAIN_WHITELIST = [
    x.strip().lower()
    for x in os.getenv("CHAIN_WHITELIST", "").split(",")
    if x.strip()
]

USD_THRESHOLD = float(os.getenv("USD_THRESHOLD", "100000"))
MIN_ALERT_SCORE = int(os.getenv("MIN_ALERT_SCORE", "60"))
SIGNAL_WINDOW_SECONDS = int(os.getenv("SIGNAL_WINDOW_SECONDS", "300"))

UPBIT_KEYWORDS = [
    x.strip().lower()
    for x in os.getenv("UPBIT_KEYWORDS", "upbit,up-bit").split(",")
    if x.strip()
]

EXCHANGE_KEYWORDS = [
    x.strip().lower()
    for x in os.getenv(
        "EXCHANGE_KEYWORDS",
        "upbit,binance,coinbase,kraken,okx,bybit,bitget,gate,kucoin,htx,mexc,bithumb,bitfinex,crypto.com",
    ).split(",")
    if x.strip()
]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

BEIJING_TZ = timezone(timedelta(hours=8))

RECONNECT_MIN = 3
RECONNECT_MAX = 60

MAX_SEEN_SIZE = 5000

recent_signals: List[Dict[str, Any]] = []


def setup_logger() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


def arkham_headers() -> Dict[str, str]:
    return {
        "API-Key": API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "upbit-arkham-monitor/2.1",
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


def contains_any_keyword(text: str, keywords: List[str]) -> bool:
    t = text.lower()
    return any(k in t for k in keywords)


def contains_upbit(text: str) -> bool:
    return contains_any_keyword(text, UPBIT_KEYWORDS)


def contains_exchange(text: str) -> bool:
    return contains_any_keyword(text, EXCHANGE_KEYWORDS)


def classify_direction(item: Dict[str, Any]) -> str:
    from_text = get_from_text(item)
    to_text = get_to_text(item)

    from_is_upbit = contains_upbit(from_text)
    to_is_upbit = contains_upbit(to_text)

    if from_is_upbit and not to_is_upbit:
        return "流出Upbit"
    if not from_is_upbit and to_is_upbit:
        return "流入Upbit"
    if from_is_upbit and to_is_upbit:
        return "Upbit内部/关联地址调拨"
    return "非Upbit"


def is_exchange_to_exchange(item: Dict[str, Any]) -> bool:
    from_text = get_from_text(item)
    to_text = get_to_text(item)
    return contains_exchange(from_text) and contains_exchange(to_text)


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
    if not TOKEN_WHITELIST:
        return True

    symbol = get_symbol(item).lower()
    token_id = get_token_id(item).lower()
    token_address = get_token_address(item).lower()

    wanted = set(TOKEN_WHITELIST)
    return symbol in wanted or token_id in wanted or token_address in wanted


def chain_matches(item: Dict[str, Any]) -> bool:
    if not CHAIN_WHITELIST:
        return True
    return get_chain(item).lower() in set(CHAIN_WHITELIST)


def is_upbit_related(item: Dict[str, Any]) -> bool:
    from_text = get_from_text(item)
    to_text = get_to_text(item)
    return contains_upbit(from_text) or contains_upbit(to_text)


def should_alert(item: Dict[str, Any]) -> bool:
    if not is_upbit_related(item):
        return False
    if not token_matches(item):
        return False
    if not chain_matches(item):
        return False
    if get_usd(item) < USD_THRESHOLD:
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
    if TOKEN_WHITELIST:
        payload["tokens"] = TOKEN_WHITELIST
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
    if TOKEN_WHITELIST:
        params["tokens"] = ",".join(TOKEN_WHITELIST)
    if CHAIN_WHITELIST:
        params["chains"] = ",".join(CHAIN_WHITELIST)
    if USD_THRESHOLD > 0:
        params["usdGte"] = str(int(USD_THRESHOLD))
    return f"{ARKHAM_WS_BASE}?{urlencode(params)}"


def telegram_send(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
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


def cleanup_recent_signals() -> None:
    now = time.time()
    recent_signals[:] = [x for x in recent_signals if now - x["ts"] <= SIGNAL_WINDOW_SECONDS]


def add_recent_signal(symbol: str, direction: str, usd_value: float, score: int) -> None:
    cleanup_recent_signals()
    recent_signals.append(
        {
            "ts": time.time(),
            "symbol": symbol.lower(),
            "direction": direction,
            "usd": usd_value,
            "score": score,
        }
    )


def count_same_direction_signals(symbol: str, direction: str) -> int:
    cleanup_recent_signals()
    symbol = symbol.lower()
    return sum(
        1 for x in recent_signals
        if x["symbol"] == symbol and x["direction"] == direction
    )


def score_signal(item: Dict[str, Any]) -> Dict[str, Any]:
    direction = classify_direction(item)
    usd_value = get_usd(item)
    symbol = get_symbol(item) or get_token_id(item) or "-"
    reasons: List[str] = []
    score = 0

    if direction == "流入Upbit":
        score += 35
        reasons.append("大额转入Upbit，存在卖压风险")
    elif direction == "流出Upbit":
        score += 35
        reasons.append("大额转出Upbit，偏向提币囤币")
    elif direction == "Upbit内部/关联地址调拨":
        score += 5
        reasons.append("检测到Upbit内部或关联地址调拨")
    else:
        reasons.append("非Upbit核心信号")

    if usd_value >= USD_THRESHOLD:
        score += 20
        reasons.append(f"金额达到阈值 ${USD_THRESHOLD:,.0f}")
    if usd_value >= max(USD_THRESHOLD * 2, USD_THRESHOLD + 1):
        score += 15
        reasons.append("金额显著高于基础阈值")
    if usd_value >= max(USD_THRESHOLD * 5, USD_THRESHOLD + 1):
        score += 10
        reasons.append("金额极大")

    if TOKEN_WHITELIST:
        score += 10
        reasons.append("命中自定义币种白名单")
    else:
        reasons.append("当前未设置币种白名单")

    if CHAIN_WHITELIST:
        score += 5
        reasons.append("命中链白名单")

    if is_exchange_to_exchange(item):
        score -= 25
        reasons.append("疑似交易所之间搬砖/调拨，信号降权")

    if direction == "Upbit内部/关联地址调拨":
        score -= 40
        reasons.append("内部调拨显著降权")

    same_count = count_same_direction_signals(symbol, direction)
    if same_count >= 1:
        bonus = min(20, same_count * 8)
        score += bonus
        reasons.append(f"短时间同币种同方向连续异动 {same_count + 1} 笔")

    score = max(0, min(100, score))

    if direction == "流入Upbit":
        bias = "偏空 / 可考虑做空"
    elif direction == "流出Upbit":
        bias = "偏多 / 可考虑做多"
    elif direction == "Upbit内部/关联地址调拨":
        bias = "中性 / 观望"
    else:
        bias = "观望"

    if score >= 85:
        strength = "🔥🔥🔥 强信号"
    elif score >= 70:
        strength = "🔥🔥 中强信号"
    elif score >= 60:
        strength = "🔥 中等信号"
    else:
        strength = "⚪ 弱信号"

    return {
        "score": score,
        "direction": direction,
        "bias": bias,
        "strength": strength,
        "reasons": reasons,
    }


def build_message(item: Dict[str, Any], signal: Dict[str, Any]) -> str:
    symbol = get_symbol(item) or get_token_id(item) or "-"
    chain = get_chain(item) or "-"
    amount = get_amount(item) or "-"
    usd_value = get_usd(item)
    ts_bj = format_beijing_time(get_timestamp(item))
    from_text = get_from_text(item) or "-"
    to_text = get_to_text(item) or "-"
    tx = get_tx_hash(item) or "-"

    direction = signal["direction"]
    if direction == "流入Upbit":
        emoji = "🔴"
    elif direction == "流出Upbit":
        emoji = "🟢"
    elif direction == "Upbit内部/关联地址调拨":
        emoji = "🟡"
    else:
        emoji = "⚪"

    reasons_text = "\n".join([f"- {x}" for x in signal["reasons"]])

    return (
        f"{emoji} Upbit 小币种大额转账告警 V2\n"
        f"时间(北京): {ts_bj}\n"
        f"方向: {direction}\n"
        f"建议: {signal['bias']}\n"
        f"强度: {signal['strength']}\n"
        f"评分: {signal['score']}/100\n"
        f"币种: {symbol}\n"
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
                    "User-Agent": "upbit-arkham-monitor/2.1",
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

                        signal = score_signal(item)
                        if signal["score"] < MIN_ALERT_SCORE:
                            continue

                        symbol = get_symbol(item) or get_token_id(item) or "-"
                        add_recent_signal(symbol, signal["direction"], get_usd(item), signal["score"])

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

    logging.info("Upbit Arkham 监控 V2 启动")
    logging.info("TOKEN_WHITELIST=%s", TOKEN_WHITELIST if TOKEN_WHITELIST else "全部")
    logging.info("CHAIN_WHITELIST=%s", CHAIN_WHITELIST if CHAIN_WHITELIST else "全部")
    logging.info("USD_THRESHOLD=%s", USD_THRESHOLD)
    logging.info("MIN_ALERT_SCORE=%s", MIN_ALERT_SCORE)
    logging.info("UPBIT_KEYWORDS=%s", UPBIT_KEYWORDS)
    logging.info("EXCHANGE_KEYWORDS=%s", EXCHANGE_KEYWORDS)

    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
