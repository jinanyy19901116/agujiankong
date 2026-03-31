import os
import re
import json
import time
import asyncio
import logging
import hashlib
from dataclasses import dataclass
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta
from typing import Any, Deque, Dict, List, Optional, Tuple
from urllib.parse import urlencode

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

USD_THRESHOLD = float(os.getenv("USD_THRESHOLD", "100000") or "100000")
MIN_ALERT_SCORE = int(os.getenv("MIN_ALERT_SCORE", "60") or "60")
SIGNAL_WINDOW_SECONDS = int(os.getenv("SIGNAL_WINDOW_SECONDS", "300") or "300")
MAX_SEEN_SIZE = int(os.getenv("MAX_SEEN_SIZE", "8000") or "8000")
RECONNECT_MIN = int(os.getenv("RECONNECT_MIN", "3") or "3")
RECONNECT_MAX = int(os.getenv("RECONNECT_MAX", "60") or "60")

# Upbit labels sometimes appear in different forms depending on Arkham entity naming.
UPBIT_KEYWORDS = [
    x.strip().lower()
    for x in os.getenv("UPBIT_KEYWORDS", "upbit,up-bit").split(",")
    if x.strip()
]

# Common exchange names for exchange-to-exchange detection.
EXCHANGE_KEYWORDS = [
    x.strip().lower()
    for x in os.getenv(
        "EXCHANGE_KEYWORDS",
        "upbit,binance,coinbase,kraken,okx,bybit,bitget,gate,kucoin,htx,mexc,bithumb,bitfinex,crypto.com"
    ).split(",")
    if x.strip()
]

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

BEIJING_TZ = timezone(timedelta(hours=8))

# Runtime state
recent_signals: Dict[str, Deque[Tuple[float, str, float]]] = defaultdict(deque)
recent_wallet_activity: Dict[str, Deque[Tuple[float, str, float]]] = defaultdict(deque)
seen_order: Deque[str] = deque()
seen_set = set()


@dataclass
class SignalDecision:
    should_alert: bool
    action: str
    strength: str
    score: int
    reason: str
    direction: str
    event_type: str


@dataclass
class ParsedTransfer:
    symbol: str
    token_id: str
    token_address: str
    chain: str
    amount: str
    usd_value: float
    tx_hash: str
    timestamp: Any
    from_text: str
    to_text: str
    from_address: str
    to_address: str


class TelegramRateLimiter:
    def __init__(self, min_interval: float = 0.8):
        self.min_interval = min_interval
        self._last_sent = 0.0

    def wait(self) -> None:
        delta = time.time() - self._last_sent
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last_sent = time.time()


telegram_limiter = TelegramRateLimiter()


def setup_logger() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s"
    )


def arkham_headers() -> Dict[str, str]:
    return {
        "API-Key": API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "upbit-arkham-monitor-v2/1.0",
    }


def safe_get(d: Any, *keys: str, default=None):
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def as_str(v: Any) -> str:
    return "" if v is None else str(v)


def as_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def get_symbol(item: Dict[str, Any]) -> str:
    return normalize_space(as_str(safe_get(item, "symbol", "tokenSymbol", "token", "tokenId", default="")))


def get_token_id(item: Dict[str, Any]) -> str:
    return normalize_space(as_str(safe_get(item, "tokenId", default="")))


def get_token_address(item: Dict[str, Any]) -> str:
    return normalize_space(as_str(safe_get(item, "tokenAddress", default="")))


def get_chain(item: Dict[str, Any]) -> str:
    return normalize_space(as_str(safe_get(item, "chain", "chainType", "network", default="")))


def get_amount(item: Dict[str, Any]) -> str:
    return normalize_space(as_str(safe_get(item, "amount", "value", "tokenAmount", "quantity", default="")))


def get_usd(item: Dict[str, Any]) -> float:
    return as_float(safe_get(item, "usd", "valueUsd", "historicalUSD", "amountUsd", "usdValue", default=0), 0.0)


def get_tx_hash(item: Dict[str, Any]) -> str:
    return normalize_space(as_str(safe_get(item, "txHash", "hash", "transactionHash", default="")))


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
    return " | ".join([normalize_space(as_str(x)) for x in parts if x])


def get_to_text(item: Dict[str, Any]) -> str:
    parts = [
        safe_get(item, "toLabel", default=None),
        safe_get(item, "toEntity", default=None),
        safe_get(item, "toName", default=None),
        safe_get(item, "toAddress", default=None),
        safe_get(item, "to", default=None),
    ]
    return " | ".join([normalize_space(as_str(x)) for x in parts if x])


def get_from_address(item: Dict[str, Any]) -> str:
    return normalize_space(as_str(safe_get(item, "fromAddress", "from", default=""))).lower()


def get_to_address(item: Dict[str, Any]) -> str:
    return normalize_space(as_str(safe_get(item, "toAddress", "to", default=""))).lower()


def contains_any_keyword(text: str, keywords: List[str]) -> bool:
    t = text.lower()
    return any(k in t for k in keywords)


def first_matching_keyword(text: str, keywords: List[str]) -> str:
    t = text.lower()
    for k in keywords:
        if k in t:
            return k
    return ""


def extract_exchange_name(text: str) -> str:
    name = first_matching_keyword(text, EXCHANGE_KEYWORDS)
    return name or "unknown-exchange"


def classify_direction(parsed: ParsedTransfer) -> str:
    from_is_upbit = contains_any_keyword(parsed.from_text, UPBIT_KEYWORDS)
    to_is_upbit = contains_any_keyword(parsed.to_text, UPBIT_KEYWORDS)

    if from_is_upbit and not to_is_upbit:
        return "流出Upbit"
    if not from_is_upbit and to_is_upbit:
        return "流入Upbit"
    if from_is_upbit and to_is_upbit:
        return "Upbit内部/关联地址调拨"
    return "非Upbit"


def classify_event_type(parsed: ParsedTransfer) -> str:
    from_exchange = contains_any_keyword(parsed.from_text, EXCHANGE_KEYWORDS)
    to_exchange = contains_any_keyword(parsed.to_text, EXCHANGE_KEYWORDS)
    from_upbit = contains_any_keyword(parsed.from_text, UPBIT_KEYWORDS)
    to_upbit = contains_any_keyword(parsed.to_text, UPBIT_KEYWORDS)

    if from_upbit and to_exchange and not to_upbit:
        return f"交易所→交易所 ({extract_exchange_name(parsed.from_text)} → {extract_exchange_name(parsed.to_text)})"
    if from_exchange and to_upbit and not from_upbit:
        return f"交易所→交易所 ({extract_exchange_name(parsed.from_text)} → {extract_exchange_name(parsed.to_text)})"
    if from_upbit and to_upbit:
        return "Upbit内部调拨"
    if from_upbit:
        return "Upbit提币/外流"
    if to_upbit:
        return "Upbit充值/流入"
    return "普通链上转账"


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
        return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(raw_ts)


def make_event_id(parsed: ParsedTransfer) -> str:
    raw = json.dumps(
        {
            "tx": parsed.tx_hash,
            "symbol": parsed.symbol,
            "amount": parsed.amount,
            "usd": parsed.usd_value,
            "time": parsed.timestamp,
            "from": parsed.from_text,
            "to": parsed.to_text,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def token_matches(parsed: ParsedTransfer) -> bool:
    if not TOKEN_WHITELIST:
        return True
    wanted = set(TOKEN_WHITELIST)
    return (
        parsed.symbol.lower() in wanted
        or parsed.token_id.lower() in wanted
        or parsed.token_address.lower() in wanted
    )


def chain_matches(parsed: ParsedTransfer) -> bool:
    if not CHAIN_WHITELIST:
        return True
    return parsed.chain.lower() in set(CHAIN_WHITELIST)


def parse_transfer(item: Dict[str, Any]) -> ParsedTransfer:
    return ParsedTransfer(
        symbol=get_symbol(item),
        token_id=get_token_id(item),
        token_address=get_token_address(item),
        chain=get_chain(item),
        amount=get_amount(item),
        usd_value=get_usd(item),
        tx_hash=get_tx_hash(item),
        timestamp=get_timestamp(item),
        from_text=get_from_text(item),
        to_text=get_to_text(item),
        from_address=get_from_address(item),
        to_address=get_to_address(item),
    )


def prune_old_signals(now_ts: float) -> None:
    for symbol, dq in list(recent_signals.items()):
        while dq and now_ts - dq[0][0] > SIGNAL_WINDOW_SECONDS:
            dq.popleft()
        if not dq:
            recent_signals.pop(symbol, None)

    for wallet, dq in list(recent_wallet_activity.items()):
        while dq and now_ts - dq[0][0] > SIGNAL_WINDOW_SECONDS:
            dq.popleft()
        if not dq:
            recent_wallet_activity.pop(wallet, None)


def wallet_cluster_score(parsed: ParsedTransfer, direction: str, now_ts: float) -> int:
    wallet = parsed.to_address if direction == "流出Upbit" else parsed.from_address
    if not wallet:
        return 0

    dq = recent_wallet_activity[wallet]
    dq.append((now_ts, parsed.symbol.lower(), parsed.usd_value))
    recent_same_wallet = [x for x in dq if now_ts - x[0] <= SIGNAL_WINDOW_SECONDS]

    if len(recent_same_wallet) >= 4:
        return -25
    if len(recent_same_wallet) == 3:
        return -15
    return 0


def continuity_score(parsed: ParsedTransfer, direction: str, now_ts: float) -> int:
    key = parsed.symbol.lower() or parsed.token_id.lower() or parsed.token_address.lower()
    dq = recent_signals[key]
    dq.append((now_ts, direction, parsed.usd_value))

    recent_same = [x for x in dq if x[1] == direction and now_ts - x[0] <= SIGNAL_WINDOW_SECONDS]
    count = len(recent_same)

    if count >= 4:
        return 35
    if count == 3:
        return 25
    if count == 2:
        return 15
    return 0


def amount_score(usd_value: float) -> int:
    if usd_value >= max(USD_THRESHOLD * 4, USD_THRESHOLD + 300000):
        return 30
    if usd_value >= max(USD_THRESHOLD * 2, USD_THRESHOLD + 100000):
        return 20
    if usd_value >= USD_THRESHOLD:
        return 10
    return 0


def detect_false_breakout(parsed: ParsedTransfer, direction: str) -> Tuple[int, str]:
    event_type = classify_event_type(parsed)

    if event_type.startswith("交易所→交易所"):
        return -30, "疑似交易所间调拨，优先降权"
    if event_type == "Upbit内部调拨":
        return -50, "Upbit内部调拨，视为无效信号"

    from_upbit = contains_any_keyword(parsed.from_text, UPBIT_KEYWORDS)
    to_upbit = contains_any_keyword(parsed.to_text, UPBIT_KEYWORDS)
    from_exchange = contains_any_keyword(parsed.from_text, EXCHANGE_KEYWORDS)
    to_exchange = contains_any_keyword(parsed.to_text, EXCHANGE_KEYWORDS)

    if direction == "流入Upbit" and from_exchange and not from_upbit:
        return -18, "其他交易所转入 Upbit，可能是跨所搬砖/做市"
    if direction == "流出Upbit" and to_exchange and not to_upbit:
        return -18, "Upbit 转往其他交易所，可能是跨所搬砖/做市"

    return 0, ""


def decide_signal(parsed: ParsedTransfer) -> SignalDecision:
    direction = classify_direction(parsed)
    event_type = classify_event_type(parsed)

    if direction == "非Upbit":
        return SignalDecision(False, "忽略", "无", 0, "非 Upbit 相关", direction, event_type)
    if not token_matches(parsed):
        return SignalDecision(False, "忽略", "无", 0, "不在币种白名单", direction, event_type)
    if not chain_matches(parsed):
        return SignalDecision(False, "忽略", "无", 0, "不在链白名单", direction, event_type)
    if parsed.usd_value < USD_THRESHOLD:
        return SignalDecision(False, "忽略", "无", 0, "金额低于阈值", direction, event_type)

    now_ts = time.time()
    prune_old_signals(now_ts)

    score = 50
    reasons: List[str] = []

    score += amount_score(parsed.usd_value)
    if parsed.usd_value >= USD_THRESHOLD:
        reasons.append("达到大额阈值")

    continuity = continuity_score(parsed, direction, now_ts)
    score += continuity
    if continuity > 0:
        reasons.append(f"同币种 {SIGNAL_WINDOW_SECONDS // 60} 分钟内连续同向")

    wallet_penalty = wallet_cluster_score(parsed, direction, now_ts)
    score += wallet_penalty
    if wallet_penalty < 0:
        reasons.append("短时同钱包频繁转账，疑似拆单/归集")

    false_break_penalty, fb_reason = detect_false_breakout(parsed, direction)
    score += false_break_penalty
    if fb_reason:
        reasons.append(fb_reason)

    if direction == "流入Upbit":
        action = "建议偏空 / 可观察做空机会"
    elif direction == "流出Upbit":
        action = "建议偏多 / 可观察做多机会"
    else:
        action = "建议观望"

    if event_type == "Upbit内部调拨":
        action = "建议观望"

    if score >= 90:
        strength = "🔥🔥🔥 强信号"
    elif score >= 75:
        strength = "🔥🔥 中强信号"
    elif score >= 60:
        strength = "🔥 普通信号"
    else:
        strength = "⚪ 弱信号"

    should_alert = score >= MIN_ALERT_SCORE and event_type != "Upbit内部调拨"
    reason = "；".join(reasons) if reasons else "满足基础条件"
    return SignalDecision(should_alert, action, strength, score, reason, direction, event_type)


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
        telegram_limiter.wait()
        requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text[:4000],
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
    except Exception as e:
        logging.warning("Telegram 发送失败: %s", e)


def add_seen(eid: str) -> None:
    if eid in seen_set:
        return
    seen_set.add(eid)
    seen_order.append(eid)
    while len(seen_order) > MAX_SEEN_SIZE:
        old = seen_order.popleft()
        seen_set.discard(old)


def build_message(parsed: ParsedTransfer, decision: SignalDecision) -> str:
    if decision.direction == "流入Upbit":
        emoji = "🔴"
    elif decision.direction == "流出Upbit":
        emoji = "🟢"
    else:
        emoji = "🟡"

    symbol = parsed.symbol or parsed.token_id or parsed.token_address or "-"
    chain = parsed.chain or "-"
    amount = parsed.amount or "-"
    ts_bj = format_beijing_time(parsed.timestamp)
    tx = parsed.tx_hash or "-"

    return (
        f"{emoji} Upbit 小币种大额转账 V2\n"
        f"{decision.strength}\n"
        f"{decision.action}\n"
        f"评分: {decision.score}\n"
        f"事件: {decision.event_type}\n"
        f"方向: {decision.direction}\n"
        f"时间(北京): {ts_bj}\n"
        f"币种: {symbol}\n"
        f"链: {chain}\n"
        f"金额(USD): ${parsed.usd_value:,.2f}\n"
        f"数量: {amount}\n"
        f"From: {parsed.from_text or '-'}\n"
        f"To: {parsed.to_text or '-'}\n"
        f"Tx: {tx}\n"
        f"判定: {decision.reason}"
    )


async def run_forever() -> None:
    if not API_KEY:
        raise ValueError("缺少 ARKHAM_API_KEY")

    backoff = RECONNECT_MIN

    while True:
        session_id: Optional[str] = None
        try:
            session_id = create_ws_session()
            ws_url = build_ws_url(session_id)

            logging.info("连接 WebSocket: %s", ws_url)

            async with websockets.connect(
                ws_url,
                additional_headers={
                    "API-Key": API_KEY,
                    "User-Agent": "upbit-arkham-monitor-v2/1.0",
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
                        parsed = parse_transfer(item)
                        eid = make_event_id(parsed)
                        if eid in seen_set:
                            continue

                        decision = decide_signal(parsed)
                        add_seen(eid)

                        if not decision.should_alert:
                            logging.info(
                                "忽略 | %s | %s | $%.2f | %s",
                                parsed.symbol or parsed.token_id or "-",
                                decision.event_type,
                                parsed.usd_value,
                                decision.reason,
                            )
                            continue

                        msg = build_message(parsed, decision)
                        logging.info("\n%s", msg)
                        telegram_send(msg)

        except Exception as e:
            logging.exception("主循环异常: %s", e)
            if session_id:
                delete_ws_session(session_id)

            logging.info("准备重连，%s 秒后继续", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, RECONNECT_MAX)


def main() -> None:
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
