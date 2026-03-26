import asyncio
import json
import logging
import os
import signal
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Set, Tuple

import aiohttp
from aiohttp import web
import websockets
from websockets.exceptions import InvalidStatus


# =========================================================
# 配置
# =========================================================

REST_BASE = os.getenv("REST_BASE", "https://data-api.binance.vision").rstrip("/")
WS_BASE = os.getenv("WS_BASE", "wss://data-stream.binance.vision/stream?streams=").strip()
FUTURES_REST_BASE =   "https://fapi1.binance.com"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
PORT = int(os.getenv("PORT", "8080"))
ENABLE_HEALTHCHECK = os.getenv("ENABLE_HEALTHCHECK", "true").lower() == "true"
RECONNECT_DELAY_SECONDS = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))

TELEGRAM_BOT_TOKEN = "8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TELEGRAM_CHAT_ID = "6308781694"

QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT").upper()
DISCOVERY_STREAM = "!miniTicker@arr"

UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "1800"))
TOP_N_SUBSCRIBE = int(os.getenv("TOP_N_SUBSCRIBE", "80"))
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "1000000"))
ALLOW_SPOT_FALLBACK = os.getenv("ALLOW_SPOT_FALLBACK", "true").lower() == "true"

EXCLUDED_BASE_ASSETS: Set[str] = {
    x.strip().upper()
    for x in os.getenv(
        "EXCLUDED_BASE_ASSETS",
        "BTC,ETH,BNB,SOL,XRP,ADA,DOGE,TRX,TON,AVAX,LINK,DOT,LTC,BCH,SHIB,PEPE,XLM,ATOM,UNI,ETC,APT,NEAR,FIL,HBAR,ARB,OP,SUI"
    ).split(",")
    if x.strip()
}

EXCLUDED_SYMBOLS: Set[str] = {
    x.strip().upper()
    for x in os.getenv(
        "EXCLUDED_SYMBOLS",
        "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,ADAUSDT,DOGEUSDT,TRXUSDT,TONUSDT"
    ).split(",")
    if x.strip()
}

UPBIT_HOT_SYMBOLS: Set[str] = {
    x.strip().upper()
    for x in os.getenv(
        "UPBIT_HOT_SYMBOLS",
        (
            "XRPUSDT,DOGEUSDT,SEIUSDT,SUIUSDT,APTUSDT,ARBUSDT,OPUSDT,"
            "WLDUSDT,STXUSDT,INJUSDT,RUNEUSDT,IMXUSDT,FETUSDT,GRTUSDT,"
            "THETAUSDT,AAVEUSDT,NEARUSDT,HBARUSDT,ALGOUSDT,ATOMUSDT,"
            "TONUSDT,TIAUSDT,JUPUSDT,BLURUSDT,PEOPLEUSDT,ARKMUSDT,"
            "WIFUSDT,ORDIUSDT,SATSUSDT,1000PEPEUSDT,1000BONKUSDT,"
            "ENAUSDT,NOTUSDT,TURBOUSDT,MEMEUSDT,BOMEUSDT,AEVOUSDT,"
            "IDUSDT,AIUSDT,PORTALUSDT,STRKUSDT,PIXELUSDT,BEAMXUSDT,"
            "CFXUSDT,ROSEUSDT,CELOUSDT,ANKRUSDT,SKLUSDT,API3USDT,"
            "MINAUSDT,ASTRUSDT,KAVAUSDT,ZILUSDT,ICXUSDT,ONEUSDT"
        )
    ).split(",")
    if x.strip()
}

ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "180"))

# ==================== 趋势确认 ====================
TREND_CONFIRM_WINDOW_SEC = float(os.getenv("TREND_CONFIRM_WINDOW_SEC", "4.0"))
TREND_CONFIRM_MIN_NOTIONAL = float(os.getenv("TREND_CONFIRM_MIN_NOTIONAL", "220000"))
TREND_CONFIRM_MIN_DOMINANCE = float(os.getenv("TREND_CONFIRM_MIN_DOMINANCE", "0.72"))
TREND_CONFIRM_MIN_MOVE_PCT = float(os.getenv("TREND_CONFIRM_MIN_MOVE_PCT", "0.18"))
TREND_CONFIRM_MIN_IMBALANCE = float(os.getenv("TREND_CONFIRM_MIN_IMBALANCE", "0.12"))
TREND_CONFIRM_MIN_TRADE_COUNT = int(os.getenv("TREND_CONFIRM_MIN_TRADE_COUNT", "3"))

# ==================== 盘口 ====================
IMBALANCE_WINDOW_SEC = float(os.getenv("IMBALANCE_WINDOW_SEC", "2.0"))
MIN_BOOK_SNAPSHOT_FOR_IMBALANCE = int(os.getenv("MIN_BOOK_SNAPSHOT_FOR_IMBALANCE", "3"))

# ==================== 噪音过滤 ====================
BOT_MAX_MICRO_NOTIONAL = float(os.getenv("BOT_MAX_MICRO_NOTIONAL", "3000"))
BOT_ALTERNATING_COUNT = int(os.getenv("BOT_ALTERNATING_COUNT", "8"))
BOT_BURST_WINDOW_SEC = float(os.getenv("BOT_BURST_WINDOW_SEC", "2.0"))


# =========================================================
# 数据结构
# =========================================================

@dataclass
class MiniTickerState:
    symbol: str
    last_price: float = 0.0
    quote_volume_24h: float = 0.0
    last_update_ts: float = 0.0


@dataclass
class BookTickerSnapshot:
    ts: float
    bid: float
    ask: float
    bid_qty: float
    ask_qty: float

    @property
    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2.0
        return max(self.bid, self.ask, 0.0)


@dataclass
class AggTradeEvent:
    ts: float
    price: float
    qty: float
    notional: float
    side: str  # buy / sell


@dataclass
class RuntimeStatus:
    started_at: float = field(default_factory=time.time)
    connected: bool = False
    ws_url: str = ""
    last_message_at: Optional[float] = None
    reconnect_count: int = 0
    universe_size: int = 0
    subscribed_count: int = 0
    last_error: str = ""


# =========================================================
# Telegram
# =========================================================

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.session: Optional[aiohttp.ClientSession] = None

    @property
    def enabled(self) -> bool:
        return bool(self.token and self.chat_id)

    async def start(self) -> None:
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=10)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    async def send(self, text: str) -> None:
        if not self.enabled:
            return

        if self.session is None:
            await self.start()

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text}

        try:
            async with self.session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logging.error("Telegram 发送失败: %s %s", resp.status, body)
        except Exception:
            logging.exception("Telegram 发送异常")


# =========================================================
# Universe
# =========================================================

class BinanceUniverse:
    def __init__(self, rest_base: str, futures_rest_base: str):
        self.rest_base = rest_base
        self.futures_rest_base = futures_rest_base
        self.session: Optional[aiohttp.ClientSession] = None
        self.allowed_symbols: Set[str] = set()
        self.spot_symbols: Set[str] = set()
        self.futures_symbols: Set[str] = set()
        self.futures_source: str = "unknown"   # api / env / missing_env / error
        self.monitor_mode: str = "unknown"     # futures_intersection / spot_fallback / empty

    async def start(self) -> None:
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    async def _fetch_spot_symbols(self) -> Set[str]:
        if self.session is None:
            await self.start()

        url = f"{self.rest_base}/api/v3/exchangeInfo?permissions=SPOT&symbolStatus=TRADING"
        async with self.session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()

        allowed: Set[str] = set()
        for item in data.get("symbols", []):
            if not isinstance(item, dict):
                continue

            symbol = item.get("symbol", "").upper()
            status = item.get("status", "")
            quote_asset = item.get("quoteAsset", "").upper()
            base_asset = item.get("baseAsset", "").upper()
            is_spot = bool(item.get("isSpotTradingAllowed", False))

            if status != "TRADING":
                continue
            if quote_asset != QUOTE_ASSET:
                continue
            if not is_spot:
                continue
            if base_asset in EXCLUDED_BASE_ASSETS:
                continue
            if symbol in EXCLUDED_SYMBOLS:
                continue

            allowed.add(symbol)
        return allowed

    def _load_futures_symbols_from_env(self) -> Set[str]:
        raw = os.getenv("FUTURES_SYMBOLS", "").strip()
        if not raw:
            return set()
        return {s.strip().upper() for s in raw.split(",") if s.strip()}

    async def _fetch_usdm_futures_symbols(self) -> Set[str]:
        if self.session is None:
            await self.start()

        env_symbols = self._load_futures_symbols_from_env()
        url = f"{self.futures_rest_base}/fapi/v1/exchangeInfo"

        try:
            async with self.session.get(url) as resp:
                if resp.status == 451:
                    if env_symbols:
                        self.futures_source = "env"
                        logging.info("期货API返回451，已改用 FUTURES_SYMBOLS，共 %s 个币种", len(env_symbols))
                        return env_symbols

                    self.futures_source = "missing_env"
                    logging.info("期货API返回451，未设置FUTURES_SYMBOLS，当前可切到 spot_fallback。")
                    return set()

                resp.raise_for_status()
                data = await resp.json()

        except aiohttp.ClientResponseError as e:
            if e.status == 451:
                if env_symbols:
                    self.futures_source = "env"
                    logging.info("期货API返回451，已改用 FUTURES_SYMBOLS，共 %s 个币种", len(env_symbols))
                    return env_symbols

                self.futures_source = "missing_env"
                logging.info("期货API返回451，未设置FUTURES_SYMBOLS，当前可切到 spot_fallback。")
                return set()

            self.futures_source = "error"
            logging.exception("获取 Futures 交易对失败")
            return env_symbols if env_symbols else set()

        except Exception:
            if env_symbols:
                self.futures_source = "env"
                logging.exception("获取 Futures 交易对失败，已改用 FUTURES_SYMBOLS")
                return env_symbols

            self.futures_source = "error"
            logging.exception("获取 Futures 交易对失败，且没有 FUTURES_SYMBOLS，返回空集合")
            return set()

        futures_symbols: Set[str] = set()
        for item in data.get("symbols", []):
            if not isinstance(item, dict):
                continue

            symbol = item.get("symbol", "").upper()
            status = item.get("status", "")
            quote_asset = item.get("quoteAsset", "").upper()

            if status != "TRADING":
                continue
            if quote_asset != QUOTE_ASSET:
                continue

            futures_symbols.add(symbol)

        self.futures_source = "api"
        return futures_symbols

    async def refresh(self) -> None:
        if self.session is None:
            await self.start()

        spot_symbols, futures_symbols = await asyncio.gather(
            self._fetch_spot_symbols(),
            self._fetch_usdm_futures_symbols(),
        )

        self.spot_symbols = spot_symbols
        self.futures_symbols = futures_symbols

        if futures_symbols:
            self.allowed_symbols = (spot_symbols & futures_symbols) & UPBIT_HOT_SYMBOLS
            self.monitor_mode = "futures_intersection"
        elif ALLOW_SPOT_FALLBACK:
            self.allowed_symbols = spot_symbols & UPBIT_HOT_SYMBOLS
            self.monitor_mode = "spot_fallback"
        else:
            self.allowed_symbols = set()
            self.monitor_mode = "empty"

        logging.info(
            "交易池刷新完成：现货山寨币=%s，合约符号种类=%s，Upbit白名单=%s，最终可监控=%s，合约来源=%s，监控模式=%s",
            len(spot_symbols),
            len(futures_symbols),
            len(UPBIT_HOT_SYMBOLS),
            len(self.allowed_symbols),
            self.futures_source,
            self.monitor_mode,
        )


# =========================================================
# Scanner
# =========================================================

class OrderFlowScanner:
    def __init__(self):
        self.runtime = RuntimeStatus()
        self.universe = BinanceUniverse(REST_BASE, FUTURES_REST_BASE)
        self.notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

        self.market: Dict[str, MiniTickerState] = {}
        self.trade_flow: Dict[str, Deque[AggTradeEvent]] = defaultdict(lambda: deque(maxlen=4000))
        self.book_flow: Dict[str, Deque[BookTickerSnapshot]] = defaultdict(lambda: deque(maxlen=500))

        self.active_symbols: Set[str] = set()
        self.last_alert_at: Dict[str, float] = defaultdict(float)

        self.ws = None
        self.health_runner: Optional[web.AppRunner] = None
        self._stop_event = asyncio.Event()
        self._last_universe_refresh = 0.0

    def stop(self) -> None:
        self._stop_event.set()

    async def _maybe_alert(self, key: str, text: str) -> None:
        now = time.time()
        if now - self.last_alert_at[key] < ALERT_COOLDOWN_SEC:
            return
        self.last_alert_at[key] = now
        logging.warning(text.replace("\n", " | "))
        await self.notifier.send(text)

    def _is_bot_noise(self, events: List[AggTradeEvent]) -> bool:
        micro = [e for e in events if e.notional <= BOT_MAX_MICRO_NOTIONAL]
        if len(micro) < BOT_ALTERNATING_COUNT:
            return False

        recent = micro[-BOT_ALTERNATING_COUNT:]
        sides = [e.side for e in recent]
        alternating = sum(1 for i in range(1, len(sides)) if sides[i] != sides[i - 1])
        span = recent[-1].ts - recent[0].ts

        return alternating >= BOT_ALTERNATING_COUNT - 1 and span <= BOT_BURST_WINDOW_SEC

    def _get_trade_window(self, symbol: str, sec: float) -> List[AggTradeEvent]:
        now = time.time()
        dq = self.trade_flow[symbol]
        cutoff = now - max(sec, 10.0)
        while dq and dq[0].ts < cutoff:
            dq.popleft()
        return [x for x in dq if x.ts >= now - sec]

    def _get_book_window(self, symbol: str, sec: float) -> List[BookTickerSnapshot]:
        now = time.time()
        dq = self.book_flow[symbol]
        cutoff = now - max(sec, 10.0)
        while dq and dq[0].ts < cutoff:
            dq.popleft()
        return [x for x in dq if x.ts >= now - sec]

    @staticmethod
    def _pct_change(a: float, b: float) -> float:
        if a <= 0:
            return 0.0
        return (b - a) / a * 100.0

    def _book_imbalance(self, symbol: str, sec: float = IMBALANCE_WINDOW_SEC) -> float:
        books = self._get_book_window(symbol, sec)
        if len(books) < MIN_BOOK_SNAPSHOT_FOR_IMBALANCE:
            return 0.0

        vals: List[float] = []
        for b in books:
            denom = b.bid_qty + b.ask_qty
            if denom <= 0:
                continue
            vals.append((b.bid_qty - b.ask_qty) / denom)

        if not vals:
            return 0.0
        return sum(vals) / len(vals)

    def _detect_trend_confirmation(self, symbol: str) -> Optional[dict]:
        events = self._get_trade_window(symbol, TREND_CONFIRM_WINDOW_SEC)
        books = self._get_book_window(symbol, TREND_CONFIRM_WINDOW_SEC)

        if len(events) < TREND_CONFIRM_MIN_TRADE_COUNT:
            return None
        if len(books) < MIN_BOOK_SNAPSHOT_FOR_IMBALANCE:
            return None
        if self._is_bot_noise(events):
            return None

        buy_notional = sum(e.notional for e in events if e.side == "buy")
        sell_notional = sum(e.notional for e in events if e.side == "sell")
        total_notional = buy_notional + sell_notional
        if total_notional < TREND_CONFIRM_MIN_NOTIONAL:
            return None

        side = "buy" if buy_notional >= sell_notional else "sell"
        dominance = max(buy_notional, sell_notional) / total_notional
        if dominance < TREND_CONFIRM_MIN_DOMINANCE:
            return None

        start_price = events[0].price
        end_price = events[-1].price
        move_pct = self._pct_change(start_price, end_price)
        imbalance = self._book_imbalance(symbol, TREND_CONFIRM_WINDOW_SEC)

        if side == "buy":
            if move_pct >= TREND_CONFIRM_MIN_MOVE_PCT and imbalance >= TREND_CONFIRM_MIN_IMBALANCE:
                return {
                    "type": "bull_trend_confirmation",
                    "total_notional": total_notional,
                    "dominance": dominance,
                    "move_pct": move_pct,
                    "imbalance": imbalance,
                    "start_price": start_price,
                    "end_price": end_price,
                    "trade_count": len(events),
                }
        else:
            if move_pct <= -TREND_CONFIRM_MIN_MOVE_PCT and imbalance <= -TREND_CONFIRM_MIN_IMBALANCE:
                return {
                    "type": "bear_trend_confirmation",
                    "total_notional": total_notional,
                    "dominance": dominance,
                    "move_pct": move_pct,
                    "imbalance": imbalance,
                    "start_price": start_price,
                    "end_price": end_price,
                    "trade_count": len(events),
                }

        return None

    async def _alert_trend_confirmation(self, symbol: str, result: dict) -> None:
        if result["type"] == "bull_trend_confirmation":
            title = "【上涨确认】"
            suggestion = "买入"
            desc = "主动买盘推动价格上行，且盘口买盘占优"
        else:
            title = "【下跌确认】"
            suggestion = "卖出"
            desc = "主动卖盘推动价格下行，且盘口卖盘占优"

        msg = (
            f"{title}\n"
            f"币种：{symbol}\n"
            f"操作建议：{suggestion}\n"
            f"确认原因：{desc}\n\n"
            f"总成交额：{result['total_notional']:,.0f} {QUOTE_ASSET}\n"
            f"主导占比：{result['dominance'] * 100:.1f}%\n"
            f"价格变化：{result['move_pct']:.3f}%\n"
            f"盘口偏向：{result['imbalance']:.3f}\n"
            f"成交笔数：{result['trade_count']}\n"
            f"价格区间：{result['start_price']} -> {result['end_price']}"
        )
        await self._maybe_alert(f"trend:{symbol}:{result['type']}", msg)

    async def _health(self, request: web.Request) -> web.Response:
        now = time.time()
        return web.json_response({
            "ok": True,
            "connected": self.runtime.connected,
            "ws_url": self.runtime.ws_url,
            "uptime_sec": round(now - self.runtime.started_at, 2),
            "last_message_at": self.runtime.last_message_at,
            "reconnect_count": self.runtime.reconnect_count,
            "universe_size": self.runtime.universe_size,
            "subscribed_count": self.runtime.subscribed_count,
            "spot_symbol_count": len(self.universe.spot_symbols),
            "futures_symbol_count": len(self.universe.futures_symbols),
            "futures_source": self.universe.futures_source,
            "monitor_mode": self.universe.monitor_mode,
            "last_error": self.runtime.last_error,
        })

    async def _root(self, request: web.Request) -> web.Response:
        return web.Response(text="order-flow scanner is running")

    async def _start_health_server(self) -> None:
        app = web.Application()
        app.router.add_get("/", self._root)
        app.router.add_get("/health", self._health)

        self.health_runner = web.AppRunner(app)
        await self.health_runner.setup()
        site = web.TCPSite(self.health_runner, "0.0.0.0", PORT)
        await site.start()
        logging.info("健康检查服务已启动: 0.0.0.0:%s", PORT)

    async def _subscribe_streams(self, params: List[str]) -> None:
        if not params or self.ws is None:
            return
        req = {
            "method": "SUBSCRIBE",
            "params": params,
            "id": int(time.time() * 1000) % 100000000,
        }
        await self.ws.send(json.dumps(req))
        logging.info("已发送订阅请求: %s", ", ".join(params))

    async def _unsubscribe_streams(self, params: List[str]) -> None:
        if not params or self.ws is None:
            return
        req = {
            "method": "UNSUBSCRIBE",
            "params": params,
            "id": int(time.time() * 1000) % 100000000,
        }
        await self.ws.send(json.dumps(req))
        logging.info("已发送取消订阅请求: %s", ", ".join(params))

    async def _handle_control_response(self, payload: dict) -> None:
        if "result" in payload:
            if payload.get("result") is None:
                logging.info("WebSocket订阅操作成功，id=%s", payload.get("id"))
            else:
                logging.info("WebSocket 控制响应: %s", payload)

    def _reselect_active_symbols(self) -> Tuple[Set[str], Set[str]]:
        ranked = sorted(
            (
                s for s, st in self.market.items()
                if s in self.universe.allowed_symbols and st.quote_volume_24h >= MIN_24H_QUOTE_VOLUME
            ),
            key=lambda x: self.market[x].quote_volume_24h,
            reverse=True
        )

        next_set = set(ranked[:TOP_N_SUBSCRIBE])
        added = next_set - self.active_symbols
        removed = self.active_symbols - next_set
        self.active_symbols = next_set
        self.runtime.subscribed_count = len(self.active_symbols)
        return added, removed

    async def _handle_mini_ticker_arr(self, arr: list) -> None:
        now = time.time()

        for item in arr:
            if not isinstance(item, dict):
                continue

            symbol = item.get("s", "").upper()
            if symbol not in self.universe.allowed_symbols:
                continue

            st = self.market.get(symbol)
            if st is None:
                st = MiniTickerState(symbol=symbol)
                self.market[symbol] = st

            try:
                st.last_price = float(item.get("c", 0.0) or 0.0)
                st.quote_volume_24h = float(item.get("q", 0.0) or 0.0)
            except (TypeError, ValueError):
                continue

            st.last_update_ts = now

        added, removed = self._reselect_active_symbols()

        unsub = []
        sub = []

        for s in sorted(removed):
            unsub.append(f"{s.lower()}@aggTrade")
            unsub.append(f"{s.lower()}@bookTicker")

        for s in sorted(added):
            sub.append(f"{s.lower()}@aggTrade")
            sub.append(f"{s.lower()}@bookTicker")

        if unsub:
            await self._unsubscribe_streams(unsub)
        if sub:
            await self._subscribe_streams(sub)

    async def _handle_book_ticker(self, data: dict) -> None:
        if not isinstance(data, dict):
            return

        symbol = data.get("s", "").upper()
        if symbol not in self.active_symbols:
            return

        try:
            snap = BookTickerSnapshot(
                ts=time.time(),
                bid=float(data.get("b", 0.0) or 0.0),
                ask=float(data.get("a", 0.0) or 0.0),
                bid_qty=float(data.get("B", 0.0) or 0.0),
                ask_qty=float(data.get("A", 0.0) or 0.0),
            )
        except (TypeError, ValueError):
            return

        if snap.bid <= 0 and snap.ask <= 0:
            return

        self.book_flow[symbol].append(snap)

    async def _handle_agg_trade(self, data: dict) -> None:
        if not isinstance(data, dict):
            return

        symbol = data.get("s", "").upper()
        if symbol not in self.active_symbols:
            return

        try:
            price = float(data.get("p", 0.0) or 0.0)
            qty = float(data.get("q", 0.0) or 0.0)
            ts = float(data.get("T", 0) or 0) / 1000.0
        except (TypeError, ValueError):
            return

        if price <= 0 or qty <= 0 or ts <= 0:
            return

        notional = price * qty
        is_buyer_maker = bool(data.get("m", False))
        side = "sell" if is_buyer_maker else "buy"

        event = AggTradeEvent(
            ts=ts,
            price=price,
            qty=qty,
            notional=notional,
            side=side,
        )
        self.trade_flow[symbol].append(event)

        # 只推送确认后的趋势信号
        trend = self._detect_trend_confirmation(symbol)
        if trend:
            await self._alert_trend_confirmation(symbol, trend)

    async def _handle_message(self, raw: str) -> None:
        self.runtime.last_message_at = time.time()

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logging.warning("收到无法解析的消息")
            return

        if not isinstance(payload, dict):
            return

        if "result" in payload and "id" in payload:
            await self._handle_control_response(payload)
            return

        stream = payload.get("stream", "")
        data = payload.get("data")

        if stream == DISCOVERY_STREAM and isinstance(data, list):
            await self._handle_mini_ticker_arr(data)
            return

        if isinstance(stream, str) and stream.endswith("@aggTrade"):
            await self._handle_agg_trade(data)
            return

        if isinstance(stream, str) and stream.endswith("@bookTicker"):
            await self._handle_book_ticker(data)
            return

    async def _periodic_universe_refresh(self) -> None:
        now = time.time()
        if now - self._last_universe_refresh < UNIVERSE_REFRESH_SEC:
            return

        self._last_universe_refresh = now
        await self.universe.refresh()
        self.runtime.universe_size = len(self.universe.allowed_symbols)

        invalid_active = {s for s in self.active_symbols if s not in self.universe.allowed_symbols}
        if invalid_active:
            unsub = []
            for s in sorted(invalid_active):
                unsub.append(f"{s.lower()}@aggTrade")
                unsub.append(f"{s.lower()}@bookTicker")
            await self._unsubscribe_streams(unsub)
            self.active_symbols -= invalid_active
            self.runtime.subscribed_count = len(self.active_symbols)

    async def start(self) -> None:
        await self.universe.start()
        await self.universe.refresh()
        self.runtime.universe_size = len(self.universe.allowed_symbols)
        self._last_universe_refresh = time.time()

        await self.notifier.start()

        if ENABLE_HEALTHCHECK:
            await self._start_health_server()

        if self.universe.futures_source == "api":
            source_text = "Binance Futures API"
        elif self.universe.futures_source == "env":
            source_text = "环境变量 FUTURES_SYMBOLS"
        elif self.universe.futures_source == "missing_env":
            source_text = "未提供 FUTURES_SYMBOLS"
        elif self.universe.futures_source == "error":
            source_text = "Futures 获取失败"
        else:
            source_text = "未知"

        if self.universe.monitor_mode == "futures_intersection":
            mode_text = "仅监控有 USDⓈ-M 合约且在 Upbit 白名单中的币种"
        elif self.universe.monitor_mode == "spot_fallback":
            mode_text = "Futures 不可用，已退回现货白名单监控模式"
        else:
            mode_text = "当前监控池为空"

        await self.notifier.send(
            "【系统启动成功】\n"
            f"{mode_text}\n"
            "监控内容：仅推送确认后的上涨 / 下跌信号\n"
            f"合约币种来源：{source_text}"
        )

        if self.universe.monitor_mode == "empty":
            await self.notifier.send(
                "【配置提醒】\n"
                "当前没有可监控币种。\n"
                "如 Futures API 返回 451，请设置 FUTURES_SYMBOLS，"
                "或将 ALLOW_SPOT_FALLBACK=true。"
            )

        await self._run()

    async def close(self) -> None:
        self.runtime.connected = False
        await self.universe.close()
        await self.notifier.close()

        if self.health_runner is not None:
            await self.health_runner.cleanup()
            self.health_runner = None

    async def _run(self) -> None:
        ws_url = WS_BASE + DISCOVERY_STREAM
        self.runtime.ws_url = ws_url

        while not self._stop_event.is_set():
            try:
                logging.info("正在连接 %s", ws_url)

                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=1000,
                ) as ws:
                    self.ws = ws
                    self.runtime.connected = True
                    self.runtime.last_error = ""
                    logging.info("WebSocket 已连接")

                    while not self._stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=10)
                            await self._handle_message(msg)
                            await self._periodic_universe_refresh()
                        except asyncio.TimeoutError:
                            await self._periodic_universe_refresh()
                            continue

            except InvalidStatus as e:
                self.runtime.connected = False
                self.runtime.reconnect_count += 1
                code = getattr(getattr(e, "response", None), "status_code", None)
                self.runtime.last_error = f"InvalidStatus: {code}"
                logging.exception("WebSocket 握手失败")
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                self.runtime.connected = False
                self.runtime.reconnect_count += 1
                self.runtime.last_error = repr(e)
                logging.exception("WebSocket 连接中断")
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)

            finally:
                self.ws = None


# =========================================================
# 启动入口
# =========================================================

def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s |%(levelname)s| %(message)s",
    )


async def main() -> None:
    setup_logging()
    scanner = OrderFlowScanner()
    loop = asyncio.get_running_loop()

    def _shutdown() -> None:
        logging.info("收到停止信号")
        scanner.stop()

    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            try:
                loop.add_signal_handler(getattr(signal, sig_name), _shutdown)
            except NotImplementedError:
                pass

    try:
        await scanner.start()
    finally:
        await scanner.close()


if __name__ == "__main__":
    asyncio.run(main())
