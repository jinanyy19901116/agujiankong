import asyncio
import json
import logging
import os
import signal
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

import aiohttp
from aiohttp import web
import websockets
from websockets.exceptions import InvalidStatus


PRIMARY_WS_BASE = os.getenv(
    "PRIMARY_WS_BASE",
    "wss://stream.binance.com:9443/stream?streams=",
).strip()

FALLBACK_WS_BASE = os.getenv(
    "FALLBACK_WS_BASE",
    "wss://data-stream.binance.vision/stream?streams=",
).strip()

RECONNECT_DELAY_SECONDS = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

SYMBOLS = [
    s.strip().lower()
    for s in os.getenv("SYMBOLS", "btcusdt,ethusdt,solusdt").split(",")
    if s.strip()
]

PRICE_CHANGE_WINDOW_SEC = int(os.getenv("PRICE_CHANGE_WINDOW_SEC", "60"))
PRICE_CHANGE_THRESHOLD_PCT = float(os.getenv("PRICE_CHANGE_THRESHOLD_PCT", "1.0"))

VOLUME_WINDOW_SEC = int(os.getenv("VOLUME_WINDOW_SEC", "60"))
VOLUME_MULTIPLIER_THRESHOLD = float(os.getenv("VOLUME_MULTIPLIER_THRESHOLD", "3.0"))

SPREAD_THRESHOLD_PCT = float(os.getenv("SPREAD_THRESHOLD_PCT", "0.15"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "180"))

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

ENABLE_HEALTHCHECK = os.getenv("ENABLE_HEALTHCHECK", "true").lower() == "true"
PORT = int(os.getenv("PORT", "8080"))


@dataclass
class TradePoint:
    ts: float
    price: float
    qty: float


@dataclass
class SymbolState:
    trades: Deque[TradePoint] = field(default_factory=lambda: deque(maxlen=5000))
    volume_windows: Deque[float] = field(default_factory=lambda: deque(maxlen=30))
    current_window_volume: float = 0.0
    current_window_start: float = field(default_factory=time.time)

    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_price: Optional[float] = None
    last_trade_ts: Optional[float] = None

    last_alert_at: Dict[str, float] = field(default_factory=lambda: defaultdict(float))


@dataclass
class RuntimeStatus:
    started_at: float = field(default_factory=time.time)
    connected: bool = False
    active_ws_url: str = ""
    last_message_at: Optional[float] = None
    reconnect_count: int = 0
    fallback_in_use: bool = False
    last_error: str = ""


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def enabled(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    async def start(self) -> None:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=10)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def send(self, text: str) -> None:
        if not self.enabled:
            return

        if self._session is None:
            await self.start()

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
        }

        try:
            async with self._session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logging.error("Telegram send failed: %s %s", resp.status, body)
        except Exception:
            logging.exception("Telegram send exception")


class AlertManager:
    def __init__(self, notifier: TelegramNotifier):
        self.notifier = notifier

    async def alert(self, symbol: str, category: str, message: str, state: SymbolState) -> None:
        now = time.time()
        last_time = state.last_alert_at.get(category, 0.0)

        if now - last_time < ALERT_COOLDOWN_SEC:
            return

        state.last_alert_at[category] = now
        full_message = f"[{symbol.upper()}] {message}"
        logging.warning(full_message)
        await self.notifier.send(full_message)


class HealthServer:
    def __init__(self, runtime_status: RuntimeStatus, states: Dict[str, SymbolState]):
        self.runtime_status = runtime_status
        self.states = states
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None

    async def handle_health(self, request: web.Request) -> web.Response:
        now = time.time()
        data = {
            "ok": True,
            "connected": self.runtime_status.connected,
            "active_ws_url": self.runtime_status.active_ws_url,
            "fallback_in_use": self.runtime_status.fallback_in_use,
            "uptime_sec": round(now - self.runtime_status.started_at, 2),
            "last_message_at": self.runtime_status.last_message_at,
            "reconnect_count": self.runtime_status.reconnect_count,
            "last_error": self.runtime_status.last_error,
            "symbols": {
                symbol: {
                    "last_price": state.last_price,
                    "best_bid": state.best_bid,
                    "best_ask": state.best_ask,
                    "last_trade_ts": state.last_trade_ts,
                    "recent_trade_count": len(state.trades),
                    "current_window_volume": state.current_window_volume,
                }
                for symbol, state in self.states.items()
            },
        }
        return web.json_response(data)

    async def handle_root(self, request: web.Request) -> web.Response:
        return web.Response(text="crypto-monitor is running")

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/", self.handle_root)
        app.router.add_get("/health", self.handle_health)

        self.runner = web.AppRunner(app)
        await self.runner.setup()

        self.site = web.TCPSite(self.runner, "0.0.0.0", PORT)
        await self.site.start()
        logging.info("Health server listening on 0.0.0.0:%s", PORT)

    async def close(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None
            self.site = None


class CryptoMonitor:
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.states: Dict[str, SymbolState] = {s: SymbolState() for s in symbols}
        self.runtime_status = RuntimeStatus()
        self.notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
        self.alert_manager = AlertManager(self.notifier)
        self.health_server = HealthServer(self.runtime_status, self.states)
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def _build_stream_path(self) -> str:
        streams = []
        for symbol in self.symbols:
            streams.append(f"{symbol}@trade")
            streams.append(f"{symbol}@bookTicker")
        return "/".join(streams)

    def _build_ws_url(self, base: str) -> str:
        return base + self._build_stream_path()

    def _cleanup_old_trades(self, symbol: str, now: float) -> None:
        state = self.states[symbol]
        cutoff = now - PRICE_CHANGE_WINDOW_SEC
        while state.trades and state.trades[0].ts < cutoff:
            state.trades.popleft()

    def _roll_volume_window_if_needed(self, symbol: str, now: float) -> None:
        state = self.states[symbol]
        if now - state.current_window_start >= VOLUME_WINDOW_SEC:
            state.volume_windows.append(state.current_window_volume)
            state.current_window_volume = 0.0
            state.current_window_start = now

    async def _check_price_change(self, symbol: str) -> None:
        state = self.states[symbol]
        if len(state.trades) < 2:
            return

        oldest = state.trades[0]
        latest = state.trades[-1]

        if oldest.price <= 0:
            return

        pct_change = (latest.price - oldest.price) / oldest.price * 100
        if abs(pct_change) >= PRICE_CHANGE_THRESHOLD_PCT:
            direction = "上涨" if pct_change > 0 else "下跌"
            await self.alert_manager.alert(
                symbol=symbol,
                category="price_change",
                message=(
                    f"{PRICE_CHANGE_WINDOW_SEC}秒内价格{direction} {pct_change:.2f}% | "
                    f"起始={oldest.price:.4f}, 当前={latest.price:.4f}"
                ),
                state=state,
            )

    async def _check_volume_spike(self, symbol: str) -> None:
        state = self.states[symbol]
        if len(state.volume_windows) < 5:
            return

        historical_avg = statistics.mean(state.volume_windows)
        if historical_avg <= 0:
            return

        ratio = state.current_window_volume / historical_avg
        if ratio >= VOLUME_MULTIPLIER_THRESHOLD:
            await self.alert_manager.alert(
                symbol=symbol,
                category="volume_spike",
                message=(
                    f"{VOLUME_WINDOW_SEC}秒成交量异常 | "
                    f"当前窗口={state.current_window_volume:.4f}, "
                    f"历史均值={historical_avg:.4f}, "
                    f"倍数={ratio:.2f}x"
                ),
                state=state,
            )

    async def _check_spread(self, symbol: str) -> None:
        state = self.states[symbol]
        if state.best_bid is None or state.best_ask is None:
            return
        if state.best_bid <= 0 or state.best_ask <= 0:
            return

        mid = (state.best_bid + state.best_ask) / 2
        if mid <= 0:
            return

        spread_pct = (state.best_ask - state.best_bid) / mid * 100
        if spread_pct >= SPREAD_THRESHOLD_PCT:
            await self.alert_manager.alert(
                symbol=symbol,
                category="spread",
                message=(
                    f"买卖价差过大 | bid={state.best_bid:.4f}, "
                    f"ask={state.best_ask:.4f}, spread={spread_pct:.4f}%"
                ),
                state=state,
            )

    async def _handle_trade(self, data: dict) -> None:
        symbol = data["s"].lower()
        if symbol not in self.states:
            return

        price = float(data["p"])
        qty = float(data["q"])
        ts = data["T"] / 1000.0

        state = self.states[symbol]
        state.last_price = price
        state.last_trade_ts = ts
        state.trades.append(TradePoint(ts=ts, price=price, qty=qty))

        now = time.time()
        self._cleanup_old_trades(symbol, now)
        self._roll_volume_window_if_needed(symbol, now)
        state.current_window_volume += qty

        logging.info("[TRADE] %s price=%.4f qty=%.6f", symbol.upper(), price, qty)

        await self._check_price_change(symbol)
        await self._check_volume_spike(symbol)

    async def _handle_book_ticker(self, data: dict) -> None:
        symbol = data["s"].lower()
        if symbol not in self.states:
            return

        state = self.states[symbol]
        state.best_bid = float(data["b"])
        state.best_ask = float(data["a"])

        await self._check_spread(symbol)

    async def _consume_message(self, raw_message: str) -> None:
        self.runtime_status.last_message_at = time.time()

        payload = json.loads(raw_message)
        stream = payload.get("stream", "")
        data = payload.get("data", {})

        if stream.endswith("@trade"):
            await self._handle_trade(data)
        elif stream.endswith("@bookTicker"):
            await self._handle_book_ticker(data)

    async def _connect_and_consume(self, ws_url: str, fallback_in_use: bool) -> None:
        logging.info("正在连接到 %s", ws_url)

        self.runtime_status.active_ws_url = ws_url
        self.runtime_status.fallback_in_use = fallback_in_use

        async with websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_queue=1000,
        ) as ws:
            self.runtime_status.connected = True
            self.runtime_status.last_error = ""
            logging.info("WebSocket 已连接")

            await self.notifier.send(
                f"监控服务已启动\n"
                f"symbols={','.join(self.symbols)}\n"
                f"endpoint={ws_url}"
            )

            async for msg in ws:
                if self._stop_event.is_set():
                    break
                await self._consume_message(msg)

    async def _run_loop(self) -> None:
        primary_url = self._build_ws_url(PRIMARY_WS_BASE)
        fallback_url = self._build_ws_url(FALLBACK_WS_BASE)

        while not self._stop_event.is_set():
            try:
                await self._connect_and_consume(primary_url, fallback_in_use=False)

            except InvalidStatus as e:
                status_code = getattr(getattr(e, "response", None), "status_code", None)
                self.runtime_status.connected = False
                self.runtime_status.reconnect_count += 1
                self.runtime_status.last_error = f"primary invalid status: {status_code}"

                if status_code == 451:
                    logging.warning("主端点返回 HTTP 451，尝试切换到 fallback 端点")
                    await self.notifier.send("主端点返回 451，正在切换到 fallback market-data 端点")

                    try:
                        await self._connect_and_consume(fallback_url, fallback_in_use=True)
                    except Exception as fallback_exc:
                        self.runtime_status.connected = False
                        self.runtime_status.reconnect_count += 1
                        self.runtime_status.last_error = f"fallback failed: {repr(fallback_exc)}"
                        logging.exception("fallback 端点连接失败，%s 秒后重试", RECONNECT_DELAY_SECONDS)
                        await asyncio.sleep(RECONNECT_DELAY_SECONDS)
                else:
                    logging.exception("WebSocket 握手失败，%s 秒后重试", RECONNECT_DELAY_SECONDS)
                    await asyncio.sleep(RECONNECT_DELAY_SECONDS)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                self.runtime_status.connected = False
                self.runtime_status.reconnect_count += 1
                self.runtime_status.last_error = repr(e)
                logging.exception("WebSocket 连接已断开，%s 秒后重试", RECONNECT_DELAY_SECONDS)
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    async def start(self) -> None:
        if not self.symbols:
            raise RuntimeError("SYMBOLS is empty")

        await self.notifier.start()

        if ENABLE_HEALTHCHECK:
            await self.health_server.start()

        await self._run_loop()

    async def close(self) -> None:
        self.runtime_status.connected = False
        await self.health_server.close()
        await self.notifier.close()


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


async def main() -> None:
    setup_logging()

    monitor = CryptoMonitor(SYMBOLS)
    loop = asyncio.get_running_loop()

    def _shutdown_handler() -> None:
        logging.info("收到停止信号，准备退出")
        monitor.stop()

    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            loop.add_signal_handler(getattr(signal, sig_name), _shutdown_handler)

    try:
        await monitor.start()
    finally:
        await monitor.close()


if __name__ == "__main__":
    asyncio.run(main())
