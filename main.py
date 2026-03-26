import asyncio
import json
import logging
import os
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional

import aiohttp
import websockets


BINANCE_WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443/stream?streams=")
RECONNECT_DELAY_SECONDS = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))

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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


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

    last_alert_at: Dict[str, float] = field(default_factory=lambda: defaultdict(float))


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
        if self._session:
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


class CryptoMonitor:
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
        self.states: Dict[str, SymbolState] = {s: SymbolState() for s in symbols}
        self.notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
        self.alert_manager = AlertManager(self.notifier)

    def _build_ws_url(self) -> str:
        streams = []
        for symbol in self.symbols:
            streams.append(f"{symbol}@trade")
            streams.append(f"{symbol}@bookTicker")
        return BINANCE_WS_BASE + "/".join(streams)

    def _cleanup_old_trades(self, symbol: str, now: float) -> None:
        state = self.states[symbol]
        cutoff = now - PRICE_CHANGE_WINDOW_SEC
        while state.trades and state.trades[0].ts < cutoff:
            state.trades.popleft()

    def _roll_volume_window_if_needed(self, symbol: str, now: float) -> None:
        state = self.states[symbol]
        elapsed = now - state.current_window_start
        if elapsed >= VOLUME_WINDOW_SEC:
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
                symbol,
                "price_change",
                f"{PRICE_CHANGE_WINDOW_SEC}秒内价格{direction} {pct_change:.2f}% | 起始={oldest.price:.4f}, 当前={latest.price:.4f}",
                state,
            )

    async def _check_volume_spike(self, symbol: str) -> None:
        state = self.states[symbol]
        if len(state.volume_windows) < 5:
            return

        historical_avg = statistics.mean(state.volume_windows)
        current_volume = state.current_window_volume
        if historical_avg <= 0:
            return

        ratio = current_volume / historical_avg
        if ratio >= VOLUME_MULTIPLIER_THRESHOLD:
            await self.alert_manager.alert(
                symbol,
                "volume_spike",
                f"{VOLUME_WINDOW_SEC}秒成交量异常 | 当前窗口={current_volume:.4f}, 历史均值={historical_avg:.4f}, 倍数={ratio:.2f}x",
                state,
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
                symbol,
                "spread",
                f"买卖价差过大 | bid={state.best_bid:.4f}, ask={state.best_ask:.4f}, spread={spread_pct:.4f}%",
                state,
            )

    async def _handle_trade(self, data: dict) -> None:
        symbol = data["s"].lower()
        price = float(data["p"])
        qty = float(data["q"])
        ts = data["T"] / 1000.0

        state = self.states[symbol]
        state.last_price = price
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
        bid = float(data["b"])
        ask = float(data["a"])

        state = self.states[symbol]
        state.best_bid = bid
        state.best_ask = ask

        await self._check_spread(symbol)

    async def _consume_message(self, raw_message: str) -> None:
        payload = json.loads(raw_message)
        stream = payload.get("stream", "")
        data = payload.get("data", {})

        if stream.endswith("@trade"):
            await self._handle_trade(data)
        elif stream.endswith("@bookTicker"):
            await self._handle_book_ticker(data)

    async def run(self) -> None:
        ws_url = self._build_ws_url()
        logging.info("Connecting to %s", ws_url)
        await self.notifier.start()

        while True:
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=1000,
                ) as ws:
                    logging.info("Connected to Binance WebSocket")
                    await self.notifier.send("加密货币监控服务已启动")

                    async for msg in ws:
                        await self._consume_message(msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("WebSocket disconnected, retrying in %s seconds", RECONNECT_DELAY_SECONDS)
                await self.notifier.send(f"监控连接中断，{RECONNECT_DELAY_SECONDS}秒后重连")
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)


def setup_logging() -> None:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


async def main() -> None:
    if not SYMBOLS:
        raise RuntimeError("SYMBOLS is empty")

    setup_logging()
    monitor = CryptoMonitor(SYMBOLS)
    try:
        await monitor.run()
    finally:
        await monitor.notifier.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("stopped")
