import asyncio
import json
import logging
import math
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

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
PORT = int(os.getenv("PORT", "8080"))
ENABLE_HEALTHCHECK = os.getenv("ENABLE_HEALTHCHECK", "true").lower() == "true"

RECONNECT_DELAY_SECONDS = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))

# Telegram
TELEGRAM_BOT_TOKEN = "8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TELEGRAM_CHAT_ID =  "6308781694"

# 市场扫描
QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT").upper()
DISCOVERY_STREAM = "!miniTicker@arr"
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "1800"))
HOT_REBALANCE_SEC = int(os.getenv("HOT_REBALANCE_SEC", "45"))
TOP_N_HOT = int(os.getenv("TOP_N_HOT", "40"))
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "500000"))
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.0000001"))

# 主流币过滤
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

# 告警与策略
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "300"))
HOT_ALERT_THRESHOLD = float(os.getenv("HOT_ALERT_THRESHOLD", "65"))
SPREAD_ALERT_PCT = float(os.getenv("SPREAD_ALERT_PCT", "0.8"))

MOMENTUM_WINDOW_SEC = int(os.getenv("MOMENTUM_WINDOW_SEC", "180"))
MOMENTUM_ALERT_PCT = float(os.getenv("MOMENTUM_ALERT_PCT", "5.0"))

BUY_SCORE_THRESHOLD = float(os.getenv("BUY_SCORE_THRESHOLD", "75"))
BUY_MOMENTUM_THRESHOLD = float(os.getenv("BUY_MOMENTUM_THRESHOLD", "3.0"))
WEAK_BUY_SCORE_THRESHOLD = float(os.getenv("WEAK_BUY_SCORE_THRESHOLD", "60"))
WEAK_BUY_MOMENTUM_THRESHOLD = float(os.getenv("WEAK_BUY_MOMENTUM_THRESHOLD", "1.5"))
SELL_MOMENTUM_THRESHOLD = float(os.getenv("SELL_MOMENTUM_THRESHOLD", "-3.0"))
BUY_MAX_SPREAD_PCT = float(os.getenv("BUY_MAX_SPREAD_PCT", "0.5"))


# =========================================================
# 数据结构
# =========================================================

@dataclass
class MiniTickerState:
    symbol: str
    last_price: float = 0.0
    open_24h: float = 0.0
    high_24h: float = 0.0
    low_24h: float = 0.0
    base_volume_24h: float = 0.0
    quote_volume_24h: float = 0.0
    last_update_ts: float = 0.0
    recent_prices: Deque[Tuple[float, float]] = field(default_factory=lambda: deque(maxlen=900))
    hot_score: float = 0.0
    is_hot: bool = False


@dataclass
class BookTickerState:
    bid: Optional[float] = None
    ask: Optional[float] = None
    last_update_ts: float = 0.0


@dataclass
class RuntimeStatus:
    started_at: float = field(default_factory=time.time)
    connected: bool = False
    ws_url: str = ""
    last_message_at: Optional[float] = None
    reconnect_count: int = 0
    universe_size: int = 0
    hot_pool_size: int = 0
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
        payload = {
            "chat_id": self.chat_id,
            "text": text,
        }

        try:
            async with self.session.post(url, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logging.error("Telegram 发送失败: %s %s", resp.status, body)
        except Exception:
            logging.exception("Telegram 发送异常")


# =========================================================
# 交易对扫描
# =========================================================

class BinanceUniverse:
    def __init__(self, rest_base: str):
        self.rest_base = rest_base
        self.session: Optional[aiohttp.ClientSession] = None
        self.allowed_symbols: Set[str] = set()
        self.meta: Dict[str, dict] = {}

    async def start(self) -> None:
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=15)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    async def refresh(self) -> None:
        if self.session is None:
            await self.start()

        url = f"{self.rest_base}/api/v3/exchangeInfo?permissions=SPOT&symbolStatus=TRADING"

        async with self.session.get(url) as resp:
            resp.raise_for_status()
            data = await resp.json()

        symbols = data.get("symbols", [])
        allowed: Set[str] = set()
        meta: Dict[str, dict] = {}

        for item in symbols:
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
            meta[symbol] = {
                "baseAsset": base_asset,
                "quoteAsset": quote_asset,
            }

        self.allowed_symbols = allowed
        self.meta = meta
        logging.info("交易池刷新完成: %s 个山寨币", len(self.allowed_symbols))


# =========================================================
# 主扫描器
# =========================================================

class AltcoinScanner:
    def __init__(self):
        self.runtime = RuntimeStatus()
        self.universe = BinanceUniverse(REST_BASE)
        self.notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

        self.market: Dict[str, MiniTickerState] = {}
        self.book: Dict[str, BookTickerState] = defaultdict(BookTickerState)

        self.hot_symbols: Set[str] = set()
        self.last_alert_at: Dict[str, float] = defaultdict(float)

        self.ws = None
        self._stop_event = asyncio.Event()
        self._last_universe_refresh = 0.0
        self._last_hot_rebalance = 0.0
        self.health_runner: Optional[web.AppRunner] = None

    def stop(self) -> None:
        self._stop_event.set()

    # -------------------------
    # 工具函数
    # -------------------------

    @staticmethod
    def _pct_change(start_price: float, end_price: float) -> float:
        if start_price <= 0:
            return 0.0
        return (end_price - start_price) / start_price * 100.0

    def _recent_momentum_pct(self, state: MiniTickerState, now: float) -> float:
        if not state.recent_prices:
            return 0.0

        cutoff = now - MOMENTUM_WINDOW_SEC
        while state.recent_prices and state.recent_prices[0][0] < cutoff:
            state.recent_prices.popleft()

        if len(state.recent_prices) < 2:
            return 0.0

        first_price = state.recent_prices[0][1]
        last_price = state.recent_prices[-1][1]
        return self._pct_change(first_price, last_price)

    def _get_spread_pct(self, symbol: str) -> float:
        bid = self.book[symbol].bid
        ask = self.book[symbol].ask

        if not bid or not ask or bid <= 0 or ask <= 0:
            return 0.0

        mid = (bid + ask) / 2.0
        if mid <= 0:
            return 0.0

        return (ask - bid) / mid * 100.0

    def _score_symbol(self, state: MiniTickerState, now: float) -> float:
        if state.open_24h <= 0 or state.last_price <= 0:
            return 0.0

        pct_24h = self._pct_change(state.open_24h, state.last_price)
        momentum = self._recent_momentum_pct(state, now)
        quote_volume = state.quote_volume_24h

        if quote_volume < MIN_24H_QUOTE_VOLUME:
            return 0.0
        if state.last_price < MIN_PRICE:
            return 0.0

        volume_score = min(30.0, max(0.0, math.log10(max(quote_volume, 1.0)) * 4.0))
        pct_score = max(0.0, min(35.0, pct_24h * 1.2))
        momentum_score = max(0.0, min(35.0, momentum * 4.0))

        return round(volume_score + pct_score + momentum_score, 2)

    def _rebalance_hot_pool(self) -> Tuple[Set[str], Set[str]]:
        now = time.time()

        candidates: List[MiniTickerState] = []
        for symbol, state in self.market.items():
            if symbol not in self.universe.allowed_symbols:
                continue
            state.hot_score = self._score_symbol(state, now)
            if state.hot_score > 0:
                candidates.append(state)

        candidates.sort(key=lambda x: x.hot_score, reverse=True)
        next_hot = {x.symbol for x in candidates[:TOP_N_HOT]}

        added = next_hot - self.hot_symbols
        removed = self.hot_symbols - next_hot

        for symbol, state in self.market.items():
            state.is_hot = symbol in next_hot

        self.hot_symbols = next_hot
        self.runtime.hot_pool_size = len(self.hot_symbols)
        return added, removed

    def _get_trade_signal(self, symbol: str) -> Tuple[str, str]:
        state = self.market.get(symbol)
        if not state:
            return "观望", "缺少市场数据"

        momentum = self._recent_momentum_pct(state, time.time())
        pct_24h = self._pct_change(state.open_24h, state.last_price)
        spread_pct = self._get_spread_pct(symbol)

        if state.hot_score >= BUY_SCORE_THRESHOLD and momentum >= BUY_MOMENTUM_THRESHOLD and spread_pct <= BUY_MAX_SPREAD_PCT:
            return "买入", (
                f"热度较高（{state.hot_score}），"
                f"短线动量强（{momentum:.2f}%），"
                f"买卖价差正常（{spread_pct:.3f}%）"
            )

        if momentum <= SELL_MOMENTUM_THRESHOLD:
            return "卖出", f"短线动量转弱（{momentum:.2f}%）"

        if spread_pct >= SPREAD_ALERT_PCT and spread_pct > 0:
            return "卖出", f"买卖价差过大（{spread_pct:.3f}%），流动性风险上升"

        if state.hot_score >= WEAK_BUY_SCORE_THRESHOLD and momentum >= WEAK_BUY_MOMENTUM_THRESHOLD and pct_24h > 0:
            return "买入", (
                f"热度偏强（{state.hot_score}），"
                f"短线继续上行（{momentum:.2f}%）"
            )

        return "观望", (
            f"当前信号不够一致：热度={state.hot_score}，"
            f"短线动量={momentum:.2f}%"
        )

    # -------------------------
    # 提醒
    # -------------------------

    async def _maybe_alert(self, key: str, text: str) -> None:
        now = time.time()
        if now - self.last_alert_at[key] < ALERT_COOLDOWN_SEC:
            return
        self.last_alert_at[key] = now
        logging.warning(text.replace("\n", " | "))
        await self.notifier.send(text)

    async def _alert_hot_entry(self, symbol: str) -> None:
        state = self.market.get(symbol)
        if not state:
            return

        pct_24h = self._pct_change(state.open_24h, state.last_price)
        momentum = self._recent_momentum_pct(state, time.time())

        if state.hot_score < HOT_ALERT_THRESHOLD:
            return

        signal_text, signal_reason = self._get_trade_signal(symbol)

        msg = (
            f"【热点币预警】\n"
            f"币种：{symbol}\n"
            f"操作建议：{signal_text}\n"
            f"原因：{signal_reason}\n\n"
            f"热度分数：{state.hot_score}\n"
            f"24小时涨幅：{pct_24h:.2f}%\n"
            f"24小时成交额：{state.quote_volume_24h:,.0f} {QUOTE_ASSET}\n"
            f"{MOMENTUM_WINDOW_SEC // 60}分钟动量：{momentum:.2f}%\n"
            f"最新价格：{state.last_price}"
        )

        await self._maybe_alert(f"hot:{symbol}", msg)

    async def _alert_momentum(self, symbol: str) -> None:
        state = self.market.get(symbol)
        if not state or not state.is_hot:
            return

        momentum = self._recent_momentum_pct(state, time.time())
        if abs(momentum) < MOMENTUM_ALERT_PCT:
            return

        direction = "快速拉升" if momentum > 0 else "快速回落"
        signal_text, signal_reason = self._get_trade_signal(symbol)

        msg = (
            f"【短线异动预警】\n"
            f"币种：{symbol}\n"
            f"异动方向：{direction}\n"
            f"操作建议：{signal_text}\n"
            f"原因：{signal_reason}\n\n"
            f"{MOMENTUM_WINDOW_SEC // 60}分钟涨跌幅：{momentum:.2f}%\n"
            f"热度分数：{state.hot_score}\n"
            f"最新价格：{state.last_price}"
        )

        await self._maybe_alert(f"mom:{symbol}", msg)

    async def _alert_spread(self, symbol: str) -> None:
        if symbol not in self.hot_symbols:
            return

        bid = self.book[symbol].bid
        ask = self.book[symbol].ask
        if not bid or not ask or bid <= 0 or ask <= 0:
            return

        spread_pct = self._get_spread_pct(symbol)
        if spread_pct >= SPREAD_ALERT_PCT:
            msg = (
                f"【流动性风险预警】\n"
                f"币种：{symbol}\n"
                f"操作建议：卖出 / 观望\n"
                f"原因：买卖价差过大，可能滑点增加\n\n"
                f"买一价：{bid}\n"
                f"卖一价：{ask}\n"
                f"价差：{spread_pct:.3f}%"
            )
            await self._maybe_alert(f"spread:{symbol}", msg)

    # -------------------------
    # 健康检查
    # -------------------------

    async def _health(self, request: web.Request) -> web.Response:
        now = time.time()
        data = {
            "ok": True,
            "connected": self.runtime.connected,
            "ws_url": self.runtime.ws_url,
            "uptime_sec": round(now - self.runtime.started_at, 2),
            "last_message_at": self.runtime.last_message_at,
            "reconnect_count": self.runtime.reconnect_count,
            "universe_size": self.runtime.universe_size,
            "hot_pool_size": self.runtime.hot_pool_size,
            "last_error": self.runtime.last_error,
            "hot_symbols": sorted(list(self.hot_symbols))[:50],
        }
        return web.json_response(data)

    async def _root(self, request: web.Request) -> web.Response:
        return web.Response(text="altcoin scanner is running")

    async def _start_health_server(self) -> None:
        app = web.Application()
        app.router.add_get("/", self._root)
        app.router.add_get("/health", self._health)

        self.health_runner = web.AppRunner(app)
        await self.health_runner.setup()

        site = web.TCPSite(self.health_runner, "0.0.0.0", PORT)
        await site.start()
        logging.info("健康检查服务已启动: 0.0.0.0:%s", PORT)

    # -------------------------
    # WebSocket 控制
    # -------------------------

    async def _subscribe_streams(self, params: List[str]) -> None:
        if not params or self.ws is None:
            return
        req = {
            "method": "SUBSCRIBE",
            "params": params,
            "id": int(time.time() * 1000) % 100000000,
        }
        await self.ws.send(json.dumps(req))
        logging.info("订阅 %s 个流", len(params))

    async def _unsubscribe_streams(self, params: List[str]) -> None:
        if not params or self.ws is None:
            return
        req = {
            "method": "UNSUBSCRIBE",
            "params": params,
            "id": int(time.time() * 1000) % 100000000,
        }
        await self.ws.send(json.dumps(req))
        logging.info("取消订阅 %s 个流", len(params))

    # -------------------------
    # 消息处理
    # -------------------------

    async def _handle_control_response(self, payload: dict) -> None:
        if "result" in payload:
            logging.info("WS 控制响应: %s", payload)

    async def _handle_mini_ticker_arr(self, arr: list) -> None:
        now = time.time()

        for item in arr:
            symbol = item.get("s", "").upper()
            if symbol not in self.universe.allowed_symbols:
                continue

            price = float(item.get("c", 0.0))
            open_24h = float(item.get("o", 0.0))
            high_24h = float(item.get("h", 0.0))
            low_24h = float(item.get("l", 0.0))
            base_volume = float(item.get("v", 0.0))
            quote_volume = float(item.get("q", 0.0))

            st = self.market.get(symbol)
            if st is None:
                st = MiniTickerState(symbol=symbol)
                self.market[symbol] = st

            st.last_price = price
            st.open_24h = open_24h
            st.high_24h = high_24h
            st.low_24h = low_24h
            st.base_volume_24h = base_volume
            st.quote_volume_24h = quote_volume
            st.last_update_ts = now
            st.recent_prices.append((now, price))

        if now - self._last_hot_rebalance >= HOT_REBALANCE_SEC:
            self._last_hot_rebalance = now
            added, removed = self._rebalance_hot_pool()

            unsubscribe_list = [f"{s.lower()}@bookTicker" for s in sorted(removed)]
            subscribe_list = [f"{s.lower()}@bookTicker" for s in sorted(added)]

            if unsubscribe_list:
                await self._unsubscribe_streams(unsubscribe_list)
            if subscribe_list:
                await self._subscribe_streams(subscribe_list)

            for symbol in sorted(added):
                await self._alert_hot_entry(symbol)

            for symbol in list(self.hot_symbols):
                await self._alert_momentum(symbol)

    async def _handle_book_ticker(self, data: dict) -> None:
        symbol = data.get("s", "").upper()
        if symbol not in self.hot_symbols:
            return

        self.book[symbol].bid = float(data.get("b", 0.0))
        self.book[symbol].ask = float(data.get("a", 0.0))
        self.book[symbol].last_update_ts = time.time()

        await self._alert_spread(symbol)

    async def _handle_message(self, raw: str) -> None:
        self.runtime.last_message_at = time.time()

        payload = json.loads(raw)

        if "result" in payload and "id" in payload:
            await self._handle_control_response(payload)
            return

        stream = payload.get("stream", "")
        data = payload.get("data")

        if stream == DISCOVERY_STREAM:
            if isinstance(data, list):
                await self._handle_mini_ticker_arr(data)
            return

        if stream.endswith("@bookTicker"):
            await self._handle_book_ticker(data)
            return

    # -------------------------
    # 定时刷新 universe
    # -------------------------

    async def _periodic_universe_refresh(self) -> None:
        now = time.time()
        if now - self._last_universe_refresh < UNIVERSE_REFRESH_SEC:
            return

        self._last_universe_refresh = now
        await self.universe.refresh()
        self.runtime.universe_size = len(self.universe.allowed_symbols)

        invalid_hot = {s for s in self.hot_symbols if s not in self.universe.allowed_symbols}
        if invalid_hot:
            await self._unsubscribe_streams([f"{s.lower()}@bookTicker" for s in sorted(invalid_hot)])
            self.hot_symbols -= invalid_hot
            self.runtime.hot_pool_size = len(self.hot_symbols)

    # -------------------------
    # 启动 / 关闭
    # -------------------------

    async def start(self) -> None:
        await self.universe.start()
        await self.universe.refresh()
        self.runtime.universe_size = len(self.universe.allowed_symbols)

        await self.notifier.start()

        if ENABLE_HEALTHCHECK:
            await self._start_health_server()

        await self.notifier.send(
            "【系统启动成功】\n"
            "山寨币全市场扫描已启动\n"
            f"计价币种：{QUOTE_ASSET}\n"
            f"热点池数量：{TOP_N_HOT}\n"
            f"已屏蔽主流币数量：{len(EXCLUDED_BASE_ASSETS)}"
        )

        await self._run()

    async def close(self) -> None:
        self.runtime.connected = False
        await self.universe.close()
        await self.notifier.close()

        if self.health_runner is not None:
            await self.health_runner.cleanup()
            self.health_runner = None

    # -------------------------
    # 主循环
    # -------------------------

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
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


async def main() -> None:
    setup_logging()
    scanner = AltcoinScanner()
    loop = asyncio.get_running_loop()

    def _shutdown() -> None:
        logging.info("收到停止信号")
        scanner.stop()

    for sig_name in ("SIGINT", "SIGTERM"):
        if hasattr(signal, sig_name):
            loop.add_signal_handler(getattr(signal, sig_name), _shutdown)

    try:
        await scanner.start()
    finally:
        await scanner.close()


if __name__ == "__main__":
    asyncio.run(main())
