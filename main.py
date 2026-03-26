import asyncio
import json
import logging
import math
import os
import signal
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Set

import aiohttp
from aiohttp import web
import websockets
from websockets.exceptions import InvalidStatus


# =========================================================
# 配置
# =========================================================

FUTURES_REST_BASE = os.getenv("FUTURES_REST_BASE", "https://fapi.binance.com").rstrip("/")
FUTURES_WS_BASE = os.getenv("FUTURES_WS_BASE", "wss://fstream.binance.com/stream?streams=").strip()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
PORT = int(os.getenv("PORT", "8080"))
ENABLE_HEALTHCHECK = os.getenv("ENABLE_HEALTHCHECK", "true").lower() == "true"
RECONNECT_DELAY_SECONDS = int(os.getenv("RECONNECT_DELAY_SECONDS", "5"))
UNIVERSE_REFRESH_SEC = int(os.getenv("UNIVERSE_REFRESH_SEC", "1800"))
EMPTY_UNIVERSE_RETRY_SEC = int(os.getenv("EMPTY_UNIVERSE_RETRY_SEC", "60"))

TELEGRAM_BOT_TOKEN = "8457400925:AAFGn5R2VEaNqnxWMl_udv2tTeUnkMCK5FM"
TELEGRAM_CHAT_ID = "6308781694"

QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT").upper()
TOP_N_FUTURES = int(os.getenv("TOP_N_FUTURES", "50"))
MIN_24H_QUOTE_VOLUME = float(os.getenv("MIN_24H_QUOTE_VOLUME", "50000000"))
ALERT_COOLDOWN_SEC = int(os.getenv("ALERT_COOLDOWN_SEC", "60"))

# ==================== 即时大额市价单监控 ====================
FLOW_WINDOW_SEC = float(os.getenv("FLOW_WINDOW_SEC", "1.5"))
FLOW_MIN_TOTAL_NOTIONAL = float(os.getenv("FLOW_MIN_TOTAL_NOTIONAL", "120000"))
FLOW_MIN_DOMINANCE = float(os.getenv("FLOW_MIN_DOMINANCE", "0.75"))
FLOW_MIN_TRADE_COUNT = int(os.getenv("FLOW_MIN_TRADE_COUNT", "2"))
FLOW_SINGLE_LARGE_NOTIONAL = float(os.getenv("FLOW_SINGLE_LARGE_NOTIONAL", "50000"))
FLOW_BALANCE_NOISE_MAX_DOMINANCE = float(os.getenv("FLOW_BALANCE_NOISE_MAX_DOMINANCE", "0.58"))

# 单笔超大单，独立秒推
SINGLE_PRINT_NOTIONAL = float(os.getenv("SINGLE_PRINT_NOTIONAL", "100000"))

# ==================== 机器人/套利噪音过滤 ====================
BOT_MAX_MICRO_NOTIONAL = float(os.getenv("BOT_MAX_MICRO_NOTIONAL", "3000"))
BOT_ALTERNATING_COUNT = int(os.getenv("BOT_ALTERNATING_COUNT", "8"))
BOT_BURST_WINDOW_SEC = float(os.getenv("BOT_BURST_WINDOW_SEC", "2.0"))

# 固定手数/固定金额刷单
BOT_REPEAT_SAMPLE_SIZE = int(os.getenv("BOT_REPEAT_SAMPLE_SIZE", "8"))
BOT_REPEAT_QTY_UNIQUENESS_MAX = int(os.getenv("BOT_REPEAT_QTY_UNIQUENESS_MAX", "2"))
BOT_REPEAT_NOTIONAL_UNIQUENESS_MAX = int(os.getenv("BOT_REPEAT_NOTIONAL_UNIQUENESS_MAX", "2"))
BOT_REPEAT_WINDOW_SEC = float(os.getenv("BOT_REPEAT_WINDOW_SEC", "2.0"))

# 固定节奏机器人
BOT_TIMING_SAMPLE_SIZE = int(os.getenv("BOT_TIMING_SAMPLE_SIZE", "8"))
BOT_TIMING_STD_MAX = float(os.getenv("BOT_TIMING_STD_MAX", "0.035"))
BOT_TIMING_WINDOW_SEC = float(os.getenv("BOT_TIMING_WINDOW_SEC", "2.0"))


# =========================================================
# 数据结构
# =========================================================

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
# Futures Universe（纯合约）
# =========================================================

class FuturesUniverse:
    def __init__(self, futures_rest_base: str):
        self.futures_rest_base = futures_rest_base
        self.session: Optional[aiohttp.ClientSession] = None

        self.all_symbols: Set[str] = set()
        self.allowed_symbols: Set[str] = set()
        self.source: str = "unknown"  # api / env / missing_env / error

    async def start(self) -> None:
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=20)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self.session:
            await self.session.close()
            self.session = None

    def _load_futures_symbols_from_env(self) -> Set[str]:
        raw = os.getenv("FUTURES_SYMBOLS", "").strip()
        if not raw:
            return set()
        return {s.strip().upper() for s in raw.split(",") if s.strip()}

    async def _fetch_futures_exchange_info(self) -> Set[str]:
        if self.session is None:
            await self.start()

        env_symbols = self._load_futures_symbols_from_env()
        url = f"{self.futures_rest_base}/fapi/v1/exchangeInfo"

        try:
            async with self.session.get(url) as resp:
                text = await resp.text()

                if resp.status == 451:
                    if env_symbols:
                        self.source = "env"
                        logging.info("期货API返回451，已改用 FUTURES_SYMBOLS，共 %s 个币种", len(env_symbols))
                        return env_symbols

                    self.source = "missing_env"
                    logging.warning("期货API返回451，且未设置 FUTURES_SYMBOLS。纯合约模式下当前无法监控。")
                    return set()

                if resp.status != 200:
                    self.source = "error"
                    logging.warning("Futures exchangeInfo 非200: status=%s body=%s", resp.status, text[:300])

                    if env_symbols:
                        self.source = "env"
                        logging.info("已改用 FUTURES_SYMBOLS，共 %s 个币种", len(env_symbols))
                        return env_symbols

                    return set()

                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    self.source = "error"
                    logging.warning("Futures exchangeInfo 返回的不是JSON: %s", text[:300])

                    if env_symbols:
                        self.source = "env"
                        logging.info("已改用 FUTURES_SYMBOLS，共 %s 个币种", len(env_symbols))
                        return env_symbols

                    return set()

        except Exception:
            if env_symbols:
                self.source = "env"
                logging.exception("获取 Futures exchangeInfo 失败，已改用 FUTURES_SYMBOLS")
                return env_symbols

            self.source = "error"
            logging.exception("获取 Futures exchangeInfo 失败，且没有 FUTURES_SYMBOLS")
            return set()

        symbols: Set[str] = set()
        for item in data.get("symbols", []):
            if not isinstance(item, dict):
                continue

            symbol = item.get("symbol", "").upper()
            status = item.get("status", "")
            quote_asset = item.get("quoteAsset", "").upper()
            contract_type = item.get("contractType", "")

            if status != "TRADING":
                continue
            if quote_asset != QUOTE_ASSET:
                continue
            if contract_type != "PERPETUAL":
                continue

            symbols.add(symbol)

        self.source = "api"
        return symbols

    async def _fetch_futures_24h_tickers(self) -> List[dict]:
        if self.session is None:
            await self.start()

        url = f"{self.futures_rest_base}/fapi/v1/ticker/24hr"
        try:
            async with self.session.get(url) as resp:
                text = await resp.text()

                if resp.status != 200:
                    logging.warning("Futures 24hr 非200: status=%s body=%s", resp.status, text[:300])
                    return []

                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    logging.warning("Futures 24hr 返回的不是JSON: %s", text[:300])
                    return []

                return data if isinstance(data, list) else []
        except Exception:
            logging.exception("获取 Futures 24hr 数据失败")
            return []

    async def refresh(self) -> None:
        if self.session is None:
            await self.start()

        symbols = await self._fetch_futures_exchange_info()
        self.all_symbols = symbols

        if not symbols:
            self.allowed_symbols = set()
            logging.warning("纯合约模式：当前可监控合约为空，source=%s", self.source)
            return

        tickers = await self._fetch_futures_24h_tickers()
        if not tickers:
            self.allowed_symbols = set(sorted(symbols)[:TOP_N_FUTURES])
            logging.info(
                "纯合约交易池刷新完成：全部合约=%s，24h排行不可用，退化使用前%s个，最终=%s，来源=%s",
                len(symbols),
                TOP_N_FUTURES,
                len(self.allowed_symbols),
                self.source,
            )
            return

        ranked = []
        for item in tickers:
            if not isinstance(item, dict):
                continue

            symbol = item.get("symbol", "").upper()
            if symbol not in symbols:
                continue

            try:
                quote_volume = float(item.get("quoteVolume", 0.0) or 0.0)
            except (TypeError, ValueError):
                continue

            if quote_volume < MIN_24H_QUOTE_VOLUME:
                continue

            ranked.append((symbol, quote_volume))

        ranked.sort(key=lambda x: x[1], reverse=True)
        self.allowed_symbols = {s for s, _ in ranked[:TOP_N_FUTURES]}

        if not self.allowed_symbols:
            self.allowed_symbols = set(sorted(symbols)[:TOP_N_FUTURES])

        logging.info(
            "纯合约交易池刷新完成：全部合约=%s，24h达标=%s，最终订阅=%s，来源=%s",
            len(symbols),
            len(ranked),
            len(self.allowed_symbols),
            self.source,
        )


# =========================================================
# Scanner（纯合约即时主动成交）
# =========================================================

class FuturesOrderFlowScanner:
    def __init__(self):
        self.runtime = RuntimeStatus()
        self.universe = FuturesUniverse(FUTURES_REST_BASE)
        self.notifier = TelegramNotifier(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)

        self.trade_flow: Dict[str, Deque[AggTradeEvent]] = defaultdict(lambda: deque(maxlen=5000))
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

    def _get_trade_window(self, symbol: str, sec: float) -> List[AggTradeEvent]:
        now = time.time()
        dq = self.trade_flow[symbol]
        cutoff = now - max(sec, 10.0)

        while dq and dq[0].ts < cutoff:
            dq.popleft()

        return [x for x in dq if x.ts >= now - sec]

    def _is_alternating_micro_bot(self, events: List[AggTradeEvent]) -> bool:
        micro = [e for e in events if e.notional <= BOT_MAX_MICRO_NOTIONAL]
        if len(micro) < BOT_ALTERNATING_COUNT:
            return False

        recent = micro[-BOT_ALTERNATING_COUNT:]
        sides = [e.side for e in recent]
        alternating = sum(1 for i in range(1, len(sides)) if sides[i] != sides[i - 1])
        span = recent[-1].ts - recent[0].ts
        return alternating >= BOT_ALTERNATING_COUNT - 1 and span <= BOT_BURST_WINDOW_SEC

    def _is_repetitive_size_bot(self, events: List[AggTradeEvent]) -> bool:
        if len(events) < BOT_REPEAT_SAMPLE_SIZE:
            return False

        recent = events[-BOT_REPEAT_SAMPLE_SIZE:]
        span = recent[-1].ts - recent[0].ts
        if span > BOT_REPEAT_WINDOW_SEC:
            return False

        qty_keys = {round(e.qty, 6) for e in recent}
        notional_keys = {round(e.notional, 1) for e in recent}

        same_side_ratio = max(
            sum(1 for e in recent if e.side == "buy"),
            sum(1 for e in recent if e.side == "sell"),
        ) / len(recent)

        return (
            len(qty_keys) <= BOT_REPEAT_QTY_UNIQUENESS_MAX
            and len(notional_keys) <= BOT_REPEAT_NOTIONAL_UNIQUENESS_MAX
            and same_side_ratio < 0.95
        )

    def _is_uniform_timing_bot(self, events: List[AggTradeEvent]) -> bool:
        if len(events) < BOT_TIMING_SAMPLE_SIZE:
            return False

        recent = events[-BOT_TIMING_SAMPLE_SIZE:]
        span = recent[-1].ts - recent[0].ts
        if span > BOT_TIMING_WINDOW_SEC:
            return False

        intervals = []
        for i in range(1, len(recent)):
            dt = recent[i].ts - recent[i - 1].ts
            if dt > 0:
                intervals.append(dt)

        if len(intervals) < BOT_TIMING_SAMPLE_SIZE - 1:
            return False

        try:
            std = statistics.pstdev(intervals)
        except statistics.StatisticsError:
            return False

        mean_notional = sum(e.notional for e in recent) / len(recent)
        side_balance = abs(
            sum(1 for e in recent if e.side == "buy") - sum(1 for e in recent if e.side == "sell")
        ) / len(recent)

        return std <= BOT_TIMING_STD_MAX and mean_notional <= FLOW_SINGLE_LARGE_NOTIONAL and side_balance < 0.75

    def _is_bot_or_arb_noise(self, events: List[AggTradeEvent]) -> bool:
        if not events:
            return False

        if self._is_alternating_micro_bot(events):
            return True

        if self._is_repetitive_size_bot(events):
            return True

        if self._is_uniform_timing_bot(events):
            return True

        # 双边过于均衡，常见于对敲/套利/刷量噪音
        buy_notional = sum(e.notional for e in events if e.side == "buy")
        sell_notional = sum(e.notional for e in events if e.side == "sell")
        total_notional = buy_notional + sell_notional
        if total_notional <= 0:
            return True

        dominance = max(buy_notional, sell_notional) / total_notional
        if dominance <= FLOW_BALANCE_NOISE_MAX_DOMINANCE:
            return True

        return False

    def _detect_single_large_market_order(self, symbol: str, event: AggTradeEvent) -> Optional[dict]:
        if event.notional < SINGLE_PRINT_NOTIONAL:
            return None

        # 单笔超大单也要防机器人：如果最近窗口明显是机械噪音，就忽略
        recent = self._get_trade_window(symbol, 1.2)
        if self._is_bot_or_arb_noise(recent):
            return None

        return {
            "type": "single_buy" if event.side == "buy" else "single_sell",
            "price": event.price,
            "qty": event.qty,
            "notional": event.notional,
            "side": event.side,
        }

    def _detect_aggressive_cluster(self, symbol: str) -> Optional[dict]:
        events = self._get_trade_window(symbol, FLOW_WINDOW_SEC)

        if len(events) < FLOW_MIN_TRADE_COUNT:
            return None

        if self._is_bot_or_arb_noise(events):
            return None

        buy_notional = sum(e.notional for e in events if e.side == "buy")
        sell_notional = sum(e.notional for e in events if e.side == "sell")
        total_notional = buy_notional + sell_notional

        if total_notional < FLOW_MIN_TOTAL_NOTIONAL:
            return None

        dominant_side = "buy" if buy_notional >= sell_notional else "sell"
        dominant_notional = max(buy_notional, sell_notional)
        dominance = dominant_notional / total_notional if total_notional > 0 else 0.0

        if dominance < FLOW_MIN_DOMINANCE:
            return None

        max_single = max(e.notional for e in events)
        if max_single < FLOW_SINGLE_LARGE_NOTIONAL:
            return None

        start_price = events[0].price
        end_price = events[-1].price

        return {
            "type": "cluster_buy" if dominant_side == "buy" else "cluster_sell",
            "side": dominant_side,
            "total_notional": total_notional,
            "dominance": dominance,
            "max_single": max_single,
            "trade_count": len(events),
            "start_price": start_price,
            "end_price": end_price,
        }

    async def _alert_single_large_market_order(self, symbol: str, result: dict) -> None:
        if result["type"] == "single_buy":
            title = "【超大市价买单】"
            action = "关注上冲"
            desc = "出现单笔超大主动买盘"
        else:
            title = "【超大市价卖单】"
            action = "关注下砸"
            desc = "出现单笔超大主动卖盘"

        msg = (
            f"{title}\n"
            f"合约：{symbol}\n"
            f"即时判断：{action}\n"
            f"原因：{desc}\n\n"
            f"成交额：{result['notional']:,.0f} {QUOTE_ASSET}\n"
            f"成交数量：{result['qty']}\n"
            f"成交价格：{result['price']}"
        )
        await self._maybe_alert(f"single:{symbol}:{result['type']}", msg)

    async def _alert_aggressive_cluster(self, symbol: str, result: dict) -> None:
        if result["type"] == "cluster_buy":
            title = "【大额市价买盘涌入】"
            action = "关注拉升"
            desc = "短时内主动买盘主导，可能推动价格继续上冲"
        else:
            title = "【大额市价卖盘涌入】"
            action = "关注下跌"
            desc = "短时内主动卖盘主导，可能推动价格继续下压"

        msg = (
            f"{title}\n"
            f"合约：{symbol}\n"
            f"即时判断：{action}\n"
            f"原因：{desc}\n\n"
            f"窗口时长：{FLOW_WINDOW_SEC:.1f}s\n"
            f"总主动成交额：{result['total_notional']:,.0f} {QUOTE_ASSET}\n"
            f"主导占比：{result['dominance'] * 100:.1f}%\n"
            f"最大单笔：{result['max_single']:,.0f} {QUOTE_ASSET}\n"
            f"成交笔数：{result['trade_count']}\n"
            f"价格区间：{result['start_price']} -> {result['end_price']}"
        )
        await self._maybe_alert(f"cluster:{symbol}:{result['type']}", msg)

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
            "futures_source": self.universe.source,
            "last_error": self.runtime.last_error,
        })

    async def _root(self, request: web.Request) -> web.Response:
        return web.Response(text="futures aggressive scanner is running")

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

    async def _apply_active_symbols(self, next_set: Set[str]) -> None:
        added = next_set - self.active_symbols
        removed = self.active_symbols - next_set

        unsub = [f"{s.lower()}@aggTrade" for s in sorted(removed)]
        sub = [f"{s.lower()}@aggTrade" for s in sorted(added)]

        if unsub:
            await self._unsubscribe_streams(unsub)
        if sub:
            await self._subscribe_streams(sub)

        self.active_symbols = next_set
        self.runtime.subscribed_count = len(self.active_symbols)

    async def _refresh_active_symbols(self) -> None:
        await self.universe.refresh()
        self.runtime.universe_size = len(self.universe.allowed_symbols)
        await self._apply_active_symbols(set(sorted(self.universe.allowed_symbols)))

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

        # m=true => buyer is maker => taker sell（主动卖）
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

        single_result = self._detect_single_large_market_order(symbol, event)
        if single_result:
            await self._alert_single_large_market_order(symbol, single_result)

        cluster_result = self._detect_aggressive_cluster(symbol)
        if cluster_result:
            await self._alert_aggressive_cluster(symbol, cluster_result)

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

        if isinstance(stream, str) and stream.endswith("@aggTrade"):
            await self._handle_agg_trade(data)

    async def _periodic_universe_refresh(self) -> None:
        now = time.time()
        if now - self._last_universe_refresh < UNIVERSE_REFRESH_SEC:
            return

        self._last_universe_refresh = now
        await self._refresh_active_symbols()

    async def _idle_until_universe_available(self) -> None:
        while not self._stop_event.is_set() and not self.universe.allowed_symbols:
            logging.info("当前无可监控合约，%s 秒后重试刷新 Universe", EMPTY_UNIVERSE_RETRY_SEC)
            await asyncio.sleep(EMPTY_UNIVERSE_RETRY_SEC)
            await self.universe.refresh()
            self.runtime.universe_size = len(self.universe.allowed_symbols)
            self._last_universe_refresh = time.time()

    async def start(self) -> None:
        await self.universe.start()
        await self.notifier.start()

        await self.universe.refresh()
        self.runtime.universe_size = len(self.universe.allowed_symbols)
        self._last_universe_refresh = time.time()

        if ENABLE_HEALTHCHECK:
            await self._start_health_server()

        source_map = {
            "api": "Binance Futures API",
            "env": "环境变量 FUTURES_SYMBOLS",
            "missing_env": "未提供 FUTURES_SYMBOLS",
            "error": "Futures 获取失败",
        }
        source_text = source_map.get(self.universe.source, "未知")

        await self.notifier.send(
            "【系统启动成功】\n"
            "模式：纯合约即时主动成交监控\n"
            "仅推送：超大市价单 / 短时大额主动买卖簇\n"
            "已启用：刷单机器人 / 套利噪音过滤\n"
            f"当前合约数量：{len(self.universe.allowed_symbols)}\n"
            f"合约来源：{source_text}"
        )

        if not self.universe.allowed_symbols:
            await self.notifier.send(
                "【配置提醒】\n"
                "当前没有可监控合约。\n"
                "如果 Futures API 返回 451，请设置 FUTURES_SYMBOLS，"
                "或者更换可访问 Binance Futures 的网络/IP。"
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
        self.runtime.ws_url = FUTURES_WS_BASE

        while not self._stop_event.is_set():
            try:
                # 没有可监控合约时，不去无意义连接 WS
                if not self.universe.allowed_symbols:
                    self.runtime.connected = False
                    await self._idle_until_universe_available()
                    continue

                logging.info("正在连接 Futures WebSocket")

                async with websockets.connect(
                    FUTURES_WS_BASE + "btcusdt@aggTrade",
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_queue=1000,
                ) as ws:
                    self.ws = ws
                    self.runtime.connected = True
                    self.runtime.last_error = ""
                    logging.info("Futures WebSocket 已连接")

                    # 占位订阅去掉，然后直接应用当前已有 Universe，不再重复 refresh
                    await self._unsubscribe_streams(["btcusdt@aggTrade"])
                    await self._apply_active_symbols(set(sorted(self.universe.allowed_symbols)))

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
                logging.exception("Futures WebSocket 握手失败")
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                self.runtime.connected = False
                self.runtime.reconnect_count += 1
                self.runtime.last_error = repr(e)
                logging.exception("Futures WebSocket 连接中断")
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
    scanner = FuturesOrderFlowScanner()
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
