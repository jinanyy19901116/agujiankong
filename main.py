import os
import json
import time
import asyncio
import logging
from dataclasses import dataclass
from collections import defaultdict, deque
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
import websockets
from dotenv import load_dotenv


load_dotenv()

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def bj_now_str() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")


def safe_float(v, default=0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


@dataclass
class BigTrade:
    ts: float
    symbol: str
    side: str           # "buy" or "sell"
    price: float
    qty: float
    amount: float


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, enabled: bool = True):
        self.token = token
        self.chat_id = chat_id
        self.enabled = enabled and bool(token) and bool(chat_id)
        self.session: aiohttp.ClientSession | None = None

    async def start(self):
        if self.session is None:
            timeout = aiohttp.ClientTimeout(total=15)
            self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        if self.session:
            await self.session.close()
            self.session = None

    async def send(self, text: str):
        if not self.enabled:
            logging.info("Telegram disabled, message=%s", text.replace("\n", " | "))
            return

        try:
            if self.session is None:
                await self.start()

            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": text,
                "disable_web_page_preview": True,
            }
            async with self.session.post(url, data=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logging.error("Telegram send failed: %s %s", resp.status, body)
        except Exception as e:
            logging.exception("Telegram send exception: %s", e)


class SignalBot:
    def __init__(self):
        self.ws_base = os.getenv(
            "BINANCE_WS_URL",
            "wss://fstream.binance.com/stream"
        )

        whitelist_raw = os.getenv(
            "TOKEN_WHITELIST",
            "SIGNUSDT,DOGEUSDT,KITEUSDT,HYPEUSDT,SIRENUSDT,PHAUSDT,POWERUSDT,"
            "SKYAIUSDT,BARDUSDT,QUSDT,UAIUSDT,HUSDT,ICXUSDT,ROBOUSDT,OGNUSDT,"
            "XAIUSDT,IPUSDT,XAGUSDT,GUSDT,ANKRUSDT,ANIMEUSDT,BANUSDT,GUNUSDT,"
            "ZROUSDT,CUSDT,LIGHTUSDT,CVCUSDT,AVAUSDT"
        )

        self.whitelist = {
            x.strip().upper()
            for x in whitelist_raw.split(",")
            if x.strip()
        }

        self.stream_symbols = [s.lower() for s in self.whitelist]

        self.big_order_threshold = env_float("BIG_ORDER_THRESHOLD", 50000.0)

        # 机器人过滤参数
        self.repeat_window_sec = env_int("REPEAT_WINDOW_SEC", 10)
        self.repeat_tolerance = env_float("REPEAT_TOLERANCE", 0.01)  # 1%
        self.repeat_min_count = env_int("REPEAT_MIN_COUNT", 4)

        self.hedge_window_sec = env_int("HEDGE_WINDOW_SEC", 5)
        self.hedge_min_count = env_int("HEDGE_MIN_COUNT", 3)
        self.hedge_min_total = env_float("HEDGE_MIN_TOTAL", 100000.0)
        self.hedge_diff_ratio = env_float("HEDGE_DIFF_RATIO", 0.20)

        self.price_move_check_sec = env_int("PRICE_MOVE_CHECK_SEC", 3)
        self.price_move_threshold = env_float("PRICE_MOVE_THRESHOLD", 0.0005)  # 0.05%

        self.cooldown_sec = env_int("ALERT_COOLDOWN_SEC", 8)
        self.enable_console = env_bool("ENABLE_CONSOLE_LOG", True)

        self.tele_notifier = TelegramNotifier(
            token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", "").strip(),
            enabled=env_bool("ENABLE_TELEGRAM", True),
        )

        # 运行时数据
        self.recent_big_trades: dict[str, deque[BigTrade]] = defaultdict(deque)
        self.pending_signals: deque[dict] = deque()
        self.last_prices: dict[str, float] = {}
        self.last_alert_ts: dict[str, float] = {}

        self._stop = False

    def log_config(self):
        logging.info("Arkham/大单过滤版启动")
        logging.info("北京时间: %s", bj_now_str())
        logging.info("TOKEN_WHITELIST=%s", ",".join(sorted(self.whitelist)))
        logging.info("TOKEN_WHITELIST数量=%s", len(self.whitelist))
        logging.info("BIG_ORDER_THRESHOLD=%s", self.big_order_threshold)
        logging.info("REPEAT_WINDOW_SEC=%s", self.repeat_window_sec)
        logging.info("REPEAT_TOLERANCE=%s", self.repeat_tolerance)
        logging.info("REPEAT_MIN_COUNT=%s", self.repeat_min_count)
        logging.info("HEDGE_WINDOW_SEC=%s", self.hedge_window_sec)
        logging.info("HEDGE_MIN_COUNT=%s", self.hedge_min_count)
        logging.info("HEDGE_MIN_TOTAL=%s", self.hedge_min_total)
        logging.info("HEDGE_DIFF_RATIO=%s", self.hedge_diff_ratio)
        logging.info("PRICE_MOVE_CHECK_SEC=%s", self.price_move_check_sec)
        logging.info("PRICE_MOVE_THRESHOLD=%s", self.price_move_threshold)
        logging.info("ALERT_COOLDOWN_SEC=%s", self.cooldown_sec)
        logging.info("ENABLE_TELEGRAM=%s", self.tele_notifier.enabled)

    def build_ws_url(self) -> str:
        streams = "/".join(f"{symbol}@aggTrade" for symbol in self.stream_symbols)
        return f"{self.ws_base}?streams={streams}"

    def cleanup_old_trades(self, symbol: str, now_ts: float):
        q = self.recent_big_trades[symbol]
        max_keep = max(self.repeat_window_sec, self.hedge_window_sec) + 2
        while q and now_ts - q[0].ts > max_keep:
            q.popleft()

    def is_repeating_bot_pattern(self, symbol: str, current_amount: float) -> bool:
        """
        10秒内，如果相近金额的大单重复 >= repeat_min_count，视为程序刷单/拆单。
        """
        q = self.recent_big_trades[symbol]
        similar = 0

        for t in q:
            if current_amount <= 0:
                continue
            diff = abs(t.amount - current_amount) / current_amount
            if diff <= self.repeat_tolerance:
                similar += 1

        return similar >= self.repeat_min_count

    def is_hedge_pattern(self, symbol: str, now_ts: float) -> bool:
        """
        短时间内大买和大卖都很多，且差值很小，像对冲/做市/套利。
        """
        q = self.recent_big_trades[symbol]
        buy_total = 0.0
        sell_total = 0.0
        count = 0

        for t in q:
            if now_ts - t.ts <= self.hedge_window_sec:
                count += 1
                if t.side == "buy":
                    buy_total += t.amount
                else:
                    sell_total += t.amount

        if count < self.hedge_min_count:
            return False

        if buy_total >= self.hedge_min_total and sell_total >= self.hedge_min_total:
            total = buy_total + sell_total
            if total <= 0:
                return False
            diff_ratio = abs(buy_total - sell_total) / total
            if diff_ratio < self.hedge_diff_ratio:
                return True

        return False

    def format_alert(self, symbol: str, side: str, amount: float, trade_price: float, current_price: float, move_ratio: float) -> str:
        side_cn = "🟢买单推动" if side == "buy" else "🔴卖单推动"
        move_pct = move_ratio * 100

        return (
            f"⚡ 大单报警\n"
            f"时间: {bj_now_str()}\n"
            f"币种: {symbol}\n"
            f"方向: {side_cn}\n"
            f"成交额: {amount:,.0f} USDT\n"
            f"成交价: {trade_price:.8f}\n"
            f"当前价: {current_price:.8f}\n"
            f"推动幅度: {move_pct:.4f}%\n"
            f"阈值: ≥ {self.big_order_threshold:,.0f} USDT"
        )

    async def send_alert_if_not_cooldown(self, symbol: str, text: str):
        now_ts = time.time()
        last_ts = self.last_alert_ts.get(symbol, 0.0)
        if now_ts - last_ts < self.cooldown_sec:
            logging.info("%s 冷却中，跳过报警", symbol)
            return

        self.last_alert_ts[symbol] = now_ts
        if self.enable_console:
            print("\n" + "=" * 70)
            print(text)
            print("=" * 70 + "\n")
        await self.tele_notifier.send(text)

    async def process_pending_signals_loop(self):
        """
        候选信号二次确认：
        大单后等待几秒，如果价格确实被推动，再报警。
        """
        while not self._stop:
            try:
                now_ts = time.time()
                remain = deque()

                while self.pending_signals:
                    s = self.pending_signals.popleft()
                    symbol = s["symbol"]

                    if now_ts - s["ts"] < self.price_move_check_sec:
                        remain.append(s)
                        continue

                    entry_price = s["price"]
                    current_price = self.last_prices.get(symbol, entry_price)
                    side = s["side"]
                    amount = s["amount"]

                    if entry_price <= 0:
                        continue

                    if side == "buy":
                        move_ratio = (current_price - entry_price) / entry_price
                        if move_ratio >= self.price_move_threshold:
                            text = self.format_alert(
                                symbol=symbol,
                                side=side,
                                amount=amount,
                                trade_price=entry_price,
                                current_price=current_price,
                                move_ratio=move_ratio,
                            )
                            await self.send_alert_if_not_cooldown(symbol, text)
                        else:
                            logging.info(
                                "%s 买单未推动价格，忽略。entry=%s current=%s move=%.6f",
                                symbol, entry_price, current_price, move_ratio
                            )
                    else:
                        move_ratio = (entry_price - current_price) / entry_price
                        if move_ratio >= self.price_move_threshold:
                            text = self.format_alert(
                                symbol=symbol,
                                side=side,
                                amount=amount,
                                trade_price=entry_price,
                                current_price=current_price,
                                move_ratio=move_ratio,
                            )
                            await self.send_alert_if_not_cooldown(symbol, text)
                        else:
                            logging.info(
                                "%s 卖单未推动价格，忽略。entry=%s current=%s move=%.6f",
                                symbol, entry_price, current_price, move_ratio
                            )

                self.pending_signals = remain
                await asyncio.sleep(1)
            except Exception as e:
                logging.exception("process_pending_signals_loop error: %s", e)
                await asyncio.sleep(1)

    async def handle_trade_message(self, raw: str):
        """
        Binance futures combined stream:
        {
          "stream":"dogeusdt@aggTrade",
          "data":{
             "e":"aggTrade",
             "E":123456789,
             "s":"DOGEUSDT",
             "a":5933014,
             "p":"0.10450",
             "q":"500000",
             "f":100,
             "l":105,
             "T":123456785,
             "m":false
          }
        }
        """
        try:
            msg = json.loads(raw)
            data = msg.get("data", {})
            if not data:
                return

            symbol = str(data.get("s", "")).upper()
            if symbol not in self.whitelist:
                return

            price = safe_float(data.get("p"))
            qty = safe_float(data.get("q"))
            amount = price * qty

            if price <= 0 or qty <= 0:
                return

            self.last_prices[symbol] = price

            if amount < self.big_order_threshold:
                return

            now_ts = time.time()
            is_buyer_maker = bool(data.get("m", False))
            side = "sell" if is_buyer_maker else "buy"

            trade = BigTrade(
                ts=now_ts,
                symbol=symbol,
                side=side,
                price=price,
                qty=qty,
                amount=amount,
            )

            q = self.recent_big_trades[symbol]
            q.append(trade)
            self.cleanup_old_trades(symbol, now_ts)

            logging.info(
                "%s 候选大单 | %s | side=%s | amount=%.2f | price=%s | qty=%s",
                bj_now_str(), symbol, side, amount, price, qty
            )

            # 过滤1：重复金额刷单
            if self.is_repeating_bot_pattern(symbol, amount):
                logging.info(
                    "%s 疑似重复金额程序单，过滤。amount=%.2f",
                    symbol, amount
                )
                return

            # 过滤2：对冲/套利/做市
            if self.is_hedge_pattern(symbol, now_ts):
                logging.info("%s 疑似对冲/套利/做市，过滤。", symbol)
                return

            # 进入待确认队列
            self.pending_signals.append({
                "ts": now_ts,
                "symbol": symbol,
                "side": side,
                "price": price,
                "amount": amount,
            })

        except Exception as e:
            logging.exception("handle_trade_message error: %s", e)

    async def consume_ws(self):
        ws_url = self.build_ws_url()
        logging.info("WebSocket URL: %s", ws_url)

        backoff = 3
        max_backoff = 30

        while not self._stop:
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=10,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    logging.info("WebSocket connected")
                    backoff = 3

                    async for message in ws:
                        await self.handle_trade_message(message)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.exception("WebSocket error: %s", e)
                logging.info("将在 %s 秒后重连...", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def run(self):
        self.log_config()
        await self.tele_notifier.start()

        tasks = [
            asyncio.create_task(self.consume_ws(), name="consume_ws"),
            asyncio.create_task(self.process_pending_signals_loop(), name="pending_loop"),
        ]

        try:
            await asyncio.gather(*tasks)
        finally:
            self._stop = True
            for t in tasks:
                if not t.done():
                    t.cancel()
            await self.tele_notifier.close()


def setup_logging():
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )


async def main():
    setup_logging()
    logging.info("机器人启动")
    bot = SignalBot()
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("已手动停止")
