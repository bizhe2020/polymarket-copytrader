from __future__ import annotations

import json
import threading
import time
from typing import Dict, Iterable, Optional, Set

from .models import OrderBook, PriceLevel


MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


class NullMarketStream:
    available = False

    def __init__(self, reason: str = "disabled") -> None:
        self.reason = reason

    def ensure_asset(self, asset_id: str) -> None:
        return None

    def wait_for_book(self, asset_id: str, timeout_seconds: float) -> OrderBook | None:
        return None

    def get_order_book(self, asset_id: str) -> OrderBook | None:
        return None

    def get_last_price(self, asset_id: str) -> float | None:
        return None

    def close(self) -> None:
        return None


class PolymarketMarketStream:
    available = True

    def __init__(
        self,
        url: str = MARKET_WS_URL,
        connect_timeout_seconds: float = 5.0,
        ping_interval_seconds: float = 15.0,
        receive_timeout_seconds: float = 0.25,
    ) -> None:
        try:
            import websocket  # type: ignore
        except ImportError as exc:
            raise RuntimeError("market websocket requires websocket-client") from exc

        self._websocket = websocket
        self.url = url
        self.connect_timeout_seconds = connect_timeout_seconds
        self.ping_interval_seconds = ping_interval_seconds
        self.receive_timeout_seconds = receive_timeout_seconds
        self._books: Dict[str, OrderBook] = {}
        self._last_prices: Dict[str, float] = {}
        self._desired_assets: Set[str] = set()
        self._subscribed_assets: Set[str] = set()
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def ensure_asset(self, asset_id: str) -> None:
        normalized = str(asset_id)
        if not normalized:
            return
        with self._lock:
            self._desired_assets.add(normalized)
            should_start = self._thread is None
        if should_start:
            self._start()

    def wait_for_book(self, asset_id: str, timeout_seconds: float) -> OrderBook | None:
        deadline = time.time() + max(timeout_seconds, 0.0)
        while time.time() < deadline:
            book = self.get_order_book(asset_id)
            if book is not None:
                return book
            time.sleep(0.02)
        return self.get_order_book(asset_id)

    def get_order_book(self, asset_id: str) -> OrderBook | None:
        with self._lock:
            return self._books.get(str(asset_id))

    def get_last_price(self, asset_id: str) -> float | None:
        with self._lock:
            book = self._books.get(str(asset_id))
            if book is not None and book.last_trade_price > 0:
                return book.last_trade_price
            return self._last_prices.get(str(asset_id))

    def close(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)

    def _start(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            self._thread = threading.Thread(target=self._run, name="polymarket-market-ws", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                ws = self._websocket.create_connection(
                    self.url,
                    timeout=self.connect_timeout_seconds,
                    enable_multithread=False,
                )
                ws.settimeout(self.receive_timeout_seconds)
                self._subscribed_assets = set()
                last_ping_at = time.time()
                while not self._stop_event.is_set():
                    self._flush_subscriptions(ws)
                    if time.time() - last_ping_at >= self.ping_interval_seconds:
                        try:
                            ws.ping("ping")
                        except Exception:
                            break
                        last_ping_at = time.time()
                    try:
                        raw_message = ws.recv()
                    except self._websocket.WebSocketTimeoutException:
                        continue
                    except Exception:
                        break
                    self._handle_message(raw_message)
            except Exception:
                time.sleep(1.0)
            finally:
                try:
                    ws.close()  # type: ignore[name-defined]
                except Exception:
                    pass
        with self._lock:
            self._thread = None

    def _flush_subscriptions(self, ws: object) -> None:
        with self._lock:
            pending = sorted(self._desired_assets - self._subscribed_assets)
            if not pending:
                return
            self._subscribed_assets.update(pending)
        payload = {
            "assets_ids": pending,
            "type": "market",
            "custom_feature_enabled": True,
        }
        ws.send(json.dumps(payload))

    def _handle_message(self, raw_message: object) -> None:
        if not isinstance(raw_message, str) or raw_message in {"PONG", "PING"}:
            return
        try:
            payload = json.loads(raw_message)
        except json.JSONDecodeError:
            return
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self._handle_event(item)
            return
        if isinstance(payload, dict):
            self._handle_event(payload)

    def _handle_event(self, event: Dict[str, object]) -> None:
        event_type = str(event.get("event_type") or "")
        if event_type == "book":
            book = self._parse_book(event)
            with self._lock:
                self._books[book.asset_id] = book
                if book.last_trade_price > 0:
                    self._last_prices[book.asset_id] = book.last_trade_price
            return
        if event_type == "best_bid_ask":
            self._merge_best_bid_ask(event)
            return
        if event_type == "last_trade_price":
            asset_id = str(event.get("asset_id") or "")
            if not asset_id:
                return
            price = _safe_float(event.get("price"))
            if price is None:
                return
            with self._lock:
                self._last_prices[asset_id] = price
                book = self._books.get(asset_id)
                if book is not None:
                    book.last_trade_price = price
            return
        if event_type == "price_change":
            self._merge_price_changes(event)

    def _parse_book(self, event: Dict[str, object]) -> OrderBook:
        asset_id = str(event.get("asset_id") or "")
        market = str(event.get("market") or "")
        timestamp = str(event.get("timestamp") or "")
        bids = _parse_levels(event.get("bids"))
        asks = _parse_levels(event.get("asks"))
        return OrderBook(
            market=market,
            asset_id=asset_id,
            timestamp=timestamp,
            bids=bids,
            asks=asks,
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=self._last_prices.get(asset_id, 0.0),
        )

    def _merge_best_bid_ask(self, event: Dict[str, object]) -> None:
        asset_id = str(event.get("asset_id") or "")
        if not asset_id:
            return
        best_bid = _safe_float(event.get("best_bid"))
        best_ask = _safe_float(event.get("best_ask"))
        timestamp = str(event.get("timestamp") or "")
        market = str(event.get("market") or "")
        with self._lock:
            book = self._books.get(asset_id)
            if book is None:
                book = OrderBook(
                    market=market,
                    asset_id=asset_id,
                    timestamp=timestamp,
                    bids=[],
                    asks=[],
                    min_order_size=1.0,
                    tick_size=0.01,
                    neg_risk=False,
                    last_trade_price=self._last_prices.get(asset_id, 0.0),
                )
                self._books[asset_id] = book
            book.timestamp = timestamp or book.timestamp
            if best_bid is not None and best_bid > 0:
                book.bids = [PriceLevel(price=best_bid, size=book.bids[0].size if book.bids else 1.0)]
            if best_ask is not None and best_ask > 0:
                book.asks = [PriceLevel(price=best_ask, size=book.asks[0].size if book.asks else 1.0)]

    def _merge_price_changes(self, event: Dict[str, object]) -> None:
        changes = event.get("price_changes")
        if not isinstance(changes, list):
            return
        timestamp = str(event.get("timestamp") or "")
        market = str(event.get("market") or "")
        with self._lock:
            for raw_change in changes:
                if not isinstance(raw_change, dict):
                    continue
                asset_id = str(raw_change.get("asset_id") or "")
                if not asset_id:
                    continue
                side = str(raw_change.get("side") or "").upper()
                price = _safe_float(raw_change.get("price"))
                size = _safe_float(raw_change.get("size"))
                if price is None or size is None:
                    continue
                book = self._books.get(asset_id)
                if book is None:
                    book = OrderBook(
                        market=market,
                        asset_id=asset_id,
                        timestamp=timestamp,
                        bids=[],
                        asks=[],
                        min_order_size=1.0,
                        tick_size=0.01,
                        neg_risk=False,
                        last_trade_price=self._last_prices.get(asset_id, 0.0),
                    )
                    self._books[asset_id] = book
                book.timestamp = timestamp or book.timestamp
                levels = book.bids if side == "BUY" else book.asks
                _upsert_level(levels, side=side, price=price, size=size)
                if raw_change.get("best_bid") is not None and side == "BUY":
                    best_bid = _safe_float(raw_change.get("best_bid"))
                    if best_bid is not None and best_bid > 0:
                        if book.bids:
                            book.bids[0] = PriceLevel(price=best_bid, size=book.bids[0].size)
                        else:
                            book.bids = [PriceLevel(price=best_bid, size=max(size, 1.0))]
                if raw_change.get("best_ask") is not None and side == "SELL":
                    best_ask = _safe_float(raw_change.get("best_ask"))
                    if best_ask is not None and best_ask > 0:
                        if book.asks:
                            book.asks[0] = PriceLevel(price=best_ask, size=book.asks[0].size)
                        else:
                            book.asks = [PriceLevel(price=best_ask, size=max(size, 1.0))]


def build_market_stream() -> PolymarketMarketStream | NullMarketStream:
    try:
        return PolymarketMarketStream()
    except RuntimeError as exc:
        return NullMarketStream(reason=str(exc))


def _parse_levels(raw_levels: object) -> list[PriceLevel]:
    if not isinstance(raw_levels, Iterable):
        return []
    levels: list[PriceLevel] = []
    for level in raw_levels:
        if not isinstance(level, dict):
            continue
        price = _safe_float(level.get("price"))
        size = _safe_float(level.get("size"))
        if price is None or size is None:
            continue
        levels.append(PriceLevel(price=price, size=size))
    reverse = False
    if levels and len(levels) > 1:
        reverse = levels[0].price > levels[-1].price
    return sorted(levels, key=lambda item: item.price, reverse=reverse)


def _upsert_level(levels: list[PriceLevel], side: str, price: float, size: float) -> None:
    for index, level in enumerate(levels):
        if abs(level.price - price) < 1e-12:
            if size <= 1e-12:
                levels.pop(index)
            else:
                levels[index] = PriceLevel(price=price, size=size)
            break
    else:
        if size > 1e-12:
            levels.append(PriceLevel(price=price, size=size))
    levels.sort(key=lambda item: item.price, reverse=(side == "BUY"))


def _safe_float(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
