from __future__ import annotations

import json
import ast
import socket
import time
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .models import OrderBook, PriceHistoryPoint, PriceLevel, TradeActivity


class HttpJsonClient:
    def __init__(self, timeout_seconds: float, max_attempts: int = 3) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts

    def get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        if params:
            filtered = {
                key: value
                for key, value in params.items()
                if value is not None and value != []
            }
            if filtered:
                url = f"{url}?{urlencode(filtered, doseq=True)}"
        request = Request(url, headers={"User-Agent": "polymarket-copytrader/0.1"})
        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                with urlopen(request, timeout=self.timeout_seconds) as response:
                    payload = response.read().decode("utf-8")
                return json.loads(payload)
            except HTTPError as exc:
                body = exc.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"HTTP {exc.code} when requesting {url}: {body[:300]}"
                ) from exc
            except (URLError, TimeoutError, socket.timeout) as exc:
                last_error = exc
                if attempt >= self.max_attempts:
                    reason = exc.reason if isinstance(exc, URLError) else str(exc)
                    raise RuntimeError(f"Network error when requesting {url}: {reason}") from exc
                time.sleep(0.5 * attempt)
        raise RuntimeError(f"Network error when requesting {url}: {last_error}")


class PolymarketPublicApi:
    def __init__(self, timeout_seconds: float) -> None:
        self.http = HttpJsonClient(timeout_seconds=timeout_seconds)
        self.gamma_url = "https://gamma-api.polymarket.com"
        self.data_url = "https://data-api.polymarket.com"
        self.clob_url = "https://clob.polymarket.com"

    def public_search_profiles(self, query: str) -> List[Dict[str, Any]]:
        payload = self.http.get(
            f"{self.gamma_url}/public-search",
            params={
                "q": query,
                "search_profiles": "true",
                "search_tags": "false",
                "limit_per_type": 10,
                "optimized": "true",
                "cache": "false",
            },
        )
        return list(payload.get("profiles") or [])

    def public_search_events(
        self, query: str, limit_per_type: int = 10
    ) -> List[Dict[str, Any]]:
        payload = self.http.get(
            f"{self.gamma_url}/public-search",
            params={
                "q": query,
                "search_profiles": "false",
                "search_tags": "false",
                "limit_per_type": limit_per_type,
                "optimized": "true",
                "cache": "false",
            },
        )
        return list(payload.get("events") or [])

    def get_public_profile(self, wallet: str) -> Dict[str, Any]:
        return self.http.get(
            f"{self.gamma_url}/public-profile", params={"address": wallet}
        )

    def get_events(
        self,
        slug: str | None = None,
        limit: int | None = None,
        active: bool | None = None,
        closed: bool | None = None,
    ) -> List[Dict[str, Any]]:
        payload = self.http.get(
            f"{self.gamma_url}/events",
            params={
                "slug": slug,
                "limit": limit,
                "active": str(active).lower() if active is not None else None,
                "closed": str(closed).lower() if closed is not None else None,
            },
        )
        return list(payload or [])

    def get_markets(
        self,
        limit: int,
        active: bool = True,
        closed: bool = False,
        offset: int | None = None,
    ) -> List[Dict[str, Any]]:
        payload = self.http.get(
            f"{self.gamma_url}/markets",
            params={
                "limit": limit,
                "offset": offset,
                "active": str(active).lower(),
                "closed": str(closed).lower(),
            },
        )
        return list(payload or [])

    def get_activity(
        self,
        wallet: str,
        limit: int,
        start_ms: Optional[int] = None,
        side: Optional[str] = None,
        end_ms: Optional[int] = None,
        offset: Optional[int] = None,
        sort_direction: str = "ASC",
    ) -> List[TradeActivity]:
        payload = self.http.get(
            f"{self.data_url}/activity",
            params={
                "user": wallet,
                "limit": limit,
                "type": "TRADE",
                "side": side,
                "start": start_ms,
                "end": end_ms,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": sort_direction,
            },
        )
        trades: List[TradeActivity] = []
        for item in payload:
            try:
                trades.append(self._parse_trade_activity(item))
            except (KeyError, TypeError, ValueError):
                continue
        return trades

    def get_trades(
        self, wallet: str, limit: int, side: Optional[str] = None
    ) -> List[TradeActivity]:
        payload = self.http.get(
            f"{self.data_url}/trades",
            params={"user": wallet, "limit": limit, "side": side, "takerOnly": "true"},
        )
        trades: List[TradeActivity] = []
        for item in payload:
            try:
                trades.append(self._parse_trade_trade(item))
            except (KeyError, TypeError, ValueError):
                continue
        return trades

    def get_order_book(self, asset_id: str) -> OrderBook:
        payload = self.http.get(f"{self.clob_url}/book", params={"token_id": asset_id})
        return OrderBook(
            market=str(payload["market"]),
            asset_id=str(payload["asset_id"]),
            timestamp=str(payload["timestamp"]),
            bids=self._parse_levels(payload.get("bids", [])),
            asks=self._parse_levels(payload.get("asks", [])),
            min_order_size=float(payload["min_order_size"]),
            tick_size=float(payload["tick_size"]),
            neg_risk=bool(payload["neg_risk"]),
            last_trade_price=float(payload["last_trade_price"]),
        )

    def get_price_history(
        self, asset_id: str, start_ts: int, end_ts: int, fidelity_minutes: int
    ) -> List[PriceHistoryPoint]:
        payload = self.http.get(
            f"{self.clob_url}/prices-history",
            params={
                "market": asset_id,
                "startTs": start_ts,
                "endTs": end_ts,
                "fidelity": fidelity_minutes,
            },
        )
        history = payload.get("history") if isinstance(payload, dict) else payload
        points: List[PriceHistoryPoint] = []
        for item in history or []:
            timestamp = int(
                item.get("t")
                or item.get("timestamp")
                or item.get("time")
                or item.get("ts")
            )
            price = float(item.get("p") or item.get("price") or item.get("value"))
            points.append(PriceHistoryPoint(timestamp_seconds=timestamp, price=price))
        return sorted(points, key=lambda point: point.timestamp_seconds)

    @staticmethod
    def _parse_levels(raw_levels: Iterable[Dict[str, Any]]) -> List[PriceLevel]:
        return [
            PriceLevel(price=float(level["price"]), size=float(level["size"]))
            for level in raw_levels
        ]

    @staticmethod
    def _parse_trade_activity(item: Dict[str, Any]) -> TradeActivity:
        return TradeActivity(
            proxy_wallet=str(item["proxyWallet"]),
            timestamp_ms=int(item["timestamp"]),
            condition_id=str(item["conditionId"]),
            activity_type=str(item["type"]),
            size=float(item["size"]),
            usdc_size=float(item["usdcSize"]),
            transaction_hash=str(item.get("transactionHash") or ""),
            price=float(item["price"]),
            asset=str(item["asset"]),
            side=str(item["side"]),
            outcome_index=int(item["outcomeIndex"]),
            title=str(item["title"]),
            slug=str(item["slug"]),
            event_slug=str(item["eventSlug"]),
            outcome=str(item["outcome"]),
            name=str(item.get("name") or ""),
            pseudonym=str(item.get("pseudonym") or ""),
        )

    @staticmethod
    def _parse_trade_trade(item: Dict[str, Any]) -> TradeActivity:
        return TradeActivity(
            proxy_wallet=str(item["proxyWallet"]),
            timestamp_ms=int(item["timestamp"]),
            condition_id=str(item["conditionId"]),
            activity_type="TRADE",
            size=float(item["size"]),
            usdc_size=float(item["size"]) * float(item["price"]),
            transaction_hash=str(item.get("transactionHash") or ""),
            price=float(item["price"]),
            asset=str(item["asset"]),
            side=str(item["side"]),
            outcome_index=int(item["outcomeIndex"]),
            title=str(item["title"]),
            slug=str(item["slug"]),
            event_slug=str(item["eventSlug"]),
            outcome=str(item["outcome"]),
            name=str(item.get("name") or ""),
            pseudonym=str(item.get("pseudonym") or ""),
        )


def extract_market_token_ids(market: Dict[str, Any]) -> List[str]:
    clob_token_ids = market.get("clobTokenIds")
    if isinstance(clob_token_ids, list):
        return [str(token_id) for token_id in clob_token_ids if str(token_id)]
    if isinstance(clob_token_ids, str):
        parsed = _parse_string_array(clob_token_ids)
        if parsed:
            return parsed

    tokens = market.get("tokens")
    if isinstance(tokens, list):
        token_ids: List[str] = []
        for token in tokens:
            if not isinstance(token, dict):
                continue
            token_id = token.get("token_id") or token.get("tokenId") or token.get("id")
            if token_id:
                token_ids.append(str(token_id))
        return token_ids
    return []


def extract_market_tokens_with_outcomes(market: Dict[str, Any]) -> Dict[str, str]:
    token_ids = extract_market_token_ids(market)
    if not token_ids:
        return {}

    outcomes_raw = (
        market.get("outcomes")
        or market.get("outcomeNames")
        or market.get("outcome_names")
    )
    outcome_names: List[str] = []
    if isinstance(outcomes_raw, list):
        outcome_names = [str(item) for item in outcomes_raw if str(item)]
    elif isinstance(outcomes_raw, str):
        outcome_names = _parse_string_array(outcomes_raw)

    if len(outcome_names) == len(token_ids):
        return {
            str(outcome_names[index]): str(token_ids[index])
            for index in range(len(token_ids))
        }

    tokens = market.get("tokens")
    if isinstance(tokens, list):
        mapped: Dict[str, str] = {}
        for token in tokens:
            if not isinstance(token, dict):
                continue
            outcome = (
                token.get("outcome")
                or token.get("name")
                or token.get("label")
                or token.get("title")
            )
            token_id = token.get("token_id") or token.get("tokenId") or token.get("id")
            if outcome and token_id:
                mapped[str(outcome)] = str(token_id)
        if mapped:
            return mapped
    return {}


def _parse_string_array(raw: str) -> List[str]:
    text = raw.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item)]
