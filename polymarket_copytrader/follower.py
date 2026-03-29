from __future__ import annotations

import time
from dataclasses import asdict
from typing import Dict, Iterable, List, Optional

from .api import PolymarketPublicApi, extract_market_token_ids
from .config import load_config
from .execution import ExecutionClient, build_execution_client
from .market_ws import build_market_stream
from .models import AppConfig, FollowDecision, OrderBook, PriceLevel, StateSnapshot, TradeActivity
from .resolve import resolve_target_wallet
from .store import EventSink, StateStore
from .strategy import MirrorTradeStrategy


class CopyTraderApp:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.api = PolymarketPublicApi(timeout_seconds=config.runtime.request_timeout_seconds)
        self.store = StateStore(config.runtime.state_path)
        self.events = EventSink(config.runtime.event_log_path)
        self.state = self.store.load()
        self.strategy = MirrorTradeStrategy(config.strategy)
        self._execution: ExecutionClient | None = None
        self.market_stream = build_market_stream() if config.runtime.market_websocket_enabled else None
        self._event_assets_cache: Dict[str, List[str]] = {}
        self._hot_pool_assets_cache: Dict[str, List[str]] = {}

        if self.market_stream is None:
            self.events.write("market_ws_disabled", {"reason": "disabled_in_config"})
        elif not self.market_stream.available:
            self.events.write("market_ws_disabled", {"reason": self.market_stream.reason})
        else:
            self.events.write("market_ws_enabled", {"mode": "market_channel"})

    def resolve_target(self) -> str:
        wallet, details = resolve_target_wallet(
            self.api,
            self.config.target.profile,
            self.config.target.wallet,
        )
        self.state.target_wallet = wallet
        self.store.save(self.state)
        payload = {"wallet": wallet, "details": details}
        self.events.write("target_resolved", payload)
        return wallet

    def run(self, once: bool = False) -> None:
        wallet = self.state.target_wallet or self.resolve_target()
        try:
            while True:
                self._poll_once(wallet)
                if once:
                    return
                time.sleep(self.config.runtime.poll_interval_seconds)
        finally:
            if self.market_stream is not None:
                self.market_stream.close()

    def backfill(self, limit: int, output_path: str) -> int:
        wallet = self.state.target_wallet or self.resolve_target()
        trades = self.api.get_trades(wallet, limit=limit, side=None)
        sink = EventSink(output_path)
        count = 0
        for trade in trades:
            sink.write("historical_trade", asdict(trade))
            count += 1
        return count

    def replay(self, trades: Iterable[TradeActivity]) -> int:
        count = 0
        for trade in sorted(trades, key=lambda item: item.timestamp_ms):
            synthetic_book = self._build_synthetic_book(trade)
            self._handle_trade(trade, order_book=synthetic_book)
            count += 1
        self.store.save(self.state)
        return count

    def doctor(self) -> List[str]:
        checks = []
        wallet = self.state.target_wallet or self.config.target.wallet
        checks.append(f"execution_mode={self.config.execution.mode}")
        checks.append(f"configured_wallet={wallet or 'missing'}")
        checks.append(f"market_ws_enabled={bool(self.market_stream and self.market_stream.available)}")
        checks.append(f"state_path={self.config.runtime.state_path}")
        checks.append(f"event_log_path={self.config.runtime.event_log_path}")
        return checks

    @property
    def execution(self) -> ExecutionClient:
        if self._execution is None:
            self._execution = build_execution_client(self.config.execution, self.state)
        return self._execution

    def _poll_once(self, wallet: str) -> None:
        recent_start = self._start_cursor_ms()
        trades = self._fetch_candidate_trades(wallet, recent_start)
        trades = self._aggregate_trades(trades)
        related_assets = self._discover_trade_context_assets(trades)
        self._warm_market_stream_assets(
            core_assets=[trade.asset for trade in trades],
            related_assets=related_assets,
        )

        for trade in sorted(trades, key=self._trade_priority_key):
            self._handle_trade(trade)
        self.store.save(self.state)

    def _start_cursor_ms(self) -> int:
        if self.state.last_event_timestamp_ms > 0:
            last_seen = int(self.state.last_event_timestamp_ms)
            last_seen_seconds = (last_seen // 1000) if last_seen > 100_000_000_000 else last_seen
            return max(last_seen_seconds - self.config.runtime.requery_overlap_seconds, 0)
        now_seconds = int(time.time())
        return max(now_seconds - self.config.runtime.lookback_seconds_on_start, 0)

    def _fetch_candidate_trades(self, wallet: str, recent_start: int) -> List[TradeActivity]:
        fast_recent: List[TradeActivity] = []
        if self.state.last_event_timestamp_ms > 0:
            now_seconds = int(time.time())
            fast_window_seconds = max(
                int(self.config.runtime.requery_overlap_seconds) * 2,
                int(max(self.config.runtime.poll_interval_seconds * 3, 6)),
            )
            fast_start = max(recent_start, now_seconds - fast_window_seconds, 0)
            if fast_start > recent_start:
                fast_recent = self.api.get_activity(
                    wallet=wallet,
                    limit=max(min(self.config.runtime.activity_limit // 2, 50), 15),
                    start_ms=fast_start,
                    side=None,
                    sort_direction="DESC",
                )

        recent = self.api.get_activity(
            wallet=wallet,
            limit=self.config.runtime.activity_limit,
            start_ms=recent_start,
            side=None,
            sort_direction="DESC",
        )

        late_backfill: List[TradeActivity] = []
        if self.state.last_event_timestamp_ms > 0:
            backfill_seconds = max(self.config.runtime.requery_overlap_seconds * 4, 15)
            last_seen = int(self.state.last_event_timestamp_ms)
            last_seen_seconds = (last_seen // 1000) if last_seen > 100_000_000_000 else last_seen
            backfill_start = max(last_seen_seconds - backfill_seconds, 0)
            late_backfill = self.api.get_activity(
                wallet=wallet,
                limit=max(min(self.config.runtime.activity_limit // 2, 75), 25),
                start_ms=backfill_start,
                side=None,
                sort_direction="ASC",
            )

        self.events.write(
            "poll_window",
            {
                "recent_start": recent_start,
                "fast_recent_count": len(fast_recent),
                "recent_count": len(recent),
                "late_backfill_count": len(late_backfill),
            },
        )
        return fast_recent + recent + late_backfill

    def _handle_trade(self, trade: TradeActivity, order_book: OrderBook | None = None) -> None:
        if trade.dedupe_key in set(self.state.seen_keys):
            return

        stale_reason = self.strategy.stale_reason(trade)
        if stale_reason is not None:
            self.events.write("decision", FollowDecision(False, stale_reason, trade).to_dict())
            self.state.last_event_timestamp_ms = max(
                self.state.last_event_timestamp_ms,
                trade.timestamp_ms,
            )
            self.state.remember(trade.dedupe_key)
            return

        if order_book is not None:
            book = order_book
            self.events.write(
                "order_book_source",
                {"asset_id": trade.asset, "market_slug": trade.slug, "source": "provided"},
            )
        else:
            book, source = self._order_book_for_trade(trade)
            self.events.write(
                "order_book_source",
                {"asset_id": trade.asset, "market_slug": trade.slug, "source": source},
            )
        current_position_size = self.execution.current_position_size(trade.asset)
        reference_price = book.best_ask or book.best_bid or book.last_trade_price or trade.price
        current_exposure = current_position_size * float(reference_price)

        decision = self.strategy.decide(
            trade,
            book,
            current_exposure,
            current_position_size,
            current_timestamp_seconds=time.time(),
        )
        self.events.write("decision", decision.to_dict())

        if decision.should_follow:
            report = self.execution.place_follow_trade(
                decision,
                order_book=book,
                execution_policy=self.config.strategy.execution_policy,
            )
            self.events.write("execution", report.to_dict())

        self.state.last_event_timestamp_ms = max(
            self.state.last_event_timestamp_ms,
            trade.timestamp_ms,
        )
        self.state.remember(trade.dedupe_key)

    def _order_book_for_trade(self, trade: TradeActivity) -> tuple[OrderBook, str]:
        if self.market_stream is not None and self.market_stream.available:
            self.market_stream.ensure_asset(trade.asset)
            book = self.market_stream.wait_for_book(
                trade.asset,
                timeout_seconds=max(self.config.runtime.market_websocket_warmup_ms, 0) / 1000.0,
            )
            if book is not None:
                return book, "market_ws"
        return self.api.get_order_book(trade.asset), "rest_book"

    def _warm_market_stream_assets(
        self,
        core_assets: List[str],
        related_assets: List[str],
    ) -> None:
        if self.market_stream is None or not self.market_stream.available:
            return
        unique_core_assets = sorted({asset_id for asset_id in core_assets if asset_id})
        if not unique_core_assets:
            return
        unique_related_assets = sorted(
            {
                asset_id
                for asset_id in related_assets
                if asset_id and asset_id not in set(unique_core_assets)
            }
        )
        uncached_assets = [
            asset_id
            for asset_id in unique_core_assets
            if self.market_stream.get_order_book(asset_id) is None
        ]
        for asset_id in unique_core_assets + unique_related_assets:
            self.market_stream.ensure_asset(asset_id)
        wait_seconds = self._market_stream_wait_seconds(
            core_assets_count=len(uncached_assets),
            related_assets_count=len(unique_related_assets),
        )
        if not uncached_assets or wait_seconds <= 0:
            return
        deadline = time.time() + wait_seconds
        while time.time() < deadline:
            ready_count = sum(
                1 for asset_id in uncached_assets if self.market_stream.get_order_book(asset_id) is not None
            )
            if ready_count >= len(uncached_assets):
                break
            time.sleep(0.02)
        self.events.write(
            "market_ws_batch_warmup",
            {
                "core_assets_count": len(unique_core_assets),
                "related_assets_count": len(unique_related_assets),
                "uncached_core_assets_count": len(uncached_assets),
                "wait_ms": int(wait_seconds * 1000),
            },
        )

    def _market_stream_wait_seconds(
        self, core_assets_count: int, related_assets_count: int = 0
    ) -> float:
        base_seconds = max(self.config.runtime.market_websocket_warmup_ms, 0) / 1000.0
        if base_seconds <= 0:
            return 0.0
        multiplier = 1.0 + min(max(core_assets_count - 1, 0), 3) * 0.5
        if related_assets_count > 0:
            multiplier += 0.5
        return min(base_seconds * multiplier, 2.0)

    def _discover_trade_context_assets(self, trades: List[TradeActivity]) -> List[str]:
        assets: set[str] = set()
        for trade in trades:
            assets.update(self._assets_for_event_slug(trade.event_slug or trade.slug))
        prefixes = self._infer_hot_market_prefixes({"batch": trades})
        assets.update(self._hot_pool_assets_for_prefixes(prefixes))
        return sorted(asset_id for asset_id in assets if asset_id)

    def _assets_for_event_slug(self, event_slug: str) -> List[str]:
        normalized = str(event_slug or "").strip()
        if not normalized:
            return []
        cached = self._event_assets_cache.get(normalized)
        if cached is not None:
            return list(cached)
        assets: List[str] = []
        try:
            events = self.api.get_events(slug=normalized, active=True, closed=False)
        except Exception:
            self._event_assets_cache[normalized] = []
            return []
        for event in events:
            markets = event.get("markets") or []
            if not isinstance(markets, list):
                continue
            for market in markets:
                if not isinstance(market, dict):
                    continue
                assets.extend(extract_market_token_ids(market))
        deduped = sorted({asset_id for asset_id in assets if asset_id})
        self._event_assets_cache[normalized] = deduped
        return list(deduped)

    def _hot_pool_assets_for_prefixes(self, prefixes: List[str]) -> List[str]:
        if not prefixes:
            return []
        cache_key = "|".join(sorted(prefixes))
        cached = self._hot_pool_assets_cache.get(cache_key)
        if cached is not None:
            return list(cached)
        assets: List[str] = []
        for event_slug in self._discover_hot_event_slugs(prefixes):
            assets.extend(self._assets_for_event_slug(event_slug))
        deduped = sorted({asset_id for asset_id in assets if asset_id})
        self._hot_pool_assets_cache[cache_key] = deduped
        return list(deduped)

    def _discover_hot_event_slugs(self, prefixes: List[str]) -> List[str]:
        event_slugs: set[str] = set()
        for query in self._build_hot_market_queries(prefixes):
            try:
                events = self.api.public_search_events(query, limit_per_type=10)
            except Exception:
                continue
            for event in events:
                slug = str(event.get("slug") or "")
                if slug:
                    event_slugs.add(slug)
        return sorted(event_slugs)

    @staticmethod
    def _build_hot_market_queries(prefixes: List[str]) -> List[str]:
        query_map = {
            "btc": "bitcoin up or down",
            "bitcoin": "bitcoin up or down",
            "eth": "ethereum up or down",
            "ethereum": "ethereum up or down",
            "sol": "solana up or down",
            "solana": "solana up or down",
            "xrp": "xrp up or down",
        }
        return sorted({query_map[prefix] for prefix in prefixes if prefix in query_map})

    @staticmethod
    def _infer_hot_market_prefixes(
        recent_trades_by_target: Dict[str, List[TradeActivity]]
    ) -> List[str]:
        keyword_map = {
            "btc": ["btc", "bitcoin"],
            "bitcoin": ["btc", "bitcoin"],
            "eth": ["eth", "ethereum"],
            "ethereum": ["eth", "ethereum"],
            "sol": ["sol", "solana"],
            "solana": ["sol", "solana"],
            "xrp": ["xrp"],
        }
        prefixes: set[str] = set()
        for trades in recent_trades_by_target.values():
            for trade in trades:
                haystack = f"{trade.slug} {trade.event_slug} {trade.title}".lower()
                for marker, values in keyword_map.items():
                    if marker in haystack:
                        prefixes.update(values)
                if trade.is_priority_short_market:
                    prefixes.update(["btc", "bitcoin", "eth", "ethereum"])
        return sorted(prefixes)

    @staticmethod
    def _trade_priority_key(trade: TradeActivity) -> tuple[int, int]:
        if trade.is_priority_short_market:
            priority = 0
        elif trade.is_short_duration_market:
            priority = 1
        elif trade.market_duration_bucket == "hourly":
            priority = 2
        else:
            priority = 3
        return (priority, -trade.timestamp_ms)

    @staticmethod
    def _aggregate_trades(trades: List[TradeActivity]) -> List[TradeActivity]:
        grouped: Dict[tuple[str, str, str, int, int], List[TradeActivity]] = {}
        passthrough: List[TradeActivity] = []
        for trade in trades:
            tx_hash = (trade.transaction_hash or "").strip().lower()
            if not tx_hash:
                passthrough.append(trade)
                continue
            key = (
                tx_hash,
                trade.asset,
                trade.side.upper(),
                trade.outcome_index,
                trade.timestamp_seconds,
            )
            grouped.setdefault(key, []).append(trade)

        aggregated: List[TradeActivity] = list(passthrough)
        for bucket in grouped.values():
            if len(bucket) == 1:
                aggregated.append(bucket[0])
                continue
            total_size = sum(item.size for item in bucket)
            total_usdc = sum(item.usdc_size for item in bucket)
            if total_size <= 0 or total_usdc <= 0:
                aggregated.append(max(bucket, key=lambda item: item.timestamp_ms))
                continue
            latest = max(bucket, key=lambda item: (item.timestamp_ms, item.usdc_size, item.size))
            aggregated.append(
                TradeActivity(
                    proxy_wallet=latest.proxy_wallet,
                    timestamp_ms=latest.timestamp_ms,
                    condition_id=latest.condition_id,
                    activity_type=latest.activity_type,
                    size=round(total_size, 6),
                    usdc_size=round(total_usdc, 6),
                    transaction_hash=latest.transaction_hash,
                    price=total_usdc / total_size,
                    asset=latest.asset,
                    side=latest.side,
                    outcome_index=latest.outcome_index,
                    title=latest.title,
                    slug=latest.slug,
                    event_slug=latest.event_slug,
                    outcome=latest.outcome,
                    name=latest.name,
                    pseudonym=latest.pseudonym,
                )
            )
        return sorted(aggregated, key=lambda item: item.timestamp_ms)

    @staticmethod
    def _build_synthetic_book(trade: TradeActivity) -> OrderBook:
        price = max(min(float(trade.price), 0.9999), 0.0001)
        size = max(float(trade.size), 1.0)
        return OrderBook(
            market=trade.slug,
            asset_id=trade.asset,
            timestamp=str(trade.timestamp_ms),
            bids=[PriceLevel(price=price, size=size)],
            asks=[PriceLevel(price=price, size=size)],
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=price,
        )

def build_app(config_path: str) -> CopyTraderApp:
    return CopyTraderApp(load_config(config_path))
