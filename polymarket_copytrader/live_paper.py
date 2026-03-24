from __future__ import annotations

import csv
import json
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd

from .api import PolymarketPublicApi, extract_market_token_ids
from .alpha_baseline import _DEFAULT_FEATURE_COLUMNS
from .alpha_features import (
    ExternalMarketDataLookup,
    PriceHistoryLookup,
    RollingWindowTracker,
    _build_feature_row,
    _make_window_tracker_map,
    _observe_trade,
)
from .alpha_outcome import _build_regression_pipeline, _load_regression_frame
from .execution import PaperExecutionClient
from .matching import build_synthetic_order_book
from .market_ws import build_market_stream
from .models import (
    ExecutionConfig,
    FollowDecision,
    OrderBook,
    StateSnapshot,
    StrategyConfig,
    TradeActivity,
)
from .resolve import resolve_target_wallet
from .store import EventSink
from .strategy import MirrorTradeStrategy

_DURATION_RESOLUTION_RE = re.compile(r"-(\d+)([mh])-([0-9]{10})$")
_MONTH_NAME_TO_NUMBER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_CALENDAR_RESOLUTION_RE = re.compile(
    r"-(january|february|march|april|may|june|july|august|september|october|november|december)-"
    r"([0-9]{1,2})(?:-([0-9]{4}))?-([0-9]{1,4})(am|pm)-et$"
)
_ET_ZONE = ZoneInfo("America/New_York")


@dataclass
class LivePaperTarget:
    name: str
    profile: Optional[str]
    wallet: Optional[str]
    weight: float


@dataclass
class LivePaperRuntime:
    poll_interval_seconds: float
    request_timeout_seconds: float
    activity_limit: int
    lookback_seconds_on_start: int
    requery_overlap_seconds: int
    market_websocket_enabled: bool
    market_websocket_warmup_ms: int
    require_real_order_book: bool
    duration_hours: float
    heartbeat_interval_seconds: float
    state_path: str
    event_log_path: str
    hourly_stats_path: str


@dataclass
class LivePaperPortfolio:
    initial_capital_usdc: float


@dataclass
class LivePaperAlphaBucket:
    labeled_outcome_csv_path: str
    top_fraction: float = 0.2
    min_predicted_pnl: float = 0.0
    market_family: str = "btc"
    market_duration_bucket: str = "5m"
    min_seconds_to_resolution: float = 30.0
    external_market_data_dir: Optional[str] = None
    name: Optional[str] = None
    target_names: Optional[List[str]] = None


@dataclass
class LivePaperAlphaFilter:
    enabled: bool = False
    buckets: List[LivePaperAlphaBucket] = field(default_factory=list)
    labeled_outcome_csv_path: Optional[str] = None
    top_fraction: float = 0.2
    min_predicted_pnl: float = 0.0
    market_family: str = "btc"
    market_duration_bucket: str = "5m"
    min_seconds_to_resolution: float = 30.0
    external_market_data_dir: Optional[str] = None


@dataclass
class LivePaperConfig:
    targets: List[LivePaperTarget]
    runtime: LivePaperRuntime
    portfolio: LivePaperPortfolio
    strategy: StrategyConfig
    execution: ExecutionConfig
    alpha_filter: LivePaperAlphaFilter = field(default_factory=LivePaperAlphaFilter)


@dataclass
class LivePaperPosition:
    asset_id: str
    size: float
    cost_usdc: float
    market_slug: str
    outcome: str
    condition_id: str
    resolution_timestamp_seconds: int | None = None


@dataclass
class LivePaperState:
    started_at_seconds: int = 0
    end_at_seconds: int = 0
    next_hourly_snapshot_seconds: int = 0
    cash_usdc: float = 0.0
    last_heartbeat_timestamp_seconds: int = 0
    target_wallets: Dict[str, str] = field(default_factory=dict)
    target_last_event_timestamp_ms: Dict[str, int] = field(default_factory=dict)
    seen_keys: List[str] = field(default_factory=list)
    positions: Dict[str, LivePaperPosition] = field(default_factory=dict)

    def remember(self, key: str, max_size: int = 20000) -> None:
        self.seen_keys.append(key)
        if len(self.seen_keys) > max_size:
            self.seen_keys = self.seen_keys[-max_size:]


class LivePaperStateStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def load(self) -> LivePaperState:
        if not self.path.exists():
            return LivePaperState()
        with self.path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        positions = {
            str(asset_id): LivePaperPosition(**payload)
            for asset_id, payload in dict(raw.get("positions", {})).items()
        }
        return LivePaperState(
            started_at_seconds=int(raw.get("started_at_seconds", 0)),
            end_at_seconds=int(raw.get("end_at_seconds", 0)),
            next_hourly_snapshot_seconds=int(raw.get("next_hourly_snapshot_seconds", 0)),
            cash_usdc=float(raw.get("cash_usdc", 0.0)),
            target_wallets={str(k): str(v) for k, v in dict(raw.get("target_wallets", {})).items()},
            target_last_event_timestamp_ms={
                str(k): int(v)
                for k, v in dict(raw.get("target_last_event_timestamp_ms", {})).items()
            },
            seen_keys=list(raw.get("seen_keys", [])),
            positions=positions,
        )

    def save(self, state: LivePaperState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = asdict(state)
        with self.path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)


class HourlyStatsWriter:
    def __init__(self, path: str, target_names: List[str]) -> None:
        self.path = Path(path)
        self.target_names = target_names
        self.fieldnames = [
            "timestamp_seconds",
            "timestamp_iso",
            "equity_usdc",
            "cash_usdc",
            "holdings_value_usdc",
            "return_pct",
            "positions",
        ] + [f"mark_{name}" for name in target_names]

    def append(self, row: Dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self.path.exists()
        with self.path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


class MultiTargetLivePaperApp:
    def __init__(self, config: LivePaperConfig) -> None:
        self.config = config
        self.api = PolymarketPublicApi(timeout_seconds=config.runtime.request_timeout_seconds)
        self.store = LivePaperStateStore(config.runtime.state_path)
        self.events = EventSink(config.runtime.event_log_path)
        self.state = self.store.load()
        self.strategy = MirrorTradeStrategy(config.strategy)
        self.market_stream = (
            build_market_stream()
            if config.runtime.market_websocket_enabled
            else None
        )
        self.execution_state = StateSnapshot(
            asset_exposures_usdc={
                asset_id: position.cost_usdc for asset_id, position in self.state.positions.items()
            },
            asset_positions_size={
                asset_id: position.size for asset_id, position in self.state.positions.items()
            },
        )
        self.execution = PaperExecutionClient(config.execution, self.execution_state)
        self.targets = self._resolve_targets()
        self.target_weight_fractions = self._build_target_weight_fractions(self.targets)
        self._event_assets_cache: Dict[str, List[str]] = {}
        self._hot_pool_assets_cache: Dict[str, List[str]] = {}
        self.alpha_filter = self._build_alpha_filter()
        self.hourly_stats = HourlyStatsWriter(
            config.runtime.hourly_stats_path,
            [target.name for target in self.targets],
        )
        self._initialize_session()
        if self.market_stream is None:
            self.events.write("market_ws_disabled", {"reason": "disabled_in_config"})
        elif not self.market_stream.available:
            self.events.write("market_ws_disabled", {"reason": self.market_stream.reason})
        else:
            self.events.write("market_ws_enabled", {"mode": "market_channel"})
            self._prime_market_stream()

    def run(self, once: bool = False) -> None:
        try:
            while True:
                now_seconds = int(time.time())
                self._emit_heartbeat_if_due(now_seconds)
                self._redeem_resolved_positions(now_seconds)
                for target in self.targets:
                    self._poll_target(target)
                self._redeem_resolved_positions(int(time.time()))
                self._record_hourly_snapshot_if_due(int(time.time()))
                self._sync_state()
                self.store.save(self.state)
                if once or int(time.time()) >= self.state.end_at_seconds:
                    self._record_final_snapshot()
                    self.store.save(self.state)
                    return
                time.sleep(self.config.runtime.poll_interval_seconds)
        finally:
            if self.market_stream is not None:
                self.market_stream.close()

    def doctor(self) -> List[str]:
        return [
            "mode=live_paper",
            f"targets={','.join(target.name for target in self.targets)}",
            f"execution_policy={self.config.strategy.execution_policy.upper()}",
            f"market_ws_enabled={bool(self.market_stream and self.market_stream.available)}",
            f"require_real_order_book={self.config.runtime.require_real_order_book}",
            f"alpha_filter_enabled={bool(self.alpha_filter)}",
            f"alpha_filter_buckets={len(getattr(self.alpha_filter, 'bucket_runtimes', {}))}",
            f"state_path={self.config.runtime.state_path}",
            f"hourly_stats_path={self.config.runtime.hourly_stats_path}",
            f"end_at={_isoformat(self.state.end_at_seconds)}",
        ]

    def _resolve_targets(self) -> List[LivePaperTarget]:
        resolved: List[LivePaperTarget] = []
        for target in self.config.targets:
            wallet, details = resolve_target_wallet(self.api, target.profile, target.wallet)
            resolved.append(
                LivePaperTarget(
                    name=target.name,
                    profile=target.profile,
                    wallet=wallet,
                    weight=target.weight,
                )
            )
            self.state.target_wallets[target.name] = wallet
            self.events.write(
                "target_resolved",
                {"name": target.name, "wallet": wallet, "details": details},
            )
        return resolved

    def _initialize_session(self) -> None:
        now_seconds = int(time.time())
        if self.state.started_at_seconds <= 0:
            self.state.started_at_seconds = now_seconds
            self.state.end_at_seconds = now_seconds + int(self.config.runtime.duration_hours * 3600)
            self.state.next_hourly_snapshot_seconds = _next_hour_mark(now_seconds)
        elif self.state.end_at_seconds <= 0:
            self.state.end_at_seconds = (
                self.state.started_at_seconds + int(self.config.runtime.duration_hours * 3600)
            )
        if self.state.cash_usdc <= 0 and not self.state.positions:
            self.state.cash_usdc = self.config.portfolio.initial_capital_usdc

    def _poll_target(self, target: LivePaperTarget) -> None:
        wallet = target.wallet or self.state.target_wallets[target.name]
        recent_start = self._start_cursor_ms(target.name)
        try:
            trades = self._fetch_candidate_trades(target.name, wallet, recent_start)
            trades = self._aggregate_trades(trades)
            self.events.write(
                "poll_ok",
                {
                    "target_name": target.name,
                    "wallet": wallet,
                    "start_ms": recent_start,
                    "trades_seen": len(trades),
                },
            )
            if self.market_stream is not None:
                batch_assets = sorted({trade.asset for trade in trades})
                related_assets = self._discover_trade_context_assets(trades)
                self._warm_market_stream_assets(
                    target_name=target.name,
                    core_assets=batch_assets,
                    related_assets=related_assets,
                )
            for trade in sorted(trades, key=self._trade_priority_key):
                try:
                    self._handle_trade(target.name, trade)
                except Exception as exc:
                    self.events.write(
                        "trade_error",
                        {
                            "target_name": target.name,
                            "wallet": wallet,
                            "tx": trade.transaction_hash,
                            "asset": trade.asset,
                            "market_slug": trade.slug,
                            "error": str(exc),
                        },
                    )
                    self.state.target_last_event_timestamp_ms[target.name] = max(
                        self.state.target_last_event_timestamp_ms.get(target.name, 0),
                        trade.timestamp_seconds,
                    )
                    self.state.remember(trade.dedupe_key)
        except Exception as exc:
            self.events.write(
                "poll_error",
                {
                    "target_name": target.name,
                    "wallet": wallet,
                    "start_ms": recent_start,
                    "error": str(exc),
                },
            )

    def _prime_market_stream(self) -> None:
        if self.market_stream is None:
            return
        ensured_assets = {asset_id for asset_id in self.state.positions}
        for asset_id in ensured_assets:
            self.market_stream.ensure_asset(asset_id)

        recent_trades_by_target: Dict[str, List[TradeActivity]] = {}
        primed_trades = 0
        for target in self.targets:
            wallet = target.wallet or self.state.target_wallets[target.name]
            start_ms = self._start_cursor_ms(target.name)
            try:
                recent_trades = self.api.get_activity(
                    wallet=wallet,
                    limit=max(self.config.runtime.activity_limit, 200),
                    start_ms=start_ms,
                    side=None,
                    sort_direction="DESC",
                )
            except Exception as exc:
                self.events.write(
                    "market_ws_prime_error",
                    {
                        "target_name": target.name,
                        "wallet": wallet,
                        "start_ms": start_ms,
                        "error": str(exc),
                    },
                )
                continue
            recent_trades_by_target[target.name] = recent_trades
            primed_trades += len(recent_trades)
            for asset_id in {trade.asset for trade in recent_trades}:
                ensured_assets.add(asset_id)
                self.market_stream.ensure_asset(asset_id)
        pool_assets, prefixes = self._discover_hot_market_pool(recent_trades_by_target)
        for asset_id in pool_assets:
            ensured_assets.add(asset_id)
            self.market_stream.ensure_asset(asset_id)
        self.events.write(
            "market_ws_prime",
            {
                "assets_ensured": len(ensured_assets),
                "recent_trades_scanned": primed_trades,
                "pool_assets_ensured": len(pool_assets),
                "pool_prefixes": prefixes,
            },
        )

    def _discover_hot_market_pool(
        self, recent_trades_by_target: Dict[str, List[TradeActivity]]
    ) -> tuple[List[str], List[str]]:
        prefixes = self._infer_hot_market_prefixes(recent_trades_by_target)
        assets = self._hot_pool_assets_for_prefixes(prefixes)
        return assets, prefixes

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
        except Exception as exc:
            self.events.write(
                "market_ws_context_event_error",
                {"event_slug": normalized, "error": str(exc)},
            )
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
        if not prefixes:
            self._hot_pool_assets_cache[cache_key] = []
            return []
        event_slugs = self._discover_hot_event_slugs(prefixes)
        for event_slug in event_slugs:
            assets.extend(self._assets_for_event_slug(event_slug))
        deduped = sorted({asset_id for asset_id in assets if asset_id})
        self._hot_pool_assets_cache[cache_key] = deduped
        return list(deduped)

    def _discover_hot_event_slugs(self, prefixes: List[str]) -> List[str]:
        queries = self._build_hot_market_queries(prefixes)
        event_slugs: set[str] = set()
        for query in queries:
            try:
                events = self.api.public_search_events(query, limit_per_type=10)
            except Exception as exc:
                self.events.write(
                    "market_ws_pool_search_error",
                    {"query": query, "error": str(exc)},
                )
                continue
            for event in events:
                slug = str(event.get("slug") or "")
                if not slug:
                    continue
                if not _is_hot_short_duration_market(event):
                    continue
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
        queries = {
            query_map[prefix]
            for prefix in prefixes
            if prefix in query_map
        }
        return sorted(queries)

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

    def _aggregate_trades(self, trades: List[TradeActivity]) -> List[TradeActivity]:
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

    def _start_cursor_ms(self, target_name: str) -> int:
        last_seen = int(self.state.target_last_event_timestamp_ms.get(target_name, 0))
        last_seen_seconds = (last_seen // 1000) if last_seen > 100_000_000_000 else last_seen
        if last_seen_seconds > 0:
            return max(last_seen_seconds - int(self.config.runtime.requery_overlap_seconds), 0)
        now_seconds = int(time.time())
        cold_start_floor = int(self.state.started_at_seconds or now_seconds)
        return max(
            now_seconds - int(self.config.runtime.lookback_seconds_on_start),
            cold_start_floor,
            0,
        )

    def _fetch_candidate_trades(
        self, target_name: str, wallet: str, recent_start: int
    ) -> List[TradeActivity]:
        fast_recent: List[TradeActivity] = []
        last_seen = int(self.state.target_last_event_timestamp_ms.get(target_name, 0))
        last_seen_seconds = (last_seen // 1000) if last_seen > 100_000_000_000 else last_seen
        if last_seen_seconds > 0:
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
        if last_seen_seconds > 0:
            backfill_seconds = max(int(self.config.runtime.requery_overlap_seconds) * 4, 15)
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
                "target_name": target_name,
                "recent_start": recent_start,
                "fast_recent_count": len(fast_recent),
                "recent_count": len(recent),
                "late_backfill_count": len(late_backfill),
            },
        )
        return fast_recent + recent + late_backfill

    def _handle_trade(self, target_name: str, trade: TradeActivity) -> None:
        if trade.dedupe_key in set(self.state.seen_keys):
            return
        if trade.side.upper() != "BUY":
            self.state.target_last_event_timestamp_ms[target_name] = max(
                self.state.target_last_event_timestamp_ms.get(target_name, 0),
                trade.timestamp_seconds,
            )
            self.state.remember(trade.dedupe_key)
            return

        if self.alpha_filter is not None:
            alpha_decision = self.alpha_filter.evaluate(target_name, trade)
            self.events.write(
                "alpha_filter_decision",
                {
                    "target_name": target_name,
                    "market_slug": trade.slug,
                    "event_slug": trade.event_slug,
                    "market_family": trade.market_family,
                    "market_duration_bucket": trade.market_duration_bucket,
                    "outcome": trade.outcome,
                    "asset_id": trade.asset,
                    "trade_timestamp_seconds": trade.timestamp_seconds,
                    **alpha_decision,
                },
            )
            if not bool(alpha_decision.get("should_follow")):
                self.state.target_last_event_timestamp_ms[target_name] = max(
                    self.state.target_last_event_timestamp_ms.get(target_name, 0),
                    trade.timestamp_seconds,
                )
                self.state.remember(trade.dedupe_key)
                return

        stale_reason = self.strategy.stale_reason(trade)
        if stale_reason is not None:
            decision = FollowDecision(
                should_follow=False,
                reason=stale_reason,
                target_trade=trade,
                follow_side="BUY",
            )
            self.events.write("decision", {"target_name": target_name, **decision.to_dict()})
            self.state.target_last_event_timestamp_ms[target_name] = max(
                self.state.target_last_event_timestamp_ms.get(target_name, 0),
                trade.timestamp_seconds,
            )
            self.state.remember(trade.dedupe_key)
            return

        order_book, order_book_source = self._order_book_for_trade(target_name, trade)
        if order_book is None:
            decision = FollowDecision(
                should_follow=False,
                reason="skip_no_real_order_book",
                target_trade=trade,
                follow_side="BUY",
            )
            self.events.write("decision", {"target_name": target_name, **decision.to_dict()})
            self.state.target_last_event_timestamp_ms[target_name] = max(
                self.state.target_last_event_timestamp_ms.get(target_name, 0),
                trade.timestamp_seconds,
            )
            self.state.remember(trade.dedupe_key)
            return
        current_exposure = float(self.execution_state.asset_exposures_usdc.get(trade.asset, 0.0))
        current_position_size = float(self.execution_state.asset_positions_size.get(trade.asset, 0.0))
        decision = self.strategy.decide(
            trade=trade,
            order_book=order_book,
            current_exposure_usdc=current_exposure,
            current_position_size=current_position_size,
            current_timestamp_seconds=time.time(),
        )
        decision = self._apply_target_weight(target_name, decision, order_book)
        self.events.write("decision", {"target_name": target_name, **decision.to_dict()})

        if decision.should_follow:
            requested_usdc = float(decision.follow_usdc or 0.0)
            if requested_usdc > self.state.cash_usdc:
                decision = FollowDecision(
                    should_follow=False,
                    reason="skip_no_cash",
                    target_trade=trade,
                    follow_side="BUY",
                )
                self.events.write("decision", {"target_name": target_name, **decision.to_dict()})
            else:
                report = self.execution.place_follow_trade(
                    decision,
                    order_book=order_book,
                    execution_policy=self.config.strategy.execution_policy,
                )
                self.events.write(
                    "execution",
                    {
                        "target_name": target_name,
                        "order_book_source": order_book_source,
                        **report.to_dict(),
                    },
                )
                if report.ok:
                    filled_usdc = float(report.requested_usdc)
                    filled_size = float(report.requested_size or 0.0)
                    self.state.cash_usdc -= filled_usdc
                    self._merge_position(trade, filled_size, filled_usdc)

        self.state.target_last_event_timestamp_ms[target_name] = max(
            self.state.target_last_event_timestamp_ms.get(target_name, 0),
            trade.timestamp_seconds,
        )
        self.state.remember(trade.dedupe_key)

    def _apply_target_weight(
        self,
        target_name: str,
        decision: FollowDecision,
        order_book: OrderBook,
    ) -> FollowDecision:
        if not decision.should_follow:
            return decision
        weight_fraction = float(self.target_weight_fractions.get(target_name, 1.0))
        if weight_fraction >= 0.999999:
            return decision

        follow_usdc = float(decision.follow_usdc or 0.0)
        follow_price = float(decision.follow_price or 0.0)
        if follow_usdc <= 0 or follow_price <= 0:
            return FollowDecision(
                should_follow=False,
                reason="skip_invalid_weight_scaled_order",
                target_trade=decision.target_trade,
                follow_side=decision.follow_side,
                slippage_bps=decision.slippage_bps,
            )

        scaled_follow_usdc = round(follow_usdc * weight_fraction, 6)
        if scaled_follow_usdc < self.config.strategy.min_order_usdc:
            return FollowDecision(
                should_follow=False,
                reason="skip_weight_scaled_below_min_order_usdc",
                target_trade=decision.target_trade,
                follow_side=decision.follow_side,
                slippage_bps=decision.slippage_bps,
            )

        scaled_follow_size = scaled_follow_usdc / follow_price
        if scaled_follow_size < order_book.min_order_size:
            return FollowDecision(
                should_follow=False,
                reason="skip_weight_scaled_below_min_order_size",
                target_trade=decision.target_trade,
                follow_side=decision.follow_side,
                slippage_bps=decision.slippage_bps,
            )

        return FollowDecision(
            should_follow=True,
            reason="follow",
            target_trade=decision.target_trade,
            follow_side=decision.follow_side,
            follow_price=decision.follow_price,
            follow_usdc=scaled_follow_usdc,
            follow_size=round(scaled_follow_size, 6),
            slippage_bps=decision.slippage_bps,
        )

    @staticmethod
    def _build_target_weight_fractions(targets: List[LivePaperTarget]) -> Dict[str, float]:
        if not targets:
            return {}
        positive_weights = {
            target.name: max(float(target.weight), 0.0)
            for target in targets
        }
        weight_sum = sum(positive_weights.values())
        if weight_sum <= 0:
            equal_fraction = 1.0 / len(targets)
            return {target.name: equal_fraction for target in targets}
        return {
            target.name: positive_weights[target.name] / weight_sum
            for target in targets
        }

    def _build_alpha_filter(self) -> Optional["_LiveAlphaFilterRuntime"]:
        config = self.config.alpha_filter
        if not config.enabled:
            return None
        return _LiveAlphaFilterRuntime(config)

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

    def _warm_market_stream_assets(
        self,
        target_name: str,
        core_assets: List[str],
        related_assets: List[str],
    ) -> None:
        if self.market_stream is None:
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
        uncached_core_assets = [
            asset_id
            for asset_id in unique_core_assets
            if self.market_stream.get_order_book(asset_id) is None
        ]
        for asset_id in unique_core_assets + unique_related_assets:
            self.market_stream.ensure_asset(asset_id)
        wait_seconds = self._market_stream_wait_seconds(
            core_assets_count=len(uncached_core_assets),
            related_assets_count=len(unique_related_assets),
        )
        if not uncached_core_assets or wait_seconds <= 0:
            return
        deadline = time.time() + wait_seconds
        ready_assets = [
            asset_id
            for asset_id in uncached_core_assets
            if self.market_stream.get_order_book(asset_id) is not None
        ]
        while len(ready_assets) < len(uncached_core_assets) and time.time() < deadline:
            time.sleep(min(0.05, max(deadline - time.time(), 0.0)))
            ready_assets = [
                asset_id
                for asset_id in uncached_core_assets
                if self.market_stream.get_order_book(asset_id) is not None
            ]
        self.events.write(
            "market_ws_batch_warmup",
            {
                "target_name": target_name,
                "core_assets_count": len(unique_core_assets),
                "related_assets_count": len(unique_related_assets),
                "uncached_core_assets_count": len(uncached_core_assets),
                "ready_core_assets_count": len(ready_assets),
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

    def _order_book_for_trade(
        self, target_name: str, trade: TradeActivity
    ) -> tuple[OrderBook | None, str]:
        if self.market_stream is not None:
            self.market_stream.ensure_asset(trade.asset)
            book = self.market_stream.wait_for_book(
                trade.asset,
                timeout_seconds=max(self.config.runtime.market_websocket_warmup_ms, 0) / 1000.0,
            )
            if book is not None:
                self.events.write(
                    "order_book_source",
                    {
                        "target_name": target_name,
                        "asset_id": trade.asset,
                        "market_slug": trade.slug,
                        "source": "market_ws",
                    },
                )
                return book, "market_ws"
            context_assets = self._discover_trade_context_assets([trade])
            context_related_assets = [
                asset_id for asset_id in context_assets if asset_id != trade.asset
            ]
            if context_related_assets:
                self._warm_market_stream_assets(
                    target_name=target_name,
                    core_assets=[trade.asset],
                    related_assets=context_related_assets,
                )
                book = self.market_stream.get_order_book(trade.asset)
                if book is not None:
                    self.events.write(
                        "order_book_source",
                        {
                            "target_name": target_name,
                            "asset_id": trade.asset,
                            "market_slug": trade.slug,
                            "source": "market_ws_context",
                        },
                    )
                    return book, "market_ws_context"
        try:
            book = self.api.get_order_book(trade.asset)
            self.events.write(
                "order_book_source",
                {
                    "target_name": target_name,
                    "asset_id": trade.asset,
                    "market_slug": trade.slug,
                    "source": "rest_book",
                },
            )
            return book, "rest_book"
        except RuntimeError as exc:
            if self.config.runtime.require_real_order_book:
                self.events.write(
                    "order_book_missing",
                    {
                        "target_name": target_name,
                        "asset_id": trade.asset,
                        "market_slug": trade.slug,
                        "outcome": trade.outcome,
                        "reason": "require_real_order_book",
                        "error": str(exc),
                    },
                )
                return None, "missing_real_book"
            fallback = build_synthetic_order_book(
                asset_id=trade.asset,
                market=trade.slug or trade.event_slug or trade.condition_id,
                reference_price=trade.price,
                trade_size=trade.size,
                extra_slippage_bps=min(self.config.strategy.max_slippage_bps / 5.0, 25.0),
                levels=3,
                depth_multiplier=5.0,
            )
            self.events.write(
                "order_book_fallback",
                {
                    "target_name": target_name,
                    "asset_id": trade.asset,
                    "market_slug": trade.slug,
                    "outcome": trade.outcome,
                    "reference_price": trade.price,
                    "reference_size": trade.size,
                    "reason": "synthetic_from_target_trade",
                    "error": str(exc),
                },
            )
            return fallback, "synthetic_fallback"

    def _merge_position(self, trade: TradeActivity, filled_size: float, filled_usdc: float) -> None:
        if filled_size <= 0 or filled_usdc <= 0:
            return
        current = self.state.positions.get(trade.asset)
        resolution_ts = _infer_resolution_timestamp_seconds(trade)
        if current is None:
            self.state.positions[trade.asset] = LivePaperPosition(
                asset_id=trade.asset,
                size=filled_size,
                cost_usdc=filled_usdc,
                market_slug=trade.slug,
                outcome=trade.outcome,
                condition_id=trade.condition_id,
                resolution_timestamp_seconds=resolution_ts,
            )
            return
        current.size += filled_size
        current.cost_usdc += filled_usdc
        if current.resolution_timestamp_seconds is None:
            current.resolution_timestamp_seconds = resolution_ts

    def _redeem_resolved_positions(self, now_seconds: int) -> None:
        to_remove: List[str] = []
        for asset_id, position in self.state.positions.items():
            resolution_ts = position.resolution_timestamp_seconds
            if resolution_ts is None:
                resolution_ts = _infer_resolution_timestamp_seconds_from_slug(position.market_slug)
                if resolution_ts is not None:
                    position.resolution_timestamp_seconds = resolution_ts
            if resolution_ts is None or now_seconds < resolution_ts:
                continue
            latest_price = self._latest_price_for_asset(asset_id, resolution_ts, now_seconds)
            if latest_price is None:
                continue
            if 0.05 < latest_price < 0.95:
                continue
            payout = 1.0 if latest_price >= 0.5 else 0.0
            proceeds = position.size * payout
            pnl = proceeds - position.cost_usdc
            self.state.cash_usdc += proceeds
            to_remove.append(asset_id)
            self.events.write(
                "redeem",
                {
                    "asset_id": asset_id,
                    "market_slug": position.market_slug,
                    "outcome": position.outcome,
                    "size": round(position.size, 6),
                    "cost_usdc": round(position.cost_usdc, 6),
                    "proceeds_usdc": round(proceeds, 6),
                    "pnl_usdc": round(pnl, 6),
                    "resolution_timestamp_seconds": resolution_ts,
                    "mark_price": round(latest_price, 6),
                },
            )
        for asset_id in to_remove:
            self.state.positions.pop(asset_id, None)
            self.execution_state.asset_positions_size.pop(asset_id, None)
            self.execution_state.asset_exposures_usdc.pop(asset_id, None)

    def _latest_price_for_asset(
        self, asset_id: str, resolution_ts: int, now_seconds: int
    ) -> float | None:
        try:
            history = self.api.get_price_history(
                asset_id=asset_id,
                start_ts=max(resolution_ts - 120, 0),
                end_ts=now_seconds,
                fidelity_minutes=1,
            )
        except RuntimeError:
            return None
        if not history:
            return None
        return history[-1].price

    def _record_hourly_snapshot_if_due(self, now_seconds: int) -> None:
        if now_seconds < self.state.next_hourly_snapshot_seconds:
            return
        row = self._snapshot_row(now_seconds)
        self.hourly_stats.append(row)
        self.events.write("hourly_snapshot", row)
        self.state.next_hourly_snapshot_seconds = _next_hour_mark(now_seconds)

    def _record_final_snapshot(self) -> None:
        now_seconds = int(time.time())
        row = self._snapshot_row(now_seconds)
        self.hourly_stats.append(row)
        self.events.write("session_complete", row)

    def _emit_heartbeat_if_due(self, now_seconds: int) -> None:
        if (
            self.state.last_heartbeat_timestamp_seconds > 0
            and now_seconds - self.state.last_heartbeat_timestamp_seconds
            < int(self.config.runtime.heartbeat_interval_seconds)
        ):
            return
        row = self._snapshot_row(now_seconds)
        self.events.write(
            "heartbeat",
            {
                **row,
                "end_at_iso": _isoformat(self.state.end_at_seconds),
                "next_hourly_snapshot_iso": _isoformat(self.state.next_hourly_snapshot_seconds),
            },
        )
        self.state.last_heartbeat_timestamp_seconds = now_seconds

    def _snapshot_row(self, now_seconds: int) -> Dict[str, object]:
        marks = self._mark_positions()
        holdings_value = 0.0
        mark_payload: Dict[str, float] = {f"mark_{target.name}": 0.0 for target in self.targets}
        for asset_id, position in self.state.positions.items():
            price = marks.get(asset_id, 0.0)
            holdings_value += position.size * price
        equity = self.state.cash_usdc + holdings_value
        return {
            "timestamp_seconds": now_seconds,
            "timestamp_iso": _isoformat(now_seconds),
            "equity_usdc": round(equity, 6),
            "cash_usdc": round(self.state.cash_usdc, 6),
            "holdings_value_usdc": round(holdings_value, 6),
            "return_pct": round(
                ((equity / self.config.portfolio.initial_capital_usdc) - 1.0) * 100.0,
                6,
            ),
            "positions": len(self.state.positions),
            **mark_payload,
        }

    def _mark_positions(self) -> Dict[str, float]:
        marks: Dict[str, float] = {}
        now_seconds = int(time.time())
        for asset_id, position in self.state.positions.items():
            if self.market_stream is not None:
                ws_book = self.market_stream.get_order_book(asset_id)
                if ws_book is not None:
                    marks[asset_id] = ws_book.best_bid or ws_book.last_trade_price or 0.0
                    continue
                ws_last_price = self.market_stream.get_last_price(asset_id)
                if ws_last_price is not None:
                    marks[asset_id] = ws_last_price
                    continue
            try:
                book = self.api.get_order_book(asset_id)
                marks[asset_id] = book.best_bid or book.last_trade_price or 0.0
                continue
            except RuntimeError:
                pass
            try:
                history = self.api.get_price_history(
                    asset_id=asset_id,
                    start_ts=max(now_seconds - 3600, 0),
                    end_ts=now_seconds,
                    fidelity_minutes=1,
                )
            except RuntimeError:
                history = []
            if history:
                marks[asset_id] = history[-1].price
                continue
            if position.size > 0:
                marks[asset_id] = position.cost_usdc / position.size
        return marks

    def _sync_state(self) -> None:
        self.execution_state.seen_keys = list(self.state.seen_keys)


def _isoformat(timestamp_seconds: int) -> str:
    return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc).isoformat()


def _next_hour_mark(timestamp_seconds: int) -> int:
    return ((timestamp_seconds // 3600) + 1) * 3600


def _infer_resolution_timestamp_seconds(trade: TradeActivity) -> int | None:
    for candidate in [trade.event_slug, trade.slug]:
        resolution_ts = _infer_resolution_timestamp_seconds_from_slug(
            candidate,
            reference_timestamp_seconds=trade.timestamp_seconds,
        )
        if resolution_ts is not None:
            return resolution_ts
    return None


def _infer_resolution_timestamp_seconds_from_slug(
    slug: str | None,
    reference_timestamp_seconds: int | None = None,
) -> int | None:
    if not slug:
        return None
    duration_match = _DURATION_RESOLUTION_RE.search(slug)
    if duration_match:
        duration = int(duration_match.group(1))
        unit = duration_match.group(2)
        start_ts = int(duration_match.group(3))
        return start_ts + duration * (60 if unit == "m" else 3600)
    calendar_match = _CALENDAR_RESOLUTION_RE.search(slug)
    if not calendar_match:
        return None
    month = _MONTH_NAME_TO_NUMBER[calendar_match.group(1)]
    day = int(calendar_match.group(2))
    year = _infer_calendar_year(calendar_match.group(3), reference_timestamp_seconds)
    time_token = calendar_match.group(4)
    meridiem = calendar_match.group(5)
    parsed_time = _parse_calendar_time_token(time_token, meridiem)
    if parsed_time is None:
        return None
    hour, minute = parsed_time
    # Calendar slugs like "...-2pm-et" represent the hourly market bucket.
    # They settle at the end of that labeled hour.
    return int(datetime(year, month, day, hour, minute, tzinfo=_ET_ZONE).timestamp()) + 3600


def _infer_calendar_year(raw_year: str | None, reference_timestamp_seconds: int | None) -> int:
    if raw_year:
        return int(raw_year)
    if reference_timestamp_seconds is not None:
        return datetime.fromtimestamp(reference_timestamp_seconds, tz=_ET_ZONE).year
    return datetime.now(_ET_ZONE).year


def _parse_calendar_time_token(time_token: str, meridiem: str) -> tuple[int, int] | None:
    if not time_token.isdigit():
        return None
    if len(time_token) <= 2:
        hour = int(time_token)
        minute = 0
    elif len(time_token) == 3:
        hour = int(time_token[0])
        minute = int(time_token[1:])
    elif len(time_token) == 4:
        hour = int(time_token[:2])
        minute = int(time_token[2:])
    else:
        return None
    if hour < 1 or hour > 12 or minute < 0 or minute >= 60:
        return None
    if meridiem == "am":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12
    return hour, minute


def _is_hot_short_duration_market(payload: Dict[str, object]) -> bool:
    slug = str(payload.get("slug") or "").lower()
    title = str(payload.get("title") or payload.get("question") or "").lower()
    description = str(payload.get("description") or "").lower()
    haystack = f"{slug} {title} {description}"
    if "updown" not in haystack and "up or down" not in haystack:
        return False
    return bool(re.search(r"-(5|15)m-", slug))


def load_live_paper_config(path: str) -> LivePaperConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    alpha_raw = raw.get("alpha_filter") or {}
    alpha_buckets_raw = list(alpha_raw.get("buckets", []))
    alpha_buckets = [
        LivePaperAlphaBucket(
            labeled_outcome_csv_path=str(item["labeled_outcome_csv_path"]),
            top_fraction=float(item.get("top_fraction", 0.2)),
            min_predicted_pnl=float(item.get("min_predicted_pnl", 0.0)),
            market_family=str(item.get("market_family", "btc")),
            market_duration_bucket=str(item.get("market_duration_bucket", "5m")),
            min_seconds_to_resolution=float(item.get("min_seconds_to_resolution", 30.0)),
            external_market_data_dir=item.get("external_market_data_dir"),
            name=item.get("name"),
            target_names=[str(name) for name in item.get("target_names", [])] or None,
        )
        for item in alpha_buckets_raw
    ]
    return LivePaperConfig(
        targets=[
            LivePaperTarget(
                name=str(item["name"]),
                profile=item.get("profile"),
                wallet=item.get("wallet"),
                weight=float(item.get("weight", 1.0)),
            )
            for item in raw["targets"]
        ],
        runtime=LivePaperRuntime(
            poll_interval_seconds=float(raw["runtime"]["poll_interval_seconds"]),
            request_timeout_seconds=float(raw["runtime"]["request_timeout_seconds"]),
            activity_limit=int(raw["runtime"]["activity_limit"]),
            lookback_seconds_on_start=int(raw["runtime"]["lookback_seconds_on_start"]),
            requery_overlap_seconds=int(raw["runtime"]["requery_overlap_seconds"]),
            market_websocket_enabled=bool(raw["runtime"].get("market_websocket_enabled", True)),
            market_websocket_warmup_ms=int(raw["runtime"].get("market_websocket_warmup_ms", 350)),
            require_real_order_book=bool(raw["runtime"].get("require_real_order_book", False)),
            duration_hours=float(raw["runtime"]["duration_hours"]),
            heartbeat_interval_seconds=float(raw["runtime"].get("heartbeat_interval_seconds", 30.0)),
            state_path=str(raw["runtime"]["state_path"]),
            event_log_path=str(raw["runtime"]["event_log_path"]),
            hourly_stats_path=str(raw["runtime"]["hourly_stats_path"]),
        ),
        portfolio=LivePaperPortfolio(
            initial_capital_usdc=float(raw["portfolio"]["initial_capital_usdc"]),
        ),
        strategy=StrategyConfig(
            follow_fraction=float(raw["strategy"]["follow_fraction"]),
            fixed_order_usdc=(
                None
                if raw["strategy"].get("fixed_order_usdc") is None
                else float(raw["strategy"]["fixed_order_usdc"])
            ),
            min_target_usdc_size=float(raw["strategy"]["min_target_usdc_size"]),
            max_follow_price=float(raw["strategy"]["max_follow_price"]),
            max_slippage_bps=float(raw["strategy"]["max_slippage_bps"]),
            max_position_per_asset_usdc=float(raw["strategy"]["max_position_per_asset_usdc"]),
            min_order_usdc=float(raw["strategy"]["min_order_usdc"]),
            skip_outcomes=list(raw["strategy"].get("skip_outcomes", [])),
            skip_market_slugs=list(raw["strategy"].get("skip_market_slugs", [])),
            execution_policy=str(raw["strategy"].get("execution_policy", "IOC")),
            aggressive_price_enabled=bool(raw["strategy"].get("aggressive_price_enabled", False)),
            aggressive_price_max_ticks=int(raw["strategy"].get("aggressive_price_max_ticks", 0)),
            max_trade_age_seconds=(
                None
                if raw["strategy"].get("max_trade_age_seconds") is None
                else float(raw["strategy"]["max_trade_age_seconds"])
            ),
            short_market_max_trade_age_seconds=(
                None
                if raw["strategy"].get("short_market_max_trade_age_seconds") is None
                else float(raw["strategy"]["short_market_max_trade_age_seconds"])
            ),
        ),
        execution=ExecutionConfig(
            mode="paper",
            host=str(raw["execution"]["host"]),
            chain_id=int(raw["execution"]["chain_id"]),
            signature_type=int(raw["execution"]["signature_type"]),
            private_key_env=str(raw["execution"]["private_key_env"]),
            funder_env=str(raw["execution"]["funder_env"]),
        ),
        alpha_filter=LivePaperAlphaFilter(
            enabled=bool(alpha_raw.get("enabled", False)),
            buckets=alpha_buckets,
            labeled_outcome_csv_path=alpha_raw.get("labeled_outcome_csv_path"),
            top_fraction=float(alpha_raw.get("top_fraction", 0.2)),
            min_predicted_pnl=float(alpha_raw.get("min_predicted_pnl", 0.0)),
            market_family=str(alpha_raw.get("market_family", "btc")),
            market_duration_bucket=str(alpha_raw.get("market_duration_bucket", "5m")),
            min_seconds_to_resolution=float(alpha_raw.get("min_seconds_to_resolution", 30.0)),
            external_market_data_dir=alpha_raw.get("external_market_data_dir"),
        ),
    )


def build_live_paper_app(config_path: str) -> MultiTargetLivePaperApp:
    return MultiTargetLivePaperApp(load_live_paper_config(config_path))


class _LiveAlphaFilterRuntime:
    def __init__(self, config: LivePaperAlphaFilter) -> None:
        self.config = config
        bucket_configs = self._bucket_configs(config)
        if not bucket_configs:
            raise ValueError("alpha_filter requires at least one bucket configuration")
        self.bucket_runtimes: Dict[tuple[Optional[str], str, str], _LiveAlphaBucketRuntime] = {}
        for bucket in bucket_configs:
            target_names = list(bucket.target_names or [None])
            for target_name in target_names:
                key = (target_name, bucket.market_family, bucket.market_duration_bucket)
                self.bucket_runtimes[key] = _LiveAlphaBucketRuntime(bucket)

    def evaluate(self, target_name: str, trade: TradeActivity) -> Dict[str, object]:
        key = (target_name, trade.market_family, trade.market_duration_bucket)
        bucket_runtime = self.bucket_runtimes.get(key)
        if bucket_runtime is None:
            key = (None, trade.market_family, trade.market_duration_bucket)
            bucket_runtime = self.bucket_runtimes.get(key)
        if bucket_runtime is None:
            return {
                "should_follow": False,
                "reason": "skip_alpha_market_filter",
                "predicted_pnl_per_stake_usdc": None,
                "prediction_threshold": None,
            }
        return bucket_runtime.evaluate(trade)

    @staticmethod
    def _bucket_configs(config: LivePaperAlphaFilter) -> List[LivePaperAlphaBucket]:
        if config.buckets:
            return list(config.buckets)
        if not config.labeled_outcome_csv_path:
            return []
        return [
            LivePaperAlphaBucket(
                labeled_outcome_csv_path=str(config.labeled_outcome_csv_path),
                top_fraction=float(config.top_fraction),
                min_predicted_pnl=float(config.min_predicted_pnl),
                market_family=str(config.market_family),
                market_duration_bucket=str(config.market_duration_bucket),
                min_seconds_to_resolution=float(config.min_seconds_to_resolution),
                external_market_data_dir=config.external_market_data_dir,
                target_names=None,
            )
        ]


class _LiveAlphaBucketRuntime:
    def __init__(self, config: LivePaperAlphaBucket) -> None:
        self.config = config
        self.price_lookup = PriceHistoryLookup(None)
        self.external_market_lookup = ExternalMarketDataLookup(config.external_market_data_dir)
        self.overall_trackers = {window: RollingWindowTracker() for window in (60, 300, 900)}
        self.condition_trackers: Dict[str, Dict[int, RollingWindowTracker]] = defaultdict(_make_window_tracker_map)
        self.condition_outcome_trackers: Dict[tuple, Dict[int, RollingWindowTracker]] = defaultdict(_make_window_tracker_map)
        self.market_trackers: Dict[str, Dict[int, RollingWindowTracker]] = defaultdict(_make_window_tracker_map)
        self.market_outcome_trackers: Dict[tuple, Dict[int, RollingWindowTracker]] = defaultdict(_make_window_tracker_map)
        self.prev_trade_ts_by_wallet: Dict[str, int] = {}
        self.prev_trade_ts_by_market: Dict[str, int] = {}
        self.prev_trade_ts_by_condition: Dict[str, int] = {}
        self.prev_trade_ts_by_market_outcome: Dict[tuple, int] = {}

        frame, feature_columns, _ = _load_regression_frame(
            input_csv_path=config.labeled_outcome_csv_path,
            max_rows=None,
            feature_columns=_DEFAULT_FEATURE_COLUMNS,
            target_column="pnl_per_stake_usdc",
        )
        filtered = frame[
            frame["market_family"].astype(str).eq(config.market_family)
            & frame["market_duration_bucket"].astype(str).eq(config.market_duration_bucket)
        ].copy()
        if filtered.empty:
            raise ValueError("alpha filter training frame is empty after market filtering")
        self.feature_columns = [column for column in feature_columns if column in filtered.columns]
        self.model = _build_regression_pipeline(self.feature_columns)
        self.model.fit(filtered[self.feature_columns], filtered["pnl_per_stake_usdc"].astype(float))
        train_predictions = self.model.predict(filtered[self.feature_columns])
        quantile = max(0.0, min(1.0, 1.0 - float(config.top_fraction)))
        self.prediction_threshold = float(pd.Series(train_predictions).quantile(quantile))

    def evaluate(self, trade: TradeActivity) -> Dict[str, object]:
        row = _build_feature_row(
            trade=trade,
            label_buy=1,
            candidate_outcome=trade.outcome,
            candidate_asset=trade.asset,
            candidate_price=trade.price,
            source_kind="live_alpha_candidate",
            price_lookup=self.price_lookup,
            external_market_lookup=self.external_market_lookup,
            overall_trackers=self.overall_trackers,
            condition_trackers=self.condition_trackers,
            condition_outcome_trackers=self.condition_outcome_trackers,
            market_trackers=self.market_trackers,
            market_outcome_trackers=self.market_outcome_trackers,
            prev_trade_ts_by_wallet=self.prev_trade_ts_by_wallet,
            prev_trade_ts_by_market=self.prev_trade_ts_by_market,
            prev_trade_ts_by_condition=self.prev_trade_ts_by_condition,
            prev_trade_ts_by_market_outcome=self.prev_trade_ts_by_market_outcome,
        )
        _observe_trade(
            trade=trade,
            overall_trackers=self.overall_trackers,
            condition_trackers=self.condition_trackers,
            condition_outcome_trackers=self.condition_outcome_trackers,
            market_trackers=self.market_trackers,
            market_outcome_trackers=self.market_outcome_trackers,
            prev_trade_ts_by_wallet=self.prev_trade_ts_by_wallet,
            prev_trade_ts_by_market=self.prev_trade_ts_by_market,
            prev_trade_ts_by_condition=self.prev_trade_ts_by_condition,
            prev_trade_ts_by_market_outcome=self.prev_trade_ts_by_market_outcome,
        )
        row_frame = pd.DataFrame([row])
        predicted_pnl = float(self.model.predict(row_frame[self.feature_columns])[0])
        seconds_to_resolution = float(row.get("seconds_to_resolution") or 0.0)
        if seconds_to_resolution < float(self.config.min_seconds_to_resolution):
            return {
                "should_follow": False,
                "reason": "skip_alpha_resolution_too_near",
                "predicted_pnl_per_stake_usdc": round(predicted_pnl, 6),
                "prediction_threshold": round(self.prediction_threshold, 6),
                "seconds_to_resolution": seconds_to_resolution,
                "alpha_bucket": self.config.name or f"{self.config.market_family}_{self.config.market_duration_bucket}",
            }
        threshold = max(self.prediction_threshold, float(self.config.min_predicted_pnl))
        should_follow = predicted_pnl >= threshold
        return {
            "should_follow": bool(should_follow),
            "reason": "alpha_follow" if should_follow else "skip_alpha_score_too_low",
            "predicted_pnl_per_stake_usdc": round(predicted_pnl, 6),
            "prediction_threshold": round(threshold, 6),
            "seconds_to_resolution": seconds_to_resolution,
            "alpha_bucket": self.config.name or f"{self.config.market_family}_{self.config.market_duration_bucket}",
        }
