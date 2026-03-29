from __future__ import annotations

import csv
import json
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from .api import PolymarketPublicApi, extract_market_tokens_with_outcomes
from .config.skeleton_assembler import load_skeleton
from .market_ws import build_market_stream
from .models import OrderBook, TradeActivity
from .pair_unit_strategy import PairUnitStrategy
from .resolve import resolve_target_wallet
from .store import EventSink


@dataclass
class PairLivePaperTarget:
    name: str
    profile: Optional[str]
    wallet: Optional[str]


@dataclass
class PairLivePaperRuntime:
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
class PairLivePaperPortfolio:
    initial_capital_usdc: float


@dataclass
class PairScannerConfig:
    families: List[str] = field(default_factory=lambda: ["btc", "eth", "sol", "xrp"])
    durations: List[str] = field(default_factory=lambda: ["hourly"])
    skeleton_config_path: Optional[str] = None
    pair_stake_usdc: float = 100.0
    max_effective_pair_sum: float = 1.0
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    max_entries_per_market: int = 1
    observe_target_activity: bool = True
    market_trigger_enabled: bool = False
    market_search_limit: int = 10
    min_seconds_to_resolution: Optional[int] = None
    max_seconds_to_resolution: Optional[int] = None
    staged_entry_enabled: bool = False
    first_leg_price_floor: float = 0.4
    first_leg_price_ceiling: float = 0.6
    max_leg_wait_seconds: int = 60
    rebalance_enabled: bool = False
    rebalance_stake_usdc: Optional[float] = None
    max_rebalances_per_market: int = 1
    rebalance_window_seconds: int = 180
    rebalance_min_improve: float = 0.005


@dataclass
class PairLivePaperConfig:
    target: PairLivePaperTarget
    runtime: PairLivePaperRuntime
    portfolio: PairLivePaperPortfolio
    scanner: PairScannerConfig


@dataclass
class PairPosition:
    market_slug: str
    condition_id: str
    up_asset: str
    down_asset: str
    up_outcome: str
    down_outcome: str
    share_count: float
    cost_usdc: float
    effective_pair_sum: float
    resolution_timestamp_seconds: Optional[int] = None
    up_share_count: float = 0.0
    down_share_count: float = 0.0
    up_cost_usdc: float = 0.0
    down_cost_usdc: float = 0.0
    opened_at_seconds: int = 0
    paired_at_seconds: int = 0
    rebalance_count: int = 0


@dataclass
class PairPendingEntry:
    market_slug: str
    condition_id: str
    event_slug: str
    market_family: str
    market_duration_bucket: str
    up_asset: str
    down_asset: str
    up_outcome: str
    down_outcome: str
    first_leg_outcome: str
    first_leg_asset: str
    first_leg_price: float
    first_leg_book_source: str
    opened_at_seconds: int
    resolution_timestamp_seconds: Optional[int] = None
    unit_id: str = ""
    strategy_reason: str = ""


@dataclass
class PairLivePaperState:
    started_at_seconds: int = 0
    end_at_seconds: int = 0
    next_hourly_snapshot_seconds: int = 0
    cash_usdc: float = 0.0
    last_heartbeat_timestamp_seconds: int = 0
    target_wallet: str = ""
    target_last_event_timestamp_ms: int = 0
    seen_keys: List[str] = field(default_factory=list)
    positions: Dict[str, PairPosition] = field(default_factory=dict)
    pending_entries: Dict[str, PairPendingEntry] = field(default_factory=dict)
    market_entry_counts: Dict[str, int] = field(default_factory=dict)

    def remember(self, key: str, max_size: int = 20000) -> None:
        self.seen_keys.append(key)
        if len(self.seen_keys) > max_size:
            self.seen_keys = self.seen_keys[-max_size:]


@dataclass
class PairMarketDescriptor:
    market_slug: str
    condition_id: str
    up_outcome: str
    down_outcome: str
    up_asset: str
    down_asset: str
    resolution_timestamp_seconds: Optional[int]


@dataclass
class _ObservedPairContext:
    market_slug: str
    condition_id: str
    outcomes_to_assets: Dict[str, str] = field(default_factory=dict)


@dataclass
class PairExecutionPlan:
    raw_pair_sum: float
    effective_pair_sum: float
    share_count: float
    up_usdc: float
    down_usdc: float
    locked_payout_usdc: float
    locked_pnl_usdc: float


@dataclass
class PairFirstLegCandidate:
    outcome: str
    asset: str
    ask_price: float
    book_source: str


class PairHourlyStatsWriter:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.fieldnames = [
            "timestamp_seconds",
            "timestamp_iso",
            "equity_usdc",
            "cash_usdc",
            "holdings_value_usdc",
            "return_pct",
            "positions",
        ]

    def append(self, row: Dict[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self.path.exists()
        with self.path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=self.fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)


class PairLivePaperStateStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def load(self) -> PairLivePaperState:
        if not self.path.exists():
            return PairLivePaperState()
        with self.path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        positions = {
            str(key): PairPosition(**payload)
            for key, payload in dict(raw.get("positions", {})).items()
        }
        pending_entries = {
            str(key): PairPendingEntry(**payload)
            for key, payload in dict(raw.get("pending_entries", {})).items()
        }
        return PairLivePaperState(
            started_at_seconds=int(raw.get("started_at_seconds", 0)),
            end_at_seconds=int(raw.get("end_at_seconds", 0)),
            next_hourly_snapshot_seconds=int(raw.get("next_hourly_snapshot_seconds", 0)),
            cash_usdc=float(raw.get("cash_usdc", 0.0)),
            last_heartbeat_timestamp_seconds=int(raw.get("last_heartbeat_timestamp_seconds", 0)),
            target_wallet=str(raw.get("target_wallet", "")),
            target_last_event_timestamp_ms=int(raw.get("target_last_event_timestamp_ms", 0)),
            seen_keys=list(raw.get("seen_keys", [])),
            positions=positions,
            pending_entries=pending_entries,
            market_entry_counts={str(k): int(v) for k, v in dict(raw.get("market_entry_counts", {})).items()},
        )

    def save(self, state: PairLivePaperState) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as handle:
            json.dump(asdict(state), handle, ensure_ascii=False, indent=2, sort_keys=True)


def compute_pair_execution_plan(
    *,
    up_ask: float,
    down_ask: float,
    stake_usdc: float,
    fee_bps: float,
    slippage_bps: float,
    max_effective_pair_sum: float,
) -> Optional[PairExecutionPlan]:
    if up_ask <= 0 or down_ask <= 0 or stake_usdc <= 0:
        return None
    raw_pair_sum = up_ask + down_ask
    effective_pair_sum = raw_pair_sum * (1.0 + (float(fee_bps) + float(slippage_bps)) / 10000.0)
    if effective_pair_sum > float(max_effective_pair_sum):
        return None
    share_count = float(stake_usdc) / effective_pair_sum
    up_usdc = share_count * up_ask
    down_usdc = share_count * down_ask
    locked_payout_usdc = share_count
    locked_pnl_usdc = locked_payout_usdc - float(stake_usdc)
    return PairExecutionPlan(
        raw_pair_sum=raw_pair_sum,
        effective_pair_sum=effective_pair_sum,
        share_count=share_count,
        up_usdc=up_usdc,
        down_usdc=down_usdc,
        locked_payout_usdc=locked_payout_usdc,
        locked_pnl_usdc=locked_pnl_usdc,
    )


def compute_rebalanced_effective_pair_sum(
    *,
    current_cost_usdc: float,
    current_share_count: float,
    additional_cost_usdc: float,
    additional_share_count: float,
) -> Optional[float]:
    total_share_count = float(current_share_count) + float(additional_share_count)
    if total_share_count <= 0:
        return None
    return (float(current_cost_usdc) + float(additional_cost_usdc)) / total_share_count


def select_first_leg_candidate(
    *,
    up_ask: float,
    down_ask: float,
    up_asset: str,
    down_asset: str,
    up_outcome: str,
    down_outcome: str,
    up_book_source: str,
    down_book_source: str,
    preferred_outcome: Optional[str],
    price_floor: float,
    price_ceiling: float,
) -> Optional[PairFirstLegCandidate]:
    candidates: List[PairFirstLegCandidate] = []
    if float(price_floor) <= up_ask <= float(price_ceiling):
        candidates.append(
            PairFirstLegCandidate(
                outcome=up_outcome,
                asset=up_asset,
                ask_price=up_ask,
                book_source=up_book_source,
            )
        )
    if float(price_floor) <= down_ask <= float(price_ceiling):
        candidates.append(
            PairFirstLegCandidate(
                outcome=down_outcome,
                asset=down_asset,
                ask_price=down_ask,
                book_source=down_book_source,
            )
        )
    if not candidates:
        return None
    normalized_preferred = str(preferred_outcome or "").strip().lower()
    for candidate in candidates:
        if candidate.outcome.lower() == normalized_preferred:
            return candidate
    return sorted(candidates, key=lambda item: (abs(item.ask_price - 0.5), item.ask_price))[0]


class PairLivePaperApp:
    def __init__(self, config: PairLivePaperConfig) -> None:
        self.config = config
        self.api = PolymarketPublicApi(timeout_seconds=config.runtime.request_timeout_seconds)
        self.events = EventSink(config.runtime.event_log_path)
        self.store = PairLivePaperStateStore(config.runtime.state_path)
        self.state = self.store.load()
        self.market_stream = build_market_stream() if config.runtime.market_websocket_enabled else None
        self.strategy_bundle = load_skeleton(config.scanner.skeleton_config_path)
        self.rule_engine = PairUnitStrategy(self.strategy_bundle)
        self.target = self._resolve_target()
        self.hourly_stats = PairHourlyStatsWriter(config.runtime.hourly_stats_path)
        self._event_market_cache: Dict[str, List[PairMarketDescriptor]] = {}
        self._observed_pair_contexts: Dict[str, _ObservedPairContext] = {}
        self._initialize_session()
        self._rehydrate_pending_entries()
        self.events.write(
            "pair_strategy_bundle_loaded",
            {
                "bundle_id": self.strategy_bundle.get("bundle_id"),
                "skeleton_config_path": config.scanner.skeleton_config_path or "default",
                "families": self.rule_engine.families_configured(),
            },
        )
        if self.market_stream is None:
            self.events.write("market_ws_disabled", {"reason": "disabled_in_config"})
        elif not self.market_stream.available:
            self.events.write("market_ws_disabled", {"reason": self.market_stream.reason})
        else:
            self.events.write("market_ws_enabled", {"mode": "market_channel"})

    def doctor(self) -> List[str]:
        return [
            "mode=pair_live_paper",
            f"target={self.target.name}",
            f"market_ws_enabled={bool(self.market_stream and self.market_stream.available)}",
            f"max_effective_pair_sum={self.config.scanner.max_effective_pair_sum}",
            f"pair_stake_usdc={self.config.scanner.pair_stake_usdc}",
            f"families={','.join(self.config.scanner.families)}",
            f"durations={','.join(self.config.scanner.durations)}",
            f"skeleton_config_path={self.config.scanner.skeleton_config_path or 'default'}",
            f"active_strategy_families={','.join(self.rule_engine.families_configured())}",
            f"pending_units={self.rule_engine.pending_unit_count()}",
            f"observe_target_activity={self.config.scanner.observe_target_activity}",
            f"market_trigger_enabled={self.config.scanner.market_trigger_enabled}",
            f"min_seconds_to_resolution={self.config.scanner.min_seconds_to_resolution}",
            f"max_seconds_to_resolution={self.config.scanner.max_seconds_to_resolution}",
            f"staged_entry_enabled={self.config.scanner.staged_entry_enabled}",
            f"legacy_first_leg_price_floor={self.config.scanner.first_leg_price_floor}",
            f"legacy_first_leg_price_ceiling={self.config.scanner.first_leg_price_ceiling}",
            f"legacy_max_leg_wait_seconds={self.config.scanner.max_leg_wait_seconds}",
            f"rebalance_enabled={self.config.scanner.rebalance_enabled}",
            f"max_rebalances_per_market={self.config.scanner.max_rebalances_per_market}",
            f"rebalance_window_seconds={self.config.scanner.rebalance_window_seconds}",
            f"rebalance_min_improve={self.config.scanner.rebalance_min_improve}",
            f"state_path={self.config.runtime.state_path}",
            f"end_at={_isoformat(self.state.end_at_seconds)}",
        ]

    def run(self, once: bool = False) -> None:
        try:
            while True:
                now_seconds = int(time.time())
                self._emit_heartbeat_if_due(now_seconds)
                self._redeem_resolved_positions(now_seconds)
                if self.config.scanner.market_trigger_enabled:
                    self._scan_hot_markets()
                if self.config.scanner.observe_target_activity:
                    self._poll_target()
                self._redeem_resolved_positions(int(time.time()))
                self._record_hourly_snapshot_if_due(int(time.time()))
                self.store.save(self.state)
                if once or int(time.time()) >= self.state.end_at_seconds:
                    self._record_final_snapshot()
                    self.store.save(self.state)
                    return
                time.sleep(self.config.runtime.poll_interval_seconds)
        finally:
            if self.market_stream is not None:
                self.market_stream.close()

    def _resolve_target(self) -> PairLivePaperTarget:
        wallet, details = resolve_target_wallet(self.api, self.config.target.profile, self.config.target.wallet)
        self.state.target_wallet = wallet
        self.events.write(
            "target_resolved",
            {"name": self.config.target.name, "wallet": wallet, "details": details},
        )
        return PairLivePaperTarget(
            name=self.config.target.name,
            profile=self.config.target.profile,
            wallet=wallet,
        )

    def _initialize_session(self) -> None:
        now_seconds = int(time.time())
        if self.state.started_at_seconds <= 0:
            self.state.started_at_seconds = now_seconds
            self.state.end_at_seconds = now_seconds + int(self.config.runtime.duration_hours * 3600)
            self.state.next_hourly_snapshot_seconds = _next_hour_mark(now_seconds)
        elif self.state.end_at_seconds <= 0:
            self.state.end_at_seconds = self.state.started_at_seconds + int(self.config.runtime.duration_hours * 3600)
        if self.state.cash_usdc <= 0 and not self.state.positions:
            self.state.cash_usdc = self.config.portfolio.initial_capital_usdc

    def _start_cursor_ms(self) -> int:
        last_seen = int(self.state.target_last_event_timestamp_ms)
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

    def _poll_target(self) -> None:
        wallet = self.target.wallet or self.state.target_wallet
        recent_start = self._start_cursor_ms()
        try:
            trades = self.api.get_activity(
                wallet=wallet,
                limit=self.config.runtime.activity_limit,
                start_ms=recent_start,
                side=None,
                sort_direction="DESC",
            )
            trades = self._aggregate_trades(trades)
            self.events.write(
                "poll_ok",
                {
                    "target_name": self.target.name,
                    "wallet": wallet,
                    "start_ms": recent_start,
                    "trades_seen": len(trades),
                },
            )
            self._observe_pair_contexts(trades)
            for trade in sorted(trades, key=lambda item: item.timestamp_ms):
                self._handle_trade(trade)
        except Exception as exc:
            self.events.write(
                "poll_error",
                {
                    "target_name": self.target.name,
                    "wallet": wallet,
                    "start_ms": recent_start,
                    "error": str(exc),
                },
            )

    def _scan_hot_markets(self) -> None:
        event_slugs = self._discover_hot_event_slugs()
        self.events.write(
            "pair_market_scan",
            {
                "target_name": self.target.name,
                "families": list(self.config.scanner.families),
                "durations": list(self.config.scanner.durations),
                "event_slugs_seen": len(event_slugs),
            },
        )
        for event_slug in event_slugs:
            descriptors = self._event_market_cache.get(event_slug)
            if descriptors is None:
                descriptors = self._load_event_market_descriptors(event_slug)
                self._event_market_cache[event_slug] = descriptors
                if descriptors:
                    self.events.write(
                        "pair_event_descriptors_loaded",
                        {"event_slug": event_slug, "count": len(descriptors)},
                    )
                else:
                    self.events.write(
                        "pair_event_descriptors_empty",
                        {"event_slug": event_slug},
                    )
            for descriptor in descriptors:
                market_family = _market_family_from_text(descriptor.market_slug)
                market_duration_bucket = _market_duration_bucket_from_text(descriptor.market_slug)
                if market_family not in set(self.config.scanner.families):
                    self.events.write(
                        "pair_descriptor_filtered_family",
                        {"market_slug": descriptor.market_slug, "family": market_family},
                    )
                    continue
                if market_duration_bucket not in set(self.config.scanner.durations):
                    self.events.write(
                        "pair_descriptor_filtered_duration",
                        {"market_slug": descriptor.market_slug, "bucket": market_duration_bucket},
                    )
                    continue
                market_key = descriptor.market_slug or descriptor.condition_id
                if (
                    market_key not in self.state.positions
                    and market_key not in self.state.pending_entries
                    and int(self.state.market_entry_counts.get(market_key, 0)) >= int(self.config.scanner.max_entries_per_market)
                ):
                    continue
                self._evaluate_pair_descriptor(
                    descriptor=descriptor,
                    market_family=market_family,
                    market_duration_bucket=market_duration_bucket,
                    event_slug=event_slug,
                    trigger_mode="market_scan",
                    reference_payload={
                        "target_name": self.target.name,
                        "market_slug": descriptor.market_slug,
                        "event_slug": event_slug,
                        "market_family": market_family,
                        "market_duration_bucket": market_duration_bucket,
                        "outcome": None,
                        "asset_id": None,
                        "trade_timestamp_seconds": int(time.time()),
                    },
                )

    def _discover_hot_event_slugs(self) -> List[str]:
        event_slugs: set[str] = set()
        for query in self._build_hot_market_queries(self.config.scanner.families):
            try:
                events = self.api.public_search_events(
                    query,
                    limit_per_type=max(int(self.config.scanner.market_search_limit), 1),
                )
            except Exception as exc:
                self.events.write(
                    "pair_market_search_error",
                    {"query": query, "error": str(exc)},
                )
                continue
            for event in events:
                slug = str(event.get("slug") or "").strip()
                title = str(event.get("title") or event.get("name") or slug).strip()
                haystack = f"{slug} {title}".strip()
                if not slug:
                    continue
                if _market_family_from_text(haystack) not in set(self.config.scanner.families):
                    continue
                if _market_duration_bucket_from_text(haystack) not in set(self.config.scanner.durations):
                    continue
                event_slugs.add(slug)
        return sorted(event_slugs)

    @staticmethod
    def _build_hot_market_queries(families: List[str]) -> List[str]:
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
            query_map[str(family).strip().lower()]
            for family in families
            if str(family).strip().lower() in query_map
        }
        return sorted(queries)

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
                    price=total_usdc / total_size if total_size > 0 else latest.price,
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

    def _handle_trade(self, trade: TradeActivity) -> None:
        if trade.dedupe_key in set(self.state.seen_keys):
            return
        if trade.side.upper() != "BUY":
            self.state.target_last_event_timestamp_ms = max(self.state.target_last_event_timestamp_ms, trade.timestamp_seconds)
            self.state.remember(trade.dedupe_key)
            return
        if trade.market_family not in set(self.config.scanner.families):
            self.events.write("pair_scanner_decision", self._decision_payload(trade, "skip_family_filter"))
            self._finalize_trade_cursor(trade)
            return
        if trade.market_duration_bucket not in set(self.config.scanner.durations):
            self.events.write("pair_scanner_decision", self._decision_payload(trade, "skip_duration_filter"))
            self._finalize_trade_cursor(trade)
            return
        market_key = trade.slug or trade.event_slug or trade.condition_id
        if (
            market_key not in self.state.positions
            and market_key not in self.state.pending_entries
            and int(self.state.market_entry_counts.get(market_key, 0)) >= int(self.config.scanner.max_entries_per_market)
        ):
            self.events.write("pair_scanner_decision", self._decision_payload(trade, "skip_market_entry_limit"))
            self._finalize_trade_cursor(trade)
            return

        descriptor = self._resolve_pair_descriptor(trade)
        if descriptor is None:
            self.events.write("pair_scanner_decision", self._decision_payload(trade, "skip_pair_descriptor_missing"))
            self._finalize_trade_cursor(trade)
            return
        self._evaluate_pair_descriptor(
            descriptor=descriptor,
            market_family=trade.market_family,
            market_duration_bucket=trade.market_duration_bucket,
            event_slug=trade.event_slug,
            trigger_mode="target_activity",
            reference_payload=self._decision_payload(trade, "pending"),
        )
        self._finalize_trade_cursor(trade)

    def _family_strategy(self, market_family: str):
        return self.rule_engine.families.get(market_family)

    def _register_market_open(
        self,
        *,
        descriptor: PairMarketDescriptor,
        market_family: str,
        market_duration_bucket: str,
        now_seconds: int,
    ) -> None:
        open_timestamp = _infer_market_open_timestamp_seconds(
            descriptor.resolution_timestamp_seconds,
            market_duration_bucket,
        )
        if open_timestamp is None:
            open_timestamp = float(now_seconds)
        self.rule_engine.on_market_open(descriptor.market_slug, market_family, float(open_timestamp))

    def _effective_resolution_window(
        self,
        *,
        market_family: str,
    ) -> tuple[Optional[int], Optional[int]]:
        family_cfg = self._family_strategy(market_family)
        family_min = family_cfg.entry.min_seconds_to_resolution if family_cfg is not None else None
        scanner_min = self.config.scanner.min_seconds_to_resolution
        scanner_max = self.config.scanner.max_seconds_to_resolution

        min_candidates = [value for value in (family_min, scanner_min) if value is not None]
        effective_min = max(min_candidates) if min_candidates else None
        return effective_min, scanner_max

    def _rehydrate_pending_entries(self) -> None:
        for market_key, pending_entry in list(self.state.pending_entries.items()):
            if not pending_entry.market_slug:
                self.state.pending_entries.pop(market_key, None)
                continue
            open_timestamp = _infer_market_open_timestamp_seconds(
                pending_entry.resolution_timestamp_seconds,
                pending_entry.market_duration_bucket,
            )
            if open_timestamp is None:
                open_timestamp = int(pending_entry.opened_at_seconds)
            self.rule_engine.on_market_open(
                pending_entry.market_slug,
                pending_entry.market_family,
                float(open_timestamp),
            )
            decision = self.rule_engine.open_market_candidate(
                market_slug=pending_entry.market_slug,
                family=pending_entry.market_family,
                candidate_price=float(pending_entry.first_leg_price),
                candidate_usdc_size=float(self.config.scanner.pair_stake_usdc),
                current_time=float(pending_entry.opened_at_seconds),
            )
            if decision.action != "enter_first_leg" or decision.unit_id is None:
                self.events.write(
                    "pending_entry_rehydrate_skip",
                    {
                        "market_slug": pending_entry.market_slug,
                        "reason": decision.reason,
                    },
                )
                self.state.pending_entries.pop(market_key, None)
                continue
            pending_entry.unit_id = decision.unit_id

    def _tick_pending_entry(
        self,
        pending_entry: PairPendingEntry,
        *,
        trigger_mode: str,
        reference_payload: Dict[str, object],
        now_seconds: int,
    ) -> bool:
        decisions = self.rule_engine.tick(pending_entry.market_slug, float(now_seconds))
        for decision in decisions:
            if decision.unit_id != pending_entry.unit_id or decision.action != "timeout":
                continue
            self._expire_pending_entry(
                pending_entry,
                trigger_mode=trigger_mode,
                reference_payload=reference_payload,
                strategy_reason=decision.reason,
            )
            return True
        return False

    def _evaluate_pair_descriptor(
        self,
        *,
        descriptor: PairMarketDescriptor,
        market_family: str,
        market_duration_bucket: str,
        event_slug: str,
        trigger_mode: str,
        reference_payload: Dict[str, object],
    ) -> None:
        family_cfg = self._family_strategy(market_family)
        if family_cfg is None:
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": "skip_family_not_in_skeleton",
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return

        market_key = descriptor.market_slug or descriptor.condition_id
        existing_position = self.state.positions.get(market_key)
        pending_entry = self.state.pending_entries.get(market_key)
        if (
            existing_position is None
            and pending_entry is None
            and int(self.state.market_entry_counts.get(market_key, 0)) >= int(self.config.scanner.max_entries_per_market)
        ):
            payload = dict(reference_payload)
            payload.update({"reason": "skip_market_entry_limit", "trigger_mode": trigger_mode})
            self.events.write("pair_scanner_decision", payload)
            return
        now_seconds = int(time.time())
        self._register_market_open(
            descriptor=descriptor,
            market_family=market_family,
            market_duration_bucket=market_duration_bucket,
            now_seconds=now_seconds,
        )
        resolution_ts = descriptor.resolution_timestamp_seconds
        seconds_to_resolution = (
            resolution_ts - now_seconds if resolution_ts is not None else None
        )
        min_seconds_to_resolution, max_seconds_to_resolution = self._effective_resolution_window(
            market_family=market_family,
        )
        if (
            seconds_to_resolution is not None
            and (
                (
                    min_seconds_to_resolution is not None
                    and seconds_to_resolution < int(min_seconds_to_resolution)
                )
                or (
                    max_seconds_to_resolution is not None
                    and seconds_to_resolution > int(max_seconds_to_resolution)
                )
            )
        ):
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": "skip_resolution_window",
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "resolution_timestamp_seconds": resolution_ts,
                    "seconds_to_resolution": seconds_to_resolution,
                    "effective_min_seconds_to_resolution": min_seconds_to_resolution,
                    "effective_max_seconds_to_resolution": max_seconds_to_resolution,
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return

        up_book, up_source = self._order_book_for_asset(descriptor.up_asset)
        down_book, down_source = self._order_book_for_asset(descriptor.down_asset)
        if up_book is None or down_book is None:
            if pending_entry is not None and self._tick_pending_entry(
                pending_entry,
                trigger_mode=trigger_mode,
                reference_payload=reference_payload,
                now_seconds=now_seconds,
            ):
                return
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": "skip_missing_pair_order_book",
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "seconds_to_resolution": seconds_to_resolution,
                    "up_asset": descriptor.up_asset,
                    "down_asset": descriptor.down_asset,
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return

        up_ask = up_book.best_ask
        down_ask = down_book.best_ask
        if up_ask is None or down_ask is None:
            if pending_entry is not None and self._tick_pending_entry(
                pending_entry,
                trigger_mode=trigger_mode,
                reference_payload=reference_payload,
                now_seconds=now_seconds,
            ):
                return
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": "skip_missing_pair_ask",
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "seconds_to_resolution": seconds_to_resolution,
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return

        if existing_position is not None:
            self._maybe_rebalance_position(
                descriptor=descriptor,
                position=existing_position,
                up_ask=up_ask,
                down_ask=down_ask,
                up_source=up_source,
                down_source=down_source,
                market_family=market_family,
                market_duration_bucket=market_duration_bucket,
                event_slug=event_slug,
                trigger_mode=trigger_mode,
                reference_payload=reference_payload,
                seconds_to_resolution=seconds_to_resolution,
                now_seconds=now_seconds,
            )
            return

        if pending_entry is not None:
            self._maybe_complete_pending_entry(
                descriptor=descriptor,
                pending_entry=pending_entry,
                up_ask=up_ask,
                down_ask=down_ask,
                up_source=up_source,
                down_source=down_source,
                market_family=market_family,
                market_duration_bucket=market_duration_bucket,
                event_slug=event_slug,
                trigger_mode=trigger_mode,
                reference_payload=reference_payload,
                seconds_to_resolution=seconds_to_resolution,
                now_seconds=now_seconds,
            )
            return

        if self.config.scanner.staged_entry_enabled:
            self._maybe_open_pending_entry(
                descriptor=descriptor,
                up_ask=up_ask,
                down_ask=down_ask,
                up_source=up_source,
                down_source=down_source,
                market_family=market_family,
                market_duration_bucket=market_duration_bucket,
                event_slug=event_slug,
                trigger_mode=trigger_mode,
                reference_payload=reference_payload,
                seconds_to_resolution=seconds_to_resolution,
                now_seconds=now_seconds,
            )
            return

        candidate = select_first_leg_candidate(
            up_ask=up_ask,
            down_ask=down_ask,
            up_asset=descriptor.up_asset,
            down_asset=descriptor.down_asset,
            up_outcome=descriptor.up_outcome,
            down_outcome=descriptor.down_outcome,
            up_book_source=up_source,
            down_book_source=down_source,
            preferred_outcome=str(reference_payload.get("outcome") or "") or None,
            price_floor=family_cfg.entry.price_band_lower,
            price_ceiling=family_cfg.entry.price_band_upper,
        )
        if candidate is None:
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": "skip_first_leg_price_window",
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "seconds_to_resolution": seconds_to_resolution,
                    "up_ask": round(up_ask, 6),
                    "down_ask": round(down_ask, 6),
                    "skeleton_price_band": [
                        family_cfg.entry.price_band_lower,
                        family_cfg.entry.price_band_upper,
                    ],
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return

        entry_decision = self.rule_engine.open_market_candidate(
            market_slug=descriptor.market_slug,
            family=market_family,
            candidate_price=float(candidate.ask_price),
            candidate_usdc_size=float(self.config.scanner.pair_stake_usdc),
            current_time=float(now_seconds),
        )
        if entry_decision.action != "enter_first_leg":
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": entry_decision.reason,
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "seconds_to_resolution": seconds_to_resolution,
                    "first_leg_outcome": candidate.outcome,
                    "first_leg_price": round(candidate.ask_price, 6),
                    "strategy_details": entry_decision.details,
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return

        plan = compute_pair_execution_plan(
            up_ask=up_ask,
            down_ask=down_ask,
            stake_usdc=self.config.scanner.pair_stake_usdc,
            fee_bps=self.config.scanner.fee_bps,
            slippage_bps=self.config.scanner.slippage_bps,
            max_effective_pair_sum=self.config.scanner.max_effective_pair_sum,
        )
        if plan is None:
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": "skip_pair_sum_too_high",
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "seconds_to_resolution": seconds_to_resolution,
                    "up_ask": round(up_ask, 6),
                    "down_ask": round(down_ask, 6),
                    "raw_pair_sum": round(up_ask + down_ask, 6),
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return
        if self.state.cash_usdc < self.config.scanner.pair_stake_usdc:
            payload = dict(reference_payload)
            payload.update({"reason": "skip_no_cash", "trigger_mode": trigger_mode})
            self.events.write("pair_scanner_decision", payload)
            return

        payload = dict(reference_payload)
        payload.update(
            {
                "reason": "follow_pair",
                "trigger_mode": trigger_mode,
                "market_slug": descriptor.market_slug,
                "event_slug": event_slug,
                "market_family": market_family,
                "market_duration_bucket": market_duration_bucket,
                "seconds_to_resolution": seconds_to_resolution,
                "up_asset": descriptor.up_asset,
                "down_asset": descriptor.down_asset,
                "up_outcome": descriptor.up_outcome,
                "down_outcome": descriptor.down_outcome,
                "up_ask": round(up_ask, 6),
                "down_ask": round(down_ask, 6),
                "up_order_book_source": up_source,
                "down_order_book_source": down_source,
                "raw_pair_sum": round(plan.raw_pair_sum, 6),
                "effective_pair_sum": round(plan.effective_pair_sum, 6),
                "locked_payout_usdc": round(plan.locked_payout_usdc, 6),
                "locked_pnl_usdc": round(plan.locked_pnl_usdc, 6),
            }
        )
        self.events.write("pair_scanner_decision", payload)
        self.state.cash_usdc -= self.config.scanner.pair_stake_usdc
        self.state.positions[market_key] = PairPosition(
            market_slug=descriptor.market_slug,
            condition_id=descriptor.condition_id,
            up_asset=descriptor.up_asset,
            down_asset=descriptor.down_asset,
            up_outcome=descriptor.up_outcome,
            down_outcome=descriptor.down_outcome,
            share_count=round(plan.share_count, 6),
            cost_usdc=float(self.config.scanner.pair_stake_usdc),
            effective_pair_sum=round(plan.effective_pair_sum, 6),
            resolution_timestamp_seconds=descriptor.resolution_timestamp_seconds,
            up_share_count=round(plan.share_count, 6),
            down_share_count=round(plan.share_count, 6),
            up_cost_usdc=round(plan.up_usdc, 6),
            down_cost_usdc=round(plan.down_usdc, 6),
            opened_at_seconds=now_seconds,
            paired_at_seconds=now_seconds,
            rebalance_count=0,
        )
        self.state.market_entry_counts[market_key] = int(self.state.market_entry_counts.get(market_key, 0)) + 1
        self.events.write(
            "pair_execution",
            {
                "target_name": self.target.name,
                "market_slug": descriptor.market_slug,
                "condition_id": descriptor.condition_id,
                "up_asset": descriptor.up_asset,
                "down_asset": descriptor.down_asset,
                "up_usdc": round(plan.up_usdc, 6),
                "down_usdc": round(plan.down_usdc, 6),
                "share_count": round(plan.share_count, 6),
                "effective_pair_sum": round(plan.effective_pair_sum, 6),
                "locked_payout_usdc": round(plan.locked_payout_usdc, 6),
                "locked_pnl_usdc": round(plan.locked_pnl_usdc, 6),
                "resolution_timestamp_seconds": descriptor.resolution_timestamp_seconds,
            },
        )

    def _maybe_open_pending_entry(
        self,
        *,
        descriptor: PairMarketDescriptor,
        up_ask: float,
        down_ask: float,
        up_source: str,
        down_source: str,
        market_family: str,
        market_duration_bucket: str,
        event_slug: str,
        trigger_mode: str,
        reference_payload: Dict[str, object],
        seconds_to_resolution: Optional[int],
        now_seconds: int,
    ) -> None:
        family_cfg = self._family_strategy(market_family)
        if family_cfg is None:
            payload = dict(reference_payload)
            payload.update({"reason": "skip_family_not_in_skeleton", "trigger_mode": trigger_mode})
            self.events.write("pair_scanner_decision", payload)
            return

        preferred_outcome = reference_payload.get("outcome")
        candidate = select_first_leg_candidate(
            up_ask=up_ask,
            down_ask=down_ask,
            up_asset=descriptor.up_asset,
            down_asset=descriptor.down_asset,
            up_outcome=descriptor.up_outcome,
            down_outcome=descriptor.down_outcome,
            up_book_source=up_source,
            down_book_source=down_source,
            preferred_outcome=str(preferred_outcome) if preferred_outcome is not None else None,
            price_floor=family_cfg.entry.price_band_lower,
            price_ceiling=family_cfg.entry.price_band_upper,
        )
        if candidate is None:
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": "skip_first_leg_price_window",
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "seconds_to_resolution": seconds_to_resolution,
                    "up_ask": round(up_ask, 6),
                    "down_ask": round(down_ask, 6),
                    "skeleton_price_band": [
                        family_cfg.entry.price_band_lower,
                        family_cfg.entry.price_band_upper,
                    ],
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return

        entry_decision = self.rule_engine.open_market_candidate(
            market_slug=descriptor.market_slug,
            family=market_family,
            candidate_price=float(candidate.ask_price),
            candidate_usdc_size=float(self.config.scanner.pair_stake_usdc),
            current_time=float(now_seconds),
        )
        if entry_decision.action != "enter_first_leg":
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": entry_decision.reason,
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "seconds_to_resolution": seconds_to_resolution,
                    "first_leg_outcome": candidate.outcome,
                    "first_leg_price": round(candidate.ask_price, 6),
                    "strategy_details": entry_decision.details,
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return

        market_key = descriptor.market_slug or descriptor.condition_id
        self.state.pending_entries[market_key] = PairPendingEntry(
            market_slug=descriptor.market_slug,
            condition_id=descriptor.condition_id,
            event_slug=event_slug,
            market_family=market_family,
            market_duration_bucket=market_duration_bucket,
            up_asset=descriptor.up_asset,
            down_asset=descriptor.down_asset,
            up_outcome=descriptor.up_outcome,
            down_outcome=descriptor.down_outcome,
            first_leg_outcome=candidate.outcome,
            first_leg_asset=candidate.asset,
            unit_id=str(entry_decision.unit_id or ""),
            first_leg_price=round(candidate.ask_price, 6),
            first_leg_book_source=candidate.book_source,
            opened_at_seconds=now_seconds,
            resolution_timestamp_seconds=descriptor.resolution_timestamp_seconds,
            strategy_reason=entry_decision.reason,
        )
        payload = dict(reference_payload)
        payload.update(
            {
                "reason": "pending_first_leg",
                "trigger_mode": trigger_mode,
                "market_slug": descriptor.market_slug,
                "event_slug": event_slug,
                "market_family": market_family,
                "market_duration_bucket": market_duration_bucket,
                "seconds_to_resolution": seconds_to_resolution,
                "first_leg_outcome": candidate.outcome,
                "first_leg_price": round(candidate.ask_price, 6),
                "unit_id": entry_decision.unit_id,
                "strategy_reason": entry_decision.reason,
                "strategy_details": entry_decision.details,
            }
        )
        self.events.write("pair_scanner_decision", payload)

    def _maybe_complete_pending_entry(
        self,
        *,
        descriptor: PairMarketDescriptor,
        pending_entry: PairPendingEntry,
        up_ask: float,
        down_ask: float,
        up_source: str,
        down_source: str,
        market_family: str,
        market_duration_bucket: str,
        event_slug: str,
        trigger_mode: str,
        reference_payload: Dict[str, object],
        seconds_to_resolution: Optional[int],
        now_seconds: int,
    ) -> None:
        opposite_ask = down_ask if pending_entry.first_leg_outcome == pending_entry.up_outcome else up_ask
        decision = self.rule_engine.evaluate_second_leg_completion(
            unit_id=pending_entry.unit_id,
            market_slug=descriptor.market_slug,
            current_price=float(opposite_ask),
            current_time=float(now_seconds),
        )
        if decision.action == "timeout":
            self._expire_pending_entry(
                pending_entry,
                trigger_mode=trigger_mode,
                reference_payload=reference_payload,
                strategy_reason=decision.reason,
            )
            return

        if decision.action != "complete_second_leg":
            payload = dict(reference_payload)
            payload.update(
                {
                    "reason": decision.reason,
                    "trigger_mode": trigger_mode,
                    "market_slug": descriptor.market_slug,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "seconds_to_resolution": seconds_to_resolution,
                    "first_leg_outcome": pending_entry.first_leg_outcome,
                    "first_leg_price": round(pending_entry.first_leg_price, 6),
                    "current_opposite_price": round(opposite_ask, 6),
                    "strategy_details": decision.details,
                }
            )
            self.events.write("pair_scanner_decision", payload)
            return

        plan_up_ask = pending_entry.first_leg_price if pending_entry.first_leg_outcome == pending_entry.up_outcome else up_ask
        plan_down_ask = pending_entry.first_leg_price if pending_entry.first_leg_outcome == pending_entry.down_outcome else down_ask
        plan = compute_pair_execution_plan(
            up_ask=plan_up_ask,
            down_ask=plan_down_ask,
            stake_usdc=self.config.scanner.pair_stake_usdc,
            fee_bps=self.config.scanner.fee_bps,
            slippage_bps=self.config.scanner.slippage_bps,
            max_effective_pair_sum=self.config.scanner.max_effective_pair_sum,
        )
        if plan is None:
            self._expire_pending_entry(
                pending_entry,
                trigger_mode=trigger_mode,
                reference_payload={
                    **reference_payload,
                    "event_slug": event_slug,
                    "market_family": market_family,
                    "market_duration_bucket": market_duration_bucket,
                    "seconds_to_resolution": seconds_to_resolution,
                    "first_leg_outcome": pending_entry.first_leg_outcome,
                    "first_leg_price": round(pending_entry.first_leg_price, 6),
                    "current_up_ask": round(up_ask, 6),
                    "current_down_ask": round(down_ask, 6),
                },
                strategy_reason="strategy_accept_but_pair_sum_too_high",
            )
            return
        if self.state.cash_usdc < self.config.scanner.pair_stake_usdc:
            self._expire_pending_entry(
                pending_entry,
                trigger_mode=trigger_mode,
                reference_payload=reference_payload,
                strategy_reason="strategy_accept_but_no_cash",
            )
            return
        market_key = descriptor.market_slug or descriptor.condition_id
        self.state.pending_entries.pop(market_key, None)
        self.rule_engine.forget_pending_unit(pending_entry.unit_id)
        self._open_pair_position(
            descriptor=descriptor,
            plan=plan,
            trigger_mode=trigger_mode,
            reference_payload=reference_payload,
            market_family=market_family,
            market_duration_bucket=market_duration_bucket,
            event_slug=event_slug,
            seconds_to_resolution=seconds_to_resolution,
            up_source=up_source if pending_entry.first_leg_outcome != pending_entry.up_outcome else pending_entry.first_leg_book_source,
            down_source=down_source if pending_entry.first_leg_outcome != pending_entry.down_outcome else pending_entry.first_leg_book_source,
            now_seconds=now_seconds,
            execution_kind="pair_execution_from_pending",
        )

    def _expire_pending_entry(
        self,
        pending_entry: PairPendingEntry,
        *,
        trigger_mode: str,
        reference_payload: Dict[str, object],
        strategy_reason: Optional[str] = None,
    ) -> None:
        market_key = pending_entry.market_slug or pending_entry.condition_id
        self.state.pending_entries.pop(market_key, None)
        if pending_entry.unit_id:
            self.rule_engine.forget_pending_unit(pending_entry.unit_id)
        payload = dict(reference_payload)
        payload.update(
            {
                "reason": strategy_reason or "pending_first_leg_expired",
                "trigger_mode": trigger_mode,
                "market_slug": pending_entry.market_slug,
                "event_slug": pending_entry.event_slug,
                "market_family": pending_entry.market_family,
                "market_duration_bucket": pending_entry.market_duration_bucket,
                "first_leg_outcome": pending_entry.first_leg_outcome,
                "first_leg_price": round(pending_entry.first_leg_price, 6),
                "opened_at_seconds": pending_entry.opened_at_seconds,
                "unit_id": pending_entry.unit_id,
                "strategy_reason": pending_entry.strategy_reason,
            }
        )
        self.events.write("pair_scanner_decision", payload)

    def _maybe_rebalance_position(
        self,
        *,
        descriptor: PairMarketDescriptor,
        position: PairPosition,
        up_ask: float,
        down_ask: float,
        up_source: str,
        down_source: str,
        market_family: str,
        market_duration_bucket: str,
        event_slug: str,
        trigger_mode: str,
        reference_payload: Dict[str, object],
        seconds_to_resolution: Optional[int],
        now_seconds: int,
    ) -> None:
        if not self.config.scanner.rebalance_enabled:
            return
        if position.rebalance_count >= int(self.config.scanner.max_rebalances_per_market):
            return
        if now_seconds - int(position.paired_at_seconds or position.opened_at_seconds or now_seconds) > int(self.config.scanner.rebalance_window_seconds):
            return
        rebalance_stake = float(self.config.scanner.rebalance_stake_usdc or self.config.scanner.pair_stake_usdc)
        plan = compute_pair_execution_plan(
            up_ask=up_ask,
            down_ask=down_ask,
            stake_usdc=rebalance_stake,
            fee_bps=self.config.scanner.fee_bps,
            slippage_bps=self.config.scanner.slippage_bps,
            max_effective_pair_sum=self.config.scanner.max_effective_pair_sum,
        )
        if plan is None:
            return
        new_effective_pair_sum = compute_rebalanced_effective_pair_sum(
            current_cost_usdc=position.cost_usdc,
            current_share_count=position.share_count,
            additional_cost_usdc=rebalance_stake,
            additional_share_count=plan.share_count,
        )
        if new_effective_pair_sum is None:
            return
        if new_effective_pair_sum > (float(position.effective_pair_sum) - float(self.config.scanner.rebalance_min_improve)):
            return
        if self.state.cash_usdc < rebalance_stake:
            return
        previous_effective_pair_sum = float(position.effective_pair_sum)
        self.state.cash_usdc -= rebalance_stake
        position.cost_usdc = round(position.cost_usdc + rebalance_stake, 6)
        position.share_count = round(position.share_count + plan.share_count, 6)
        position.up_cost_usdc = round(position.up_cost_usdc + plan.up_usdc, 6)
        position.down_cost_usdc = round(position.down_cost_usdc + plan.down_usdc, 6)
        position.up_share_count = round(position.up_share_count + plan.share_count, 6)
        position.down_share_count = round(position.down_share_count + plan.share_count, 6)
        position.effective_pair_sum = round(new_effective_pair_sum, 6)
        position.rebalance_count = int(position.rebalance_count) + 1
        self.events.write(
            "pair_rebalance_execution",
            {
                "target_name": self.target.name,
                "trigger_mode": trigger_mode,
                "market_slug": descriptor.market_slug,
                "condition_id": descriptor.condition_id,
                "event_slug": event_slug,
                "market_family": market_family,
                "market_duration_bucket": market_duration_bucket,
                "seconds_to_resolution": seconds_to_resolution,
                "up_ask": round(up_ask, 6),
                "down_ask": round(down_ask, 6),
                "up_order_book_source": up_source,
                "down_order_book_source": down_source,
                "rebalance_stake_usdc": round(rebalance_stake, 6),
                "previous_effective_pair_sum": round(previous_effective_pair_sum, 6),
                "new_effective_pair_sum": round(new_effective_pair_sum, 6),
                "rebalance_count": position.rebalance_count,
            },
        )

    def _open_pair_position(
        self,
        *,
        descriptor: PairMarketDescriptor,
        plan: PairExecutionPlan,
        trigger_mode: str,
        reference_payload: Dict[str, object],
        market_family: str,
        market_duration_bucket: str,
        event_slug: str,
        seconds_to_resolution: Optional[int],
        up_source: str,
        down_source: str,
        now_seconds: int,
        execution_kind: str,
    ) -> None:
        payload = dict(reference_payload)
        payload.update(
            {
                "reason": "follow_pair",
                "trigger_mode": trigger_mode,
                "market_slug": descriptor.market_slug,
                "event_slug": event_slug,
                "market_family": market_family,
                "market_duration_bucket": market_duration_bucket,
                "seconds_to_resolution": seconds_to_resolution,
                "up_asset": descriptor.up_asset,
                "down_asset": descriptor.down_asset,
                "up_outcome": descriptor.up_outcome,
                "down_outcome": descriptor.down_outcome,
                "up_ask": round(plan.up_usdc / plan.share_count, 6),
                "down_ask": round(plan.down_usdc / plan.share_count, 6),
                "up_order_book_source": up_source,
                "down_order_book_source": down_source,
                "raw_pair_sum": round(plan.raw_pair_sum, 6),
                "effective_pair_sum": round(plan.effective_pair_sum, 6),
                "locked_payout_usdc": round(plan.locked_payout_usdc, 6),
                "locked_pnl_usdc": round(plan.locked_pnl_usdc, 6),
            }
        )
        self.events.write("pair_scanner_decision", payload)
        self.state.cash_usdc -= self.config.scanner.pair_stake_usdc
        market_key = descriptor.market_slug or descriptor.condition_id
        self.state.positions[market_key] = PairPosition(
            market_slug=descriptor.market_slug,
            condition_id=descriptor.condition_id,
            up_asset=descriptor.up_asset,
            down_asset=descriptor.down_asset,
            up_outcome=descriptor.up_outcome,
            down_outcome=descriptor.down_outcome,
            share_count=round(plan.share_count, 6),
            cost_usdc=float(self.config.scanner.pair_stake_usdc),
            effective_pair_sum=round(plan.effective_pair_sum, 6),
            resolution_timestamp_seconds=descriptor.resolution_timestamp_seconds,
            up_share_count=round(plan.share_count, 6),
            down_share_count=round(plan.share_count, 6),
            up_cost_usdc=round(plan.up_usdc, 6),
            down_cost_usdc=round(plan.down_usdc, 6),
            opened_at_seconds=now_seconds,
            paired_at_seconds=now_seconds,
            rebalance_count=0,
        )
        self.state.market_entry_counts[market_key] = int(self.state.market_entry_counts.get(market_key, 0)) + 1
        self.events.write(
            execution_kind,
            {
                "target_name": self.target.name,
                "market_slug": descriptor.market_slug,
                "condition_id": descriptor.condition_id,
                "up_asset": descriptor.up_asset,
                "down_asset": descriptor.down_asset,
                "up_usdc": round(plan.up_usdc, 6),
                "down_usdc": round(plan.down_usdc, 6),
                "share_count": round(plan.share_count, 6),
                "effective_pair_sum": round(plan.effective_pair_sum, 6),
                "locked_payout_usdc": round(plan.locked_payout_usdc, 6),
                "locked_pnl_usdc": round(plan.locked_pnl_usdc, 6),
                "resolution_timestamp_seconds": descriptor.resolution_timestamp_seconds,
            },
        )

    def _finalize_trade_cursor(self, trade: TradeActivity) -> None:
        self.state.target_last_event_timestamp_ms = max(self.state.target_last_event_timestamp_ms, trade.timestamp_seconds)
        self.state.remember(trade.dedupe_key)

    def _decision_payload(self, trade: TradeActivity, reason: str, extra: Optional[Dict[str, object]] = None) -> Dict[str, object]:
        payload: Dict[str, object] = {
            "target_name": self.target.name,
            "reason": reason,
            "market_slug": trade.slug,
            "event_slug": trade.event_slug,
            "market_family": trade.market_family,
            "market_duration_bucket": trade.market_duration_bucket,
            "outcome": trade.outcome,
            "asset_id": trade.asset,
            "trade_timestamp_seconds": trade.timestamp_seconds,
        }
        if extra:
            payload.update(extra)
        return payload

    def _resolve_pair_descriptor(self, trade: TradeActivity) -> Optional[PairMarketDescriptor]:
        observed = self._observed_descriptor_for_trade(trade)
        if observed is not None:
            return observed
        event_key = str(trade.event_slug or trade.slug or "").strip()
        if not event_key:
            return None
        cached = self._event_market_cache.get(event_key)
        if cached is None:
            cached = self._load_event_market_descriptors(event_key)
            self._event_market_cache[event_key] = cached
        for descriptor in cached:
            if trade.asset in {descriptor.up_asset, descriptor.down_asset}:
                return descriptor
            if trade.condition_id and trade.condition_id == descriptor.condition_id:
                return descriptor
            if trade.slug and trade.slug == descriptor.market_slug:
                return descriptor
        return None

    def _observe_pair_contexts(self, trades: List[TradeActivity]) -> None:
        for trade in trades:
            if trade.side.upper() != "BUY":
                continue
            market_key = self._market_key_for_trade(trade)
            context = self._observed_pair_contexts.get(market_key)
            if context is None:
                context = _ObservedPairContext(
                    market_slug=trade.slug or trade.event_slug or market_key,
                    condition_id=trade.condition_id,
                )
                self._observed_pair_contexts[market_key] = context
            if trade.outcome and trade.asset:
                context.outcomes_to_assets[str(trade.outcome)] = str(trade.asset)

    def _observed_descriptor_for_trade(self, trade: TradeActivity) -> Optional[PairMarketDescriptor]:
        market_key = self._market_key_for_trade(trade)
        context = self._observed_pair_contexts.get(market_key)
        if context is None:
            return None
        outcome_names = list(context.outcomes_to_assets.keys())
        if len(outcome_names) < 2:
            return None
        up_outcome = next((name for name in outcome_names if str(name).lower() == "up"), None)
        down_outcome = next((name for name in outcome_names if str(name).lower() == "down"), None)
        if up_outcome is None or down_outcome is None:
            ordered = sorted(outcome_names)
            if len(ordered) < 2:
                return None
            up_outcome = ordered[0]
            down_outcome = ordered[1]
        up_asset = context.outcomes_to_assets.get(str(up_outcome))
        down_asset = context.outcomes_to_assets.get(str(down_outcome))
        if not up_asset or not down_asset:
            return None
        market_slug = context.market_slug or trade.slug or trade.event_slug or market_key
        return PairMarketDescriptor(
            market_slug=market_slug,
            condition_id=context.condition_id or trade.condition_id,
            up_outcome=str(up_outcome),
            down_outcome=str(down_outcome),
            up_asset=str(up_asset),
            down_asset=str(down_asset),
            resolution_timestamp_seconds=_infer_resolution_timestamp_seconds_from_market_slug(market_slug),
        )

    @staticmethod
    def _market_key_for_trade(trade: TradeActivity) -> str:
        return str(trade.slug or trade.event_slug or trade.condition_id or "").strip()

    def _load_event_market_descriptors(self, event_slug: str) -> List[PairMarketDescriptor]:
        descriptors: List[PairMarketDescriptor] = []
        try:
            events = self.api.get_events(slug=event_slug, active=True, closed=False)
        except Exception as exc:
            self.events.write("pair_event_lookup_error", {"event_slug": event_slug, "error": str(exc)})
            return []
        for event in events:
            markets = event.get("markets") or []
            if not isinstance(markets, list):
                continue
            for market in markets:
                if not isinstance(market, dict):
                    continue
                mapping = extract_market_tokens_with_outcomes(market)
                if len(mapping) < 2:
                    continue
                outcome_names = list(mapping.keys())
                up_outcome = next((name for name in outcome_names if str(name).lower() == "up"), None)
                down_outcome = next((name for name in outcome_names if str(name).lower() == "down"), None)
                if up_outcome is None or down_outcome is None:
                    ordered = sorted(outcome_names)
                    if len(ordered) < 2:
                        continue
                    up_outcome = ordered[0]
                    down_outcome = ordered[1]
                market_slug = str(market.get("slug") or event_slug)
                condition_id = str(
                    market.get("conditionId")
                    or market.get("condition_id")
                    or market.get("condition_id_hex")
                    or ""
                )
                resolution_ts = _infer_resolution_timestamp_seconds_from_market_slug(market_slug)
                descriptors.append(
                    PairMarketDescriptor(
                        market_slug=market_slug,
                        condition_id=condition_id,
                        up_outcome=str(up_outcome),
                        down_outcome=str(down_outcome),
                        up_asset=str(mapping[str(up_outcome)]),
                        down_asset=str(mapping[str(down_outcome)]),
                        resolution_timestamp_seconds=resolution_ts,
                    )
                )
        return descriptors

    def _order_book_for_asset(self, asset_id: str) -> tuple[OrderBook | None, str]:
        if self.market_stream is not None:
            self.market_stream.ensure_asset(asset_id)
            book = self.market_stream.wait_for_book(
                asset_id,
                timeout_seconds=max(self.config.runtime.market_websocket_warmup_ms, 0) / 1000.0,
            )
            if book is not None:
                return book, "market_ws"
        try:
            return self.api.get_order_book(asset_id), "rest_book"
        except RuntimeError as exc:
            if self.config.runtime.require_real_order_book:
                self.events.write("pair_order_book_missing", {"asset_id": asset_id, "error": str(exc)})
                return None, "missing_real_book"
            return None, "missing_book"

    def _redeem_resolved_positions(self, now_seconds: int) -> None:
        to_remove: List[str] = []
        for market_key, position in self.state.positions.items():
            resolution_ts = position.resolution_timestamp_seconds
            if resolution_ts is None or now_seconds < resolution_ts:
                continue
            proceeds = position.share_count
            pnl = proceeds - position.cost_usdc
            self.state.cash_usdc += proceeds
            to_remove.append(market_key)
            self.events.write(
                "redeem",
                {
                    "market_slug": position.market_slug,
                    "condition_id": position.condition_id,
                    "share_count": round(position.share_count, 6),
                    "cost_usdc": round(position.cost_usdc, 6),
                    "proceeds_usdc": round(proceeds, 6),
                    "pnl_usdc": round(pnl, 6),
                    "resolution_timestamp_seconds": resolution_ts,
                },
            )
        for market_key in to_remove:
            self.state.positions.pop(market_key, None)

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
        locked_value = sum(position.share_count for position in self.state.positions.values())
        equity = self.state.cash_usdc + locked_value
        return {
            "timestamp_seconds": now_seconds,
            "timestamp_iso": _isoformat(now_seconds),
            "equity_usdc": round(equity, 6),
            "cash_usdc": round(self.state.cash_usdc, 6),
            "holdings_value_usdc": round(locked_value, 6),
            "return_pct": round(((equity / self.config.portfolio.initial_capital_usdc) - 1.0) * 100.0, 6),
            "positions": len(self.state.positions),
        }


def _infer_resolution_timestamp_seconds_from_market_slug(market_slug: str) -> Optional[int]:
    from .live_paper import _infer_resolution_timestamp_seconds_from_slug

    return _infer_resolution_timestamp_seconds_from_slug(market_slug, reference_timestamp_seconds=None)


def _market_family_from_text(text: str) -> str:
    normalized = str(text or "").lower()
    if "bitcoin" in normalized or "btc" in normalized:
        return "btc"
    if "ethereum" in normalized or " eth" in normalized or normalized.startswith("eth"):
        return "eth"
    if "solana" in normalized or "sol" in normalized:
        return "sol"
    if "xrp" in normalized:
        return "xrp"
    return "other"


def _market_duration_bucket_from_text(text: str) -> str:
    normalized = str(text or "").lower()
    if "15m" in normalized or "15min" in normalized or "15pm" in normalized:
        return "15m"
    if "5m" in normalized or "5min" in normalized:
        return "5m"
    if "-2:15pm-2:30pm-et" in normalized or "-2:00pm-2:15pm-et" in normalized:
        return "15m"
    if "-2:15am-2:30am-et" in normalized or "-2:00am-2:15am-et" in normalized:
        return "15m"
    if (
        "am et" in normalized
        or "pm et" in normalized
        or re.search(r"\b\d{1,2}(am|pm)-\d{1,2}(am|pm)-et\b", normalized) is not None
        or re.search(r"\b\d{1,2}:\d{2}(am|pm)-\d{1,2}:\d{2}(am|pm)-et\b", normalized) is not None
    ):
        return "hourly"
    return "other"


def _infer_market_open_timestamp_seconds(
    resolution_timestamp_seconds: Optional[int],
    market_duration_bucket: str,
) -> Optional[int]:
    if resolution_timestamp_seconds is None:
        return None
    duration_seconds = {
        "hourly": 3600,
        "15m": 900,
        "5m": 300,
    }.get(str(market_duration_bucket), 0)
    if duration_seconds <= 0:
        return None
    return int(resolution_timestamp_seconds) - duration_seconds


def _isoformat(timestamp_seconds: int) -> str:
    return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc).isoformat()


def _next_hour_mark(timestamp_seconds: int) -> int:
    return ((timestamp_seconds // 3600) + 1) * 3600


def load_pair_live_paper_config(path: str) -> PairLivePaperConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return PairLivePaperConfig(
        target=PairLivePaperTarget(
            name=str(raw["target"]["name"]),
            profile=raw["target"].get("profile"),
            wallet=raw["target"].get("wallet"),
        ),
        runtime=PairLivePaperRuntime(
            poll_interval_seconds=float(raw["runtime"]["poll_interval_seconds"]),
            request_timeout_seconds=float(raw["runtime"]["request_timeout_seconds"]),
            activity_limit=int(raw["runtime"]["activity_limit"]),
            lookback_seconds_on_start=int(raw["runtime"]["lookback_seconds_on_start"]),
            requery_overlap_seconds=int(raw["runtime"]["requery_overlap_seconds"]),
            market_websocket_enabled=bool(raw["runtime"].get("market_websocket_enabled", True)),
            market_websocket_warmup_ms=int(raw["runtime"].get("market_websocket_warmup_ms", 350)),
            require_real_order_book=bool(raw["runtime"].get("require_real_order_book", True)),
            duration_hours=float(raw["runtime"]["duration_hours"]),
            heartbeat_interval_seconds=float(raw["runtime"].get("heartbeat_interval_seconds", 30.0)),
            state_path=str(raw["runtime"]["state_path"]),
            event_log_path=str(raw["runtime"]["event_log_path"]),
            hourly_stats_path=str(raw["runtime"]["hourly_stats_path"]),
        ),
        portfolio=PairLivePaperPortfolio(
            initial_capital_usdc=float(raw["portfolio"]["initial_capital_usdc"]),
        ),
        scanner=PairScannerConfig(
            families=[str(item) for item in raw["scanner"].get("families", ["btc", "eth", "sol", "xrp"])],
            durations=[str(item) for item in raw["scanner"].get("durations", ["hourly"])],
            skeleton_config_path=raw["scanner"].get("skeleton_config_path"),
            pair_stake_usdc=float(raw["scanner"].get("pair_stake_usdc", 100.0)),
            max_effective_pair_sum=float(raw["scanner"].get("max_effective_pair_sum", 1.0)),
            fee_bps=float(raw["scanner"].get("fee_bps", 0.0)),
            slippage_bps=float(raw["scanner"].get("slippage_bps", 0.0)),
            max_entries_per_market=int(raw["scanner"].get("max_entries_per_market", 1)),
            observe_target_activity=bool(raw["scanner"].get("observe_target_activity", True)),
            market_trigger_enabled=bool(raw["scanner"].get("market_trigger_enabled", False)),
            market_search_limit=int(raw["scanner"].get("market_search_limit", 10)),
            min_seconds_to_resolution=(
                int(raw["scanner"]["min_seconds_to_resolution"])
                if raw["scanner"].get("min_seconds_to_resolution") is not None
                else None
            ),
            max_seconds_to_resolution=(
                int(raw["scanner"]["max_seconds_to_resolution"])
                if raw["scanner"].get("max_seconds_to_resolution") is not None
                else None
            ),
            staged_entry_enabled=bool(raw["scanner"].get("staged_entry_enabled", False)),
            first_leg_price_floor=float(raw["scanner"].get("first_leg_price_floor", 0.4)),
            first_leg_price_ceiling=float(raw["scanner"].get("first_leg_price_ceiling", 0.6)),
            max_leg_wait_seconds=int(raw["scanner"].get("max_leg_wait_seconds", 60)),
            rebalance_enabled=bool(raw["scanner"].get("rebalance_enabled", False)),
            rebalance_stake_usdc=(
                float(raw["scanner"]["rebalance_stake_usdc"])
                if raw["scanner"].get("rebalance_stake_usdc") is not None
                else None
            ),
            max_rebalances_per_market=int(raw["scanner"].get("max_rebalances_per_market", 1)),
            rebalance_window_seconds=int(raw["scanner"].get("rebalance_window_seconds", 180)),
            rebalance_min_improve=float(raw["scanner"].get("rebalance_min_improve", 0.005)),
        ),
    )


def build_pair_live_paper_app(config_path: str) -> PairLivePaperApp:
    return PairLivePaperApp(load_pair_live_paper_config(config_path))
