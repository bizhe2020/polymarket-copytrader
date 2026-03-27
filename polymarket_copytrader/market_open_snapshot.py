from __future__ import annotations

import csv
import json
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from .market_time import infer_resolution_timestamp_seconds_from_slug
from .models import TradeActivity

_WINDOWS_SECONDS = (60, 300, 900)


@dataclass
class MarketOpenSnapshotConfig:
    input_trades_path: str
    output_csv_path: str
    summary_json_path: str
    market_universe_path: Optional[str] = None
    families: Tuple[str, ...] = ("btc", "eth", "sol", "xrp")
    durations: Tuple[str, ...] = ("hourly",)
    snapshot_offsets_seconds: Tuple[int, ...] = (0, 1, 3, 5, 10, 30, 60)
    entry_horizon_seconds: int = 60


@dataclass
class MarketOpenSnapshotSummary:
    total_markets: int
    touched_markets: int
    untouched_markets: int
    markets_with_first_leg: int
    markets_with_first_leg_in_window: int
    snapshot_rows: int
    positive_label_rows: int
    negative_label_rows: int
    family_counts: Dict[str, int]
    duration_counts: Dict[str, int]
    snapshot_offset_counts: Dict[str, int]
    first_leg_side_counts: Dict[str, int]


@dataclass
class _RollingWindowTracker:
    entries: Deque[Tuple[int, float]] = field(default_factory=deque)
    sum_usdc: float = 0.0

    def snapshot(self, now_seconds: int, window_seconds: int) -> Tuple[int, float]:
        self._prune(now_seconds, window_seconds)
        return len(self.entries), self.sum_usdc

    def observe(self, timestamp_seconds: int, usdc_size: float) -> None:
        self.entries.append((timestamp_seconds, usdc_size))
        self.sum_usdc += float(usdc_size)

    def _prune(self, now_seconds: int, window_seconds: int) -> None:
        cutoff = int(now_seconds) - int(window_seconds)
        while self.entries and self.entries[0][0] >= 0 and self.entries[0][0] < cutoff:
            _, usdc_size = self.entries.popleft()
            self.sum_usdc -= usdc_size


@dataclass
class _MarketTradeStats:
    count: int = 0
    usdc: float = 0.0
    last_trade_ts: Optional[int] = None
    outcomes_seen: set[str] = field(default_factory=set)

    def observe(self, trade: TradeActivity) -> None:
        self.count += 1
        self.usdc += float(trade.usdc_size)
        self.last_trade_ts = int(trade.timestamp_seconds)
        if trade.outcome:
            self.outcomes_seen.add(str(trade.outcome))


@dataclass
class _MarketDescriptor:
    market_key: str
    market_slug: str
    event_slug: str
    condition_id: str
    title: str
    market_family: str
    market_duration_bucket: str
    market_open_timestamp_seconds: int
    resolution_timestamp_seconds: Optional[int]
    target_buys: List[TradeActivity] = field(default_factory=list)

    @property
    def first_leg(self) -> Optional[TradeActivity]:
        return self.target_buys[0] if self.target_buys else None


def build_market_open_snapshot_dataset(config: MarketOpenSnapshotConfig) -> MarketOpenSnapshotSummary:
    families = {item.strip() for item in config.families if item.strip()}
    durations = {item.strip() for item in config.durations if item.strip()}
    snapshot_offsets = sorted({max(0, int(item)) for item in config.snapshot_offsets_seconds})
    if not snapshot_offsets:
        raise ValueError("snapshot_offsets_seconds must not be empty")
    if int(config.entry_horizon_seconds) < 0:
        raise ValueError("entry_horizon_seconds must be non-negative")

    target_buys = _load_target_buys(config.input_trades_path, families=families, durations=durations)
    descriptors = _build_market_descriptors(
        target_buys=target_buys,
        market_universe_path=config.market_universe_path,
        families=families,
        durations=durations,
    )

    rows = _build_snapshot_rows(
        descriptors=descriptors,
        target_buys=target_buys,
        snapshot_offsets=snapshot_offsets,
        entry_horizon_seconds=int(config.entry_horizon_seconds),
    )
    _write_csv(config.output_csv_path, rows)

    touched_markets = sum(1 for descriptor in descriptors if descriptor.target_buys)
    untouched_markets = len(descriptors) - touched_markets
    markets_with_first_leg = sum(1 for descriptor in descriptors if descriptor.first_leg is not None)
    markets_with_first_leg_in_window = 0
    family_counts: Dict[str, int] = defaultdict(int)
    duration_counts: Dict[str, int] = defaultdict(int)
    first_leg_side_counts: Dict[str, int] = defaultdict(int)
    for descriptor in descriptors:
        family_counts[descriptor.market_family] += 1
        duration_counts[descriptor.market_duration_bucket] += 1
        first_leg = descriptor.first_leg
        if first_leg is None:
            continue
        if _is_first_leg_in_window(
            first_leg=first_leg,
            market_open_timestamp_seconds=descriptor.market_open_timestamp_seconds,
            entry_horizon_seconds=int(config.entry_horizon_seconds),
        ):
            markets_with_first_leg_in_window += 1
        if first_leg.outcome:
            first_leg_side_counts[str(first_leg.outcome)] += 1

    snapshot_offset_counts: Dict[str, int] = defaultdict(int)
    positive_label_rows = 0
    for row in rows:
        snapshot_offset_counts[str(row["snapshot_offset_seconds"])] += 1
        if int(row["label_open_first_leg"]):
            positive_label_rows += 1
    negative_label_rows = len(rows) - positive_label_rows

    summary = MarketOpenSnapshotSummary(
        total_markets=len(descriptors),
        touched_markets=touched_markets,
        untouched_markets=untouched_markets,
        markets_with_first_leg=markets_with_first_leg,
        markets_with_first_leg_in_window=markets_with_first_leg_in_window,
        snapshot_rows=len(rows),
        positive_label_rows=positive_label_rows,
        negative_label_rows=negative_label_rows,
        family_counts=dict(sorted(family_counts.items())),
        duration_counts=dict(sorted(duration_counts.items())),
        snapshot_offset_counts=dict(sorted(snapshot_offset_counts.items(), key=lambda item: int(item[0]))),
        first_leg_side_counts=dict(sorted(first_leg_side_counts.items())),
    )
    summary_path = Path(config.summary_json_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def _build_snapshot_rows(
    *,
    descriptors: Sequence[_MarketDescriptor],
    target_buys: Sequence[TradeActivity],
    snapshot_offsets: Sequence[int],
    entry_horizon_seconds: int,
) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    snapshot_requests: List[Tuple[int, int, _MarketDescriptor]] = []
    for descriptor in descriptors:
        for offset in snapshot_offsets:
            snapshot_requests.append(
                (
                    descriptor.market_open_timestamp_seconds + int(offset),
                    int(offset),
                    descriptor,
                )
            )
    snapshot_requests.sort(
        key=lambda item: (
            item[0],
            item[2].market_slug or item[2].market_key,
            item[1],
        )
    )

    ordered_buys = sorted(
        target_buys,
        key=lambda trade: (trade.timestamp_seconds, trade.transaction_hash, trade.asset),
    )
    buy_index = 0
    overall_trackers = {window: _RollingWindowTracker() for window in _WINDOWS_SECONDS}
    family_trackers: Dict[str, Dict[int, _RollingWindowTracker]] = defaultdict(_make_window_tracker_map)
    market_stats: Dict[str, _MarketTradeStats] = defaultdict(_MarketTradeStats)

    for snapshot_timestamp_seconds, snapshot_offset_seconds, descriptor in snapshot_requests:
        while buy_index < len(ordered_buys) and ordered_buys[buy_index].timestamp_seconds < snapshot_timestamp_seconds:
            trade = ordered_buys[buy_index]
            for tracker in overall_trackers.values():
                tracker.observe(trade.timestamp_seconds, trade.usdc_size)
            for tracker in family_trackers[trade.market_family].values():
                tracker.observe(trade.timestamp_seconds, trade.usdc_size)
            market_stats[_market_key_for_trade(trade)].observe(trade)
            buy_index += 1

        same_market_stats = market_stats.get(descriptor.market_key) or _MarketTradeStats()
        row = _build_snapshot_row(
            descriptor=descriptor,
            snapshot_timestamp_seconds=snapshot_timestamp_seconds,
            snapshot_offset_seconds=snapshot_offset_seconds,
            entry_horizon_seconds=entry_horizon_seconds,
            overall_trackers=overall_trackers,
            family_trackers=family_trackers,
            same_market_stats=same_market_stats,
        )
        rows.append(row)

    return rows


def _build_snapshot_row(
    *,
    descriptor: _MarketDescriptor,
    snapshot_timestamp_seconds: int,
    snapshot_offset_seconds: int,
    entry_horizon_seconds: int,
    overall_trackers: Dict[int, _RollingWindowTracker],
    family_trackers: Dict[str, Dict[int, _RollingWindowTracker]],
    same_market_stats: _MarketTradeStats,
) -> Dict[str, object]:
    family_window_trackers = family_trackers.get(descriptor.market_family) or _make_window_tracker_map()
    first_leg = descriptor.first_leg
    first_leg_in_window = int(
        first_leg is not None
        and _is_first_leg_in_window(
            first_leg=first_leg,
            market_open_timestamp_seconds=descriptor.market_open_timestamp_seconds,
            entry_horizon_seconds=entry_horizon_seconds,
        )
    )
    label_open_first_leg = int(
        first_leg is not None
        and first_leg_in_window
        and first_leg.timestamp_seconds >= snapshot_timestamp_seconds
        and first_leg.timestamp_seconds
        <= descriptor.market_open_timestamp_seconds + int(entry_horizon_seconds)
    )
    first_leg_delay_seconds = (
        first_leg.timestamp_seconds - snapshot_timestamp_seconds
        if first_leg is not None and label_open_first_leg
        else None
    )
    first_leg_delay_from_open_seconds = (
        first_leg.timestamp_seconds - descriptor.market_open_timestamp_seconds if first_leg is not None else None
    )
    seconds_since_last_same_market_trade = (
        snapshot_timestamp_seconds - int(same_market_stats.last_trade_ts)
        if same_market_stats.last_trade_ts is not None
        else None
    )
    seconds_to_resolution = (
        int(descriptor.resolution_timestamp_seconds) - int(snapshot_timestamp_seconds)
        if descriptor.resolution_timestamp_seconds is not None
        else None
    )

    row: Dict[str, object] = {
        "sample_id": f"{descriptor.market_key}:{snapshot_offset_seconds}",
        "market_key": descriptor.market_key,
        "market_slug": descriptor.market_slug,
        "event_slug": descriptor.event_slug,
        "condition_id": descriptor.condition_id,
        "title": descriptor.title,
        "market_family": descriptor.market_family,
        "market_duration_bucket": descriptor.market_duration_bucket,
        "market_open_timestamp_seconds": descriptor.market_open_timestamp_seconds,
        "market_open_timestamp_iso": _isoformat(descriptor.market_open_timestamp_seconds),
        "snapshot_offset_seconds": snapshot_offset_seconds,
        "snapshot_timestamp_seconds": snapshot_timestamp_seconds,
        "snapshot_timestamp_iso": _isoformat(snapshot_timestamp_seconds),
        "resolution_timestamp_seconds": descriptor.resolution_timestamp_seconds,
        "seconds_to_resolution": seconds_to_resolution,
        "realized_market_traded_by_target": int(first_leg is not None),
        "realized_first_leg_in_entry_window": first_leg_in_window,
        "realized_first_leg_timestamp_seconds": first_leg.timestamp_seconds if first_leg is not None else None,
        "realized_first_leg_timestamp_iso": _isoformat(first_leg.timestamp_seconds) if first_leg is not None else "",
        "realized_first_leg_outcome": first_leg.outcome if first_leg is not None else "",
        "realized_first_leg_price": round(float(first_leg.price), 6) if first_leg is not None else None,
        "realized_first_leg_usdc_size": round(float(first_leg.usdc_size), 6) if first_leg is not None else None,
        "realized_first_leg_delay_from_open_seconds": first_leg_delay_from_open_seconds,
        "same_market_target_buy_count_before_snapshot": same_market_stats.count,
        "same_market_target_buy_usdc_before_snapshot": round(same_market_stats.usdc, 6),
        "same_market_target_distinct_outcomes_before_snapshot": len(same_market_stats.outcomes_seen),
        "same_market_seconds_since_last_target_buy": seconds_since_last_same_market_trade,
        "label_open_first_leg": label_open_first_leg,
        "label_first_leg_side": first_leg.outcome if label_open_first_leg and first_leg is not None else "",
        "label_first_leg_delay_seconds": first_leg_delay_seconds,
    }

    for window_seconds in _WINDOWS_SECONDS:
        overall_count, overall_usdc = overall_trackers[window_seconds].snapshot(
            snapshot_timestamp_seconds,
            window_seconds,
        )
        family_count, family_usdc = family_window_trackers[window_seconds].snapshot(
            snapshot_timestamp_seconds,
            window_seconds,
        )
        row[f"recent_target_buy_count_{window_seconds}s"] = overall_count
        row[f"recent_target_buy_usdc_{window_seconds}s"] = round(overall_usdc, 6)
        row[f"recent_family_target_buy_count_{window_seconds}s"] = family_count
        row[f"recent_family_target_buy_usdc_{window_seconds}s"] = round(family_usdc, 6)

    return row


def _build_market_descriptors(
    *,
    target_buys: Sequence[TradeActivity],
    market_universe_path: Optional[str],
    families: set[str],
    durations: set[str],
) -> List[_MarketDescriptor]:
    descriptors: Dict[str, _MarketDescriptor] = {}

    grouped_target_buys: Dict[str, List[TradeActivity]] = defaultdict(list)
    for trade in target_buys:
        grouped_target_buys[_market_key_for_trade(trade)].append(trade)

    for market_key, trades in grouped_target_buys.items():
        ordered = sorted(trades, key=lambda item: (item.timestamp_seconds, item.transaction_hash, item.asset))
        first_trade = ordered[0]
        resolution_ts = _infer_resolution_timestamp_seconds_from_trade(first_trade)
        open_ts = _infer_market_open_timestamp_seconds(
            market_duration_bucket=first_trade.market_duration_bucket,
            resolution_timestamp_seconds=resolution_ts,
        )
        if open_ts is None:
            continue
        descriptors[market_key] = _MarketDescriptor(
            market_key=market_key,
            market_slug=first_trade.slug or market_key,
            event_slug=first_trade.event_slug or first_trade.slug or market_key,
            condition_id=first_trade.condition_id,
            title=first_trade.title,
            market_family=first_trade.market_family,
            market_duration_bucket=first_trade.market_duration_bucket,
            market_open_timestamp_seconds=open_ts,
            resolution_timestamp_seconds=resolution_ts,
            target_buys=ordered,
        )

    if market_universe_path:
        for raw_entry in _load_market_universe_entries(market_universe_path):
            market_key = (
                str(raw_entry.get("market_slug") or raw_entry.get("slug") or "")
                or str(raw_entry.get("event_slug") or "")
                or str(raw_entry.get("condition_id") or "")
            ).strip()
            if not market_key:
                continue
            title = str(raw_entry.get("title") or raw_entry.get("question") or "")
            slug = str(raw_entry.get("market_slug") or raw_entry.get("slug") or market_key)
            event_slug = str(raw_entry.get("event_slug") or slug)
            condition_id = str(raw_entry.get("condition_id") or market_key)
            market_family = _market_family_from_text(" ".join([slug, event_slug, title]))
            market_duration_bucket = _market_duration_bucket_from_text(" ".join([slug, event_slug, title]))
            if market_family not in families or market_duration_bucket not in durations:
                continue

            open_ts = _read_optional_int(
                raw_entry,
                ["market_open_timestamp_seconds", "open_timestamp_seconds", "start_timestamp_seconds"],
            )
            resolution_ts = _read_optional_int(
                raw_entry,
                ["resolution_timestamp_seconds", "end_timestamp_seconds"],
            )
            reference_ts = _read_optional_int(
                raw_entry,
                ["reference_timestamp_seconds", "snapshot_timestamp_seconds", "timestamp_seconds"],
            )
            if resolution_ts is None:
                resolution_ts = infer_resolution_timestamp_seconds_from_slug(
                    event_slug or slug,
                    reference_timestamp_seconds=reference_ts,
                )
            if open_ts is None:
                open_ts = _infer_market_open_timestamp_seconds(
                    market_duration_bucket=market_duration_bucket,
                    resolution_timestamp_seconds=resolution_ts,
                )
            if open_ts is None:
                continue

            descriptor = descriptors.get(market_key)
            if descriptor is None:
                descriptors[market_key] = _MarketDescriptor(
                    market_key=market_key,
                    market_slug=slug,
                    event_slug=event_slug,
                    condition_id=condition_id,
                    title=title,
                    market_family=market_family,
                    market_duration_bucket=market_duration_bucket,
                    market_open_timestamp_seconds=open_ts,
                    resolution_timestamp_seconds=resolution_ts,
                    target_buys=[],
                )
                continue

            if not descriptor.title and title:
                descriptor.title = title
            if descriptor.market_slug == descriptor.market_key and slug:
                descriptor.market_slug = slug
            if descriptor.event_slug == descriptor.market_slug and event_slug:
                descriptor.event_slug = event_slug
            if not descriptor.condition_id and condition_id:
                descriptor.condition_id = condition_id
            if descriptor.resolution_timestamp_seconds is None and resolution_ts is not None:
                descriptor.resolution_timestamp_seconds = resolution_ts
            if descriptor.market_open_timestamp_seconds <= 0 and open_ts is not None:
                descriptor.market_open_timestamp_seconds = open_ts

    filtered = [
        descriptor
        for descriptor in descriptors.values()
        if descriptor.market_family in families
        and descriptor.market_duration_bucket in durations
        and descriptor.market_open_timestamp_seconds > 0
    ]
    filtered.sort(
        key=lambda item: (
            item.market_open_timestamp_seconds,
            item.market_slug or item.market_key,
        )
    )
    return filtered


def _load_target_buys(path: str, *, families: set[str], durations: set[str]) -> List[TradeActivity]:
    buys: List[TradeActivity] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = json.loads(line)
            payload = raw["payload"] if "payload" in raw else raw
            trade = TradeActivity(
                proxy_wallet=str(payload["proxy_wallet"]),
                timestamp_ms=int(payload["timestamp_ms"]),
                condition_id=str(payload["condition_id"]),
                activity_type=str(payload["activity_type"]),
                size=float(payload["size"]),
                usdc_size=float(payload["usdc_size"]),
                transaction_hash=str(payload["transaction_hash"]),
                price=float(payload["price"]),
                asset=str(payload["asset"]),
                side=str(payload["side"]),
                outcome_index=int(payload["outcome_index"]),
                title=str(payload["title"]),
                slug=str(payload["slug"]),
                event_slug=str(payload["event_slug"]),
                outcome=str(payload["outcome"]),
                name=str(payload.get("name") or ""),
                pseudonym=str(payload.get("pseudonym") or ""),
            )
            if trade.side.upper() != "BUY":
                continue
            if trade.market_family not in families or trade.market_duration_bucket not in durations:
                continue
            buys.append(trade)
    buys.sort(key=lambda item: (item.timestamp_seconds, item.transaction_hash, item.asset))
    return buys


def _load_market_universe_entries(path: str) -> Iterable[Dict[str, object]]:
    input_path = Path(path)
    suffix = input_path.suffix.lower()
    if suffix == ".csv":
        with input_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                yield dict(row)
        return
    if suffix == ".json":
        payload = json.loads(input_path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    yield dict(item)
        return
    with input_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if isinstance(payload, dict):
                yield payload


def _infer_resolution_timestamp_seconds_from_trade(trade: TradeActivity) -> Optional[int]:
    for candidate in [trade.event_slug, trade.slug]:
        resolution_ts = infer_resolution_timestamp_seconds_from_slug(
            candidate,
            reference_timestamp_seconds=trade.timestamp_seconds,
        )
        if resolution_ts is not None:
            return resolution_ts
    return None


def _infer_market_open_timestamp_seconds(
    *,
    market_duration_bucket: str,
    resolution_timestamp_seconds: Optional[int],
) -> Optional[int]:
    if resolution_timestamp_seconds is None:
        return None
    if market_duration_bucket == "hourly":
        return int(resolution_timestamp_seconds) - 3600
    if market_duration_bucket == "15m":
        return int(resolution_timestamp_seconds) - 900
    if market_duration_bucket == "5m":
        return int(resolution_timestamp_seconds) - 300
    return None


def _is_first_leg_in_window(
    *,
    first_leg: TradeActivity,
    market_open_timestamp_seconds: int,
    entry_horizon_seconds: int,
) -> bool:
    return (
        int(first_leg.timestamp_seconds) >= int(market_open_timestamp_seconds)
        and int(first_leg.timestamp_seconds) <= int(market_open_timestamp_seconds) + int(entry_horizon_seconds)
    )


def _market_key_for_trade(trade: TradeActivity) -> str:
    return str(trade.slug or trade.event_slug or trade.condition_id)


def _market_family_from_text(text: str) -> str:
    normalized = str(text or "").lower()
    if "bitcoin" in normalized or "btc" in normalized:
        return "btc"
    if "ethereum" in normalized or "eth" in normalized:
        return "eth"
    if "solana" in normalized or "sol" in normalized:
        return "sol"
    if "xrp" in normalized:
        return "xrp"
    return "other"


def _market_duration_bucket_from_text(text: str) -> str:
    normalized = str(text or "").lower()
    if "15m" in normalized or "15min" in normalized:
        return "15m"
    if "5m" in normalized or "5min" in normalized:
        return "5m"
    if "am et" in normalized or "pm et" in normalized:
        return "hourly"
    return "other"


def _read_optional_int(payload: Dict[str, object], keys: Sequence[str]) -> Optional[int]:
    for key in keys:
        raw = payload.get(key)
        if raw in (None, "", "None"):
            continue
        try:
            return int(float(str(raw)))
        except ValueError:
            continue
    return None


def _make_window_tracker_map() -> Dict[int, _RollingWindowTracker]:
    return {window: _RollingWindowTracker() for window in _WINDOWS_SECONDS}


def _isoformat(timestamp_seconds: int) -> str:
    return datetime.fromtimestamp(int(timestamp_seconds), tz=timezone.utc).isoformat()


def _write_csv(path: str, rows: Iterable[Dict[str, object]]) -> None:
    materialized_rows = list(rows)
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not materialized_rows:
        output.write_text("", encoding="utf-8")
        return
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(materialized_rows[0].keys()))
        writer.writeheader()
        writer.writerows(materialized_rows)
