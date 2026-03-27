from __future__ import annotations

import csv
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .market_time import infer_resolution_timestamp_seconds_from_slug
from .models import TradeActivity


@dataclass
class PostPairCycleLoopConfig:
    input_trades_path: str
    output_csv_path: str
    summary_json_path: str
    families: Tuple[str, ...] = ("btc", "eth", "sol", "xrp")
    durations: Tuple[str, ...] = ("hourly",)
    cycle_start_horizon_seconds: int = 180
    cycle_complete_horizon_seconds: int = 60


@dataclass
class PostPairCycleLoopSummary:
    total_markets: int
    markets_with_initial_pair: int
    rows: int
    rows_with_next_cycle_start: int
    rows_with_next_cycle_complete: int
    rows_with_next_cycle_complete_in_horizon: int
    family_counts: Dict[str, int]
    duration_counts: Dict[str, int]
    next_cycle_first_leg_side_counts: Dict[str, int]


@dataclass
class _OutcomeStats:
    count: int = 0
    usdc: float = 0.0
    size: float = 0.0

    def observe(self, trade: TradeActivity) -> None:
        self.count += 1
        self.usdc += float(trade.usdc_size)
        self.size += float(trade.size)

    @property
    def avg_price(self) -> Optional[float]:
        if self.size <= 0:
            return None
        return self.usdc / self.size


def build_post_pair_cycle_loop_dataset(config: PostPairCycleLoopConfig) -> PostPairCycleLoopSummary:
    families = {item.strip() for item in config.families if item.strip()}
    durations = {item.strip() for item in config.durations if item.strip()}
    if int(config.cycle_start_horizon_seconds) < 0:
        raise ValueError("cycle_start_horizon_seconds must be non-negative")
    if int(config.cycle_complete_horizon_seconds) < 0:
        raise ValueError("cycle_complete_horizon_seconds must be non-negative")

    trades = _load_filtered_buys(config.input_trades_path, families=families, durations=durations)
    grouped: Dict[str, List[TradeActivity]] = defaultdict(list)
    for trade in trades:
        grouped[_market_key_for_trade(trade)].append(trade)

    rows: List[Dict[str, object]] = []
    family_counts: Dict[str, int] = defaultdict(int)
    duration_counts: Dict[str, int] = defaultdict(int)
    next_cycle_first_leg_side_counts: Dict[str, int] = defaultdict(int)
    markets_with_initial_pair = 0

    for market_key, market_trades in sorted(grouped.items()):
        ordered = sorted(market_trades, key=lambda item: (item.timestamp_seconds, item.transaction_hash, item.asset))
        if len({str(trade.outcome or "") for trade in ordered if str(trade.outcome or "")}) < 2:
            continue
        initial_pair_index = _find_first_pair_completion_index(ordered)
        if initial_pair_index is None:
            continue
        markets_with_initial_pair += 1
        sample_trade = ordered[0]
        family_counts[sample_trade.market_family] += 1
        duration_counts[sample_trade.market_duration_bucket] += 1
        resolution_ts = _infer_resolution_timestamp_seconds_from_trade(sample_trade)

        current_lock_index = initial_pair_index
        lock_unit_index = 1
        while current_lock_index is not None and current_lock_index < len(ordered):
            row, next_lock_index = _build_cycle_row(
                ordered=ordered,
                market_key=market_key,
                lock_index=current_lock_index,
                lock_unit_index=lock_unit_index,
                resolution_timestamp_seconds=resolution_ts,
                cycle_start_horizon_seconds=int(config.cycle_start_horizon_seconds),
                cycle_complete_horizon_seconds=int(config.cycle_complete_horizon_seconds),
            )
            rows.append(row)
            first_leg_side = str(row.get("label_next_cycle_first_leg_outcome") or "")
            if first_leg_side:
                next_cycle_first_leg_side_counts[first_leg_side] += 1
            if next_lock_index is None:
                break
            current_lock_index = next_lock_index
            lock_unit_index += 1

    _write_csv(config.output_csv_path, rows)

    rows_with_next_cycle_start = sum(1 for row in rows if int(row["label_next_cycle_start"]))
    rows_with_next_cycle_complete = sum(1 for row in rows if int(row["label_next_cycle_complete"]))
    rows_with_next_cycle_complete_in_horizon = sum(
        1 for row in rows if int(row["label_next_cycle_complete_in_horizon"])
    )

    summary = PostPairCycleLoopSummary(
        total_markets=len(grouped),
        markets_with_initial_pair=markets_with_initial_pair,
        rows=len(rows),
        rows_with_next_cycle_start=rows_with_next_cycle_start,
        rows_with_next_cycle_complete=rows_with_next_cycle_complete,
        rows_with_next_cycle_complete_in_horizon=rows_with_next_cycle_complete_in_horizon,
        family_counts=dict(sorted(family_counts.items())),
        duration_counts=dict(sorted(duration_counts.items())),
        next_cycle_first_leg_side_counts=dict(sorted(next_cycle_first_leg_side_counts.items())),
    )
    summary_path = Path(config.summary_json_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def _build_cycle_row(
    *,
    ordered: Sequence[TradeActivity],
    market_key: str,
    lock_index: int,
    lock_unit_index: int,
    resolution_timestamp_seconds: Optional[int],
    cycle_start_horizon_seconds: int,
    cycle_complete_horizon_seconds: int,
) -> Tuple[Dict[str, object], Optional[int]]:
    lock_trade = ordered[lock_index]
    up_stats = _OutcomeStats()
    down_stats = _OutcomeStats()
    for trade in ordered[: lock_index + 1]:
        normalized = str(trade.outcome or "").lower()
        if normalized == "up":
            up_stats.observe(trade)
        elif normalized == "down":
            down_stats.observe(trade)
    current_pair_sum = None
    if up_stats.avg_price is not None and down_stats.avg_price is not None:
        current_pair_sum = float(up_stats.avg_price) + float(down_stats.avg_price)

    next_cycle_start = ordered[lock_index + 1] if lock_index + 1 < len(ordered) else None
    label_next_cycle_start = int(next_cycle_start is not None)
    next_cycle_start_delay_seconds = (
        next_cycle_start.timestamp_seconds - lock_trade.timestamp_seconds if next_cycle_start is not None else None
    )
    label_next_cycle_start_in_horizon = int(
        next_cycle_start is not None and next_cycle_start_delay_seconds is not None
        and next_cycle_start_delay_seconds <= cycle_start_horizon_seconds
    )

    next_cycle_completion = None
    next_lock_index = None
    next_cycle_same_side_trade_count = 0
    next_cycle_same_side_trade_usdc = 0.0
    if next_cycle_start is not None:
        start_outcome = str(next_cycle_start.outcome or "")
        for candidate_index in range(lock_index + 2, len(ordered)):
            candidate = ordered[candidate_index]
            if str(candidate.outcome or "") == start_outcome:
                next_cycle_same_side_trade_count += 1
                next_cycle_same_side_trade_usdc += float(candidate.usdc_size)
                continue
            next_cycle_completion = candidate
            next_lock_index = candidate_index
            break

    label_next_cycle_complete = int(next_cycle_completion is not None)
    next_cycle_complete_delay_seconds = (
        next_cycle_completion.timestamp_seconds - next_cycle_start.timestamp_seconds
        if next_cycle_completion is not None and next_cycle_start is not None
        else None
    )
    label_next_cycle_complete_in_horizon = int(
        next_cycle_complete_delay_seconds is not None
        and next_cycle_complete_delay_seconds <= cycle_complete_horizon_seconds
    )
    next_cycle_first_cross_pair_sum = (
        float(next_cycle_start.price) + float(next_cycle_completion.price)
        if next_cycle_start is not None and next_cycle_completion is not None
        else None
    )
    next_cycle_locked_edge = (
        1.0 - float(next_cycle_first_cross_pair_sum) if next_cycle_first_cross_pair_sum is not None else None
    )
    seconds_to_resolution = (
        int(resolution_timestamp_seconds) - int(lock_trade.timestamp_seconds)
        if resolution_timestamp_seconds is not None
        else None
    )

    row: Dict[str, object] = {
        "sample_id": f"{market_key}:lock_unit_{lock_unit_index}",
        "market_key": market_key,
        "market_slug": lock_trade.slug,
        "event_slug": lock_trade.event_slug,
        "condition_id": lock_trade.condition_id,
        "title": lock_trade.title,
        "market_family": lock_trade.market_family,
        "market_duration_bucket": lock_trade.market_duration_bucket,
        "locked_unit_index": lock_unit_index,
        "locked_timestamp_seconds": lock_trade.timestamp_seconds,
        "locked_timestamp_iso": _isoformat(lock_trade.timestamp_seconds),
        "resolution_timestamp_seconds": resolution_timestamp_seconds,
        "seconds_to_resolution": seconds_to_resolution,
        "current_locked_pair_sum": round(current_pair_sum, 6) if current_pair_sum is not None else None,
        "current_locked_edge": round(1.0 - current_pair_sum, 6) if current_pair_sum is not None else None,
        "cumulative_up_count": up_stats.count,
        "cumulative_down_count": down_stats.count,
        "cumulative_up_usdc": round(up_stats.usdc, 6),
        "cumulative_down_usdc": round(down_stats.usdc, 6),
        "label_next_cycle_start": label_next_cycle_start,
        "label_next_cycle_start_in_horizon": label_next_cycle_start_in_horizon,
        "label_next_cycle_start_delay_seconds": next_cycle_start_delay_seconds,
        "label_next_cycle_first_leg_outcome": next_cycle_start.outcome if next_cycle_start is not None else "",
        "label_next_cycle_first_leg_price": round(float(next_cycle_start.price), 6) if next_cycle_start else None,
        "label_next_cycle_first_leg_usdc_size": (
            round(float(next_cycle_start.usdc_size), 6) if next_cycle_start is not None else None
        ),
        "label_next_cycle_complete": label_next_cycle_complete,
        "label_next_cycle_complete_in_horizon": label_next_cycle_complete_in_horizon,
        "label_next_cycle_complete_delay_seconds": next_cycle_complete_delay_seconds,
        "label_next_cycle_completion_outcome": (
            next_cycle_completion.outcome if next_cycle_completion is not None else ""
        ),
        "label_next_cycle_completion_price": (
            round(float(next_cycle_completion.price), 6) if next_cycle_completion is not None else None
        ),
        "label_next_cycle_completion_usdc_size": (
            round(float(next_cycle_completion.usdc_size), 6) if next_cycle_completion is not None else None
        ),
        "label_next_cycle_first_cross_pair_sum": (
            round(float(next_cycle_first_cross_pair_sum), 6)
            if next_cycle_first_cross_pair_sum is not None
            else None
        ),
        "label_next_cycle_locked_edge": (
            round(float(next_cycle_locked_edge), 6) if next_cycle_locked_edge is not None else None
        ),
        "next_cycle_same_side_trade_count_before_completion": next_cycle_same_side_trade_count,
        "next_cycle_same_side_trade_usdc_before_completion": round(next_cycle_same_side_trade_usdc, 6),
    }
    return row, next_lock_index


def _find_first_pair_completion_index(ordered: Sequence[TradeActivity]) -> Optional[int]:
    if not ordered:
        return None
    first_outcome = str(ordered[0].outcome or "")
    if not first_outcome:
        return None
    for index in range(1, len(ordered)):
        if str(ordered[index].outcome or "") != first_outcome:
            return index
    return None


def _load_filtered_buys(path: str, *, families: set[str], durations: set[str]) -> List[TradeActivity]:
    trades: List[TradeActivity] = []
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
            trades.append(trade)
    trades.sort(key=lambda item: (item.timestamp_seconds, item.transaction_hash, item.asset))
    return trades


def _infer_resolution_timestamp_seconds_from_trade(trade: TradeActivity) -> Optional[int]:
    for candidate in [trade.event_slug, trade.slug]:
        resolution_ts = infer_resolution_timestamp_seconds_from_slug(
            candidate,
            reference_timestamp_seconds=trade.timestamp_seconds,
        )
        if resolution_ts is not None:
            return resolution_ts
    return None


def _market_key_for_trade(trade: TradeActivity) -> str:
    return str(trade.slug or trade.event_slug or trade.condition_id)


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
