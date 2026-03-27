from __future__ import annotations

import csv
import json
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional

from .models import TradeActivity


@dataclass
class PairAnalysisConfig:
    input_trades_path: str
    output_csv_path: str
    output_json_path: str
    recent_buy_limit: int = 5000
    families: tuple[str, ...] = ("btc", "eth", "sol", "xrp")
    durations: tuple[str, ...] = ("15m", "hourly", "other")


@dataclass
class PairAnalysisSummary:
    total_buy_rows: int
    filtered_buy_rows: int
    paired_markets: int
    paired_markets_below_parity: int
    paired_markets_strict_arb: int
    family_counts: Dict[str, int]
    duration_counts: Dict[str, int]
    pair_sum_bins: Dict[str, int]


@dataclass
class PairPaperReplayConfig:
    input_csv_path: str
    output_json_path: str
    curve_csv_path: Optional[str] = None
    trades_csv_path: Optional[str] = None
    initial_capital_usdc: float = 10000.0
    stake_usdc: float = 100.0
    max_effective_pair_sum: float = 1.0
    fee_bps: float = 0.0
    slippage_bps: float = 0.0
    max_pair_completion_seconds: Optional[int] = None
    max_imbalance_ratio: Optional[float] = None
    top_fraction: Optional[float] = None
    pair_sum_column: str = "pair_sum"
    pair_gap_column: str = "pair_gap_to_parity"
    timestamp_column: str = "pair_end_timestamp_seconds"
    seconds_to_resolution_column: Optional[str] = None
    min_seconds_to_resolution: Optional[int] = None
    max_seconds_to_resolution: Optional[int] = None


@dataclass
class PairPaperReplaySummary:
    total_rows: int
    eligible_rows: int
    selected_rows: int
    executed_trades: int
    skipped_for_pair_sum: int
    skipped_for_completion: int
    skipped_for_imbalance: int
    skipped_for_resolution_window: int
    initial_capital_usdc: float
    final_equity_usdc: float
    total_pnl_usdc: float
    total_return_pct: float
    max_drawdown_pct: float
    average_trade_pnl_usdc: float
    average_effective_pair_sum: float


@dataclass
class PairSequenceAnalysisConfig:
    input_trades_path: str
    output_csv_path: str
    output_json_path: str
    recent_buy_limit: int = 50000
    families: tuple[str, ...] = ("btc", "eth", "sol", "xrp")
    durations: tuple[str, ...] = ("15m", "hourly")


@dataclass
class PairSequenceAnalysisSummary:
    total_buy_rows: int
    filtered_buy_rows: int
    paired_markets: int
    markets_completed_within_5s: int
    markets_completed_within_60s: int
    markets_completed_in_tail_5m: int
    markets_completed_in_tail_15m: int
    markets_first_cross_below_parity: int
    markets_final_below_parity: int
    family_counts: Dict[str, int]
    duration_counts: Dict[str, int]
    completion_seconds_bins: Dict[str, int]
    completion_tail_bins: Dict[str, int]
    first_cross_pair_sum_bins: Dict[str, int]


@dataclass
class _OutcomeStats:
    outcome: str
    count: int = 0
    usdc: float = 0.0
    size: float = 0.0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None

    def observe(self, trade: TradeActivity) -> None:
        self.count += 1
        self.usdc += float(trade.usdc_size)
        self.size += float(trade.size)
        ts = int(trade.timestamp_seconds)
        self.first_ts = ts if self.first_ts is None else min(self.first_ts, ts)
        self.last_ts = ts if self.last_ts is None else max(self.last_ts, ts)

    @property
    def avg_price(self) -> Optional[float]:
        if self.size <= 0:
            return None
        return self.usdc / self.size


def run_pair_analysis(config: PairAnalysisConfig) -> PairAnalysisSummary:
    recent_buys = _load_recent_buys(config.input_trades_path, config.recent_buy_limit)
    filtered_buys = [
        trade
        for trade in recent_buys
        if trade.market_family in set(config.families)
        and trade.market_duration_bucket in set(config.durations)
    ]

    grouped: Dict[str, Dict[str, _OutcomeStats]] = defaultdict(dict)
    family_counts: Dict[str, int] = defaultdict(int)
    duration_counts: Dict[str, int] = defaultdict(int)
    pair_sum_bins: Dict[str, int] = defaultdict(int)

    for trade in filtered_buys:
        market_key = trade.slug or trade.event_slug or trade.condition_id
        outcome_key = str(trade.outcome or "")
        outcome_stats = grouped[market_key].get(outcome_key)
        if outcome_stats is None:
            outcome_stats = _OutcomeStats(outcome=outcome_key)
            grouped[market_key][outcome_key] = outcome_stats
        outcome_stats.observe(trade)

    rows: List[Dict[str, object]] = []
    paired_below_parity = 0
    paired_strict_arb = 0

    for market_slug, outcomes in grouped.items():
        if len(outcomes) < 2:
            continue
        sample_trade = next(
            trade for trade in filtered_buys if (trade.slug or trade.event_slug or trade.condition_id) == market_slug
        )
        sorted_outcomes = sorted(outcomes.values(), key=lambda item: item.outcome)
        up_stats = next((item for item in sorted_outcomes if item.outcome.lower() == "up"), None)
        down_stats = next((item for item in sorted_outcomes if item.outcome.lower() == "down"), None)
        if up_stats is None or down_stats is None:
            if len(sorted_outcomes) < 2:
                continue
            up_stats = sorted_outcomes[0]
            down_stats = sorted_outcomes[1]

        up_avg = up_stats.avg_price
        down_avg = down_stats.avg_price
        if up_avg is None or down_avg is None:
            continue

        pair_sum = up_avg + down_avg
        pair_gap = 1.0 - pair_sum
        total_pair_usdc = up_stats.usdc + down_stats.usdc
        total_pair_count = up_stats.count + down_stats.count
        pair_completion_seconds = max(up_stats.last_ts or 0, down_stats.last_ts or 0) - min(
            up_stats.first_ts or 0, down_stats.first_ts or 0
        )
        pair_start_timestamp_seconds = min(up_stats.first_ts or 0, down_stats.first_ts or 0)
        pair_end_timestamp_seconds = max(up_stats.last_ts or 0, down_stats.last_ts or 0)
        net_imbalance_usdc = up_stats.usdc - down_stats.usdc
        imbalance_ratio = (
            abs(net_imbalance_usdc) / total_pair_usdc if total_pair_usdc > 0 else None
        )

        if pair_sum < 1.0:
            paired_below_parity += 1
        if pair_sum < 0.97:
            paired_strict_arb += 1

        family_counts[sample_trade.market_family] += 1
        duration_counts[sample_trade.market_duration_bucket] += 1
        pair_sum_bins[_pair_sum_bin(pair_sum)] += 1

        rows.append(
            {
                "market_slug": market_slug,
                "market_family": sample_trade.market_family,
                "market_duration_bucket": sample_trade.market_duration_bucket,
                "condition_id": sample_trade.condition_id,
                "up_outcome": up_stats.outcome,
                "down_outcome": down_stats.outcome,
                "up_count": up_stats.count,
                "down_count": down_stats.count,
                "up_usdc": round(up_stats.usdc, 6),
                "down_usdc": round(down_stats.usdc, 6),
                "up_size": round(up_stats.size, 6),
                "down_size": round(down_stats.size, 6),
                "up_avg_price": round(up_avg, 6),
                "down_avg_price": round(down_avg, 6),
                "pair_sum": round(pair_sum, 6),
                "pair_gap_to_parity": round(pair_gap, 6),
                "total_pair_usdc": round(total_pair_usdc, 6),
                "total_pair_count": total_pair_count,
                "pair_start_timestamp_seconds": pair_start_timestamp_seconds,
                "pair_end_timestamp_seconds": pair_end_timestamp_seconds,
                "pair_completion_seconds": pair_completion_seconds,
                "net_imbalance_usdc": round(net_imbalance_usdc, 6),
                "imbalance_ratio": round(imbalance_ratio, 6) if imbalance_ratio is not None else None,
                "strict_pair_arb_candidate": int(pair_sum < 0.97),
                "below_parity_candidate": int(pair_sum < 1.0),
            }
        )

    rows.sort(
        key=lambda item: (
            int(item["strict_pair_arb_candidate"]),
            int(item["below_parity_candidate"]),
            float(item["pair_gap_to_parity"]),
            float(item["total_pair_usdc"]),
        ),
        reverse=True,
    )
    _write_csv(config.output_csv_path, rows)

    summary = PairAnalysisSummary(
        total_buy_rows=len(recent_buys),
        filtered_buy_rows=len(filtered_buys),
        paired_markets=len(rows),
        paired_markets_below_parity=paired_below_parity,
        paired_markets_strict_arb=paired_strict_arb,
        family_counts=dict(sorted(family_counts.items())),
        duration_counts=dict(sorted(duration_counts.items())),
        pair_sum_bins=dict(sorted(pair_sum_bins.items())),
    )
    Path(config.output_json_path).parent.mkdir(parents=True, exist_ok=True)
    Path(config.output_json_path).write_text(
        json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def run_pair_paper_replay(config: PairPaperReplayConfig) -> PairPaperReplaySummary:
    with Path(config.input_csv_path).open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        raise ValueError("input pair analysis dataset is empty")

    fee_rate = float(config.fee_bps) / 10000.0
    slippage_rate = float(config.slippage_bps) / 10000.0

    enriched_rows: List[Dict[str, object]] = []
    skipped_for_pair_sum = 0
    skipped_for_completion = 0
    skipped_for_imbalance = 0
    skipped_for_resolution_window = 0
    for row in rows:
        pair_sum = float(row[config.pair_sum_column])
        effective_pair_sum = pair_sum * (1.0 + fee_rate + slippage_rate)
        pair_completion_seconds = int(float(row["pair_completion_seconds"]))
        imbalance_ratio = row.get("imbalance_ratio")
        imbalance_ratio_value = float(imbalance_ratio) if imbalance_ratio not in (None, "", "None") else None
        seconds_to_resolution_value = None
        if config.seconds_to_resolution_column:
            raw_seconds_to_resolution = row.get(config.seconds_to_resolution_column)
            if raw_seconds_to_resolution not in (None, "", "None"):
                seconds_to_resolution_value = int(float(raw_seconds_to_resolution))

        if effective_pair_sum > float(config.max_effective_pair_sum):
            skipped_for_pair_sum += 1
            continue
        if (
            config.max_pair_completion_seconds is not None
            and pair_completion_seconds > int(config.max_pair_completion_seconds)
        ):
            skipped_for_completion += 1
            continue
        if (
            config.max_imbalance_ratio is not None
            and imbalance_ratio_value is not None
            and imbalance_ratio_value > float(config.max_imbalance_ratio)
        ):
            skipped_for_imbalance += 1
            continue
        if (
            seconds_to_resolution_value is not None
            and (
                (
                    config.min_seconds_to_resolution is not None
                    and seconds_to_resolution_value < int(config.min_seconds_to_resolution)
                )
                or (
                    config.max_seconds_to_resolution is not None
                    and seconds_to_resolution_value > int(config.max_seconds_to_resolution)
                )
            )
        ):
            skipped_for_resolution_window += 1
            continue

        payout_multiple = 1.0 / effective_pair_sum
        pnl_usdc = float(config.stake_usdc) * (payout_multiple - 1.0)
        enriched_row = dict(row)
        enriched_row["effective_pair_sum"] = round(effective_pair_sum, 6)
        enriched_row["payout_multiple"] = round(payout_multiple, 6)
        enriched_row["pnl_usdc"] = round(pnl_usdc, 6)
        enriched_rows.append(enriched_row)

    enriched_rows.sort(
        key=lambda item: (
            int(float(item[config.timestamp_column])),
            -float(item[config.pair_gap_column]),
            -float(item["total_pair_usdc"]),
        )
    )
    eligible_rows = len(enriched_rows)
    if config.top_fraction is not None:
        top_count = max(1, int(eligible_rows * float(config.top_fraction))) if eligible_rows else 0
        enriched_rows = sorted(
            enriched_rows,
            key=lambda item: (float(item["effective_pair_sum"]), -float(item["total_pair_usdc"])),
        )[:top_count]
        enriched_rows.sort(key=lambda item: int(float(item[config.timestamp_column])))

    equity = float(config.initial_capital_usdc)
    peak_equity = equity
    max_drawdown_pct = 0.0
    trade_rows: List[Dict[str, object]] = []
    curve_rows: List[Dict[str, object]] = []
    total_pnl_usdc = 0.0
    total_effective_pair_sum = 0.0

    for row in enriched_rows:
        pnl_usdc = float(row["pnl_usdc"])
        equity += pnl_usdc
        total_pnl_usdc += pnl_usdc
        total_effective_pair_sum += float(row["effective_pair_sum"])
        peak_equity = max(peak_equity, equity)
        drawdown_pct = ((peak_equity - equity) / peak_equity * 100.0) if peak_equity > 0 else 0.0
        max_drawdown_pct = max(max_drawdown_pct, drawdown_pct)
        trade_row = dict(row)
        trade_row["stake_usdc"] = round(float(config.stake_usdc), 6)
        trade_row["equity_after_trade_usdc"] = round(equity, 6)
        trade_rows.append(trade_row)
        curve_rows.append(
            {
                "timestamp_seconds": int(float(row[config.timestamp_column])),
                "equity_usdc": round(equity, 6),
                "trade_pnl_usdc": round(pnl_usdc, 6),
                "effective_pair_sum": row["effective_pair_sum"],
            }
        )

    if config.curve_csv_path:
        _write_csv(config.curve_csv_path, curve_rows)
    if config.trades_csv_path:
        _write_csv(config.trades_csv_path, trade_rows)

    average_trade_pnl = total_pnl_usdc / len(trade_rows) if trade_rows else 0.0
    average_effective_pair_sum = total_effective_pair_sum / len(trade_rows) if trade_rows else 0.0
    summary = PairPaperReplaySummary(
        total_rows=len(rows),
        eligible_rows=eligible_rows,
        selected_rows=len(enriched_rows),
        executed_trades=len(trade_rows),
        skipped_for_pair_sum=skipped_for_pair_sum,
        skipped_for_completion=skipped_for_completion,
        skipped_for_imbalance=skipped_for_imbalance,
        skipped_for_resolution_window=skipped_for_resolution_window,
        initial_capital_usdc=float(config.initial_capital_usdc),
        final_equity_usdc=round(equity, 6),
        total_pnl_usdc=round(total_pnl_usdc, 6),
        total_return_pct=round(((equity / float(config.initial_capital_usdc)) - 1.0) * 100.0, 6),
        max_drawdown_pct=round(max_drawdown_pct, 6),
        average_trade_pnl_usdc=round(average_trade_pnl, 6),
        average_effective_pair_sum=round(average_effective_pair_sum, 6),
    )
    Path(config.output_json_path).parent.mkdir(parents=True, exist_ok=True)
    Path(config.output_json_path).write_text(
        json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def run_pair_sequence_analysis(config: PairSequenceAnalysisConfig) -> PairSequenceAnalysisSummary:
    recent_buys = _load_recent_buys(config.input_trades_path, config.recent_buy_limit)
    filtered_buys = [
        trade
        for trade in recent_buys
        if trade.market_family in set(config.families)
        and trade.market_duration_bucket in set(config.durations)
    ]

    grouped: Dict[str, List[TradeActivity]] = defaultdict(list)
    for trade in filtered_buys:
        market_key = trade.slug or trade.event_slug or trade.condition_id
        grouped[market_key].append(trade)

    family_counts: Dict[str, int] = defaultdict(int)
    duration_counts: Dict[str, int] = defaultdict(int)
    completion_seconds_bins: Dict[str, int] = defaultdict(int)
    completion_tail_bins: Dict[str, int] = defaultdict(int)
    first_cross_pair_sum_bins: Dict[str, int] = defaultdict(int)

    rows: List[Dict[str, object]] = []
    markets_completed_within_5s = 0
    markets_completed_within_60s = 0
    markets_completed_in_tail_5m = 0
    markets_completed_in_tail_15m = 0
    markets_first_cross_below_parity = 0
    markets_final_below_parity = 0

    for market_slug, market_trades in grouped.items():
        ordered = sorted(market_trades, key=lambda trade: (trade.timestamp_ms, trade.transaction_hash, trade.asset))
        outcomes = {str(trade.outcome or "") for trade in ordered if str(trade.outcome or "")}
        if len(outcomes) < 2:
            continue

        up_trades = [trade for trade in ordered if str(trade.outcome).lower() == "up"]
        down_trades = [trade for trade in ordered if str(trade.outcome).lower() == "down"]
        if not up_trades or not down_trades:
            continue

        up_first = up_trades[0]
        down_first = down_trades[0]
        first_leg = min([up_first, down_first], key=lambda trade: trade.timestamp_ms)
        second_leg = max([up_first, down_first], key=lambda trade: trade.timestamp_ms)
        first_cross_pair_sum = float(up_first.price) + float(down_first.price)
        first_cross_gap = 1.0 - first_cross_pair_sum
        pair_completion_seconds = second_leg.timestamp_seconds - first_leg.timestamp_seconds

        up_stats = _OutcomeStats(outcome="Up")
        down_stats = _OutcomeStats(outcome="Down")
        for trade in up_trades:
            up_stats.observe(trade)
        for trade in down_trades:
            down_stats.observe(trade)
        if up_stats.avg_price is None or down_stats.avg_price is None:
            continue
        final_pair_sum = float(up_stats.avg_price) + float(down_stats.avg_price)
        final_pair_gap = 1.0 - final_pair_sum
        total_pair_usdc = up_stats.usdc + down_stats.usdc
        net_imbalance_usdc = up_stats.usdc - down_stats.usdc
        imbalance_ratio = abs(net_imbalance_usdc) / total_pair_usdc if total_pair_usdc > 0 else None

        resolution_timestamp_seconds = _infer_resolution_timestamp_seconds_from_market_slug(
            market_slug,
            reference_timestamp_seconds=first_leg.timestamp_seconds,
        )
        first_leg_seconds_to_resolution = (
            resolution_timestamp_seconds - first_leg.timestamp_seconds
            if resolution_timestamp_seconds is not None
            else None
        )
        second_leg_seconds_to_resolution = (
            resolution_timestamp_seconds - second_leg.timestamp_seconds
            if resolution_timestamp_seconds is not None
            else None
        )

        if pair_completion_seconds <= 5:
            markets_completed_within_5s += 1
        if pair_completion_seconds <= 60:
            markets_completed_within_60s += 1
        if second_leg_seconds_to_resolution is not None and second_leg_seconds_to_resolution <= 300:
            markets_completed_in_tail_5m += 1
        if second_leg_seconds_to_resolution is not None and second_leg_seconds_to_resolution <= 900:
            markets_completed_in_tail_15m += 1
        if first_cross_pair_sum < 1.0:
            markets_first_cross_below_parity += 1
        if final_pair_sum < 1.0:
            markets_final_below_parity += 1

        sample_trade = ordered[0]
        family_counts[sample_trade.market_family] += 1
        duration_counts[sample_trade.market_duration_bucket] += 1
        completion_seconds_bins[_completion_seconds_bin(pair_completion_seconds)] += 1
        completion_tail_bins[_tail_seconds_bin(second_leg_seconds_to_resolution)] += 1
        first_cross_pair_sum_bins[_pair_sum_bin(first_cross_pair_sum)] += 1

        rows.append(
            {
                "market_slug": market_slug,
                "market_family": sample_trade.market_family,
                "market_duration_bucket": sample_trade.market_duration_bucket,
                "condition_id": sample_trade.condition_id,
                "first_leg_outcome": first_leg.outcome,
                "second_leg_outcome": second_leg.outcome,
                "first_leg_timestamp_seconds": first_leg.timestamp_seconds,
                "second_leg_timestamp_seconds": second_leg.timestamp_seconds,
                "first_leg_price": round(float(first_leg.price), 6),
                "second_leg_price": round(float(second_leg.price), 6),
                "first_cross_pair_sum": round(first_cross_pair_sum, 6),
                "first_cross_gap_to_parity": round(first_cross_gap, 6),
                "pair_completion_seconds": pair_completion_seconds,
                "resolution_timestamp_seconds": resolution_timestamp_seconds,
                "first_leg_seconds_to_resolution": first_leg_seconds_to_resolution,
                "second_leg_seconds_to_resolution": second_leg_seconds_to_resolution,
                "up_count": up_stats.count,
                "down_count": down_stats.count,
                "up_usdc": round(up_stats.usdc, 6),
                "down_usdc": round(down_stats.usdc, 6),
                "up_avg_price": round(float(up_stats.avg_price), 6),
                "down_avg_price": round(float(down_stats.avg_price), 6),
                "final_pair_sum": round(final_pair_sum, 6),
                "final_pair_gap_to_parity": round(final_pair_gap, 6),
                "total_pair_usdc": round(total_pair_usdc, 6),
                "net_imbalance_usdc": round(net_imbalance_usdc, 6),
                "imbalance_ratio": round(imbalance_ratio, 6) if imbalance_ratio is not None else None,
                "first_cross_below_parity": int(first_cross_pair_sum < 1.0),
                "final_below_parity": int(final_pair_sum < 1.0),
            }
        )

    rows.sort(
        key=lambda item: (
            int(item["first_cross_below_parity"]),
            -float(item["first_cross_gap_to_parity"]),
            -float(item["total_pair_usdc"]),
        ),
        reverse=True,
    )
    _write_csv(config.output_csv_path, rows)

    summary = PairSequenceAnalysisSummary(
        total_buy_rows=len(recent_buys),
        filtered_buy_rows=len(filtered_buys),
        paired_markets=len(rows),
        markets_completed_within_5s=markets_completed_within_5s,
        markets_completed_within_60s=markets_completed_within_60s,
        markets_completed_in_tail_5m=markets_completed_in_tail_5m,
        markets_completed_in_tail_15m=markets_completed_in_tail_15m,
        markets_first_cross_below_parity=markets_first_cross_below_parity,
        markets_final_below_parity=markets_final_below_parity,
        family_counts=dict(sorted(family_counts.items())),
        duration_counts=dict(sorted(duration_counts.items())),
        completion_seconds_bins=dict(sorted(completion_seconds_bins.items())),
        completion_tail_bins=dict(sorted(completion_tail_bins.items())),
        first_cross_pair_sum_bins=dict(sorted(first_cross_pair_sum_bins.items())),
    )
    Path(config.output_json_path).parent.mkdir(parents=True, exist_ok=True)
    Path(config.output_json_path).write_text(
        json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return summary


def _load_recent_buys(path: str, recent_buy_limit: int) -> List[TradeActivity]:
    recent: Deque[TradeActivity] = deque(maxlen=recent_buy_limit)
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
            if trade.side.upper() == "BUY":
                recent.append(trade)
    return list(recent)


def _write_csv(path: str, rows: Iterable[Dict[str, object]]) -> None:
    rows = list(rows)
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output.write_text("", encoding="utf-8")
        return
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _pair_sum_bin(pair_sum: float) -> str:
    if pair_sum < 0.97:
        return "<0.97"
    if pair_sum < 0.99:
        return "0.97-0.99"
    if pair_sum <= 1.01:
        return "0.99-1.01"
    return ">1.01"


def _completion_seconds_bin(seconds: int) -> str:
    if seconds <= 1:
        return "<=1s"
    if seconds <= 5:
        return "1-5s"
    if seconds <= 60:
        return "5-60s"
    if seconds <= 300:
        return "1-5m"
    if seconds <= 1800:
        return "5-30m"
    return ">30m"


def _tail_seconds_bin(seconds_to_resolution: Optional[int]) -> str:
    if seconds_to_resolution is None:
        return "unknown"
    if seconds_to_resolution <= 300:
        return "<=5m_to_resolve"
    if seconds_to_resolution <= 900:
        return "5-15m_to_resolve"
    if seconds_to_resolution <= 3600:
        return "15-60m_to_resolve"
    return ">60m_to_resolve"


def _infer_resolution_timestamp_seconds_from_market_slug(
    market_slug: str,
    reference_timestamp_seconds: int = 0,
) -> Optional[int]:
    from .evaluation import _infer_resolution_timestamp_seconds

    slug = str(market_slug or "")
    if not slug:
        return None
    trade = TradeActivity(
        proxy_wallet="",
        timestamp_ms=int(reference_timestamp_seconds) * 1000,
        condition_id="",
        activity_type="TRADE",
        size=0.0,
        usdc_size=0.0,
        transaction_hash="",
        price=0.0,
        asset="",
        side="BUY",
        outcome_index=0,
        title=slug,
        slug=slug,
        event_slug=slug,
        outcome="",
        name="",
        pseudonym="",
    )
    return _infer_resolution_timestamp_seconds(trade)
