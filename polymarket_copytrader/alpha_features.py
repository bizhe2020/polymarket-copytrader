from __future__ import annotations

import csv
import json
import math
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from bisect import bisect_left, bisect_right
from typing import Deque, Dict, Iterable, List, Optional, Sequence, Tuple

from .evaluation import _infer_resolution_timestamp_seconds
from .models import PriceHistoryPoint, TradeActivity

_WINDOWS_SECONDS = (60, 300, 900)
_OUTCOME_OPPOSITES = {
    "up": "Down",
    "down": "Up",
    "yes": "No",
    "no": "Yes",
}


@dataclass
class AlphaFeatureConfig:
    input_trades_path: str
    output_csv_path: str
    summary_json_path: str
    price_cache_dir: Optional[str] = None
    external_market_data_dir: Optional[str] = None
    families: Tuple[str, ...] = ("btc", "eth")
    durations: Tuple[str, ...] = ("5m", "15m")
    include_negative_samples: bool = True
    negative_window_seconds: int = 60


class PriceHistoryLookup:
    def __init__(self, price_cache_dir: Optional[str]) -> None:
        self.price_cache_dir = Path(price_cache_dir) if price_cache_dir else None
        self._cache: Dict[str, List[PriceHistoryPoint]] = {}
        self._timestamps: Dict[str, List[int]] = {}

    def get_price(self, asset_id: str, timestamp_seconds: int) -> Optional[float]:
        points = self._load(asset_id)
        if not points:
            return None
        timestamps = self._timestamps.get(asset_id, [])
        index = bisect_right(timestamps, timestamp_seconds) - 1
        if index < 0:
            return None
        return points[index].price

    def get_return(
        self, asset_id: str, timestamp_seconds: int, lookback_seconds: int
    ) -> Optional[float]:
        now_price = self.get_price(asset_id, timestamp_seconds)
        then_price = self.get_price(asset_id, timestamp_seconds - lookback_seconds)
        if now_price is None or then_price is None or then_price <= 0:
            return None
        return (now_price - then_price) / then_price

    def _load(self, asset_id: str) -> List[PriceHistoryPoint]:
        if asset_id in self._cache:
            return self._cache[asset_id]
        if self.price_cache_dir is None:
            self._cache[asset_id] = []
            return self._cache[asset_id]
        path = self.price_cache_dir / f"{asset_id}.json"
        if not path.exists():
            self._cache[asset_id] = []
            return self._cache[asset_id]
        raw = json.loads(path.read_text(encoding="utf-8"))
        points = [
            PriceHistoryPoint(
                timestamp_seconds=int(item["timestamp_seconds"]),
                price=float(item["price"]),
            )
            for item in raw
        ]
        points.sort(key=lambda item: item.timestamp_seconds)
        self._cache[asset_id] = points
        self._timestamps[asset_id] = [point.timestamp_seconds for point in points]
        return points


class ExternalMarketDataLookup:
    def __init__(self, market_data_dir: Optional[str]) -> None:
        self.market_data_dir = Path(market_data_dir) if market_data_dir else None
        self._rows_by_family: Dict[str, List[Dict[str, float]]] = {}
        self._timestamps_by_family: Dict[str, List[int]] = {}

    def get_close(self, family: str, timestamp_seconds: int) -> Optional[float]:
        row = self._get_latest_row(family, timestamp_seconds)
        return None if row is None else row["close"]

    def get_return(self, family: str, timestamp_seconds: int, lookback_seconds: int) -> Optional[float]:
        now_price = self.get_close(family, timestamp_seconds)
        then_price = self.get_close(family, timestamp_seconds - lookback_seconds)
        if now_price is None or then_price is None or then_price <= 0:
            return None
        return (now_price - then_price) / then_price

    def get_volume_sum(self, family: str, timestamp_seconds: int, lookback_seconds: int) -> Optional[float]:
        rows = self._rows_in_window(family, timestamp_seconds, lookback_seconds)
        if not rows:
            return None
        if all(row.get("volume") is None for row in rows):
            return None
        return sum(float(row.get("volume") or 0.0) for row in rows)

    def get_trade_count_sum(self, family: str, timestamp_seconds: int, lookback_seconds: int) -> Optional[float]:
        rows = self._rows_in_window(family, timestamp_seconds, lookback_seconds)
        if not rows:
            return None
        if all(row.get("trade_count") is None for row in rows):
            return None
        return sum(float(row.get("trade_count") or 0.0) for row in rows)

    def get_realized_vol(self, family: str, timestamp_seconds: int, lookback_seconds: int) -> Optional[float]:
        rows = self._rows_in_window(family, timestamp_seconds, lookback_seconds)
        closes = [float(row["close"]) for row in rows if row.get("close") is not None]
        if len(closes) < 2:
            return None
        returns: List[float] = []
        for previous, current in zip(closes, closes[1:]):
            if previous <= 0:
                continue
            returns.append((current - previous) / previous)
        if len(returns) < 2:
            return None
        mean_value = sum(returns) / len(returns)
        variance = sum((item - mean_value) ** 2 for item in returns) / len(returns)
        return math.sqrt(variance)

    def _get_latest_row(self, family: str, timestamp_seconds: int) -> Optional[Dict[str, float]]:
        rows = self._load(family)
        if not rows:
            return None
        timestamps = self._timestamps_by_family.get(family, [])
        index = bisect_right(timestamps, timestamp_seconds) - 1
        if index < 0:
            return None
        return rows[index]

    def _rows_in_window(
        self, family: str, timestamp_seconds: int, lookback_seconds: int
    ) -> List[Dict[str, float]]:
        rows = self._load(family)
        if not rows:
            return []
        timestamps = self._timestamps_by_family.get(family, [])
        start = timestamp_seconds - lookback_seconds
        left = bisect_left(timestamps, start)
        right = bisect_right(timestamps, timestamp_seconds)
        if left >= right:
            return []
        return rows[left:right]

    def _load(self, family: str) -> List[Dict[str, float]]:
        if family in self._rows_by_family:
            return self._rows_by_family[family]
        if self.market_data_dir is None:
            self._rows_by_family[family] = []
            return self._rows_by_family[family]
        path = self.market_data_dir / f"{family}.csv"
        if not path.exists():
            self._rows_by_family[family] = []
            return self._rows_by_family[family]
        rows: List[Dict[str, float]] = []
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw in reader:
                ts_raw = raw.get("timestamp_seconds") or raw.get("timestamp")
                close_raw = raw.get("close")
                if ts_raw in (None, "") or close_raw in (None, ""):
                    continue
                rows.append(
                    {
                        "timestamp_seconds": int(float(ts_raw)),
                        "close": float(close_raw),
                        "volume": _parse_optional_float(raw.get("volume")),
                        "trade_count": _parse_optional_float(raw.get("trade_count")),
                    }
                )
        rows.sort(key=lambda item: int(item["timestamp_seconds"]))
        self._rows_by_family[family] = rows
        self._timestamps_by_family[family] = [int(row["timestamp_seconds"]) for row in rows]
        return rows


class RollingWindowTracker:
    def __init__(self) -> None:
        self._entries: Deque[Tuple[int, float]] = deque()
        self._sum_usdc: float = 0.0

    def snapshot(self, now_seconds: int, window_seconds: int) -> Tuple[int, float]:
        self._prune(now_seconds, window_seconds)
        return len(self._entries), self._sum_usdc

    def observe(self, timestamp_seconds: int, usdc_size: float) -> None:
        self._entries.append((timestamp_seconds, usdc_size))
        self._sum_usdc += usdc_size

    def _prune(self, now_seconds: int, window_seconds: int) -> None:
        cutoff = now_seconds - window_seconds
        while self._entries and self._entries[0][0] < cutoff:
            _, usdc_size = self._entries.popleft()
            self._sum_usdc -= usdc_size


@dataclass
class AlphaFeatureSummary:
    total_rows: int
    positive_rows: int
    negative_rows: int
    filtered_trades: int
    families: List[str]
    durations: List[str]
    source_kind_counts: Dict[str, int]
    label_counts: Dict[str, int]
    market_family_counts: Dict[str, int]
    duration_counts: Dict[str, int]


def build_alpha_feature_dataset(config: AlphaFeatureConfig) -> AlphaFeatureSummary:
    trades = _load_trades(config.input_trades_path)
    filtered_trades = [
        trade
        for trade in trades
        if trade.side.upper() == "BUY"
        and trade.market_family in set(config.families)
        and trade.market_duration_bucket in set(config.durations)
    ]
    filtered_trades.sort(key=lambda trade: (trade.timestamp_seconds, trade.transaction_hash, trade.asset))

    condition_outcome_assets = _build_condition_outcome_assets(filtered_trades)
    condition_outcome_timestamps = _build_condition_outcome_timestamps(filtered_trades)
    price_lookup = PriceHistoryLookup(config.price_cache_dir)
    external_market_lookup = ExternalMarketDataLookup(config.external_market_data_dir)

    overall_trackers = {window: RollingWindowTracker() for window in _WINDOWS_SECONDS}
    condition_trackers: Dict[str, Dict[int, RollingWindowTracker]] = defaultdict(_make_window_tracker_map)
    condition_outcome_trackers: Dict[Tuple[str, str], Dict[int, RollingWindowTracker]] = defaultdict(
        _make_window_tracker_map
    )
    market_trackers: Dict[str, Dict[int, RollingWindowTracker]] = defaultdict(_make_window_tracker_map)
    market_outcome_trackers: Dict[Tuple[str, str], Dict[int, RollingWindowTracker]] = defaultdict(
        _make_window_tracker_map
    )

    prev_trade_ts_by_wallet: Dict[str, int] = {}
    prev_trade_ts_by_market: Dict[str, int] = {}
    prev_trade_ts_by_condition: Dict[str, int] = {}
    prev_trade_ts_by_market_outcome: Dict[Tuple[str, str], int] = {}

    source_kind_counter: Counter[str] = Counter()
    label_counter: Counter[str] = Counter()
    family_counter: Counter[str] = Counter()
    duration_counter: Counter[str] = Counter()
    total_rows = 0

    output_path = Path(config.output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    writer = None
    csv_handle = output_path.open("w", encoding="utf-8", newline="")

    try:
        for trade in filtered_trades:
            positive_row = _build_feature_row(
                trade=trade,
                label_buy=1,
                candidate_outcome=trade.outcome,
                candidate_asset=trade.asset,
                candidate_price=price_lookup.get_price(trade.asset, trade.timestamp_seconds) or trade.price,
                source_kind="positive_trade",
                price_lookup=price_lookup,
                external_market_lookup=external_market_lookup,
                overall_trackers=overall_trackers,
                condition_trackers=condition_trackers,
                condition_outcome_trackers=condition_outcome_trackers,
                market_trackers=market_trackers,
                market_outcome_trackers=market_outcome_trackers,
                prev_trade_ts_by_wallet=prev_trade_ts_by_wallet,
                prev_trade_ts_by_market=prev_trade_ts_by_market,
                prev_trade_ts_by_condition=prev_trade_ts_by_condition,
                prev_trade_ts_by_market_outcome=prev_trade_ts_by_market_outcome,
            )
            writer = _write_row(writer, csv_handle, positive_row)
            total_rows += 1
            source_kind_counter["positive_trade"] += 1
            label_counter["1"] += 1
            family_counter[trade.market_family] += 1
            duration_counter[trade.market_duration_bucket] += 1

            if config.include_negative_samples:
                negative_row = _build_negative_row(
                    trade=trade,
                    condition_outcome_assets=condition_outcome_assets,
                    condition_outcome_timestamps=condition_outcome_timestamps,
                    negative_window_seconds=config.negative_window_seconds,
                    price_lookup=price_lookup,
                    external_market_lookup=external_market_lookup,
                    overall_trackers=overall_trackers,
                    condition_trackers=condition_trackers,
                    condition_outcome_trackers=condition_outcome_trackers,
                    market_trackers=market_trackers,
                    market_outcome_trackers=market_outcome_trackers,
                    prev_trade_ts_by_wallet=prev_trade_ts_by_wallet,
                    prev_trade_ts_by_market=prev_trade_ts_by_market,
                    prev_trade_ts_by_condition=prev_trade_ts_by_condition,
                    prev_trade_ts_by_market_outcome=prev_trade_ts_by_market_outcome,
                )
                if negative_row is not None:
                    writer = _write_row(writer, csv_handle, negative_row)
                    total_rows += 1
                    source_kind_counter[str(negative_row["source_kind"])] += 1
                    label_counter["0"] += 1
                    family_counter[trade.market_family] += 1
                    duration_counter[trade.market_duration_bucket] += 1

            _observe_trade(
                trade,
                overall_trackers=overall_trackers,
                condition_trackers=condition_trackers,
                condition_outcome_trackers=condition_outcome_trackers,
                market_trackers=market_trackers,
                market_outcome_trackers=market_outcome_trackers,
                prev_trade_ts_by_wallet=prev_trade_ts_by_wallet,
                prev_trade_ts_by_market=prev_trade_ts_by_market,
                prev_trade_ts_by_condition=prev_trade_ts_by_condition,
                prev_trade_ts_by_market_outcome=prev_trade_ts_by_market_outcome,
            )
    finally:
        csv_handle.close()

    summary = AlphaFeatureSummary(
        total_rows=total_rows,
        positive_rows=label_counter["1"],
        negative_rows=label_counter["0"],
        filtered_trades=len(filtered_trades),
        families=list(config.families),
        durations=list(config.durations),
        source_kind_counts=dict(source_kind_counter),
        label_counts=dict(label_counter),
        market_family_counts=dict(family_counter),
        duration_counts=dict(duration_counter),
    )
    summary_path = Path(config.summary_json_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(asdict(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return summary


def _build_negative_row(
    trade: TradeActivity,
    condition_outcome_assets: Dict[str, Dict[str, str]],
    condition_outcome_timestamps: Dict[Tuple[str, str], List[int]],
    negative_window_seconds: int,
    price_lookup: PriceHistoryLookup,
    external_market_lookup: ExternalMarketDataLookup,
    overall_trackers: Dict[int, RollingWindowTracker],
    condition_trackers: Dict[str, Dict[int, RollingWindowTracker]],
    condition_outcome_trackers: Dict[Tuple[str, str], Dict[int, RollingWindowTracker]],
    market_trackers: Dict[str, Dict[int, RollingWindowTracker]],
    market_outcome_trackers: Dict[Tuple[str, str], Dict[int, RollingWindowTracker]],
    prev_trade_ts_by_wallet: Dict[str, int],
    prev_trade_ts_by_market: Dict[str, int],
    prev_trade_ts_by_condition: Dict[str, int],
    prev_trade_ts_by_market_outcome: Dict[Tuple[str, str], int],
) -> Optional[Dict[str, object]]:
    opposite_outcome = _opposite_outcome(trade.outcome)
    if opposite_outcome is None:
        return None
    if _has_local_trade(
        timestamps=condition_outcome_timestamps.get((trade.condition_id, opposite_outcome), []),
        center_ts=trade.timestamp_seconds,
        window_seconds=negative_window_seconds,
    ):
        return None
    opposite_asset = condition_outcome_assets.get(trade.condition_id, {}).get(opposite_outcome, "")
    candidate_price = None
    if opposite_asset:
        candidate_price = price_lookup.get_price(opposite_asset, trade.timestamp_seconds)
    if candidate_price is None:
        candidate_price = max(0.001, min(0.999, 1.0 - trade.price))
    return _build_feature_row(
        trade=trade,
        label_buy=0,
        candidate_outcome=opposite_outcome,
        candidate_asset=opposite_asset,
        candidate_price=candidate_price,
        source_kind="paired_opposite_negative",
        price_lookup=price_lookup,
        external_market_lookup=external_market_lookup,
        overall_trackers=overall_trackers,
        condition_trackers=condition_trackers,
        condition_outcome_trackers=condition_outcome_trackers,
        market_trackers=market_trackers,
        market_outcome_trackers=market_outcome_trackers,
        prev_trade_ts_by_wallet=prev_trade_ts_by_wallet,
        prev_trade_ts_by_market=prev_trade_ts_by_market,
        prev_trade_ts_by_condition=prev_trade_ts_by_condition,
        prev_trade_ts_by_market_outcome=prev_trade_ts_by_market_outcome,
    )


def _build_feature_row(
    trade: TradeActivity,
    label_buy: int,
    candidate_outcome: str,
    candidate_asset: str,
    candidate_price: float,
    source_kind: str,
    price_lookup: PriceHistoryLookup,
    external_market_lookup: ExternalMarketDataLookup,
    overall_trackers: Dict[int, RollingWindowTracker],
    condition_trackers: Dict[str, Dict[int, RollingWindowTracker]],
    condition_outcome_trackers: Dict[Tuple[str, str], Dict[int, RollingWindowTracker]],
    market_trackers: Dict[str, Dict[int, RollingWindowTracker]],
    market_outcome_trackers: Dict[Tuple[str, str], Dict[int, RollingWindowTracker]],
    prev_trade_ts_by_wallet: Dict[str, int],
    prev_trade_ts_by_market: Dict[str, int],
    prev_trade_ts_by_condition: Dict[str, int],
    prev_trade_ts_by_market_outcome: Dict[Tuple[str, str], int],
) -> Dict[str, object]:
    outcome_key = candidate_outcome.lower()
    opposite_outcome = _opposite_outcome(candidate_outcome) or ""
    ts = trade.timestamp_seconds
    row: Dict[str, object] = {
        "sample_id": f"{trade.transaction_hash}:{trade.asset}:{source_kind}:{candidate_outcome}",
        "label_buy": label_buy,
        "source_kind": source_kind,
        "timestamp_seconds": ts,
        "timestamp_iso": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "condition_id": trade.condition_id,
        "market_slug": trade.slug,
        "event_slug": trade.event_slug,
        "title": trade.title,
        "target_wallet": trade.proxy_wallet,
        "transaction_hash": trade.transaction_hash,
        "candidate_outcome": candidate_outcome,
        "candidate_asset": candidate_asset,
        "candidate_price": round(candidate_price, 6),
        "reference_trade_asset": trade.asset,
        "reference_trade_outcome": trade.outcome,
        "reference_trade_price": round(trade.price, 6),
        "reference_trade_usdc_size": round(trade.usdc_size, 6),
        "reference_trade_size": round(trade.size, 6),
        "market_family": trade.market_family,
        "market_duration_bucket": trade.market_duration_bucket,
        "is_priority_short_market": int(trade.is_priority_short_market),
        "price_distance_from_mid": round(trade.price - 0.5, 6),
        "abs_price_distance_from_mid": round(abs(trade.price - 0.5), 6),
        "candidate_price_distance_from_mid": round(candidate_price - 0.5, 6),
        "candidate_abs_price_distance_from_mid": round(abs(candidate_price - 0.5), 6),
        "time_since_prev_trade_seconds": _elapsed_seconds(prev_trade_ts_by_wallet.get(trade.proxy_wallet), ts),
        "time_since_prev_same_market_trade_seconds": _elapsed_seconds(prev_trade_ts_by_market.get(trade.slug), ts),
        "time_since_prev_same_condition_trade_seconds": _elapsed_seconds(
            prev_trade_ts_by_condition.get(trade.condition_id), ts
        ),
        "time_since_prev_same_market_outcome_trade_seconds": _elapsed_seconds(
            prev_trade_ts_by_market_outcome.get((trade.slug, outcome_key)), ts
        ),
        "seconds_to_resolution": _seconds_to_resolution(trade),
        "asset_mark_price": _round_optional(price_lookup.get_price(candidate_asset, ts) if candidate_asset else None),
        "asset_return_60s": _round_optional(
            price_lookup.get_return(candidate_asset, ts, 60) if candidate_asset else None
        ),
        "asset_return_300s": _round_optional(
            price_lookup.get_return(candidate_asset, ts, 300) if candidate_asset else None
        ),
        "asset_return_900s": _round_optional(
            price_lookup.get_return(candidate_asset, ts, 900) if candidate_asset else None
        ),
        "external_market_close": _round_optional(external_market_lookup.get_close(trade.market_family, ts)),
        "external_market_return_60s": _round_optional(
            external_market_lookup.get_return(trade.market_family, ts, 60)
        ),
        "external_market_return_300s": _round_optional(
            external_market_lookup.get_return(trade.market_family, ts, 300)
        ),
        "external_market_return_900s": _round_optional(
            external_market_lookup.get_return(trade.market_family, ts, 900)
        ),
        "external_market_volume_60s": _round_optional(
            external_market_lookup.get_volume_sum(trade.market_family, ts, 60)
        ),
        "external_market_volume_300s": _round_optional(
            external_market_lookup.get_volume_sum(trade.market_family, ts, 300)
        ),
        "external_market_volume_900s": _round_optional(
            external_market_lookup.get_volume_sum(trade.market_family, ts, 900)
        ),
        "external_market_trade_count_60s": _round_optional(
            external_market_lookup.get_trade_count_sum(trade.market_family, ts, 60)
        ),
        "external_market_trade_count_300s": _round_optional(
            external_market_lookup.get_trade_count_sum(trade.market_family, ts, 300)
        ),
        "external_market_trade_count_900s": _round_optional(
            external_market_lookup.get_trade_count_sum(trade.market_family, ts, 900)
        ),
        "external_market_realized_vol_60s": _round_optional(
            external_market_lookup.get_realized_vol(trade.market_family, ts, 60)
        ),
        "external_market_realized_vol_300s": _round_optional(
            external_market_lookup.get_realized_vol(trade.market_family, ts, 300)
        ),
        "external_market_realized_vol_900s": _round_optional(
            external_market_lookup.get_realized_vol(trade.market_family, ts, 900)
        ),
    }

    for window in _WINDOWS_SECONDS:
        overall_count, overall_usdc = overall_trackers[window].snapshot(ts, window)
        row[f"recent_trade_count_{window}s"] = overall_count
        row[f"recent_trade_usdc_{window}s"] = round(overall_usdc, 6)

        condition_count, condition_usdc = condition_trackers[trade.condition_id][window].snapshot(ts, window)
        row[f"recent_condition_count_{window}s"] = condition_count
        row[f"recent_condition_usdc_{window}s"] = round(condition_usdc, 6)

        same_outcome_count, same_outcome_usdc = condition_outcome_trackers[
            (trade.condition_id, outcome_key)
        ][window].snapshot(ts, window)
        row[f"recent_same_outcome_count_{window}s"] = same_outcome_count
        row[f"recent_same_outcome_usdc_{window}s"] = round(same_outcome_usdc, 6)

        opposite_count, opposite_usdc = condition_outcome_trackers[
            (trade.condition_id, opposite_outcome.lower())
        ][window].snapshot(ts, window)
        row[f"recent_opposite_outcome_count_{window}s"] = opposite_count
        row[f"recent_opposite_outcome_usdc_{window}s"] = round(opposite_usdc, 6)

        same_market_count, same_market_usdc = market_trackers[trade.slug][window].snapshot(ts, window)
        row[f"recent_same_market_count_{window}s"] = same_market_count
        row[f"recent_same_market_usdc_{window}s"] = round(same_market_usdc, 6)

        same_market_outcome_count, same_market_outcome_usdc = market_outcome_trackers[
            (trade.slug, outcome_key)
        ][window].snapshot(ts, window)
        row[f"recent_same_market_outcome_count_{window}s"] = same_market_outcome_count
        row[f"recent_same_market_outcome_usdc_{window}s"] = round(same_market_outcome_usdc, 6)

    return row


def _observe_trade(
    trade: TradeActivity,
    overall_trackers: Dict[int, RollingWindowTracker],
    condition_trackers: Dict[str, Dict[int, RollingWindowTracker]],
    condition_outcome_trackers: Dict[Tuple[str, str], Dict[int, RollingWindowTracker]],
    market_trackers: Dict[str, Dict[int, RollingWindowTracker]],
    market_outcome_trackers: Dict[Tuple[str, str], Dict[int, RollingWindowTracker]],
    prev_trade_ts_by_wallet: Dict[str, int],
    prev_trade_ts_by_market: Dict[str, int],
    prev_trade_ts_by_condition: Dict[str, int],
    prev_trade_ts_by_market_outcome: Dict[Tuple[str, str], int],
) -> None:
    ts = trade.timestamp_seconds
    outcome_key = trade.outcome.lower()
    for window in _WINDOWS_SECONDS:
        overall_trackers[window].observe(ts, trade.usdc_size)
        condition_trackers[trade.condition_id][window].observe(ts, trade.usdc_size)
        condition_outcome_trackers[(trade.condition_id, outcome_key)][window].observe(ts, trade.usdc_size)
        market_trackers[trade.slug][window].observe(ts, trade.usdc_size)
        market_outcome_trackers[(trade.slug, outcome_key)][window].observe(ts, trade.usdc_size)
    prev_trade_ts_by_wallet[trade.proxy_wallet] = ts
    prev_trade_ts_by_market[trade.slug] = ts
    prev_trade_ts_by_condition[trade.condition_id] = ts
    prev_trade_ts_by_market_outcome[(trade.slug, outcome_key)] = ts


def _load_trades(path: str) -> List[TradeActivity]:
    trades: List[TradeActivity] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            payload = json.loads(line)
            trades.append(
                TradeActivity(
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
            )
    return trades


def _build_condition_outcome_assets(trades: Iterable[TradeActivity]) -> Dict[str, Dict[str, str]]:
    result: Dict[str, Dict[str, str]] = defaultdict(dict)
    for trade in trades:
        result[trade.condition_id].setdefault(trade.outcome, trade.asset)
    return result


def _build_condition_outcome_timestamps(
    trades: Iterable[TradeActivity],
) -> Dict[Tuple[str, str], List[int]]:
    result: Dict[Tuple[str, str], List[int]] = defaultdict(list)
    for trade in trades:
        result[(trade.condition_id, trade.outcome)].append(trade.timestamp_seconds)
    return dict(result)


def _has_local_trade(
    timestamps: Sequence[int],
    center_ts: int,
    window_seconds: int,
) -> bool:
    lower = center_ts - window_seconds
    upper = center_ts + window_seconds
    index = bisect_left(timestamps, lower)
    return index < len(timestamps) and timestamps[index] <= upper


def _make_window_tracker_map() -> Dict[int, RollingWindowTracker]:
    return {window: RollingWindowTracker() for window in _WINDOWS_SECONDS}


def _write_csv(path: str, rows: List[Dict[str, object]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_row(writer, handle, row: Dict[str, object]):
    if writer is None:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        writer.writeheader()
    writer.writerow(row)
    return writer


def _opposite_outcome(outcome: str) -> Optional[str]:
    return _OUTCOME_OPPOSITES.get(outcome.strip().lower())


def _elapsed_seconds(previous_ts: Optional[int], now_ts: int) -> Optional[int]:
    if previous_ts is None:
        return None
    return max(0, now_ts - previous_ts)


def _round_optional(value: Optional[float]) -> Optional[float]:
    return round(value, 6) if value is not None else None


def _parse_optional_float(value: Optional[str]) -> Optional[float]:
    if value in (None, ""):
        return None
    return float(value)


def _seconds_to_resolution(trade: TradeActivity) -> Optional[int]:
    resolution_ts = _infer_resolution_timestamp_seconds(trade)
    if resolution_ts is None:
        return None
    return resolution_ts - trade.timestamp_seconds
