from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .api import PolymarketPublicApi


@dataclass
class SignalPriceCacheConfig:
    input_csv_path: str
    output_dir: str
    output_json_path: str
    asset_column: str = "candidate_asset"
    timestamp_column: str = "timestamp_seconds"
    resolution_column: str = "resolution_timestamp_seconds"
    seconds_to_resolution_column: str = "seconds_to_resolution"
    request_timeout_seconds: float = 20.0
    fidelity_minutes: int = 1
    lookback_padding_seconds: int = 300
    forward_padding_seconds: int = 300


@dataclass
class SignalPriceCacheSummary:
    total_rows: int
    unique_assets: int
    fetched_assets: int
    skipped_assets: int
    total_points_written: int


def fetch_signal_price_cache(config: SignalPriceCacheConfig) -> SignalPriceCacheSummary:
    frame = pd.read_csv(config.input_csv_path)
    if frame.empty:
        raise ValueError("input signal dataset is empty")
    if config.asset_column not in frame.columns:
        raise ValueError(f"missing required column: {config.asset_column}")
    if config.timestamp_column not in frame.columns:
        raise ValueError(f"missing required column: {config.timestamp_column}")

    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    api = PolymarketPublicApi(timeout_seconds=config.request_timeout_seconds)

    requests = _build_asset_requests(
        frame=frame,
        asset_column=config.asset_column,
        timestamp_column=config.timestamp_column,
        resolution_column=config.resolution_column,
        seconds_to_resolution_column=config.seconds_to_resolution_column,
        lookback_padding_seconds=config.lookback_padding_seconds,
        forward_padding_seconds=config.forward_padding_seconds,
    )

    fetched_assets = 0
    skipped_assets = 0
    total_points_written = 0
    for asset_id, request in requests.items():
        cache_path = output_dir / f"{asset_id}.json"
        try:
            points = api.get_price_history(
                asset_id=asset_id,
                start_ts=request["start_ts"],
                end_ts=request["end_ts"],
                fidelity_minutes=config.fidelity_minutes,
            )
        except Exception:
            skipped_assets += 1
            continue
        if not points:
            skipped_assets += 1
            continue
        payload = [
            {"timestamp_seconds": int(point.timestamp_seconds), "price": float(point.price)}
            for point in points
        ]
        cache_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        fetched_assets += 1
        total_points_written += len(payload)

    summary = SignalPriceCacheSummary(
        total_rows=int(len(frame)),
        unique_assets=int(len(requests)),
        fetched_assets=int(fetched_assets),
        skipped_assets=int(skipped_assets),
        total_points_written=int(total_points_written),
    )
    output_path = Path(config.output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _build_asset_requests(
    frame: pd.DataFrame,
    asset_column: str,
    timestamp_column: str,
    resolution_column: str,
    seconds_to_resolution_column: str,
    lookback_padding_seconds: int,
    forward_padding_seconds: int,
) -> Dict[str, Dict[str, int]]:
    requests: Dict[str, Dict[str, int]] = {}
    for row in frame.to_dict(orient="records"):
        asset_id = str(row.get(asset_column) or "").strip()
        if not asset_id:
            continue
        try:
            start_ts = int(float(row[timestamp_column]))
        except (TypeError, ValueError, KeyError):
            continue
        resolution_ts = _row_resolution_timestamp_seconds(
            row=row,
            timestamp_column=timestamp_column,
            resolution_column=resolution_column,
            seconds_to_resolution_column=seconds_to_resolution_column,
        )
        end_ts = resolution_ts if resolution_ts is not None else start_ts
        request = requests.setdefault(
            asset_id,
            {
                "start_ts": start_ts,
                "end_ts": end_ts,
            },
        )
        request["start_ts"] = min(int(request["start_ts"]), start_ts)
        request["end_ts"] = max(int(request["end_ts"]), end_ts)
    for asset_id, request in requests.items():
        request["start_ts"] = int(request["start_ts"]) - int(lookback_padding_seconds)
        request["end_ts"] = int(request["end_ts"]) + int(forward_padding_seconds)
    return requests


def _row_resolution_timestamp_seconds(
    row: Dict[str, object],
    timestamp_column: str,
    resolution_column: str,
    seconds_to_resolution_column: str,
) -> Optional[int]:
    raw_resolution = row.get(resolution_column)
    if raw_resolution is not None and pd.notna(raw_resolution):
        try:
            return int(float(raw_resolution))
        except (TypeError, ValueError):
            pass
    raw_timestamp = row.get(timestamp_column)
    raw_delta = row.get(seconds_to_resolution_column)
    if raw_timestamp is None or raw_delta is None or not pd.notna(raw_delta):
        return None
    try:
        return int(float(raw_timestamp)) + int(float(raw_delta))
    except (TypeError, ValueError):
        return None
