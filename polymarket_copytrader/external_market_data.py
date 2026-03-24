from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import ccxt


@dataclass
class ExternalMarketFetchConfig:
    output_dir: str
    exchange_id: str = "binance"
    timeframe: str = "1m"
    start_timestamp_seconds: Optional[int] = None
    end_timestamp_seconds: Optional[int] = None
    families: Tuple[str, ...] = ("btc", "eth")


@dataclass
class ExternalMarketFetchSummary:
    output_dir: str
    exchange_id: str
    timeframe: str
    families: List[str]
    start_timestamp_seconds: Optional[int]
    end_timestamp_seconds: Optional[int]
    row_counts: Dict[str, int]


_DEFAULT_SYMBOLS: Dict[str, str] = {
    "btc": "BTC/USDT",
    "eth": "ETH/USDT",
}

_TIMEFRAME_TO_SECONDS: Dict[str, int] = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
}


def fetch_external_market_data(config: ExternalMarketFetchConfig) -> ExternalMarketFetchSummary:
    if config.timeframe not in _TIMEFRAME_TO_SECONDS:
        raise ValueError(f"unsupported timeframe: {config.timeframe}")

    exchange_cls = getattr(ccxt, config.exchange_id, None)
    if exchange_cls is None:
        raise ValueError(f"unknown exchange: {config.exchange_id}")
    exchange = exchange_cls({"enableRateLimit": True})

    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timeframe_seconds = _TIMEFRAME_TO_SECONDS[config.timeframe]
    row_counts: Dict[str, int] = {}

    for family in config.families:
        symbol = _DEFAULT_SYMBOLS.get(family)
        if symbol is None:
            raise ValueError(f"unsupported family: {family}")
        rows = _fetch_symbol_ohlcv(
            exchange=exchange,
            symbol=symbol,
            timeframe=config.timeframe,
            timeframe_seconds=timeframe_seconds,
            start_timestamp_seconds=config.start_timestamp_seconds,
            end_timestamp_seconds=config.end_timestamp_seconds,
        )
        path = output_dir / f"{family}.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["timestamp_seconds", "close", "volume", "trade_count"],
            )
            writer.writeheader()
            writer.writerows(rows)
        row_counts[family] = len(rows)

    return ExternalMarketFetchSummary(
        output_dir=str(output_dir),
        exchange_id=config.exchange_id,
        timeframe=config.timeframe,
        families=list(config.families),
        start_timestamp_seconds=config.start_timestamp_seconds,
        end_timestamp_seconds=config.end_timestamp_seconds,
        row_counts=row_counts,
    )


def _fetch_symbol_ohlcv(
    exchange,
    symbol: str,
    timeframe: str,
    timeframe_seconds: int,
    start_timestamp_seconds: Optional[int],
    end_timestamp_seconds: Optional[int],
) -> List[Dict[str, object]]:
    start_ms = None if start_timestamp_seconds is None else start_timestamp_seconds * 1000
    end_ms = None if end_timestamp_seconds is None else end_timestamp_seconds * 1000

    cursor_ms = start_ms
    all_rows: List[List[object]] = []
    seen_timestamps = set()

    while True:
        batch = _fetch_batch(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            since_ms=cursor_ms,
            limit=1000,
        )
        if not batch:
            break

        appended = 0
        for row in batch:
            ts_ms = int(row[0])
            if end_ms is not None and ts_ms > end_ms:
                continue
            if ts_ms in seen_timestamps:
                continue
            seen_timestamps.add(ts_ms)
            all_rows.append(row)
            appended += 1

        last_ts_ms = int(batch[-1][0])
        next_cursor_ms = last_ts_ms + (timeframe_seconds * 1000)
        if appended == 0 or next_cursor_ms == cursor_ms:
            break
        if end_ms is not None and next_cursor_ms > end_ms:
            break
        cursor_ms = next_cursor_ms
        if len(batch) < 1000:
            break

    all_rows.sort(key=lambda item: int(item[0]))
    return [
        {
            "timestamp_seconds": int(row[0]) // 1000,
            "close": row[4],
            "volume": row[5],
            "trade_count": row[8] if len(row) > 8 else "",
        }
        for row in all_rows
        if end_ms is None or int(row[0]) <= end_ms
    ]


def _fetch_batch(exchange, symbol: str, timeframe: str, since_ms: Optional[int], limit: int) -> List[List[object]]:
    market_id = symbol.replace("/", "")
    if getattr(exchange, "id", "") == "binance" and hasattr(exchange, "public_get_klines"):
        params = {
            "symbol": market_id,
            "interval": timeframe,
            "limit": limit,
        }
        if since_ms is not None:
            params["startTime"] = since_ms
        return exchange.public_get_klines(params)
    return exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=limit)
