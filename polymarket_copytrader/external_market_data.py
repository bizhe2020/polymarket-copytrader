from __future__ import annotations

import csv
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import requests


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
    "sol": "SOL/USDT",
    "xrp": "XRP/USDT",
}

_OKX_SYMBOLS: Dict[str, str] = {
    "btc": "BTC-USDT",
    "eth": "ETH-USDT",
    "sol": "SOL-USDT",
    "xrp": "XRP-USDT",
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

    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timeframe_seconds = _TIMEFRAME_TO_SECONDS[config.timeframe]
    row_counts: Dict[str, int] = {}

    for family in config.families:
        if config.exchange_id == "okx":
            symbol = _OKX_SYMBOLS.get(family)
            if symbol is None:
                raise ValueError(f"unsupported family: {family}")
            rows = _fetch_okx_history_candles(
                symbol=symbol,
                timeframe=config.timeframe,
                timeframe_seconds=timeframe_seconds,
                start_timestamp_seconds=config.start_timestamp_seconds,
                end_timestamp_seconds=config.end_timestamp_seconds,
            )
        else:
            rows = _fetch_via_ccxt(
                exchange_id=config.exchange_id,
                family=family,
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


def _fetch_via_ccxt(
    exchange_id: str,
    family: str,
    timeframe: str,
    timeframe_seconds: int,
    start_timestamp_seconds: Optional[int],
    end_timestamp_seconds: Optional[int],
) -> List[Dict[str, object]]:
    import ccxt

    exchange_cls = getattr(ccxt, exchange_id, None)
    if exchange_cls is None:
        raise ValueError(f"unknown exchange: {exchange_id}")
    exchange = exchange_cls({"enableRateLimit": True})
    symbol = _DEFAULT_SYMBOLS.get(family)
    if symbol is None:
        raise ValueError(f"unsupported family: {family}")
    return _fetch_symbol_ohlcv(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        timeframe_seconds=timeframe_seconds,
        start_timestamp_seconds=start_timestamp_seconds,
        end_timestamp_seconds=end_timestamp_seconds,
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


def _fetch_okx_history_candles(
    symbol: str,
    timeframe: str,
    timeframe_seconds: int,
    start_timestamp_seconds: Optional[int],
    end_timestamp_seconds: Optional[int],
) -> List[Dict[str, object]]:
    bar = _okx_bar(timeframe)
    start_ms = None if start_timestamp_seconds is None else int(start_timestamp_seconds) * 1000
    end_ms = None if end_timestamp_seconds is None else int(end_timestamp_seconds) * 1000
    cursor_ms = None if end_ms is None else end_ms + (timeframe_seconds * 1000)
    session = requests.Session()
    session.headers.update({"User-Agent": "polymarket-copytrader/okx-research"})

    rows_by_ts: Dict[int, Dict[str, object]] = {}
    while True:
        batch = _fetch_okx_batch(
            session=session,
            symbol=symbol,
            bar=bar,
            after_ms=cursor_ms,
            limit=300,
        )
        if not batch:
            break

        earliest_ts_ms: Optional[int] = None
        appended = 0
        for item in batch:
            ts_ms = int(item[0])
            earliest_ts_ms = ts_ms if earliest_ts_ms is None else min(earliest_ts_ms, ts_ms)
            if start_ms is not None and ts_ms < start_ms:
                continue
            if end_ms is not None and ts_ms > end_ms:
                continue
            rows_by_ts[ts_ms] = {
                "timestamp_seconds": ts_ms // 1000,
                "close": float(item[4]),
                # OKX candle payload uses quote-volume in slot 6 for spot pairs.
                "volume": float(item[6]) if len(item) > 6 and item[6] not in ("", None) else float(item[5]),
                "trade_count": "",
            }
            appended += 1

        if earliest_ts_ms is None:
            break
        if start_ms is not None and earliest_ts_ms < start_ms:
            break
        next_cursor_ms = earliest_ts_ms
        if cursor_ms is not None and next_cursor_ms >= cursor_ms:
            break
        cursor_ms = next_cursor_ms
        if len(batch) < 300:
            break
        if appended == 0 and start_ms is None:
            break
        time.sleep(0.12)

    return [rows_by_ts[key] for key in sorted(rows_by_ts.keys())]


def _fetch_okx_batch(
    session: requests.Session,
    symbol: str,
    bar: str,
    after_ms: Optional[int],
    limit: int,
) -> List[List[object]]:
    params = {
        "instId": symbol,
        "bar": bar,
        "limit": str(limit),
    }
    if after_ms is not None:
        params["after"] = str(int(after_ms))
    response = session.get(
        "https://www.okx.com/api/v5/market/history-candles",
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    payload = response.json()
    code = str(payload.get("code", ""))
    if code not in {"", "0"}:
        raise ValueError(f"okx history-candles error code={code} msg={payload.get('msg')}")
    data = payload.get("data") or []
    return [list(item) for item in data]


def _okx_bar(timeframe: str) -> str:
    mapping = {
        "1m": "1m",
        "3m": "3m",
        "5m": "5m",
        "15m": "15m",
        "1h": "1H",
    }
    bar = mapping.get(timeframe)
    if bar is None:
        raise ValueError(f"unsupported okx timeframe: {timeframe}")
    return bar
