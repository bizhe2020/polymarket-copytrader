from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

from .models import OrderBook, PriceLevel


@dataclass
class MatchResult:
    status: str
    reason: str
    filled_usdc: float
    filled_size: float
    avg_price: float | None
    fill_ratio: float


def build_synthetic_order_book(
    asset_id: str,
    market: str,
    reference_price: float,
    trade_size: float,
    extra_slippage_bps: float,
    levels: int,
    depth_multiplier: float,
) -> OrderBook:
    price = max(min(float(reference_price), 0.9999), 0.0001)
    level_count = max(levels, 1)
    total_size = max(float(trade_size) * max(depth_multiplier, 0.05), 1.0)
    per_level_size = max(total_size / level_count, 1.0)
    base_half_spread_bps = 3.0 + max(extra_slippage_bps / 4.0, 0.0)
    level_step_bps = 2.0 + max(extra_slippage_bps / 6.0, 0.0)

    asks: List[PriceLevel] = []
    bids: List[PriceLevel] = []
    for index in range(level_count):
        ask_bps = base_half_spread_bps + index * level_step_bps
        bid_bps = base_half_spread_bps + index * level_step_bps
        asks.append(
            PriceLevel(
                price=min(price * (1.0 + ask_bps / 10000.0), 0.9999),
                size=per_level_size,
            )
        )
        bids.append(
            PriceLevel(
                price=max(price * (1.0 - bid_bps / 10000.0), 0.0001),
                size=per_level_size,
            )
        )
    bids = sorted(bids, key=lambda item: item.price, reverse=True)
    asks = sorted(asks, key=lambda item: item.price)
    return OrderBook(
        market=market,
        asset_id=asset_id,
        timestamp="synthetic",
        bids=bids,
        asks=asks,
        min_order_size=1.0,
        tick_size=0.01,
        neg_risk=False,
        last_trade_price=price,
    )


def simulate_book_execution(
    order_book: OrderBook,
    side: str,
    requested_usdc: float,
    requested_size: float,
    execution_policy: str,
    limit_price: float | None = None,
) -> MatchResult:
    normalized_side = side.upper()
    normalized_policy = execution_policy.upper()
    if normalized_policy not in {"IOC", "FOK"}:
        raise ValueError(f"unsupported execution policy: {execution_policy}")

    if normalized_side == "BUY":
        return _consume_buy(order_book.asks, requested_usdc, normalized_policy, limit_price)
    if normalized_side == "SELL":
        return _consume_sell(order_book.bids, requested_size, normalized_policy, limit_price)
    raise ValueError(f"unsupported side: {side}")


def _consume_buy(
    asks: Iterable[PriceLevel], requested_usdc: float, policy: str, limit_price: float | None
) -> MatchResult:
    remaining_usdc = max(requested_usdc, 0.0)
    filled_usdc = 0.0
    filled_size = 0.0
    for level in asks:
        if limit_price is not None and level.price > limit_price + 1e-12:
            break
        if remaining_usdc <= 1e-12:
            break
        max_level_usdc = level.price * level.size
        take_usdc = min(remaining_usdc, max_level_usdc)
        take_size = take_usdc / level.price
        filled_usdc += take_usdc
        filled_size += take_size
        remaining_usdc -= take_usdc

    fill_ratio = (filled_usdc / requested_usdc) if requested_usdc > 0 else 0.0
    if policy == "FOK" and fill_ratio + 1e-12 < 1.0:
        return MatchResult("unfilled", "fok_unfilled", 0.0, 0.0, None, 0.0)
    if filled_usdc <= 1e-12:
        return MatchResult("unfilled", "ioc_unfilled", 0.0, 0.0, None, 0.0)
    avg_price = filled_usdc / filled_size if filled_size > 0 else None
    status = "filled" if fill_ratio + 1e-12 >= 1.0 else "partial"
    reason = "filled_buy" if status == "filled" else "partial_buy_ioc"
    return MatchResult(status, reason, filled_usdc, filled_size, avg_price, min(fill_ratio, 1.0))


def _consume_sell(
    bids: Iterable[PriceLevel], requested_size: float, policy: str, limit_price: float | None
) -> MatchResult:
    remaining_size = max(requested_size, 0.0)
    filled_usdc = 0.0
    filled_size = 0.0
    for level in bids:
        if limit_price is not None and level.price + 1e-12 < limit_price:
            break
        if remaining_size <= 1e-12:
            break
        take_size = min(remaining_size, level.size)
        filled_size += take_size
        filled_usdc += take_size * level.price
        remaining_size -= take_size

    fill_ratio = (filled_size / requested_size) if requested_size > 0 else 0.0
    if policy == "FOK" and fill_ratio + 1e-12 < 1.0:
        return MatchResult("unfilled", "fok_unfilled", 0.0, 0.0, None, 0.0)
    if filled_size <= 1e-12:
        return MatchResult("unfilled", "ioc_unfilled", 0.0, 0.0, None, 0.0)
    avg_price = filled_usdc / filled_size if filled_size > 0 else None
    status = "filled" if fill_ratio + 1e-12 >= 1.0 else "partial"
    reason = "filled_sell" if status == "filled" else "partial_sell_ioc"
    return MatchResult(status, reason, filled_usdc, filled_size, avg_price, min(fill_ratio, 1.0))
