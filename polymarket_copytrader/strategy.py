from __future__ import annotations

import time

from .models import FollowDecision, OrderBook, StrategyConfig, TradeActivity


class MirrorTradeStrategy:
    def __init__(self, config: StrategyConfig) -> None:
        self.config = config

    def decide(
        self,
        trade: TradeActivity,
        order_book: OrderBook,
        current_exposure_usdc: float,
        current_position_size: float,
        current_timestamp_seconds: float | None = None,
    ) -> FollowDecision:
        side = trade.side.upper()
        if side != "BUY":
            return FollowDecision(False, "skip_unknown_side", trade)

        stale_reason = self.stale_reason(trade, current_timestamp_seconds)
        if stale_reason is not None:
            return FollowDecision(False, stale_reason, trade)

        if trade.usdc_size < self.config.min_target_usdc_size:
            return FollowDecision(False, "skip_small_target_trade", trade)

        if trade.price > self.config.max_follow_price:
            return FollowDecision(False, "skip_price_too_high", trade)

        if trade.outcome in set(self.config.skip_outcomes):
            return FollowDecision(False, "skip_outcome_blocklist", trade)

        if trade.slug in set(self.config.skip_market_slugs):
            return FollowDecision(False, "skip_market_blocklist", trade)

        target_usdc = (
            self.config.fixed_order_usdc
            if self.config.fixed_order_usdc is not None
            else trade.usdc_size * self.config.follow_fraction
        )
        if self.config.fixed_order_usdc is None:
            target_usdc = min(target_usdc, trade.usdc_size * self.config.follow_fraction)

        return self._decide_buy(trade, order_book, current_exposure_usdc, target_usdc)

    def stale_reason(
        self, trade: TradeActivity, current_timestamp_seconds: float | None = None
    ) -> str | None:
        now_seconds = float(current_timestamp_seconds or time.time())
        trade_age_seconds = max(now_seconds - float(trade.timestamp_seconds), 0.0)
        max_age_seconds = self.config.max_trade_age_seconds
        if trade.is_short_duration_market and self.config.short_market_max_trade_age_seconds is not None:
            max_age_seconds = self.config.short_market_max_trade_age_seconds
        if max_age_seconds is None or max_age_seconds <= 0:
            return None
        if trade_age_seconds > max_age_seconds:
            return "skip_trade_too_stale"
        return None

    def _decide_buy(
        self,
        trade: TradeActivity,
        order_book: OrderBook,
        current_exposure_usdc: float,
        target_usdc: float,
    ) -> FollowDecision:
        best_ask = order_book.best_ask
        if best_ask is None:
            return FollowDecision(False, "skip_no_ask_liquidity", trade)

        cap_price = self._follow_price_cap(trade, order_book)
        slippage_bps = ((best_ask - trade.price) / trade.price) * 10000 if trade.price else 0.0
        if best_ask > cap_price:
            return FollowDecision(
                False,
                "skip_slippage_too_high",
                trade,
                follow_side="BUY",
                slippage_bps=slippage_bps,
            )

        remaining_capacity = self.config.max_position_per_asset_usdc - current_exposure_usdc
        if remaining_capacity <= 0:
            return FollowDecision(False, "skip_position_limit_reached", trade, follow_side="BUY")

        follow_usdc = min(target_usdc, remaining_capacity)
        if follow_usdc < self.config.min_order_usdc:
            return FollowDecision(False, "skip_below_min_order_usdc", trade, follow_side="BUY")

        follow_size = follow_usdc / best_ask
        if follow_size < order_book.min_order_size:
            return FollowDecision(False, "skip_below_min_order_size", trade, follow_side="BUY")

        return FollowDecision(
            should_follow=True,
            reason="follow",
            target_trade=trade,
            follow_side="BUY",
            follow_price=round(max(best_ask, cap_price), 6),
            follow_usdc=round(follow_usdc, 6),
            follow_size=round(follow_size, 6),
            slippage_bps=round(slippage_bps, 4),
        )

    def _follow_price_cap(self, trade: TradeActivity, order_book: OrderBook) -> float:
        static_cap_price = self.config.max_follow_price
        if trade.price > 0:
            static_cap_price = min(
                static_cap_price,
                trade.price * (1.0 + self.config.max_slippage_bps / 10000.0),
            )
        if not self.config.aggressive_price_enabled or self.config.aggressive_price_max_ticks <= 0:
            return static_cap_price

        tick_size = max(float(order_book.tick_size or 0.0), 0.0)
        last_trade_price = max(float(order_book.last_trade_price or 0.0), trade.price)
        if tick_size <= 0 or last_trade_price <= 0:
            return static_cap_price

        dynamic_cap_price = last_trade_price + tick_size * self.config.aggressive_price_max_ticks
        return min(self.config.max_follow_price, max(static_cap_price, dynamic_cap_price))

MirrorBuyStrategy = MirrorTradeStrategy
