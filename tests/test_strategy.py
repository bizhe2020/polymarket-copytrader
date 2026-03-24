import unittest

from polymarket_copytrader.models import OrderBook, PriceLevel, StrategyConfig, TradeActivity
from polymarket_copytrader.strategy import MirrorTradeStrategy


class StrategyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.strategy = MirrorTradeStrategy(
            StrategyConfig(
                follow_fraction=0.2,
                fixed_order_usdc=25.0,
                min_target_usdc_size=100.0,
                max_follow_price=0.92,
                max_slippage_bps=150.0,
                max_position_per_asset_usdc=100.0,
                min_order_usdc=5.0,
                skip_outcomes=[],
                skip_market_slugs=[],
                execution_policy="IOC",
            )
        )
        self.trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1,
            condition_id="condition",
            activity_type="TRADE",
            size=200.0,
            usdc_size=110.0,
            transaction_hash="0xtx",
            price=0.55,
            asset="asset",
            side="BUY",
            outcome_index=0,
            title="title",
            slug="slug",
            event_slug="event-slug",
            outcome="YES",
        )
        self.book = OrderBook(
            market="market",
            asset_id="asset",
            timestamp="now",
            bids=[PriceLevel(price=0.54, size=100.0)],
            asks=[PriceLevel(price=0.555, size=100.0)],
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=0.55,
        )

    def test_follow_when_trade_is_large_and_liquid(self) -> None:
        decision = self.strategy.decide(
            self.trade,
            self.book,
            current_exposure_usdc=0.0,
            current_position_size=0.0,
        )
        self.assertTrue(decision.should_follow)
        self.assertEqual(decision.reason, "follow")
        self.assertEqual(decision.follow_usdc, 25.0)
        self.assertEqual(decision.follow_side, "BUY")

    def test_skip_when_position_limit_reached(self) -> None:
        decision = self.strategy.decide(
            self.trade,
            self.book,
            current_exposure_usdc=100.0,
            current_position_size=0.0,
        )
        self.assertFalse(decision.should_follow)
        self.assertEqual(decision.reason, "skip_position_limit_reached")

    def test_skip_sell_trade(self) -> None:
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1,
            condition_id="condition",
            activity_type="TRADE",
            size=100.0,
            usdc_size=120.0,
            transaction_hash="0xtx-sell",
            price=0.6,
            asset="asset",
            side="SELL",
            outcome_index=0,
            title="title",
            slug="slug",
            event_slug="event-slug",
            outcome="YES",
        )
        book = OrderBook(
            market="market",
            asset_id="asset",
            timestamp="now",
            bids=[PriceLevel(price=0.6, size=100.0)],
            asks=[PriceLevel(price=0.61, size=100.0)],
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=0.6,
        )
        decision = self.strategy.decide(
            trade,
            book,
            current_exposure_usdc=25.0,
            current_position_size=50.0,
        )
        self.assertFalse(decision.should_follow)
        self.assertEqual(decision.reason, "skip_unknown_side")

    def test_dynamic_cap_can_follow_one_tick_above_static_slippage_cap(self) -> None:
        strategy = MirrorTradeStrategy(
            StrategyConfig(
                follow_fraction=0.2,
                fixed_order_usdc=25.0,
                min_target_usdc_size=100.0,
                max_follow_price=0.92,
                max_slippage_bps=150.0,
                max_position_per_asset_usdc=100.0,
                min_order_usdc=5.0,
                skip_outcomes=[],
                skip_market_slugs=[],
                execution_policy="IOC",
                aggressive_price_enabled=True,
                aggressive_price_max_ticks=1,
            )
        )
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1,
            condition_id="condition",
            activity_type="TRADE",
            size=200.0,
            usdc_size=110.0,
            transaction_hash="0xtx-aggressive",
            price=0.74,
            asset="asset",
            side="BUY",
            outcome_index=0,
            title="title",
            slug="slug",
            event_slug="event-slug",
            outcome="YES",
        )
        book = OrderBook(
            market="market",
            asset_id="asset",
            timestamp="now",
            bids=[PriceLevel(price=0.75, size=100.0)],
            asks=[PriceLevel(price=0.76, size=100.0)],
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=0.75,
        )

        decision = strategy.decide(
            trade,
            book,
            current_exposure_usdc=0.0,
            current_position_size=0.0,
        )

        self.assertTrue(decision.should_follow)
        self.assertEqual(decision.reason, "follow")
        self.assertEqual(decision.follow_price, 0.76)

    def test_skip_short_market_when_trade_is_too_stale(self) -> None:
        strategy = MirrorTradeStrategy(
            StrategyConfig(
                follow_fraction=0.2,
                fixed_order_usdc=25.0,
                min_target_usdc_size=100.0,
                max_follow_price=0.92,
                max_slippage_bps=150.0,
                max_position_per_asset_usdc=100.0,
                min_order_usdc=5.0,
                skip_outcomes=[],
                skip_market_slugs=[],
                execution_policy="IOC",
                short_market_max_trade_age_seconds=10.0,
            )
        )
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1_700_000_000,
            condition_id="condition",
            activity_type="TRADE",
            size=200.0,
            usdc_size=110.0,
            transaction_hash="0xtx-stale",
            price=0.55,
            asset="asset",
            side="BUY",
            outcome_index=0,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="YES",
        )

        decision = strategy.decide(
            trade,
            self.book,
            current_exposure_usdc=0.0,
            current_position_size=0.0,
            current_timestamp_seconds=1_700_000_020,
        )

        self.assertFalse(decision.should_follow)
        self.assertEqual(decision.reason, "skip_trade_too_stale")


if __name__ == "__main__":
    unittest.main()
