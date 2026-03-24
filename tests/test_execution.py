import unittest

from polymarket_copytrader.execution import ExecutionClient, LiveExecutionClient, PaperExecutionClient
from polymarket_copytrader.models import (
    ExecutionConfig,
    FollowDecision,
    OrderBook,
    PriceLevel,
    StateSnapshot,
    TradeActivity,
)


class ExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ExecutionConfig(
            mode="paper",
            host="https://clob.polymarket.com",
            chain_id=137,
            signature_type=1,
            private_key_env="POLY_PRIVATE_KEY",
            funder_env="POLY_FUNDER",
        )

    def test_paper_execution_updates_buy_and_sell_positions(self) -> None:
        state = StateSnapshot()
        client = PaperExecutionClient(self.config, state)
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1,
            condition_id="condition",
            activity_type="TRADE",
            size=100.0,
            usdc_size=50.0,
            transaction_hash="0xtx",
            price=0.5,
            asset="asset",
            side="BUY",
            outcome_index=0,
            title="title",
            slug="slug",
            event_slug="slug",
            outcome="YES",
        )
        buy = FollowDecision(
            should_follow=True,
            reason="follow",
            target_trade=trade,
            follow_side="BUY",
            follow_price=0.5,
            follow_usdc=25.0,
            follow_size=50.0,
        )
        sell_trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=2,
            condition_id="condition",
            activity_type="TRADE",
            size=100.0,
            usdc_size=20.0,
            transaction_hash="0xtx2",
            price=0.5,
            asset="asset",
            side="SELL",
            outcome_index=0,
            title="title",
            slug="slug",
            event_slug="slug",
            outcome="YES",
        )
        sell = FollowDecision(
            should_follow=True,
            reason="follow",
            target_trade=sell_trade,
            follow_side="SELL",
            follow_price=0.5,
            follow_usdc=10.0,
            follow_size=20.0,
        )
        buy_book = OrderBook(
            market="slug",
            asset_id="asset",
            timestamp="now",
            bids=[PriceLevel(price=0.49, size=100.0)],
            asks=[PriceLevel(price=0.5, size=100.0)],
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=0.5,
        )
        sell_book = OrderBook(
            market="slug",
            asset_id="asset",
            timestamp="now",
            bids=[PriceLevel(price=0.5, size=100.0)],
            asks=[PriceLevel(price=0.51, size=100.0)],
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=0.5,
        )

        client.place_follow_trade(buy, order_book=buy_book, execution_policy="FOK")
        self.assertEqual(client.current_exposure("asset"), 25.0)
        self.assertEqual(client.current_position_size("asset"), 50.0)

        client.place_follow_trade(sell, order_book=sell_book, execution_policy="FOK")
        self.assertEqual(client.current_exposure("asset"), 15.0)
        self.assertEqual(client.current_position_size("asset"), 30.0)

    def test_paper_execution_can_ioc_partial_fill(self) -> None:
        state = StateSnapshot()
        client = PaperExecutionClient(self.config, state)
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1,
            condition_id="condition",
            activity_type="TRADE",
            size=100.0,
            usdc_size=50.0,
            transaction_hash="0xtx",
            price=0.5,
            asset="asset",
            side="BUY",
            outcome_index=0,
            title="title",
            slug="slug",
            event_slug="slug",
            outcome="YES",
        )
        decision = FollowDecision(
            should_follow=True,
            reason="follow",
            target_trade=trade,
            follow_side="BUY",
            follow_price=0.5,
            follow_usdc=25.0,
            follow_size=50.0,
        )
        shallow_book = OrderBook(
            market="slug",
            asset_id="asset",
            timestamp="now",
            bids=[PriceLevel(price=0.49, size=100.0)],
            asks=[PriceLevel(price=0.5, size=10.0)],
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=0.5,
        )
        report = client.place_follow_trade(decision, order_book=shallow_book, execution_policy="IOC")
        self.assertTrue(report.ok)
        self.assertEqual(report.status, "paper_partial")
        self.assertEqual(client.current_position_size("asset"), 10.0)

    def test_live_execution_respects_requested_policy(self) -> None:
        class _FakeOrderType:
            FOK = "FOK"
            IOC = "IOC"

        class _FakeMarketOrderArgs:
            def __init__(self, **kwargs) -> None:
                self.kwargs = kwargs

        class _FakeClobClient:
            def __init__(self) -> None:
                self.created_order = None
                self.posted_order = None
                self.posted_policy = None

            def create_market_order(self, market_order):
                self.created_order = market_order
                return {"signed": market_order.kwargs}

            def post_order(self, signed_order, order_type):
                self.posted_order = signed_order
                self.posted_policy = order_type
                return {"status": "submitted"}

        client = LiveExecutionClient.__new__(LiveExecutionClient)
        ExecutionClient.__init__(client, self.config, StateSnapshot())
        client._market_order_args_cls = _FakeMarketOrderArgs
        client._order_type = _FakeOrderType
        client._buy = "BUY"
        client._sell = "SELL"
        client._client = _FakeClobClient()

        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1,
            condition_id="condition",
            activity_type="TRADE",
            size=100.0,
            usdc_size=50.0,
            transaction_hash="0xtx-live",
            price=0.5,
            asset="asset",
            side="BUY",
            outcome_index=0,
            title="title",
            slug="slug",
            event_slug="slug",
            outcome="YES",
        )
        decision = FollowDecision(
            should_follow=True,
            reason="follow",
            target_trade=trade,
            follow_side="BUY",
            follow_price=0.5,
            follow_usdc=25.0,
            follow_size=50.0,
        )

        report = client.place_follow_trade(decision, execution_policy="IOC")

        self.assertTrue(report.ok)
        self.assertEqual(client._client.created_order.kwargs["order_type"], "IOC")
        self.assertEqual(client._client.posted_policy, "IOC")
        self.assertEqual(report.details["execution_policy"], "IOC")

    def test_paper_execution_respects_limit_price_cap(self) -> None:
        state = StateSnapshot()
        client = PaperExecutionClient(self.config, state)
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1,
            condition_id="condition",
            activity_type="TRADE",
            size=100.0,
            usdc_size=10.0,
            transaction_hash="0xtx-cap",
            price=0.74,
            asset="asset",
            side="BUY",
            outcome_index=0,
            title="title",
            slug="slug",
            event_slug="slug",
            outcome="YES",
        )
        decision = FollowDecision(
            should_follow=True,
            reason="follow",
            target_trade=trade,
            follow_side="BUY",
            follow_price=0.75,
            follow_usdc=10.0,
            follow_size=13.333333,
        )
        layered_book = OrderBook(
            market="slug",
            asset_id="asset",
            timestamp="now",
            bids=[PriceLevel(price=0.74, size=100.0)],
            asks=[PriceLevel(price=0.75, size=10.0), PriceLevel(price=0.76, size=100.0)],
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=0.75,
        )

        report = client.place_follow_trade(decision, order_book=layered_book, execution_policy="IOC")

        self.assertTrue(report.ok)
        self.assertEqual(report.status, "paper_partial")
        self.assertAlmostEqual(report.requested_usdc, 7.5, places=6)
        self.assertAlmostEqual(client.current_position_size("asset"), 10.0, places=6)


if __name__ == "__main__":
    unittest.main()
