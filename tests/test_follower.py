import unittest
from unittest.mock import patch

from polymarket_copytrader.execution import PaperExecutionClient
from polymarket_copytrader.follower import CopyTraderApp
from polymarket_copytrader.models import (
    AppConfig,
    ExecutionConfig,
    OrderBook,
    PriceLevel,
    RuntimeConfig,
    StateSnapshot,
    StrategyConfig,
    TargetConfig,
    TradeActivity,
)
from polymarket_copytrader.strategy import MirrorTradeStrategy


class _RecordingSink:
    def __init__(self) -> None:
        self.records = []

    def write(self, kind: str, payload: dict) -> None:
        self.records.append((kind, payload))


class _RaisingApi:
    def get_order_book(self, asset_id: str):
        raise AssertionError("REST order book should not be used when websocket book is ready")


class _PollingApi(_RaisingApi):
    def __init__(self, trades):
        self.trades = trades
        self.calls = []
        self.events_by_slug = {}
        self.search_events = []

    def get_activity(self, wallet: str, limit: int, start_ms: int, side=None, sort_direction="ASC"):
        self.calls.append(
            {
                "wallet": wallet,
                "limit": limit,
                "start_ms": start_ms,
                "side": side,
                "sort_direction": sort_direction,
            }
        )
        return list(self.trades)

    def get_events(self, slug: str = None, limit: int = None, active: bool = None, closed: bool = None):
        return list(self.events_by_slug.get(slug, []))

    def public_search_events(self, query: str, limit_per_type: int = 10):
        return list(self.search_events)


class _FakeMarketStream:
    available = True

    def __init__(self, book: OrderBook) -> None:
        self.book = book
        self.ensured_assets = []

    def ensure_asset(self, asset_id: str) -> None:
        self.ensured_assets.append(asset_id)

    def wait_for_book(self, asset_id: str, timeout_seconds: float):
        return self.book

    def get_order_book(self, asset_id: str):
        return self.book

    def close(self) -> None:
        return None


class FollowerTests(unittest.TestCase):
    def _build_app(self) -> CopyTraderApp:
        app = CopyTraderApp.__new__(CopyTraderApp)
        app.config = AppConfig(
            target=TargetConfig(profile="@guh123", wallet="0xabc"),
            runtime=RuntimeConfig(
                poll_interval_seconds=1.0,
                request_timeout_seconds=5.0,
                activity_limit=100,
                lookback_seconds_on_start=20,
                requery_overlap_seconds=3,
                state_path="var/test/state.json",
                event_log_path="var/test/events.jsonl",
                market_websocket_enabled=True,
                market_websocket_warmup_ms=350,
            ),
            strategy=StrategyConfig(
                follow_fraction=1.0,
                fixed_order_usdc=5.0,
                min_target_usdc_size=1.0,
                max_follow_price=0.99,
                max_slippage_bps=250.0,
                max_position_per_asset_usdc=50.0,
                min_order_usdc=1.0,
                skip_outcomes=[],
                skip_market_slugs=[],
                execution_policy="IOC",
            ),
            execution=ExecutionConfig(
                mode="paper",
                host="https://clob.polymarket.com",
                chain_id=137,
                signature_type=1,
                private_key_env="POLY_PRIVATE_KEY",
                funder_env="POLY_FUNDER",
            ),
        )
        app.api = _RaisingApi()
        app.store = None
        app.events = _RecordingSink()
        app.state = StateSnapshot()
        app.strategy = MirrorTradeStrategy(app.config.strategy)
        app._execution = None
        app.market_stream = _FakeMarketStream(
            OrderBook(
                market="market",
                asset_id="asset-1",
                timestamp="now",
                bids=[PriceLevel(price=0.74, size=20.0)],
                asks=[PriceLevel(price=0.75, size=20.0)],
                min_order_size=1.0,
                tick_size=0.01,
                neg_risk=False,
                last_trade_price=0.745,
            )
        )
        app._execution = PaperExecutionClient(app.config.execution, app.state)
        app._event_assets_cache = {}
        app._hot_pool_assets_cache = {}
        app._pair_unit_strategy = None
        app._pair_unit_enabled = False
        return app

    def test_handle_trade_prefers_market_ws_book(self) -> None:
        app = self._build_app()
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx",
            price=0.74,
            asset="asset-1",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )

        app._handle_trade(trade)

        self.assertAlmostEqual(app.state.asset_positions_size["asset-1"], 6.666667, places=6)
        order_book_payloads = [payload for kind, payload in app.events.records if kind == "order_book_source"]
        self.assertEqual(order_book_payloads[-1]["source"], "market_ws")

    def test_start_cursor_uses_seconds_not_milliseconds(self) -> None:
        app = self._build_app()
        app.state.last_event_timestamp_ms = 1774198087
        self.assertEqual(app._start_cursor_ms(), 1774198084)

    def test_poll_once_requests_recent_activity_in_desc_order(self) -> None:
        app = self._build_app()
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx-poll",
            price=0.74,
            asset="asset-1",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )
        app.api = _PollingApi([trade])
        app.store = type("_Store", (), {"save": lambda *args, **kwargs: None})()

        with patch("polymarket_copytrader.follower.time.time", return_value=1_774_198_100):
            app._poll_once("0xabc")

        self.assertEqual(app.api.calls[-1]["sort_direction"], "DESC")
        self.assertEqual(app.api.calls[-1]["start_ms"], 1774198080)

    def test_poll_once_adds_late_backfill_window_after_first_seen_trade(self) -> None:
        app = self._build_app()
        app.state.last_event_timestamp_ms = 1774198087
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx-backfill",
            price=0.74,
            asset="asset-1",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )
        app.api = _PollingApi([trade])
        app.store = type("_Store", (), {"save": lambda *args, **kwargs: None})()

        with patch("polymarket_copytrader.follower.time.time", return_value=1_774_198_100):
            app._poll_once("0xabc")

        self.assertEqual(len(app.api.calls), 3)
        self.assertEqual(app.api.calls[0]["sort_direction"], "DESC")
        self.assertEqual(app.api.calls[0]["start_ms"], 1774198094)
        self.assertEqual(app.api.calls[1]["sort_direction"], "DESC")
        self.assertEqual(app.api.calls[1]["start_ms"], 1774198084)
        self.assertEqual(app.api.calls[2]["sort_direction"], "ASC")
        self.assertEqual(app.api.calls[2]["start_ms"], 1774198072)

    def test_poll_once_warms_context_assets_for_batch(self) -> None:
        app = self._build_app()
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx-context",
            price=0.74,
            asset="asset-1",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )
        api = _PollingApi([trade])
        api.events_by_slug = {
            "btc-updown-15m-1774172700": [
                {
                    "slug": "btc-updown-15m-1774172700",
                    "markets": [
                        {
                            "clobTokenIds": "[\"asset-1\",\"asset-helper\"]",
                        }
                    ],
                }
            ]
        }
        app.api = api
        app.store = type("_Store", (), {"save": lambda *args, **kwargs: None})()

        with patch("polymarket_copytrader.follower.time.time", return_value=1_774_198_100):
            app._poll_once("0xabc")

        self.assertIn("asset-helper", app.market_stream.ensured_assets)

    def test_poll_once_processes_newest_trades_first(self) -> None:
        app = self._build_app()
        older = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173200,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.4,
            transaction_hash="0xtx-old",
            price=0.74,
            asset="asset-1",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )
        newer = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173300,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx-new",
            price=0.75,
            asset="asset-1",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )
        app.api = _PollingApi([older, newer])
        app.store = type("_Store", (), {"save": lambda *args, **kwargs: None})()

        with patch("polymarket_copytrader.follower.time.time", return_value=1_774_198_100):
            app._poll_once("0xabc")

        decisions = [payload for kind, payload in app.events.records if kind == "decision"]
        self.assertEqual(decisions[0]["target_trade"]["transaction_hash"], "0xtx-new")

    def test_poll_once_prioritizes_btc_short_market_before_hourly(self) -> None:
        app = self._build_app()
        hourly = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173300,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx-hourly",
            price=0.75,
            asset="asset-1",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down - March 22, 1PM ET",
            slug="bitcoin-up-or-down-march-22-2026-1pm-et",
            event_slug="bitcoin-up-or-down-march-22-2026-1pm-et",
            outcome="Down",
        )
        short = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173200,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.4,
            transaction_hash="0xtx-short",
            price=0.74,
            asset="asset-1",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )
        app.api = _PollingApi([hourly, short])
        app.store = type("_Store", (), {"save": lambda *args, **kwargs: None})()

        with patch("polymarket_copytrader.follower.time.time", return_value=1_774_198_100):
            app._poll_once("0xabc")

        decisions = [payload for kind, payload in app.events.records if kind == "decision"]
        self.assertEqual(decisions[0]["target_trade"]["transaction_hash"], "0xtx-short")


if __name__ == "__main__":
    unittest.main()
