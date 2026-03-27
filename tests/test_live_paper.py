import unittest
import tempfile
from unittest.mock import patch
from pathlib import Path

import pandas as pd

from polymarket_copytrader.execution import PaperExecutionClient
from polymarket_copytrader.live_paper import (
    LivePaperAlphaBucket,
    LivePaperAlphaFilter,
    LivePaperConfig,
    LivePaperPortfolio,
    LivePaperPosition,
    LivePaperRuntime,
    LivePaperState,
    LivePaperTarget,
    MultiTargetLivePaperApp,
    _infer_resolution_timestamp_seconds,
    load_live_paper_config,
)
from polymarket_copytrader.models import (
    ExecutionConfig,
    FollowDecision,
    OrderBook,
    PriceHistoryPoint,
    PriceLevel,
    StateSnapshot,
    StrategyConfig,
    TradeActivity,
)
from polymarket_copytrader.strategy import MirrorTradeStrategy


class _RecordingSink:
    def __init__(self) -> None:
        self.records = []

    def write(self, kind: str, payload: dict) -> None:
        self.records.append((kind, payload))


class _MissingBookApi:
    def get_order_book(self, asset_id: str):
        raise RuntimeError("HTTP 404 when requesting book")

    def get_price_history(self, asset_id: str, start_ts: int, end_ts: int, fidelity_minutes: int):
        return []

    def get_activity(self, wallet: str, limit: int, start_ms: int, side=None, sort_direction="ASC"):
        return []


class _RecentActivityApi(_MissingBookApi):
    def __init__(self, trades, markets=None, search_events=None, events_by_slug=None):
        self.trades = trades
        self.markets = markets or []
        self.search_events = search_events or []
        self.events_by_slug = events_by_slug or {}

    def get_activity(self, wallet: str, limit: int, start_ms: int, side=None, sort_direction="ASC"):
        return list(self.trades)

    def get_markets(self, limit: int, active: bool = True, closed: bool = False, offset=None):
        return list(self.markets)

    def public_search_events(self, query: str, limit_per_type: int = 10):
        return list(self.search_events)

    def get_events(self, slug: str = None, limit: int = None, active: bool = None, closed: bool = None):
        return list(self.events_by_slug.get(slug, []))


class _RedeemPriceApi(_MissingBookApi):
    def __init__(self, history_points):
        self.history_points = history_points

    def get_price_history(self, asset_id: str, start_ts: int, end_ts: int, fidelity_minutes: int):
        return list(self.history_points)


class _FakeMarketStream:
    available = True

    def __init__(self, book=None) -> None:
        self.book = book
        self.ensured_assets = []

    def ensure_asset(self, asset_id: str) -> None:
        self.ensured_assets.append(asset_id)

    def wait_for_book(self, asset_id: str, timeout_seconds: float):
        return self.book

    def get_order_book(self, asset_id: str):
        return self.book

    def get_last_price(self, asset_id: str):
        return self.book.last_trade_price if self.book is not None else None

    def close(self) -> None:
        return None


class _ContextBookMarketStream(_FakeMarketStream):
    def __init__(self, target_asset: str, helper_asset: str, book: OrderBook) -> None:
        super().__init__(book=None)
        self.target_asset = target_asset
        self.helper_asset = helper_asset
        self._context_book = book

    def wait_for_book(self, asset_id: str, timeout_seconds: float):
        if asset_id == self.target_asset and self.helper_asset in self.ensured_assets:
            return self._context_book
        return None

    def get_order_book(self, asset_id: str):
        if asset_id == self.target_asset and self.helper_asset in self.ensured_assets:
            return self._context_book
        return None


class LivePaperTests(unittest.TestCase):
    def _build_app(self) -> MultiTargetLivePaperApp:
        app = MultiTargetLivePaperApp.__new__(MultiTargetLivePaperApp)
        app.config = LivePaperConfig(
            targets=[],
            runtime=LivePaperRuntime(
                poll_interval_seconds=2.0,
                request_timeout_seconds=5.0,
                activity_limit=100,
                lookback_seconds_on_start=900,
                requery_overlap_seconds=5,
                market_websocket_enabled=True,
                market_websocket_warmup_ms=350,
                require_real_order_book=False,
                duration_hours=24.0,
                heartbeat_interval_seconds=30.0,
                state_path="var/test/state.json",
                event_log_path="var/test/events.jsonl",
                hourly_stats_path="var/test/hourly_stats.csv",
            ),
            portfolio=LivePaperPortfolio(initial_capital_usdc=100.0),
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
                signature_type=0,
                private_key_env="POLY_PRIVATE_KEY",
                funder_env="POLY_FUNDER",
            ),
        )
        app.api = _MissingBookApi()
        app.events = _RecordingSink()
        app.state = LivePaperState(cash_usdc=100.0, target_last_event_timestamp_ms={})
        app.strategy = MirrorTradeStrategy(app.config.strategy)
        app.market_stream = _FakeMarketStream()
        app.execution_state = StateSnapshot()
        app.execution = PaperExecutionClient(app.config.execution, app.execution_state)
        app.targets = []
        app.target_weight_fractions = {}
        app._event_assets_cache = {}
        app._hot_pool_assets_cache = {}
        app.alpha_filter = None
        return app

    def test_handle_trade_respects_alpha_filter(self) -> None:
        app = self._build_app()
        rows = [
            {
                "sample_id": "s1",
                "timestamp_seconds": 100,
                "market_slug": "btc-updown-5m-100",
                "condition_id": "c1",
                "candidate_outcome": "Down",
                "label_resolved": 1,
                "pnl_per_stake_usdc": 20.0,
                "market_family": "btc",
                "market_duration_bucket": "5m",
                "candidate_abs_price_distance_from_mid": 0.1,
                "candidate_price_distance_from_mid": -0.1,
                "time_since_prev_same_market_trade_seconds": 1.0,
                "time_since_prev_same_condition_trade_seconds": 1.0,
                "time_since_prev_same_market_outcome_trade_seconds": 1.0,
                "seconds_to_resolution": 120.0,
                "recent_condition_count_60s": 5,
                "recent_same_outcome_count_60s": 4,
                "recent_opposite_outcome_count_60s": 1,
                "recent_same_market_count_60s": 5,
                "recent_same_market_outcome_count_60s": 4,
                "recent_condition_usdc_60s": 100.0,
                "recent_same_outcome_usdc_60s": 80.0,
                "recent_opposite_outcome_usdc_60s": 20.0,
            },
            {
                "sample_id": "s2",
                "timestamp_seconds": 200,
                "market_slug": "btc-updown-5m-200",
                "condition_id": "c2",
                "candidate_outcome": "Down",
                "label_resolved": 1,
                "pnl_per_stake_usdc": -20.0,
                "market_family": "btc",
                "market_duration_bucket": "5m",
                "candidate_abs_price_distance_from_mid": 0.3,
                "candidate_price_distance_from_mid": 0.3,
                "time_since_prev_same_market_trade_seconds": 100.0,
                "time_since_prev_same_condition_trade_seconds": 100.0,
                "time_since_prev_same_market_outcome_trade_seconds": 100.0,
                "seconds_to_resolution": 120.0,
                "recent_condition_count_60s": 1,
                "recent_same_outcome_count_60s": 1,
                "recent_opposite_outcome_count_60s": 5,
                "recent_same_market_count_60s": 1,
                "recent_same_market_outcome_count_60s": 1,
                "recent_condition_usdc_60s": 10.0,
                "recent_same_outcome_usdc_60s": 5.0,
                "recent_opposite_outcome_usdc_60s": 50.0,
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            input_csv = Path(tmpdir) / "alpha.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)
            app.config.alpha_filter = LivePaperAlphaFilter(
                enabled=True,
                labeled_outcome_csv_path=str(input_csv),
                top_fraction=0.5,
                market_family="btc",
                market_duration_bucket="5m",
                min_seconds_to_resolution=30.0,
            )
            app.alpha_filter = app._build_alpha_filter()
            trade = TradeActivity(
                proxy_wallet="0xabc",
                timestamp_ms=1774173283,
                condition_id="condition",
                activity_type="TRADE",
                size=10.0,
                usdc_size=7.5,
                transaction_hash="0xtx-alpha",
                price=0.75,
                asset="asset-alpha",
                side="BUY",
                outcome_index=1,
                title="Bitcoin Up or Down - March 24, 3PM ET",
                slug="btc-updown-5m-1774172700",
                event_slug="btc-updown-5m-1774172700",
                outcome="Down",
            )

            app._handle_trade("guh123", trade)

            event_kinds = [kind for kind, _ in app.events.records]
            self.assertIn("alpha_filter_decision", event_kinds)
            alpha_payload = [payload for kind, payload in app.events.records if kind == "alpha_filter_decision"][0]
            self.assertEqual(alpha_payload["market_family"], "btc")
            self.assertEqual(alpha_payload["market_duration_bucket"], "5m")
            self.assertEqual(alpha_payload["market_slug"], trade.slug)

    def test_handle_trade_respects_multi_bucket_alpha_filter(self) -> None:
        app = self._build_app()
        rows = [
            {
                "sample_id": "btc-1",
                "timestamp_seconds": 100,
                "market_slug": "btc-updown-5m-100",
                "condition_id": "c1",
                "candidate_outcome": "Down",
                "label_resolved": 1,
                "pnl_per_stake_usdc": 20.0,
                "market_family": "btc",
                "market_duration_bucket": "5m",
                "candidate_abs_price_distance_from_mid": 0.1,
                "candidate_price_distance_from_mid": -0.1,
                "time_since_prev_same_market_trade_seconds": 1.0,
                "time_since_prev_same_condition_trade_seconds": 1.0,
                "time_since_prev_same_market_outcome_trade_seconds": 1.0,
                "seconds_to_resolution": 120.0,
                "recent_condition_count_60s": 5,
                "recent_same_outcome_count_60s": 4,
                "recent_opposite_outcome_count_60s": 1,
                "recent_same_market_count_60s": 5,
                "recent_same_market_outcome_count_60s": 4,
                "recent_condition_usdc_60s": 100.0,
                "recent_same_outcome_usdc_60s": 80.0,
                "recent_opposite_outcome_usdc_60s": 20.0,
            },
            {
                "sample_id": "eth-1",
                "timestamp_seconds": 100,
                "market_slug": "eth-updown-15m-100",
                "condition_id": "c2",
                "candidate_outcome": "Up",
                "label_resolved": 1,
                "pnl_per_stake_usdc": 15.0,
                "market_family": "eth",
                "market_duration_bucket": "15m",
                "candidate_abs_price_distance_from_mid": 0.12,
                "candidate_price_distance_from_mid": 0.12,
                "time_since_prev_same_market_trade_seconds": 1.0,
                "time_since_prev_same_condition_trade_seconds": 1.0,
                "time_since_prev_same_market_outcome_trade_seconds": 1.0,
                "seconds_to_resolution": 300.0,
                "recent_condition_count_60s": 4,
                "recent_same_outcome_count_60s": 3,
                "recent_opposite_outcome_count_60s": 1,
                "recent_same_market_count_60s": 4,
                "recent_same_market_outcome_count_60s": 3,
                "recent_condition_usdc_60s": 90.0,
                "recent_same_outcome_usdc_60s": 60.0,
                "recent_opposite_outcome_usdc_60s": 30.0,
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            input_csv = Path(tmpdir) / "alpha.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)
            app.config.alpha_filter = LivePaperAlphaFilter(
                enabled=True,
                buckets=[
                    LivePaperAlphaBucket(
                        labeled_outcome_csv_path=str(input_csv),
                        market_family="btc",
                        market_duration_bucket="5m",
                        top_fraction=1.0,
                        min_seconds_to_resolution=30.0,
                    ),
                    LivePaperAlphaBucket(
                        labeled_outcome_csv_path=str(input_csv),
                        market_family="eth",
                        market_duration_bucket="15m",
                        top_fraction=1.0,
                        min_seconds_to_resolution=30.0,
                    ),
                ],
            )
            app.alpha_filter = app._build_alpha_filter()
            trade = TradeActivity(
                proxy_wallet="0xabc",
                timestamp_ms=1774173283,
                condition_id="condition-eth",
                activity_type="TRADE",
                size=10.0,
                usdc_size=7.5,
                transaction_hash="0xtx-alpha-eth",
                price=0.75,
                asset="asset-alpha-eth",
                side="BUY",
                outcome_index=1,
                title="Ethereum Up or Down",
                slug="eth-updown-15m-1774172700",
                event_slug="eth-updown-15m-1774172700",
                outcome="Up",
            )
            app._handle_trade("guh123", trade)
            event_payloads = [payload for kind, payload in app.events.records if kind == "alpha_filter_decision"]
            self.assertEqual(len(event_payloads), 1)
            self.assertEqual(event_payloads[0]["alpha_bucket"], "eth_15m")

    def test_handle_trade_respects_target_specific_alpha_bucket(self) -> None:
        app = self._build_app()
        rows = [
            {
                "sample_id": "guh-btc-hourly",
                "timestamp_seconds": 100,
                "market_slug": "bitcoin-up-or-down-march-24-2026-3pm-et",
                "condition_id": "cg",
                "candidate_outcome": "Down",
                "label_resolved": 1,
                "pnl_per_stake_usdc": -10.0,
                "market_family": "btc",
                "market_duration_bucket": "hourly",
                "candidate_abs_price_distance_from_mid": 0.45,
                "candidate_price_distance_from_mid": 0.45,
                "time_since_prev_same_market_trade_seconds": 100.0,
                "time_since_prev_same_condition_trade_seconds": 100.0,
                "time_since_prev_same_market_outcome_trade_seconds": 100.0,
                "seconds_to_resolution": 600.0,
                "recent_condition_count_60s": 1,
                "recent_same_outcome_count_60s": 1,
                "recent_opposite_outcome_count_60s": 5,
                "recent_same_market_count_60s": 1,
                "recent_same_market_outcome_count_60s": 1,
                "recent_condition_usdc_60s": 10.0,
                "recent_same_outcome_usdc_60s": 5.0,
                "recent_opposite_outcome_usdc_60s": 50.0,
            },
            {
                "sample_id": "blue-btc-hourly",
                "timestamp_seconds": 100,
                "market_slug": "bitcoin-up-or-down-march-24-2026-3pm-et",
                "condition_id": "cb",
                "candidate_outcome": "Down",
                "label_resolved": 1,
                "pnl_per_stake_usdc": 50.0,
                "market_family": "btc",
                "market_duration_bucket": "hourly",
                "candidate_abs_price_distance_from_mid": 0.1,
                "candidate_price_distance_from_mid": -0.1,
                "time_since_prev_same_market_trade_seconds": 1.0,
                "time_since_prev_same_condition_trade_seconds": 1.0,
                "time_since_prev_same_market_outcome_trade_seconds": 1.0,
                "seconds_to_resolution": 600.0,
                "recent_condition_count_60s": 5,
                "recent_same_outcome_count_60s": 4,
                "recent_opposite_outcome_count_60s": 1,
                "recent_same_market_count_60s": 5,
                "recent_same_market_outcome_count_60s": 4,
                "recent_condition_usdc_60s": 100.0,
                "recent_same_outcome_usdc_60s": 80.0,
                "recent_opposite_outcome_usdc_60s": 20.0,
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            input_csv = Path(tmpdir) / "alpha.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)
            app.config.alpha_filter = LivePaperAlphaFilter(
                enabled=True,
                buckets=[
                    LivePaperAlphaBucket(
                        labeled_outcome_csv_path=str(input_csv),
                        market_family="btc",
                        market_duration_bucket="hourly",
                        top_fraction=1.0,
                        min_seconds_to_resolution=30.0,
                        name="guh_btc_hourly",
                        target_names=["guh123"],
                    ),
                    LivePaperAlphaBucket(
                        labeled_outcome_csv_path=str(input_csv),
                        market_family="btc",
                        market_duration_bucket="hourly",
                        top_fraction=0.5,
                        min_seconds_to_resolution=30.0,
                        name="blue_btc_hourly",
                        target_names=["blue-walnut"],
                    ),
                ],
            )
            app.alpha_filter = app._build_alpha_filter()
            trade = TradeActivity(
                proxy_wallet="0xabc",
                timestamp_ms=1774173283,
                condition_id="condition-btc-hourly",
                activity_type="TRADE",
                size=10.0,
                usdc_size=7.5,
                transaction_hash="0xtx-alpha-btc-hourly",
                price=0.75,
                asset="asset-alpha-btc-hourly",
                side="BUY",
                outcome_index=1,
                title="Bitcoin Up or Down - March 24, 3PM ET",
                slug="bitcoin-up-or-down-march-24-2026-3pm-et",
                event_slug="bitcoin-up-or-down-march-24-2026-3pm-et",
                outcome="Down",
            )

            app._handle_trade("blue-walnut", trade)

            event_payloads = [payload for kind, payload in app.events.records if kind == "alpha_filter_decision"]
            self.assertEqual(len(event_payloads), 1)
            self.assertEqual(event_payloads[0]["alpha_bucket"], "blue_btc_hourly")

    def test_handle_trade_limits_alpha_entries_per_market(self) -> None:
        app = self._build_app()
        rows = [
            {
                "sample_id": "xrp-1",
                "timestamp_seconds": 100,
                "market_slug": "xrp-updown-15m-100",
                "condition_id": "cx1",
                "candidate_outcome": "Down",
                "label_resolved": 1,
                "pnl_per_stake_usdc": 30.0,
                "market_family": "xrp",
                "market_duration_bucket": "15m",
                "candidate_abs_price_distance_from_mid": 0.1,
                "candidate_price_distance_from_mid": -0.1,
                "time_since_prev_same_market_trade_seconds": 1.0,
                "time_since_prev_same_condition_trade_seconds": 1.0,
                "time_since_prev_same_market_outcome_trade_seconds": 1.0,
                "seconds_to_resolution": 300.0,
                "recent_condition_count_60s": 5,
                "recent_same_outcome_count_60s": 4,
                "recent_opposite_outcome_count_60s": 1,
                "recent_same_market_count_60s": 5,
                "recent_same_market_outcome_count_60s": 4,
                "recent_condition_usdc_60s": 100.0,
                "recent_same_outcome_usdc_60s": 80.0,
                "recent_opposite_outcome_usdc_60s": 20.0,
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            input_csv = Path(tmpdir) / "alpha.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)
            app.config.alpha_filter = LivePaperAlphaFilter(
                enabled=True,
                buckets=[
                    LivePaperAlphaBucket(
                        labeled_outcome_csv_path=str(input_csv),
                        market_family="xrp",
                        market_duration_bucket="15m",
                        top_fraction=1.0,
                        min_seconds_to_resolution=30.0,
                        name="blue_walnut_xrp_15m_observe",
                        target_names=["blue-walnut"],
                        max_entries_per_market=1,
                    )
                ],
            )
            app.alpha_filter = app._build_alpha_filter()
            first_trade = TradeActivity(
                proxy_wallet="0xabc",
                timestamp_ms=1774173283,
                condition_id="condition-xrp-1",
                activity_type="TRADE",
                size=10.0,
                usdc_size=7.5,
                transaction_hash="0xtx-xrp-1",
                price=0.75,
                asset="asset-xrp-1",
                side="BUY",
                outcome_index=1,
                title="XRP Up or Down",
                slug="xrp-updown-15m-1774172700",
                event_slug="xrp-updown-15m-1774172700",
                outcome="Down",
            )
            second_trade = TradeActivity(
                proxy_wallet="0xabc",
                timestamp_ms=1774173290,
                condition_id="condition-xrp-1",
                activity_type="TRADE",
                size=9.0,
                usdc_size=6.5,
                transaction_hash="0xtx-xrp-2",
                price=0.72,
                asset="asset-xrp-2",
                side="BUY",
                outcome_index=1,
                title="XRP Up or Down",
                slug="xrp-updown-15m-1774172700",
                event_slug="xrp-updown-15m-1774172700",
                outcome="Down",
            )

            app._handle_trade("blue-walnut", first_trade)
            app._handle_trade("blue-walnut", second_trade)

            executions = [payload for kind, payload in app.events.records if kind == "execution"]
            decisions = [payload for kind, payload in app.events.records if kind == "decision"]
            self.assertEqual(len(executions), 1)
            self.assertTrue(any(d["reason"] == "skip_alpha_market_entry_limit" for d in decisions))

    def test_handle_trade_limits_alpha_market_position_usdc(self) -> None:
        app = self._build_app()
        rows = [
            {
                "sample_id": "xrp-1",
                "timestamp_seconds": 100,
                "market_slug": "xrp-updown-15m-100",
                "condition_id": "cx1",
                "candidate_outcome": "Down",
                "label_resolved": 1,
                "pnl_per_stake_usdc": 30.0,
                "market_family": "xrp",
                "market_duration_bucket": "15m",
                "candidate_abs_price_distance_from_mid": 0.1,
                "candidate_price_distance_from_mid": -0.1,
                "time_since_prev_same_market_trade_seconds": 1.0,
                "time_since_prev_same_condition_trade_seconds": 1.0,
                "time_since_prev_same_market_outcome_trade_seconds": 1.0,
                "seconds_to_resolution": 300.0,
                "recent_condition_count_60s": 5,
                "recent_same_outcome_count_60s": 4,
                "recent_opposite_outcome_count_60s": 1,
                "recent_same_market_count_60s": 5,
                "recent_same_market_outcome_count_60s": 4,
                "recent_condition_usdc_60s": 100.0,
                "recent_same_outcome_usdc_60s": 80.0,
                "recent_opposite_outcome_usdc_60s": 20.0,
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            input_csv = Path(tmpdir) / "alpha.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)
            app.config.alpha_filter = LivePaperAlphaFilter(
                enabled=True,
                buckets=[
                    LivePaperAlphaBucket(
                        labeled_outcome_csv_path=str(input_csv),
                        market_family="xrp",
                        market_duration_bucket="15m",
                        top_fraction=1.0,
                        min_seconds_to_resolution=30.0,
                        name="blue_walnut_xrp_15m_observe",
                        target_names=["blue-walnut"],
                        max_position_per_market_usdc=4.0,
                    )
                ],
            )
            app.alpha_filter = app._build_alpha_filter()
            trade = TradeActivity(
                proxy_wallet="0xabc",
                timestamp_ms=1774173283,
                condition_id="condition-xrp-1",
                activity_type="TRADE",
                size=10.0,
                usdc_size=7.5,
                transaction_hash="0xtx-xrp-1",
                price=0.75,
                asset="asset-xrp-1",
                side="BUY",
                outcome_index=1,
                title="XRP Up or Down",
                slug="xrp-updown-15m-1774172700",
                event_slug="xrp-updown-15m-1774172700",
                outcome="Down",
            )

            app._handle_trade("blue-walnut", trade)

            executions = [payload for kind, payload in app.events.records if kind == "execution"]
            decisions = [payload for kind, payload in app.events.records if kind == "decision"]
            self.assertEqual(len(executions), 0)
            self.assertTrue(any(d["reason"] == "skip_alpha_market_position_limit" for d in decisions))

    def test_load_live_paper_config_parses_alpha_market_limits(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "live.json"
            config_path.write_text(
                """
{
  "targets": [{"name": "blue-walnut", "wallet": "0xabc", "weight": 1.0}],
  "runtime": {
    "poll_interval_seconds": 2.0,
    "request_timeout_seconds": 20.0,
    "activity_limit": 150,
    "lookback_seconds_on_start": 20,
    "requery_overlap_seconds": 5,
    "duration_hours": 1.0,
    "state_path": "var/test/state.json",
    "event_log_path": "var/test/events.jsonl",
    "hourly_stats_path": "var/test/hourly_stats.csv"
  },
  "portfolio": {"initial_capital_usdc": 1000.0},
  "strategy": {
    "follow_fraction": 1.0,
    "fixed_order_usdc": 100.0,
    "min_target_usdc_size": 0.5,
    "max_follow_price": 0.995,
    "max_slippage_bps": 250.0,
    "max_position_per_asset_usdc": 100.0,
    "min_order_usdc": 1.0
  },
  "execution": {
    "host": "https://clob.polymarket.com",
    "chain_id": 137,
    "signature_type": 1,
    "private_key_env": "POLY_PRIVATE_KEY",
    "funder_env": "POLY_FUNDER"
  },
  "alpha_filter": {
    "enabled": true,
    "buckets": [
      {
        "name": "blue_walnut_xrp_15m_observe",
        "target_names": ["blue-walnut"],
        "labeled_outcome_csv_path": "var/example.csv",
        "market_family": "xrp",
        "market_duration_bucket": "15m",
        "max_entries_per_market": 1,
        "max_position_per_market_usdc": 50.0
      }
    ]
  }
}
                """.strip(),
                encoding="utf-8",
            )

            loaded = load_live_paper_config(str(config_path))
            bucket = loaded.alpha_filter.buckets[0]
            self.assertEqual(bucket.max_entries_per_market, 1)
            self.assertEqual(bucket.max_position_per_market_usdc, 50.0)

    def test_handle_trade_uses_synthetic_book_when_order_book_missing(self) -> None:
        app = self._build_app()
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx",
            price=0.75,
            asset="asset-1",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )

        app._handle_trade("guh123", trade)

        self.assertIn("asset-1", app.state.positions)
        self.assertLess(app.state.cash_usdc, 100.0)
        event_kinds = [kind for kind, _ in app.events.records]
        self.assertIn("order_book_fallback", event_kinds)
        self.assertIn("decision", event_kinds)
        self.assertIn("execution", event_kinds)

    def test_handle_trade_skips_when_real_order_book_required(self) -> None:
        app = self._build_app()
        app.config.runtime.require_real_order_book = True
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx-real-only",
            price=0.75,
            asset="asset-real-only",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )

        app._handle_trade("guh123", trade)

        self.assertNotIn("asset-real-only", app.state.positions)
        event_kinds = [kind for kind, _ in app.events.records]
        self.assertIn("order_book_missing", event_kinds)
        self.assertIn("decision", event_kinds)
        self.assertNotIn("order_book_fallback", event_kinds)
        self.assertNotIn("execution", event_kinds)
        decision_payloads = [payload for kind, payload in app.events.records if kind == "decision"]
        self.assertEqual(decision_payloads[-1]["reason"], "skip_no_real_order_book")

    def test_handle_trade_uses_market_ws_book_before_rest_fallback(self) -> None:
        app = self._build_app()
        app.market_stream = _FakeMarketStream(
            book=OrderBook(
                market="market",
                asset_id="asset-2",
                timestamp="now",
                bids=[PriceLevel(price=0.74, size=20.0)],
                asks=[PriceLevel(price=0.75, size=20.0)],
                min_order_size=1.0,
                tick_size=0.01,
                neg_risk=False,
                last_trade_price=0.745,
            )
        )
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx2",
            price=0.74,
            asset="asset-2",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )

        app._handle_trade("guh123", trade)

        self.assertIn("asset-2", app.state.positions)
        event_kinds = [kind for kind, _ in app.events.records]
        self.assertIn("order_book_source", event_kinds)
        self.assertNotIn("order_book_fallback", event_kinds)

    def test_aggregate_trades_collapses_split_fills_from_same_tx(self) -> None:
        app = self._build_app()
        trades = [
            TradeActivity(
                proxy_wallet="0xabc",
                timestamp_ms=1774173283,
                condition_id="condition",
                activity_type="TRADE",
                size=6.0,
                usdc_size=3.0,
                transaction_hash="0xtx-split",
                price=0.5,
                asset="asset-3",
                side="BUY",
                outcome_index=1,
                title="Market",
                slug="mkt",
                event_slug="mkt",
                outcome="Down",
            ),
            TradeActivity(
                proxy_wallet="0xabc",
                timestamp_ms=1774173283,
                condition_id="condition",
                activity_type="TRADE",
                size=4.0,
                usdc_size=2.4,
                transaction_hash="0xtx-split",
                price=0.6,
                asset="asset-3",
                side="BUY",
                outcome_index=1,
                title="Market",
                slug="mkt",
                event_slug="mkt",
                outcome="Down",
            ),
        ]

        aggregated = app._aggregate_trades(trades)

        self.assertEqual(len(aggregated), 1)
        self.assertEqual(aggregated[0].transaction_hash, "0xtx-split")
        self.assertAlmostEqual(aggregated[0].size, 10.0)
        self.assertAlmostEqual(aggregated[0].usdc_size, 5.4)
        self.assertAlmostEqual(aggregated[0].price, 0.54)

    def test_prime_market_stream_ensures_recent_assets(self) -> None:
        app = self._build_app()
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx3",
            price=0.75,
            asset="asset-prime",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )
        app.api = _RecentActivityApi([trade])
        app.targets = [type("Target", (), {"name": "guh123", "wallet": "0xabc"})()]
        app.state.target_wallets["guh123"] = "0xabc"

        app._prime_market_stream()

        self.assertIn("asset-prime", app.market_stream.ensured_assets)
        event_kinds = [kind for kind, _ in app.events.records]
        self.assertIn("market_ws_prime", event_kinds)

    def test_prime_market_stream_adds_hot_market_pool_assets(self) -> None:
        app = self._build_app()
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx4",
            price=0.75,
            asset="asset-btc-live",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down - March 22, 6:00AM-6:15AM ET",
            slug="btc-updown-15m-1774173600",
            event_slug="btc-updown-15m-1774173600",
            outcome="Down",
        )
        search_events = [
            {
                "slug": "btc-updown-5m-1774175700",
                "title": "Bitcoin Up or Down - March 22, 6:35AM-6:40AM ET",
            }
        ]
        events_by_slug = {
            "btc-updown-5m-1774175700": [
                {
                    "slug": "btc-updown-5m-1774175700",
                    "markets": [
                        {
                            "slug": "btc-updown-5m-1774175700",
                            "question": "Bitcoin Up or Down - March 22, 6:35AM-6:40AM ET",
                            "clobTokenIds": "[\"pool-token-yes\",\"pool-token-no\"]",
                        }
                    ],
                }
            ]
        }
        app.api = _RecentActivityApi(
            [trade], search_events=search_events, events_by_slug=events_by_slug
        )
        app.targets = [type("Target", (), {"name": "guh123", "wallet": "0xabc"})()]
        app.state.target_wallets["guh123"] = "0xabc"

        app._prime_market_stream()

        self.assertIn("pool-token-yes", app.market_stream.ensured_assets)
        self.assertIn("pool-token-no", app.market_stream.ensured_assets)

    def test_start_cursor_clamps_fresh_session_to_started_at(self) -> None:
        app = self._build_app()
        app.state.started_at_seconds = 2_000

        with patch("polymarket_copytrader.live_paper.time.time", return_value=2_000):
            cursor = app._start_cursor_ms("guh123")

        self.assertEqual(cursor, 2_000)

    def test_apply_target_weight_scales_follow_size(self) -> None:
        app = self._build_app()
        app.targets = [
            LivePaperTarget(name="guh123", profile=None, wallet=None, weight=1.0),
            LivePaperTarget(name="blue-walnut", profile=None, wallet=None, weight=3.0),
        ]
        app.target_weight_fractions = app._build_target_weight_fractions(app.targets)
        order_book = OrderBook(
            market="market",
            asset_id="asset-4",
            timestamp="now",
            bids=[PriceLevel(price=0.49, size=20.0)],
            asks=[PriceLevel(price=0.5, size=20.0)],
            min_order_size=1.0,
            tick_size=0.01,
            neg_risk=False,
            last_trade_price=0.495,
        )
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=20.0,
            usdc_size=10.0,
            transaction_hash="0xtx5",
            price=0.49,
            asset="asset-4",
            side="BUY",
            outcome_index=1,
            title="Market",
            slug="mkt",
            event_slug="mkt",
            outcome="Yes",
        )
        decision = FollowDecision(
            should_follow=True,
            reason="follow",
            target_trade=trade,
            follow_side="BUY",
            follow_price=0.5,
            follow_usdc=8.0,
            follow_size=16.0,
            slippage_bps=20.0,
        )

        scaled = app._apply_target_weight("guh123", decision, order_book)

        self.assertTrue(scaled.should_follow)
        self.assertAlmostEqual(scaled.follow_usdc, 2.0)
        self.assertAlmostEqual(scaled.follow_size, 4.0)

    def test_order_book_for_trade_retries_after_context_warmup(self) -> None:
        app = self._build_app()
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx6",
            price=0.75,
            asset="asset-context",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )
        app.api = _RecentActivityApi(
            [],
            events_by_slug={
                "btc-updown-15m-1774172700": [
                    {
                        "slug": "btc-updown-15m-1774172700",
                        "markets": [
                            {
                                "slug": "btc-updown-15m-1774172700",
                                "question": "Bitcoin Up or Down",
                                "clobTokenIds": "[\"asset-context\",\"asset-helper\"]",
                            }
                        ],
                    }
                ]
            },
        )
        app.market_stream = _ContextBookMarketStream(
            target_asset="asset-context",
            helper_asset="asset-helper",
            book=OrderBook(
                market="market",
                asset_id="asset-context",
                timestamp="now",
                bids=[PriceLevel(price=0.74, size=20.0)],
                asks=[PriceLevel(price=0.75, size=20.0)],
                min_order_size=1.0,
                tick_size=0.01,
                neg_risk=False,
                last_trade_price=0.745,
            ),
        )

        book, source = app._order_book_for_trade("guh123", trade)

        self.assertIsNotNone(book)
        self.assertEqual(source, "market_ws_context")

    def test_fetch_candidate_trades_adds_late_backfill_window(self) -> None:
        app = self._build_app()
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173283,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx-window",
            price=0.74,
            asset="asset-2",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )

        class _RecordingApi(_RecentActivityApi):
            def __init__(self, trades):
                super().__init__(trades)
                self.calls = []

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

        app.api = _RecordingApi([trade])
        app.state.target_last_event_timestamp_ms["guh123"] = 1774198087

        with patch("polymarket_copytrader.live_paper.time.time", return_value=1_774_198_100):
            trades = app._fetch_candidate_trades("guh123", "0xabc", 1774198082)

        self.assertEqual(len(trades), 3)
        self.assertEqual(len(app.api.calls), 3)
        self.assertEqual(app.api.calls[0]["sort_direction"], "DESC")
        self.assertEqual(app.api.calls[0]["start_ms"], 1774198090)
        self.assertEqual(app.api.calls[1]["sort_direction"], "DESC")
        self.assertEqual(app.api.calls[1]["start_ms"], 1774198082)
        self.assertEqual(app.api.calls[2]["sort_direction"], "ASC")
        self.assertEqual(app.api.calls[2]["start_ms"], 1774198067)

    def test_poll_target_processes_newest_trades_first(self) -> None:
        app = self._build_app()
        target = LivePaperTarget(name="guh123", profile=None, wallet="0xabc", weight=1.0)
        app.targets = [target]
        app.state.target_wallets["guh123"] = "0xabc"
        app.market_stream = _FakeMarketStream(
            book=OrderBook(
                market="market",
                asset_id="asset-2",
                timestamp="now",
                bids=[PriceLevel(price=0.74, size=20.0)],
                asks=[PriceLevel(price=0.75, size=20.0)],
                min_order_size=1.0,
                tick_size=0.01,
                neg_risk=False,
                last_trade_price=0.745,
            )
        )
        older = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173200,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.4,
            transaction_hash="0xtx-old",
            price=0.74,
            asset="asset-2",
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
            asset="asset-2",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )

        class _OrderedApi(_RecentActivityApi):
            def get_activity(self, wallet: str, limit: int, start_ms: int, side=None, sort_direction="ASC"):
                return [older, newer]

        app.api = _OrderedApi([older, newer])

        with patch("polymarket_copytrader.live_paper.time.time", return_value=1_774_198_100):
            app._poll_target(target)

        decisions = [payload for kind, payload in app.events.records if kind == "decision"]
        self.assertEqual(decisions[0]["target_trade"]["transaction_hash"], "0xtx-new")

    def test_poll_target_prioritizes_btc_short_market_before_hourly(self) -> None:
        app = self._build_app()
        target = LivePaperTarget(name="guh123", profile=None, wallet="0xabc", weight=1.0)
        app.targets = [target]
        app.state.target_wallets["guh123"] = "0xabc"
        app.market_stream = _FakeMarketStream(
            book=OrderBook(
                market="market",
                asset_id="asset-2",
                timestamp="now",
                bids=[PriceLevel(price=0.74, size=20.0)],
                asks=[PriceLevel(price=0.75, size=20.0)],
                min_order_size=1.0,
                tick_size=0.01,
                neg_risk=False,
                last_trade_price=0.745,
            )
        )
        hourly = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774173300,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx-hourly",
            price=0.75,
            asset="asset-2",
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
            asset="asset-2",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down",
            slug="btc-updown-15m-1774172700",
            event_slug="btc-updown-15m-1774172700",
            outcome="Down",
        )

        class _OrderedApi(_RecentActivityApi):
            def get_activity(self, wallet: str, limit: int, start_ms: int, side=None, sort_direction="ASC"):
                return [hourly, short]

        app.api = _OrderedApi([hourly, short])

        with patch("polymarket_copytrader.live_paper.time.time", return_value=1_774_198_100):
            app._poll_target(target)

        decisions = [payload for kind, payload in app.events.records if kind == "decision"]
        self.assertEqual(decisions[0]["target_trade"]["transaction_hash"], "0xtx-short")

    def test_infer_resolution_timestamp_for_calendar_slug(self) -> None:
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1774200000,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=7.5,
            transaction_hash="0xtx-calendar",
            price=0.59,
            asset="asset-calendar",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down - March 22, 2PM ET",
            slug="bitcoin-up-or-down-march-22-2026-2pm-et",
            event_slug="bitcoin-up-or-down-march-22-2026-2pm-et",
            outcome="Down",
        )

        self.assertEqual(_infer_resolution_timestamp_seconds(trade), 1774206000)

    def test_infer_resolution_timestamp_for_calendar_slug_without_year(self) -> None:
        trade = TradeActivity(
            proxy_wallet="0xabc",
            timestamp_ms=1769806824,
            condition_id="condition",
            activity_type="TRADE",
            size=10.0,
            usdc_size=5.3,
            transaction_hash="0xtx-calendar-no-year",
            price=0.53,
            asset="asset-calendar-no-year",
            side="BUY",
            outcome_index=1,
            title="Bitcoin Up or Down - January 30, 4PM ET",
            slug="bitcoin-up-or-down-january-30-4pm-et",
            event_slug="bitcoin-up-or-down-january-30-4pm-et",
            outcome="Down",
        )

        self.assertEqual(_infer_resolution_timestamp_seconds(trade), 1769810400)

    def test_redeem_backfills_calendar_resolution_from_existing_position(self) -> None:
        app = self._build_app()
        app.api = _RedeemPriceApi(
            [PriceHistoryPoint(timestamp_seconds=1774206060, price=0.9995)]
        )
        app.state.positions["asset-calendar"] = LivePaperPosition(
            asset_id="asset-calendar",
            size=10.0,
            cost_usdc=6.0,
            market_slug="bitcoin-up-or-down-march-22-2026-2pm-et",
            outcome="Down",
            condition_id="condition",
            resolution_timestamp_seconds=None,
        )

        app._redeem_resolved_positions(1774206060)

        self.assertNotIn("asset-calendar", app.state.positions)
        redeem_payloads = [payload for kind, payload in app.events.records if kind == "redeem"]
        self.assertEqual(len(redeem_payloads), 1)
        self.assertEqual(redeem_payloads[0]["resolution_timestamp_seconds"], 1774206000)


if __name__ == "__main__":
    unittest.main()
