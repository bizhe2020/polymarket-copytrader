import json
import tempfile
import unittest
from pathlib import Path

from polymarket_copytrader.pair_live_paper import (
    PairLivePaperApp,
    PairLivePaperConfig,
    PairLivePaperState,
    PairLivePaperPortfolio,
    PairLivePaperTarget,
    PairLivePaperRuntime,
    PairMarketDescriptor,
    PairScannerConfig,
    _infer_market_open_timestamp_seconds,
    load_pair_live_paper_config,
)
from polymarket_copytrader.pair_unit_strategy import PairUnitStrategy


class _RecordingSink:
    def __init__(self) -> None:
        self.records = []

    def write(self, kind: str, payload: dict) -> None:
        self.records.append((kind, payload))


class PairLivePaperPlanBTests(unittest.TestCase):
    def _build_app(self) -> PairLivePaperApp:
        app = PairLivePaperApp.__new__(PairLivePaperApp)
        app.config = PairLivePaperConfig(
            target=PairLivePaperTarget(name="blue-walnut", profile="blue-walnut", wallet=None),
            runtime=PairLivePaperRuntime(
                poll_interval_seconds=2.0,
                request_timeout_seconds=20.0,
                activity_limit=200,
                lookback_seconds_on_start=300,
                requery_overlap_seconds=6,
                market_websocket_enabled=True,
                market_websocket_warmup_ms=350,
                require_real_order_book=True,
                duration_hours=0.1,
                heartbeat_interval_seconds=30.0,
                state_path="var/test/state.json",
                event_log_path="var/test/events.jsonl",
                hourly_stats_path="var/test/hourly.csv",
            ),
            portfolio=PairLivePaperPortfolio(initial_capital_usdc=10_000.0),
            scanner=PairScannerConfig(
                families=["btc"],
                durations=["hourly"],
                pair_stake_usdc=100.0,
                max_effective_pair_sum=1.0,
                fee_bps=0.0,
                slippage_bps=0.0,
                staged_entry_enabled=True,
            ),
        )
        app.target = PairLivePaperTarget(name="blue-walnut", profile="blue-walnut", wallet=None)
        app.events = _RecordingSink()
        app.state = PairLivePaperState(cash_usdc=10_000.0)
        app.rule_engine = PairUnitStrategy(
            {
                "families": {
                    "btc": {
                        "enabled": True,
                        "strategy_type": "family_default_state_machine",
                        "entry": {
                            "window_seconds": 12,
                            "min_seconds_to_resolution": 300,
                            "price_band_lower": 0.48,
                            "price_band_upper": 0.50,
                            "size_policy": "no_hard_size_gate",
                        },
                        "second_leg": {
                            "default_route_class": "fast_20_30",
                            "timeout_seconds": 30,
                            "wait_budget_70pct": 20,
                            "wait_budget_80pct": 30,
                            "acceptance_fast_bands": ["-2c~-1c", "-1c~0", "0~1c"],
                            "acceptance_delayed_tail_bands": ["1c~2c"],
                            "acceptance_hard_lock_bands": ["<=-2c"],
                        },
                        "regime_overrides": {},
                    }
                }
            }
        )
        return app

    def test_load_pair_live_paper_config_reads_skeleton_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "pair-live-paper.json"
            config_path.write_text(
                json.dumps(
                    {
                        "target": {"name": "blue-walnut", "profile": "blue-walnut", "wallet": None},
                        "runtime": {
                            "poll_interval_seconds": 2.0,
                            "request_timeout_seconds": 20.0,
                            "activity_limit": 200,
                            "lookback_seconds_on_start": 300,
                            "requery_overlap_seconds": 6,
                            "market_websocket_enabled": True,
                            "market_websocket_warmup_ms": 350,
                            "require_real_order_book": True,
                            "duration_hours": 0.1,
                            "heartbeat_interval_seconds": 30.0,
                            "state_path": "var/test/state.json",
                            "event_log_path": "var/test/events.jsonl",
                            "hourly_stats_path": "var/test/hourly.csv",
                        },
                        "portfolio": {"initial_capital_usdc": 10000.0},
                        "scanner": {
                            "families": ["btc"],
                            "durations": ["hourly"],
                            "skeleton_config_path": "var/research_blue_walnut/strategy_config_skeleton_v0.json",
                        },
                    }
                ),
                encoding="utf-8",
            )

            config = load_pair_live_paper_config(str(config_path))

            self.assertEqual(
                config.scanner.skeleton_config_path,
                "var/research_blue_walnut/strategy_config_skeleton_v0.json",
            )

    def test_infer_market_open_timestamp_seconds(self) -> None:
        self.assertEqual(_infer_market_open_timestamp_seconds(3_600, "hourly"), 0)
        self.assertEqual(_infer_market_open_timestamp_seconds(900, "15m"), 0)
        self.assertEqual(_infer_market_open_timestamp_seconds(300, "5m"), 0)
        self.assertIsNone(_infer_market_open_timestamp_seconds(None, "hourly"))

    def test_effective_resolution_window_uses_skeleton_min_and_scanner_window(self) -> None:
        app = PairLivePaperApp.__new__(PairLivePaperApp)
        app.config = PairLivePaperConfig(
            target=PairLivePaperTarget(name="blue-walnut", profile="blue-walnut", wallet=None),
            runtime=PairLivePaperRuntime(
                poll_interval_seconds=2.0,
                request_timeout_seconds=20.0,
                activity_limit=200,
                lookback_seconds_on_start=300,
                requery_overlap_seconds=6,
                market_websocket_enabled=True,
                market_websocket_warmup_ms=350,
                require_real_order_book=True,
                duration_hours=0.1,
                heartbeat_interval_seconds=30.0,
                state_path="var/test/state.json",
                event_log_path="var/test/events.jsonl",
                hourly_stats_path="var/test/hourly.csv",
            ),
            portfolio=PairLivePaperPortfolio(initial_capital_usdc=10_000.0),
            scanner=PairScannerConfig(
                families=["btc"],
                durations=["hourly"],
                min_seconds_to_resolution=3300,
                max_seconds_to_resolution=3600,
            ),
        )
        app.rule_engine = PairUnitStrategy(
            {
                "families": {
                    "btc": {
                        "enabled": True,
                        "strategy_type": "family_default_state_machine",
                        "entry": {
                            "window_seconds": 12,
                            "min_seconds_to_resolution": 300,
                            "price_band_lower": 0.48,
                            "price_band_upper": 0.50,
                            "size_policy": "no_hard_size_gate",
                        },
                        "second_leg": {
                            "default_route_class": "fast_20_30",
                            "timeout_seconds": 30,
                            "wait_budget_70pct": 20,
                            "wait_budget_80pct": 30,
                            "acceptance_fast_bands": ["-2c~-1c"],
                            "acceptance_delayed_tail_bands": ["0~1c"],
                            "acceptance_hard_lock_bands": [">=2c"],
                        },
                        "regime_overrides": {},
                    }
                }
            }
        )

        effective_min, effective_max = app._effective_resolution_window(market_family="btc")

        self.assertEqual(effective_min, 3300)
        self.assertEqual(effective_max, 3600)

    def test_pending_entry_registers_rule_engine_unit(self) -> None:
        app = self._build_app()
        descriptor = PairMarketDescriptor(
            market_slug="bitcoin-up-or-down-march-22-2026-1pm-et",
            condition_id="condition-1",
            up_outcome="Up",
            down_outcome="Down",
            up_asset="asset-up",
            down_asset="asset-down",
            resolution_timestamp_seconds=3600,
        )
        app.rule_engine.on_market_open(descriptor.market_slug, "btc", 0.0)

        app._maybe_open_pending_entry(
            descriptor=descriptor,
            up_ask=0.52,
            down_ask=0.49,
            up_source="market_ws",
            down_source="market_ws",
            market_family="btc",
            market_duration_bucket="hourly",
            event_slug="bitcoin-up-or-down",
            trigger_mode="market_scan",
            reference_payload={"target_name": "blue-walnut"},
            seconds_to_resolution=3500,
            now_seconds=5,
        )

        self.assertEqual(len(app.state.pending_entries), 1)
        pending_entry = next(iter(app.state.pending_entries.values()))
        self.assertTrue(pending_entry.unit_id)
        self.assertEqual(app.rule_engine.pending_unit_count(), 1)

    def test_pending_completion_uses_pair_unit_strategy_acceptance_band(self) -> None:
        app = self._build_app()
        descriptor = PairMarketDescriptor(
            market_slug="bitcoin-up-or-down-march-22-2026-1pm-et",
            condition_id="condition-1",
            up_outcome="Up",
            down_outcome="Down",
            up_asset="asset-up",
            down_asset="asset-down",
            resolution_timestamp_seconds=3600,
        )
        app.rule_engine.on_market_open(descriptor.market_slug, "btc", 0.0)
        app._maybe_open_pending_entry(
            descriptor=descriptor,
            up_ask=0.52,
            down_ask=0.49,
            up_source="market_ws",
            down_source="market_ws",
            market_family="btc",
            market_duration_bucket="hourly",
            event_slug="bitcoin-up-or-down",
            trigger_mode="market_scan",
            reference_payload={"target_name": "blue-walnut"},
            seconds_to_resolution=3500,
            now_seconds=5,
        )
        pending_entry = next(iter(app.state.pending_entries.values()))

        app._maybe_complete_pending_entry(
            descriptor=descriptor,
            pending_entry=pending_entry,
            up_ask=0.49,
            down_ask=0.52,
            up_source="market_ws",
            down_source="market_ws",
            market_family="btc",
            market_duration_bucket="hourly",
            event_slug="bitcoin-up-or-down",
            trigger_mode="market_scan",
            reference_payload={"target_name": "blue-walnut"},
            seconds_to_resolution=3490,
            now_seconds=10,
        )

        self.assertEqual(len(app.state.pending_entries), 0)
        self.assertEqual(len(app.state.positions), 1)
        self.assertEqual(app.rule_engine.pending_unit_count(), 0)


if __name__ == "__main__":
    unittest.main()
