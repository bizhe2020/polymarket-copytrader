import json
import tempfile
import unittest
from pathlib import Path

from polymarket_copytrader.config.skeleton_assembler import load_skeleton
from polymarket_copytrader.entry_ruleset_v1 import EntryRulesetV1Config, build_entry_ruleset_v1
from polymarket_copytrader.market_time import infer_resolution_timestamp_seconds_from_slug
from polymarket_copytrader.pair_unit_strategy import PairUnitStrategy


class EntryRulesetV1ResolutionGateTests(unittest.TestCase):
    def test_build_entry_ruleset_adds_min_seconds_to_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            first_leg_ruleset_path = root / "first_leg_ruleset.json"
            first_cycle_outcome_path = root / "first_cycle_outcome.json"
            output_json_path = root / "entry_ruleset_v1.json"
            output_csv_path = root / "entry_ruleset_v1.csv"

            first_leg_ruleset_path.write_text(
                json.dumps(
                    {
                        "family_rules": {
                            "btc": {
                                "open_window_seconds_default": 12,
                                "open_window_seconds_aggressive": 12,
                                "price_band_lower": 0.48,
                                "price_band_upper": 0.50,
                                "size_anchor_usdc": 21.345,
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            first_cycle_outcome_path.write_text(
                json.dumps(
                    {
                        "overall_baseline": {"hard_lock_pct": 50.0},
                        "family_component_summary": {
                            "btc": {
                                "time_price": {
                                    "coverage_pct": 20.5,
                                    "hard_lock_pct": 64.5,
                                    "bridge_cycle_pct": 3.2,
                                },
                                "time_price_size_soft_100": {
                                    "coverage_pct": 12.3,
                                    "hard_lock_pct": 67.8,
                                    "bridge_cycle_pct": 3.5,
                                },
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            summary = build_entry_ruleset_v1(
                EntryRulesetV1Config(
                    first_leg_ruleset_json_path=str(first_leg_ruleset_path),
                    first_cycle_outcome_json_path=str(first_cycle_outcome_path),
                    output_json_path=str(output_json_path),
                    output_csv_path=str(output_csv_path),
                )
            )

            self.assertEqual(summary.family_rules["btc"]["min_seconds_to_resolution"], 300)


class SkeletonAssemblerResolutionGateTests(unittest.TestCase):
    def test_load_skeleton_defaults_min_seconds_to_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            skeleton_path = Path(tmpdir) / "strategy_config_skeleton_v0.json"
            skeleton_path.write_text(
                json.dumps(
                    {
                        "config_id": "strategy_config_skeleton_v0",
                        "bundle_id": "bundle",
                        "mode": "skeleton",
                        "rollout": {"active_families": ["btc"], "deferred_families": []},
                        "families": {
                            "btc": {
                                "enabled": True,
                                "strategy_type": "family_default_state_machine",
                                "strategy_id": "btc_family_default_strategy_v0",
                                "entry": {
                                    "window_seconds": 12,
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
                        },
                    }
                ),
                encoding="utf-8",
            )

            bundle = load_skeleton(skeleton_path)

            self.assertEqual(bundle["families"]["btc"]["entry"]["min_seconds_to_resolution"], 300)


class PairUnitStrategyResolutionGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.bundle = {
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
        self.market_slug = "bitcoin-up-or-down-march-22-2026-1pm-et"
        self.resolution_timestamp = infer_resolution_timestamp_seconds_from_slug(self.market_slug)
        assert self.resolution_timestamp is not None

    def test_skip_entry_when_below_min_seconds_to_resolution(self) -> None:
        strategy = PairUnitStrategy(self.bundle)
        current_time = float(self.resolution_timestamp - 299)
        strategy.on_market_open(self.market_slug, "btc", current_time - 5.0)

        decision = strategy.evaluate_first_leg_entry(
            market_slug=self.market_slug,
            family="btc",
            trade_price=0.49,
            trade_usdc_size=5.0,
            current_time=current_time,
        )

        self.assertEqual(decision.action, "skip")
        self.assertEqual(decision.reason, "entry_min_seconds_to_resolution_btc_299s")
        self.assertEqual(decision.details["min_seconds_to_resolution"], 300)
        self.assertEqual(decision.details["seconds_to_resolution"], 299)

    def test_allow_entry_at_threshold(self) -> None:
        strategy = PairUnitStrategy(self.bundle)
        current_time = float(self.resolution_timestamp - 300)
        strategy.on_market_open(self.market_slug, "btc", current_time - 5.0)

        decision = strategy.evaluate_first_leg_entry(
            market_slug=self.market_slug,
            family="btc",
            trade_price=0.49,
            trade_usdc_size=5.0,
            current_time=current_time,
        )

        self.assertEqual(decision.action, "enter_first_leg")
        self.assertEqual(decision.details["min_seconds_to_resolution"], 300)
        self.assertEqual(decision.details["seconds_to_resolution"], 300)


if __name__ == "__main__":
    unittest.main()
