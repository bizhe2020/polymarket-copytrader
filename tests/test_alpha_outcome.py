import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from polymarket_copytrader.alpha_outcome import (
    AlphaOutcomeBaselineConfig,
    AlphaOutcomeRegressionConfig,
    AlphaOutcomeLabelConfig,
    AlphaTopKPaperReplayConfig,
    AlphaTopKPaperStrategyConfig,
    AlphaTopKWalkForwardConfig,
    run_alpha_outcome_baseline,
    run_alpha_outcome_labels,
    run_alpha_outcome_regression,
    run_alpha_topk_paper_replay,
    run_alpha_topk_paper_strategy,
    run_alpha_topk_walkforward,
)


class AlphaOutcomeTests(unittest.TestCase):
    def test_run_alpha_outcome_labels_writes_resolved_pnl(self) -> None:
        rows = [
            {
                "sample_id": "s1",
                "timestamp_seconds": 100,
                "candidate_asset": "asset_up",
                "candidate_price": 0.4,
                "seconds_to_resolution": 300,
                "market_family": "btc",
                "market_duration_bucket": "5m",
                "candidate_outcome": "Up",
            },
            {
                "sample_id": "s2",
                "timestamp_seconds": 100,
                "candidate_asset": "asset_down",
                "candidate_price": 0.6,
                "seconds_to_resolution": 300,
                "market_family": "btc",
                "market_duration_bucket": "5m",
                "candidate_outcome": "Down",
            },
        ]
        history_up = [
            {"timestamp_seconds": 100, "price": 0.4},
            {"timestamp_seconds": 400, "price": 0.99},
        ]
        history_down = [
            {"timestamp_seconds": 100, "price": 0.6},
            {"timestamp_seconds": 400, "price": 0.01},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "features.csv"
            output_csv = root / "labeled.csv"
            output_json = root / "summary.json"
            price_cache_dir = root / "prices"
            price_cache_dir.mkdir(parents=True, exist_ok=True)
            (price_cache_dir / "asset_up.json").write_text(json.dumps(history_up), encoding="utf-8")
            (price_cache_dir / "asset_down.json").write_text(json.dumps(history_down), encoding="utf-8")
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_outcome_labels(
                AlphaOutcomeLabelConfig(
                    input_csv_path=str(input_csv),
                    output_csv_path=str(output_csv),
                    output_json_path=str(output_json),
                    price_cache_dir=str(price_cache_dir),
                    stake_usdc=100.0,
                )
            )

            self.assertEqual(summary.total_rows, 2)
            self.assertEqual(summary.labeled_rows, 2)
            labeled = pd.read_csv(output_csv)
            self.assertIn("label_profit_positive", labeled.columns)
            self.assertGreater(float(labeled.loc[0, "pnl_per_stake_usdc"]), 0.0)
            self.assertLess(float(labeled.loc[1, "pnl_per_stake_usdc"]), 0.0)

    def test_run_alpha_outcome_baseline_writes_summary(self) -> None:
        rows = []
        for i in range(16):
            rows.append(
                {
                    "sample_id": f"s{i}",
                    "timestamp_seconds": i,
                    "market_slug": f"m{i%4}",
                    "candidate_outcome": "Up" if i % 2 == 0 else "Down",
                    "label_resolved": 1,
                    "label_profit_positive": 1 if i >= 8 else 0,
                    "pnl_per_stake_usdc": 20.0 if i >= 8 else -10.0,
                    "market_family": "btc" if i % 2 == 0 else "eth",
                    "market_duration_bucket": "5m" if i % 3 == 0 else "15m",
                    "candidate_abs_price_distance_from_mid": 0.1 + i * 0.01,
                    "candidate_price_distance_from_mid": -0.2 + i * 0.02,
                    "time_since_prev_same_market_trade_seconds": float(i + 1),
                    "time_since_prev_same_condition_trade_seconds": float(i + 2),
                    "time_since_prev_same_market_outcome_trade_seconds": float(i + 3),
                    "seconds_to_resolution": float(300 - i * 5),
                    "asset_return_60s": -0.02 + i * 0.01,
                    "asset_return_300s": -0.05 + i * 0.015,
                    "recent_condition_count_60s": 2 + i,
                    "recent_same_outcome_count_60s": 1 + i,
                    "recent_opposite_outcome_count_60s": 5 - min(i, 4),
                    "recent_same_market_count_60s": 3 + i,
                    "recent_same_market_outcome_count_60s": 1 + i,
                    "recent_condition_usdc_60s": 10 + i,
                    "recent_same_outcome_usdc_60s": 4 + i,
                    "recent_opposite_outcome_usdc_60s": 8 - min(i, 4),
                }
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "labeled.csv"
            output_json = root / "baseline_summary.json"
            predictions_csv = root / "predictions.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_outcome_baseline(
                AlphaOutcomeBaselineConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    predictions_csv_path=str(predictions_csv),
                    test_fraction=0.25,
                )
            )

            self.assertEqual(summary.resolved_rows, 16)
            self.assertTrue(output_json.exists())
            self.assertTrue(predictions_csv.exists())

    def test_run_alpha_outcome_regression_writes_summary(self) -> None:
        rows = []
        for i in range(20):
            rows.append(
                {
                    "sample_id": f"s{i}",
                    "timestamp_seconds": i,
                    "market_slug": f"m{i%4}",
                    "candidate_outcome": "Up" if i % 2 == 0 else "Down",
                    "label_resolved": 1,
                    "pnl_per_stake_usdc": 15.0 + i if i >= 10 else -12.0 + i,
                    "market_family": "btc" if i % 2 == 0 else "eth",
                    "market_duration_bucket": "5m" if i % 3 == 0 else "15m",
                    "candidate_abs_price_distance_from_mid": 0.1 + i * 0.01,
                    "candidate_price_distance_from_mid": -0.2 + i * 0.02,
                    "time_since_prev_same_market_trade_seconds": float(i + 1),
                    "time_since_prev_same_condition_trade_seconds": float(i + 2),
                    "time_since_prev_same_market_outcome_trade_seconds": float(i + 3),
                    "seconds_to_resolution": float(300 - i * 5),
                    "asset_return_60s": -0.02 + i * 0.01,
                    "asset_return_300s": -0.05 + i * 0.015,
                    "recent_condition_count_60s": 2 + i,
                    "recent_same_outcome_count_60s": 1 + i,
                    "recent_opposite_outcome_count_60s": 5 - min(i, 4),
                    "recent_same_market_count_60s": 3 + i,
                    "recent_same_market_outcome_count_60s": 1 + i,
                    "recent_condition_usdc_60s": 10 + i,
                    "recent_same_outcome_usdc_60s": 4 + i,
                    "recent_opposite_outcome_usdc_60s": 8 - min(i, 4),
                }
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "labeled.csv"
            output_json = root / "regression_summary.json"
            predictions_csv = root / "regression_predictions.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_outcome_regression(
                AlphaOutcomeRegressionConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    predictions_csv_path=str(predictions_csv),
                    test_fraction=0.25,
                )
            )

            self.assertEqual(summary.resolved_rows, 20)
            self.assertTrue(output_json.exists())
            self.assertTrue(predictions_csv.exists())
            self.assertIn("top_10pct", summary.topk_realized_pnl)

    def test_run_alpha_topk_paper_strategy_writes_summary(self) -> None:
        rows = [
            {
                "sample_id": "s1",
                "timestamp_seconds": 100,
                "market_slug": "m1",
                "condition_id": "c1",
                "candidate_outcome": "Up",
                "resolution_timestamp_seconds": 160,
                "pnl_per_stake_usdc": 20.0,
                "predicted_pnl_per_stake_usdc": 15.0,
            },
            {
                "sample_id": "s2",
                "timestamp_seconds": 101,
                "market_slug": "m1",
                "condition_id": "c1",
                "candidate_outcome": "Down",
                "resolution_timestamp_seconds": 160,
                "pnl_per_stake_usdc": -10.0,
                "predicted_pnl_per_stake_usdc": 3.0,
            },
            {
                "sample_id": "s3",
                "timestamp_seconds": 200,
                "market_slug": "m2",
                "condition_id": "c2",
                "candidate_outcome": "Up",
                "resolution_timestamp_seconds": 260,
                "pnl_per_stake_usdc": -30.0,
                "predicted_pnl_per_stake_usdc": 12.0,
            },
            {
                "sample_id": "s4",
                "timestamp_seconds": 300,
                "market_slug": "m3",
                "condition_id": "c3",
                "candidate_outcome": "Down",
                "resolution_timestamp_seconds": 360,
                "pnl_per_stake_usdc": 40.0,
                "predicted_pnl_per_stake_usdc": -5.0,
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "predictions.csv"
            output_json = root / "strategy_summary.json"
            curve_csv = root / "curve.csv"
            trades_csv = root / "trades.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_topk_paper_strategy(
                AlphaTopKPaperStrategyConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    curve_csv_path=str(curve_csv),
                    trades_csv_path=str(trades_csv),
                    initial_capital_usdc=1000.0,
                    stake_usdc=100.0,
                    dedupe_mode="one_market_total",
                    selection_mode="top_fraction",
                    top_fraction=0.5,
                )
            )

            self.assertEqual(summary.deduped_rows, 3)
            self.assertEqual(summary.selected_rows, 1)
            self.assertEqual(summary.executed_trades, 1)
            self.assertTrue(output_json.exists())
            self.assertTrue(curve_csv.exists())
            self.assertTrue(trades_csv.exists())

    def test_run_alpha_topk_paper_strategy_derives_resolution_when_missing(self) -> None:
        rows = [
            {
                "sample_id": "s1",
                "timestamp_seconds": 100,
                "market_slug": "m1",
                "candidate_outcome": "Up",
                "seconds_to_resolution": 60,
                "pnl_per_stake_usdc": 20.0,
                "predicted_pnl_per_stake_usdc": 15.0,
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "predictions.csv"
            output_json = root / "strategy_summary.json"
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_topk_paper_strategy(
                AlphaTopKPaperStrategyConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    initial_capital_usdc=1000.0,
                    stake_usdc=100.0,
                    selection_mode="predicted_positive",
                )
            )

            self.assertEqual(summary.executed_trades, 1)
            self.assertTrue(output_json.exists())

    def test_run_alpha_topk_walkforward_writes_outputs(self) -> None:
        rows = []
        for i in range(40):
            pnl = 20.0 if i % 3 == 0 else -10.0
            rows.append(
                {
                    "sample_id": f"s{i}",
                    "timestamp_seconds": 1000 + i,
                    "market_slug": f"m{i%10}",
                    "condition_id": f"c{i%10}",
                    "candidate_outcome": "Up" if i % 2 == 0 else "Down",
                    "resolution_timestamp_seconds": 1100 + i,
                    "label_resolved": 1,
                    "pnl_per_stake_usdc": pnl,
                    "market_family": "btc",
                    "market_duration_bucket": "5m",
                    "candidate_abs_price_distance_from_mid": 0.1 + i * 0.005,
                    "candidate_price_distance_from_mid": -0.15 + i * 0.01,
                    "time_since_prev_same_market_trade_seconds": float(1 + i),
                    "time_since_prev_same_condition_trade_seconds": float(2 + i),
                    "time_since_prev_same_market_outcome_trade_seconds": float(3 + i),
                    "seconds_to_resolution": 120.0,
                    "asset_return_60s": -0.03 + i * 0.002,
                    "asset_return_300s": -0.05 + i * 0.003,
                    "recent_condition_count_60s": 5 + i,
                    "recent_same_outcome_count_60s": 2 + i,
                    "recent_opposite_outcome_count_60s": 10 - min(i, 8),
                    "recent_same_market_count_60s": 3 + i,
                    "recent_same_market_outcome_count_60s": 1 + i,
                    "recent_condition_usdc_60s": 50 + i,
                    "recent_same_outcome_usdc_60s": 20 + i,
                    "recent_opposite_outcome_usdc_60s": 40 - min(i, 8),
                }
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "walkforward_input.csv"
            output_json = root / "walkforward_summary.json"
            folds_csv = root / "walkforward_folds.csv"
            curve_csv = root / "walkforward_curve.csv"
            trades_csv = root / "walkforward_trades.csv"
            predictions_csv = root / "walkforward_predictions.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_topk_walkforward(
                AlphaTopKWalkForwardConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    folds_csv_path=str(folds_csv),
                    curve_csv_path=str(curve_csv),
                    trades_csv_path=str(trades_csv),
                    predictions_csv_path=str(predictions_csv),
                    initial_train_fraction=0.5,
                    n_folds=4,
                    initial_capital_usdc=1000.0,
                    stake_usdc=100.0,
                    top_fraction=0.5,
                )
            )

            self.assertEqual(summary.folds, 4)
            self.assertGreater(summary.combined_test_rows, 0)
            self.assertTrue(output_json.exists())
            self.assertTrue(folds_csv.exists())
            self.assertTrue(curve_csv.exists())
            self.assertTrue(trades_csv.exists())
            self.assertTrue(predictions_csv.exists())

    def test_run_alpha_topk_paper_replay_enriches_from_source(self) -> None:
        predictions_rows = [
            {
                "sample_id": "s1",
                "timestamp_seconds": 100,
                "market_slug": "m1",
                "condition_id": "c1",
                "candidate_outcome": "Up",
                "resolution_timestamp_seconds": 160,
                "predicted_pnl_per_stake_usdc": 12.0,
            },
            {
                "sample_id": "s2",
                "timestamp_seconds": 200,
                "market_slug": "m2",
                "condition_id": "c2",
                "candidate_outcome": "Down",
                "resolution_timestamp_seconds": 260,
                "predicted_pnl_per_stake_usdc": 5.0,
            },
        ]
        source_rows = [
            {
                "sample_id": "s1",
                "candidate_price": 0.5,
                "payout_price": 1.0,
                "seconds_to_resolution": 60,
            },
            {
                "sample_id": "s2",
                "candidate_price": 0.98,
                "payout_price": 0.0,
                "seconds_to_resolution": 60,
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "predictions.csv"
            source_csv = root / "source.csv"
            output_json = root / "replay_summary.json"
            curve_csv = root / "replay_curve.csv"
            trades_csv = root / "replay_trades.csv"
            pd.DataFrame(predictions_rows).to_csv(input_csv, index=False)
            pd.DataFrame(source_rows).to_csv(source_csv, index=False)

            summary = run_alpha_topk_paper_replay(
                AlphaTopKPaperReplayConfig(
                    input_csv_path=str(input_csv),
                    source_csv_path=str(source_csv),
                    output_json_path=str(output_json),
                    curve_csv_path=str(curve_csv),
                    trades_csv_path=str(trades_csv),
                    initial_capital_usdc=1000.0,
                    stake_usdc=100.0,
                    entry_slippage_bps=100.0,
                    max_entry_price=0.95,
                    top_fraction=1.0,
                )
            )

            self.assertEqual(summary.selected_rows, 2)
            self.assertEqual(summary.executed_trades, 1)
            self.assertEqual(summary.skipped_for_price, 1)
            self.assertTrue(output_json.exists())
            self.assertTrue(curve_csv.exists())
            self.assertTrue(trades_csv.exists())


if __name__ == "__main__":
    unittest.main()
