import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from polymarket_copytrader.alpha_baseline import (
    AlphaBaselineConfig,
    AlphaPaperReplayConfig,
    AlphaSignalReplayConfig,
    AlphaSignalScorerConfig,
    AlphaTwoStageConfig,
    run_alpha_baseline,
    run_alpha_paper_replay,
    run_alpha_signal_replay,
    run_alpha_signal_scorer,
    run_alpha_two_stage_baseline,
)


class AlphaBaselineTests(unittest.TestCase):
    def test_run_alpha_baseline_writes_summary_and_predictions(self) -> None:
        rows = [
            {
                "sample_id": f"s{i}",
                "timestamp_seconds": i,
                "market_slug": f"m{i % 3}",
                "candidate_outcome": "Up" if i % 2 == 0 else "Down",
                "label_buy": 1 if i >= 6 else 0,
                "market_family": "btc" if i % 2 == 0 else "eth",
                "market_duration_bucket": "5m" if i % 3 == 0 else "15m",
                "candidate_abs_price_distance_from_mid": 0.1 + i * 0.01,
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
            for i in range(12)
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "features.csv"
            output_json = root / "summary.json"
            predictions_csv = root / "predictions.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_baseline(
                AlphaBaselineConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    predictions_csv_path=str(predictions_csv),
                    test_fraction=0.25,
                )
            )

            self.assertEqual(summary.total_rows, 12)
            self.assertEqual(summary.test_rows, 3)
            self.assertTrue(0.0 <= summary.precision <= 1.0)
            self.assertTrue(output_json.exists())
            self.assertTrue(predictions_csv.exists())

            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertIn("topk_precision", payload)
            self.assertIn("feature_columns", payload)

    def test_run_alpha_two_stage_baseline_writes_summary(self) -> None:
        rows = []
        for i in range(20):
            is_buy = 1 if i % 3 != 0 else 0
            outcome = "Up" if i % 2 == 0 else "Down"
            rows.append(
                {
                    "sample_id": f"s{i}",
                    "timestamp_seconds": i,
                    "market_slug": f"m{i % 4}",
                    "candidate_outcome": outcome,
                    "label_buy": is_buy,
                    "market_family": "btc" if i % 2 == 0 else "eth",
                    "market_duration_bucket": "5m" if i % 3 == 0 else "15m",
                    "candidate_abs_price_distance_from_mid": 0.1 + (i % 5) * 0.02,
                    "time_since_prev_same_market_trade_seconds": float(i + 1),
                    "time_since_prev_same_condition_trade_seconds": float(i + 2),
                    "time_since_prev_same_market_outcome_trade_seconds": float(i + 3),
                    "seconds_to_resolution": float(300 - i * 4),
                    "asset_return_60s": -0.02 + i * 0.01,
                    "asset_return_300s": -0.05 + i * 0.015,
                    "recent_condition_count_60s": 2 + i,
                    "recent_same_outcome_count_60s": 1 + i,
                    "recent_opposite_outcome_count_60s": 2 + (i % 4),
                    "recent_same_market_count_60s": 2 + i,
                    "recent_same_market_outcome_count_60s": 1 + i,
                    "recent_condition_usdc_60s": 10 + i,
                    "recent_same_outcome_usdc_60s": 4 + i,
                    "recent_opposite_outcome_usdc_60s": 8 + i,
                    "external_market_return_60s": 0.001 * i,
                    "external_market_return_300s": 0.002 * i,
                    "external_market_return_900s": 0.003 * i,
                    "external_market_volume_60s": 100 + i,
                    "external_market_trade_count_300s": 200 + i,
                    "external_market_realized_vol_300s": 0.01 + i * 0.001,
                }
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "features.csv"
            output_json = root / "two_stage_summary.json"
            predictions_csv = root / "two_stage_predictions.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_two_stage_baseline(
                AlphaTwoStageConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    predictions_csv_path=str(predictions_csv),
                    test_fraction=0.25,
                )
            )

            self.assertEqual(summary.total_rows, 20)
            self.assertTrue(summary.strict_rows <= 20)
            self.assertTrue(output_json.exists())
            self.assertTrue(predictions_csv.exists())

            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertIn("buy_stage", payload)
            self.assertIn("direction_stage", payload)

    def test_run_alpha_signal_scorer_writes_signal_outputs(self) -> None:
        rows = []
        for i in range(24):
            is_buy = 1 if i % 4 != 0 else 0
            outcome = "Up" if i % 2 == 0 else "Down"
            rows.append(
                {
                    "sample_id": f"s{i}",
                    "timestamp_seconds": i,
                    "timestamp_iso": f"2026-03-23T00:00:{i:02d}+00:00",
                    "market_slug": f"m{i % 4}",
                    "condition_id": f"c{i % 4}",
                    "candidate_outcome": outcome,
                    "label_buy": is_buy,
                    "market_family": "btc",
                    "market_duration_bucket": "5m",
                    "candidate_price": 0.45 + (i % 5) * 0.02,
                    "candidate_abs_price_distance_from_mid": 0.1 + (i % 5) * 0.02,
                    "candidate_price_distance_from_mid": (-0.05 if outcome == "Down" else 0.05) + (i % 3) * 0.01,
                    "time_since_prev_same_market_trade_seconds": float(i + 1),
                    "time_since_prev_same_condition_trade_seconds": float(i + 2),
                    "time_since_prev_same_market_outcome_trade_seconds": float(i + 3),
                    "seconds_to_resolution": float(300 - i * 4),
                    "asset_return_60s": -0.02 + i * 0.01,
                    "asset_return_300s": -0.05 + i * 0.015,
                    "recent_condition_count_60s": 2 + i,
                    "recent_same_outcome_count_60s": 1 + i,
                    "recent_opposite_outcome_count_60s": 2 + (i % 4),
                    "recent_same_market_count_60s": 2 + i,
                    "recent_same_market_outcome_count_60s": 1 + i,
                    "recent_condition_usdc_60s": 10 + i,
                    "recent_same_outcome_usdc_60s": 4 + i,
                    "recent_opposite_outcome_usdc_60s": 8 + i,
                    "external_market_return_60s": 0.001 * i,
                    "external_market_return_300s": 0.002 * i,
                    "external_market_return_900s": 0.003 * i,
                    "external_market_volume_60s": 100 + i,
                    "external_market_trade_count_300s": 200 + i,
                    "external_market_realized_vol_300s": 0.01 + i * 0.001,
                }
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "features.csv"
            output_json = root / "signal_summary.json"
            signals_csv = root / "signals.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_signal_scorer(
                AlphaSignalScorerConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    signals_csv_path=str(signals_csv),
                    test_fraction=0.25,
                )
            )

            self.assertEqual(summary.total_rows, 24)
            self.assertTrue(output_json.exists())
            self.assertTrue(signals_csv.exists())
            self.assertTrue(summary.scored_rows > 0)

    def test_run_alpha_signal_replay_dedupes_and_writes_outputs(self) -> None:
        rows = [
            {
                "sample_id": "a1",
                "timestamp_seconds": 100,
                "timestamp_iso": "2026-03-23T00:01:40+00:00",
                "market_slug": "btc-updown-5m-1",
                "condition_id": "c1",
                "candidate_outcome": "Up",
                "label_buy": 1,
                "buy_score": 0.95,
                "up_score": 0.70,
                "outcome_probability": 0.70,
                "final_score": 0.665,
                "passes_buy_threshold": True,
                "passes_final_threshold": True,
                "predicted_up": True,
                "predicted_outcome_match": True,
            },
            {
                "sample_id": "a2",
                "timestamp_seconds": 101,
                "timestamp_iso": "2026-03-23T00:01:41+00:00",
                "market_slug": "btc-updown-5m-1",
                "condition_id": "c1",
                "candidate_outcome": "Up",
                "label_buy": 1,
                "buy_score": 0.96,
                "up_score": 0.72,
                "outcome_probability": 0.72,
                "final_score": 0.6912,
                "passes_buy_threshold": True,
                "passes_final_threshold": True,
                "predicted_up": True,
                "predicted_outcome_match": True,
            },
            {
                "sample_id": "b1",
                "timestamp_seconds": 102,
                "timestamp_iso": "2026-03-23T00:01:42+00:00",
                "market_slug": "btc-updown-5m-1",
                "condition_id": "c1",
                "candidate_outcome": "Down",
                "label_buy": 0,
                "buy_score": 0.91,
                "up_score": 0.72,
                "outcome_probability": 0.28,
                "final_score": 0.2548,
                "passes_buy_threshold": True,
                "passes_final_threshold": False,
                "predicted_up": True,
                "predicted_outcome_match": False,
            },
            {
                "sample_id": "c1",
                "timestamp_seconds": 118,
                "timestamp_iso": "2026-03-23T00:01:58+00:00",
                "market_slug": "btc-updown-5m-2",
                "condition_id": "c2",
                "candidate_outcome": "Down",
                "label_buy": 1,
                "buy_score": 0.88,
                "up_score": 0.40,
                "outcome_probability": 0.60,
                "final_score": 0.528,
                "passes_buy_threshold": True,
                "passes_final_threshold": False,
                "predicted_up": False,
                "predicted_outcome_match": True,
            },
            {
                "sample_id": "d1",
                "timestamp_seconds": 132,
                "timestamp_iso": "2026-03-23T00:02:12+00:00",
                "market_slug": "btc-updown-5m-3",
                "condition_id": "c3",
                "candidate_outcome": "Up",
                "label_buy": 0,
                "buy_score": 0.93,
                "up_score": 0.67,
                "outcome_probability": 0.67,
                "final_score": 0.6231,
                "passes_buy_threshold": True,
                "passes_final_threshold": True,
                "predicted_up": True,
                "predicted_outcome_match": True,
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "signals.csv"
            output_json = root / "replay_summary.json"
            deduped_csv = root / "deduped_signals.csv"
            bucket_csv = root / "bucket_metrics.csv"
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_signal_replay(
                AlphaSignalReplayConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    deduped_signals_csv_path=str(deduped_csv),
                    bucket_metrics_csv_path=str(bucket_csv),
                    time_bucket_seconds=15,
                )
            )

            self.assertEqual(summary.total_rows, 5)
            self.assertEqual(summary.filtered_rows, 3)
            self.assertEqual(summary.outcome_deduped_rows, 2)
            self.assertEqual(summary.condition_deduped_rows, 2)
            self.assertTrue(output_json.exists())
            self.assertTrue(deduped_csv.exists())
            self.assertTrue(bucket_csv.exists())

    def test_run_alpha_paper_replay_resolves_trade_from_price_cache(self) -> None:
        rows = [
            {
                "sample_id": "sig1",
                "timestamp_seconds": 100,
                "market_slug": "btc-updown-5m-100",
                "condition_id": "cond1",
                "candidate_outcome": "Up",
                "candidate_asset": "asset_up",
                "candidate_price": 0.4,
                "seconds_to_resolution": 300,
                "final_score": 0.7,
            }
        ]

        history = [
            {"timestamp_seconds": 100, "price": 0.4},
            {"timestamp_seconds": 400, "price": 0.99},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "signals.csv"
            output_json = root / "paper_summary.json"
            trades_csv = root / "paper_trades.csv"
            price_cache_dir = root / "prices"
            price_cache_dir.mkdir(parents=True, exist_ok=True)
            (price_cache_dir / "asset_up.json").write_text(json.dumps(history), encoding="utf-8")
            pd.DataFrame(rows).to_csv(input_csv, index=False)

            summary = run_alpha_paper_replay(
                AlphaPaperReplayConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    price_cache_dir=str(price_cache_dir),
                    trades_csv_path=str(trades_csv),
                    initial_capital_usdc=1000.0,
                    fixed_order_usdc=100.0,
                )
            )

            self.assertEqual(summary.executed_signals, 1)
            self.assertEqual(summary.resolved_trades, 1)
            self.assertAlmostEqual(summary.final_cash_usdc, 1150.0, places=6)
            self.assertTrue(output_json.exists())
            self.assertTrue(trades_csv.exists())


if __name__ == "__main__":
    unittest.main()
