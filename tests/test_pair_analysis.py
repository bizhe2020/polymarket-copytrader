import csv
import json
import tempfile
import unittest
from pathlib import Path

from polymarket_copytrader.pair_analysis import (
    PairAnalysisConfig,
    PairPaperReplayConfig,
    PairSequenceAnalysisConfig,
    run_pair_analysis,
    run_pair_paper_replay,
    run_pair_sequence_analysis,
)


class PairAnalysisTests(unittest.TestCase):
    def test_run_pair_analysis_writes_pair_rows_and_summary(self) -> None:
        trades = [
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1000,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 10.0,
                "usdc_size": 4.8,
                "transaction_hash": "0x1",
                "price": 0.48,
                "asset": "asset-up",
                "side": "BUY",
                "outcome_index": 0,
                "title": "XRP Up or Down - March 22, 1:00PM-1:15PM ET",
                "slug": "xrp-updown-15m-1774174800",
                "event_slug": "xrp-updown-15m-1774174800",
                "outcome": "Up",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1001,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 10.0,
                "usdc_size": 4.6,
                "transaction_hash": "0x2",
                "price": 0.46,
                "asset": "asset-down",
                "side": "BUY",
                "outcome_index": 1,
                "title": "XRP Up or Down - March 22, 1:00PM-1:15PM ET",
                "slug": "xrp-updown-15m-1774174800",
                "event_slug": "xrp-updown-15m-1774174800",
                "outcome": "Down",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 2000,
                "condition_id": "cond-2",
                "activity_type": "TRADE",
                "size": 5.0,
                "usdc_size": 2.8,
                "transaction_hash": "0x3",
                "price": 0.56,
                "asset": "asset-btc-up",
                "side": "BUY",
                "outcome_index": 0,
                "title": "Bitcoin Up or Down - March 22, 1PM ET",
                "slug": "bitcoin-up-or-down-march-22-2026-1pm-et",
                "event_slug": "bitcoin-up-or-down-march-22-2026-1pm-et",
                "outcome": "Up",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 2050,
                "condition_id": "cond-2",
                "activity_type": "TRADE",
                "size": 5.0,
                "usdc_size": 2.4,
                "transaction_hash": "0x4",
                "price": 0.48,
                "asset": "asset-btc-down",
                "side": "BUY",
                "outcome_index": 1,
                "title": "Bitcoin Up or Down - March 22, 1PM ET",
                "slug": "bitcoin-up-or-down-march-22-2026-1pm-et",
                "event_slug": "bitcoin-up-or-down-march-22-2026-1pm-et",
                "outcome": "Down",
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "blue-walnut.jsonl"
            output_csv = root / "pairs.csv"
            summary_json = root / "summary.json"
            with input_path.open("w", encoding="utf-8") as handle:
                for trade in trades:
                    handle.write(json.dumps(trade, ensure_ascii=False) + "\n")

            summary = run_pair_analysis(
                PairAnalysisConfig(
                    input_trades_path=str(input_path),
                    output_csv_path=str(output_csv),
                    output_json_path=str(summary_json),
                    recent_buy_limit=100,
                    families=("btc", "xrp"),
                    durations=("15m", "hourly"),
                )
            )

            self.assertEqual(summary.total_buy_rows, 4)
            self.assertEqual(summary.filtered_buy_rows, 4)
            self.assertEqual(summary.paired_markets, 2)
            self.assertEqual(summary.paired_markets_below_parity, 1)
            self.assertEqual(summary.paired_markets_strict_arb, 1)

            with output_csv.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            strict_row = next(row for row in rows if row["market_family"] == "xrp")
            self.assertEqual(strict_row["strict_pair_arb_candidate"], "1")
            self.assertEqual(strict_row["pair_sum"], "0.94")

            payload = json.loads(summary_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["family_counts"]["xrp"], 1)

    def test_run_pair_paper_replay_filters_and_computes_locked_pnl(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_csv = root / "pairs.csv"
            output_json = root / "replay.json"
            trades_csv = root / "trades.csv"
            curve_csv = root / "curve.csv"
            with input_csv.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "market_slug",
                        "pair_sum",
                        "pair_gap_to_parity",
                        "pair_end_timestamp_seconds",
                        "pair_completion_seconds",
                        "total_pair_usdc",
                        "imbalance_ratio",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "market_slug": "good-market",
                        "pair_sum": "0.95",
                        "pair_gap_to_parity": "0.05",
                        "pair_end_timestamp_seconds": "100",
                        "pair_completion_seconds": "2",
                        "total_pair_usdc": "500",
                        "imbalance_ratio": "0.10",
                    }
                )
                writer.writerow(
                    {
                        "market_slug": "slow-market",
                        "pair_sum": "0.96",
                        "pair_gap_to_parity": "0.04",
                        "pair_end_timestamp_seconds": "200",
                        "pair_completion_seconds": "20",
                        "total_pair_usdc": "400",
                        "imbalance_ratio": "0.10",
                    }
                )
                writer.writerow(
                    {
                        "market_slug": "expensive-market",
                        "pair_sum": "1.01",
                        "pair_gap_to_parity": "-0.01",
                        "pair_end_timestamp_seconds": "300",
                        "pair_completion_seconds": "1",
                        "total_pair_usdc": "300",
                        "imbalance_ratio": "0.05",
                    }
                )

            summary = run_pair_paper_replay(
                PairPaperReplayConfig(
                    input_csv_path=str(input_csv),
                    output_json_path=str(output_json),
                    trades_csv_path=str(trades_csv),
                    curve_csv_path=str(curve_csv),
                    initial_capital_usdc=1000.0,
                    stake_usdc=100.0,
                    max_effective_pair_sum=0.98,
                    max_pair_completion_seconds=10,
                )
            )

            self.assertEqual(summary.total_rows, 3)
            self.assertEqual(summary.eligible_rows, 1)
            self.assertEqual(summary.executed_trades, 1)
            self.assertEqual(summary.skipped_for_pair_sum, 1)
            self.assertEqual(summary.skipped_for_completion, 1)
            self.assertAlmostEqual(summary.final_equity_usdc, 1005.263158, places=5)
            self.assertAlmostEqual(summary.total_return_pct, 0.526316, places=5)

            payload = json.loads(output_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["executed_trades"], 1)

            with trades_csv.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["market_slug"], "good-market")

    def test_run_pair_sequence_analysis_tracks_first_leg_and_tail(self) -> None:
        trades = [
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1_774_174_800_000,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 10.0,
                "usdc_size": 4.8,
                "transaction_hash": "0x1",
                "price": 0.48,
                "asset": "asset-up",
                "side": "BUY",
                "outcome_index": 0,
                "title": "Ethereum Up or Down - March 22, 1:00PM-1:15PM ET",
                "slug": "eth-updown-15m-1774174800",
                "event_slug": "eth-updown-15m-1774174800",
                "outcome": "Up",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1_774_174_803_000,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 10.0,
                "usdc_size": 4.7,
                "transaction_hash": "0x2",
                "price": 0.47,
                "asset": "asset-down",
                "side": "BUY",
                "outcome_index": 1,
                "title": "Ethereum Up or Down - March 22, 1:00PM-1:15PM ET",
                "slug": "eth-updown-15m-1774174800",
                "event_slug": "eth-updown-15m-1774174800",
                "outcome": "Down",
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "blue-walnut.jsonl"
            output_csv = root / "pair-sequence.csv"
            summary_json = root / "pair-sequence.json"
            with input_path.open("w", encoding="utf-8") as handle:
                for trade in trades:
                    handle.write(json.dumps(trade, ensure_ascii=False) + "\n")

            summary = run_pair_sequence_analysis(
                PairSequenceAnalysisConfig(
                    input_trades_path=str(input_path),
                    output_csv_path=str(output_csv),
                    output_json_path=str(summary_json),
                    recent_buy_limit=100,
                    families=("eth",),
                    durations=("15m",),
                )
            )

            self.assertEqual(summary.paired_markets, 1)
            self.assertEqual(summary.markets_completed_within_5s, 1)
            self.assertEqual(summary.markets_first_cross_below_parity, 1)

            with output_csv.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["first_leg_outcome"], "Up")
            self.assertEqual(rows[0]["second_leg_outcome"], "Down")
            self.assertEqual(rows[0]["first_cross_pair_sum"], "0.95")


if __name__ == "__main__":
    unittest.main()
