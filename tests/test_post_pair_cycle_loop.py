import csv
import json
import tempfile
import unittest
from pathlib import Path

from polymarket_copytrader.post_pair_cycle_loop import (
    PostPairCycleLoopConfig,
    build_post_pair_cycle_loop_dataset,
)


class PostPairCycleLoopTests(unittest.TestCase):
    def test_build_post_pair_cycle_loop_dataset_tracks_next_cycle_start_and_completion(self) -> None:
        trades = [
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1_775_000_000_000,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 10.0,
                "usdc_size": 5.0,
                "transaction_hash": "0x1",
                "price": 0.50,
                "asset": "up-asset",
                "side": "BUY",
                "outcome_index": 0,
                "title": "Bitcoin Up or Down - April 1, 3PM ET",
                "slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "event_slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "outcome": "Up",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1_775_000_005_000,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 10.0,
                "usdc_size": 4.8,
                "transaction_hash": "0x2",
                "price": 0.48,
                "asset": "down-asset",
                "side": "BUY",
                "outcome_index": 1,
                "title": "Bitcoin Up or Down - April 1, 3PM ET",
                "slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "event_slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "outcome": "Down",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1_775_000_020_000,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 8.0,
                "usdc_size": 3.92,
                "transaction_hash": "0x3",
                "price": 0.49,
                "asset": "up-asset",
                "side": "BUY",
                "outcome_index": 0,
                "title": "Bitcoin Up or Down - April 1, 3PM ET",
                "slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "event_slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "outcome": "Up",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1_775_000_045_000,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 8.0,
                "usdc_size": 3.84,
                "transaction_hash": "0x4",
                "price": 0.48,
                "asset": "down-asset",
                "side": "BUY",
                "outcome_index": 1,
                "title": "Bitcoin Up or Down - April 1, 3PM ET",
                "slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "event_slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "outcome": "Down",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "blue-walnut.jsonl"
            output_csv = root / "cycle-loop.csv"
            summary_json = root / "summary.json"
            with input_path.open("w", encoding="utf-8") as handle:
                for trade in trades:
                    handle.write(json.dumps(trade, ensure_ascii=False) + "\n")

            summary = build_post_pair_cycle_loop_dataset(
                PostPairCycleLoopConfig(
                    input_trades_path=str(input_path),
                    output_csv_path=str(output_csv),
                    summary_json_path=str(summary_json),
                    families=("btc",),
                    durations=("hourly",),
                    cycle_start_horizon_seconds=180,
                    cycle_complete_horizon_seconds=60,
                )
            )

            self.assertEqual(summary.total_markets, 1)
            self.assertEqual(summary.markets_with_initial_pair, 1)
            self.assertEqual(summary.rows, 2)
            self.assertEqual(summary.rows_with_next_cycle_start, 1)
            self.assertEqual(summary.rows_with_next_cycle_complete, 1)
            self.assertEqual(summary.rows_with_next_cycle_complete_in_horizon, 1)

            with output_csv.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 2)
            first_row = rows[0]
            self.assertEqual(first_row["locked_unit_index"], "1")
            self.assertEqual(first_row["label_next_cycle_start"], "1")
            self.assertEqual(first_row["label_next_cycle_first_leg_outcome"], "Up")
            self.assertEqual(first_row["label_next_cycle_complete"], "1")
            self.assertEqual(first_row["label_next_cycle_complete_in_horizon"], "1")
            self.assertEqual(first_row["label_next_cycle_first_cross_pair_sum"], "0.97")

            second_row = rows[1]
            self.assertEqual(second_row["locked_unit_index"], "2")
            self.assertEqual(second_row["label_next_cycle_start"], "0")

            payload = json.loads(summary_json.read_text(encoding="utf-8"))
            self.assertEqual(payload["next_cycle_first_leg_side_counts"]["Up"], 1)


if __name__ == "__main__":
    unittest.main()
