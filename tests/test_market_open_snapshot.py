import csv
import json
import tempfile
import unittest
from pathlib import Path

from polymarket_copytrader.market_open_snapshot import (
    MarketOpenSnapshotConfig,
    build_market_open_snapshot_dataset,
)


class MarketOpenSnapshotTests(unittest.TestCase):
    def test_build_market_open_snapshot_dataset_writes_positive_and_universe_negative_rows(self) -> None:
        trades = [
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1_775_070_010_000,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 10.0,
                "usdc_size": 5.1,
                "transaction_hash": "0x1",
                "price": 0.51,
                "asset": "asset-up",
                "side": "BUY",
                "outcome_index": 0,
                "title": "Bitcoin Up or Down - April 1, 3PM ET",
                "slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "event_slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "outcome": "Up",
            }
        ]
        market_universe = [
            {
                "market_slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "event_slug": "bitcoin-up-or-down-april-1-2026-3pm-et",
                "condition_id": "cond-1",
                "title": "Bitcoin Up or Down - April 1, 3PM ET",
            },
            {
                "market_slug": "ethereum-up-or-down-april-1-2026-3pm-et",
                "event_slug": "ethereum-up-or-down-april-1-2026-3pm-et",
                "condition_id": "cond-2",
                "title": "Ethereum Up or Down - April 1, 3PM ET",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "blue-walnut.jsonl"
            market_universe_path = root / "markets.json"
            output_csv = root / "snapshots.csv"
            summary_json = root / "summary.json"

            with input_path.open("w", encoding="utf-8") as handle:
                for trade in trades:
                    handle.write(json.dumps(trade, ensure_ascii=False) + "\n")
            market_universe_path.write_text(json.dumps(market_universe, ensure_ascii=False), encoding="utf-8")

            summary = build_market_open_snapshot_dataset(
                MarketOpenSnapshotConfig(
                    input_trades_path=str(input_path),
                    output_csv_path=str(output_csv),
                    summary_json_path=str(summary_json),
                    market_universe_path=str(market_universe_path),
                    families=("btc", "eth"),
                    durations=("hourly",),
                    snapshot_offsets_seconds=(0, 10),
                    entry_horizon_seconds=60,
                )
            )

            self.assertEqual(summary.total_markets, 2)
            self.assertEqual(summary.touched_markets, 1)
            self.assertEqual(summary.untouched_markets, 1)
            self.assertEqual(summary.markets_with_first_leg, 1)
            self.assertEqual(summary.snapshot_rows, 4)
            self.assertEqual(summary.positive_label_rows, 2)

            with output_csv.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 4)
            btc_at_open = next(
                row
                for row in rows
                if row["market_slug"] == "bitcoin-up-or-down-april-1-2026-3pm-et"
                and row["snapshot_offset_seconds"] == "0"
            )
            self.assertEqual(btc_at_open["label_open_first_leg"], "1")
            self.assertEqual(btc_at_open["label_first_leg_side"], "Up")

            eth_at_open = next(
                row
                for row in rows
                if row["market_slug"] == "ethereum-up-or-down-april-1-2026-3pm-et"
                and row["snapshot_offset_seconds"] == "0"
            )
            self.assertEqual(eth_at_open["realized_market_traded_by_target"], "0")
            self.assertEqual(eth_at_open["label_open_first_leg"], "0")


if __name__ == "__main__":
    unittest.main()
