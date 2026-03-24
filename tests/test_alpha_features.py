import csv
import json
import tempfile
import unittest
from pathlib import Path

from polymarket_copytrader.alpha_features import AlphaFeatureConfig, build_alpha_feature_dataset


class AlphaFeatureTests(unittest.TestCase):
    def test_build_alpha_feature_dataset_writes_csv_and_summary(self) -> None:
        trades = [
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1000,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 10.0,
                "usdc_size": 6.0,
                "transaction_hash": "0x1",
                "price": 0.6,
                "asset": "asset-up",
                "side": "BUY",
                "outcome_index": 0,
                "title": "Bitcoin Up or Down - March 22, 1:00PM-1:05PM ET",
                "slug": "btc-updown-5m-1774174800",
                "event_slug": "btc-updown-5m-1774174800",
                "outcome": "Up",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1010,
                "condition_id": "cond-2",
                "activity_type": "TRADE",
                "size": 12.0,
                "usdc_size": 8.4,
                "transaction_hash": "0x2",
                "price": 0.7,
                "asset": "asset-eth-down",
                "side": "BUY",
                "outcome_index": 1,
                "title": "Ethereum Up or Down - March 22, 1:00PM-1:15PM ET",
                "slug": "eth-updown-15m-1774174800",
                "event_slug": "eth-updown-15m-1774174800",
                "outcome": "Down",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1300,
                "condition_id": "cond-1",
                "activity_type": "TRADE",
                "size": 8.0,
                "usdc_size": 3.2,
                "transaction_hash": "0x3",
                "price": 0.4,
                "asset": "asset-down",
                "side": "BUY",
                "outcome_index": 1,
                "title": "Bitcoin Up or Down - March 22, 1:00PM-1:05PM ET",
                "slug": "btc-updown-5m-1774174800",
                "event_slug": "btc-updown-5m-1774174800",
                "outcome": "Down",
            },
            {
                "proxy_wallet": "0xabc",
                "timestamp_ms": 1400,
                "condition_id": "cond-3",
                "activity_type": "TRADE",
                "size": 5.0,
                "usdc_size": 2.5,
                "transaction_hash": "0x4",
                "price": 0.5,
                "asset": "asset-sol",
                "side": "BUY",
                "outcome_index": 0,
                "title": "Solana Up or Down - March 22, 1PM ET",
                "slug": "solana-up-or-down-march-22-2026-1pm-et",
                "event_slug": "solana-up-or-down-march-22-2026-1pm-et",
                "outcome": "Up",
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            input_path = root / "guh123.jsonl"
            output_csv = root / "features.csv"
            summary_json = root / "summary.json"
            prices_dir = root / "prices"
            prices_dir.mkdir(parents=True, exist_ok=True)

            with input_path.open("w", encoding="utf-8") as handle:
                for trade in trades:
                    handle.write(json.dumps(trade, ensure_ascii=False) + "\n")

            (prices_dir / "asset-up.json").write_text(
                json.dumps(
                    [
                        {"timestamp_seconds": 940, "price": 0.55},
                        {"timestamp_seconds": 1000, "price": 0.6},
                    ]
                ),
                encoding="utf-8",
            )
            (prices_dir / "asset-down.json").write_text(
                json.dumps(
                    [
                        {"timestamp_seconds": 940, "price": 0.45},
                        {"timestamp_seconds": 1000, "price": 0.4},
                        {"timestamp_seconds": 1300, "price": 0.4},
                    ]
                ),
                encoding="utf-8",
            )
            (prices_dir / "asset-eth-down.json").write_text(
                json.dumps(
                    [
                        {"timestamp_seconds": 950, "price": 0.68},
                        {"timestamp_seconds": 1010, "price": 0.7},
                    ]
                ),
                encoding="utf-8",
            )
            market_dir = root / "market_data"
            market_dir.mkdir(parents=True, exist_ok=True)
            (market_dir / "btc.csv").write_text(
                "\n".join(
                    [
                        "timestamp_seconds,close,volume,trade_count",
                        "940,100,1000,10",
                        "970,105,1100,11",
                        "1000,110,1200,12",
                        "1300,120,1500,15",
                    ]
                ),
                encoding="utf-8",
            )
            (market_dir / "eth.csv").write_text(
                "\n".join(
                    [
                        "timestamp_seconds,close,volume,trade_count",
                        "950,200,2000,20",
                        "980,205,2100,21",
                        "1010,210,2200,22",
                    ]
                ),
                encoding="utf-8",
            )

            summary = build_alpha_feature_dataset(
                AlphaFeatureConfig(
                    input_trades_path=str(input_path),
                    output_csv_path=str(output_csv),
                    summary_json_path=str(summary_json),
                    price_cache_dir=str(prices_dir),
                    external_market_data_dir=str(market_dir),
                    families=("btc", "eth"),
                    durations=("5m", "15m"),
                    include_negative_samples=True,
                    negative_window_seconds=60,
                )
            )

            self.assertEqual(summary.filtered_trades, 3)
            self.assertEqual(summary.positive_rows, 3)
            self.assertEqual(summary.negative_rows, 3)

            with output_csv.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(len(rows), 6)
            second_positive = next(
                row
                for row in rows
                if row["transaction_hash"] == "0x2" and row["source_kind"] == "positive_trade"
            )
            self.assertEqual(second_positive["recent_trade_count_60s"], "1")
            self.assertEqual(second_positive["market_family"], "eth")
            self.assertEqual(second_positive["external_market_close"], "210.0")
            self.assertEqual(second_positive["external_market_trade_count_60s"], "63.0")

            first_negative = next(
                row
                for row in rows
                if row["transaction_hash"] == "0x1"
                and row["source_kind"] == "paired_opposite_negative"
            )
            self.assertEqual(first_negative["candidate_outcome"], "Down")
            self.assertEqual(first_negative["candidate_asset"], "asset-down")
            self.assertEqual(first_negative["candidate_price"], "0.4")
            self.assertEqual(first_negative["external_market_volume_60s"], "3300.0")

            summary_payload = json.loads(summary_json.read_text(encoding="utf-8"))
            self.assertEqual(summary_payload["total_rows"], 6)


if __name__ == "__main__":
    unittest.main()
