import unittest

import pandas as pd

from polymarket_copytrader.signal_price_cache import _build_asset_requests


class SignalPriceCacheTests(unittest.TestCase):
    def test_build_asset_requests_merges_asset_ranges(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "candidate_asset": "asset_a",
                    "timestamp_seconds": 100,
                    "seconds_to_resolution": 60,
                },
                {
                    "candidate_asset": "asset_a",
                    "timestamp_seconds": 160,
                    "resolution_timestamp_seconds": 260,
                },
                {
                    "candidate_asset": "asset_b",
                    "timestamp_seconds": 200,
                    "seconds_to_resolution": 30,
                },
            ]
        )

        requests = _build_asset_requests(
            frame=frame,
            asset_column="candidate_asset",
            timestamp_column="timestamp_seconds",
            resolution_column="resolution_timestamp_seconds",
            seconds_to_resolution_column="seconds_to_resolution",
            lookback_padding_seconds=10,
            forward_padding_seconds=20,
        )

        self.assertEqual(set(requests.keys()), {"asset_a", "asset_b"})
        self.assertEqual(requests["asset_a"]["start_ts"], 90)
        self.assertEqual(requests["asset_a"]["end_ts"], 280)
        self.assertEqual(requests["asset_b"]["start_ts"], 190)
        self.assertEqual(requests["asset_b"]["end_ts"], 250)


if __name__ == "__main__":
    unittest.main()
