import unittest

from polymarket_copytrader.api import extract_market_tokens_with_outcomes
from polymarket_copytrader.pair_live_paper import (
    _market_duration_bucket_from_text,
    _market_family_from_text,
    compute_opposite_leg_price_limit,
    compute_pair_execution_plan,
    compute_rebalanced_effective_pair_sum,
    select_first_leg_candidate,
)


class PairLivePaperTests(unittest.TestCase):
    def test_extract_market_tokens_with_outcomes_from_parallel_lists(self) -> None:
        market = {
            "outcomes": '["Up","Down"]',
            "clobTokenIds": '["token-up","token-down"]',
        }
        mapping = extract_market_tokens_with_outcomes(market)
        self.assertEqual(mapping, {"Up": "token-up", "Down": "token-down"})

    def test_compute_pair_execution_plan_below_threshold(self) -> None:
        plan = compute_pair_execution_plan(
            up_ask=0.48,
            down_ask=0.46,
            stake_usdc=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            max_effective_pair_sum=1.0,
        )
        assert plan is not None
        self.assertAlmostEqual(plan.raw_pair_sum, 0.94, places=6)
        self.assertAlmostEqual(plan.share_count, 106.3829787234, places=6)
        self.assertAlmostEqual(plan.locked_pnl_usdc, 6.3829787234, places=6)

    def test_compute_pair_execution_plan_above_threshold(self) -> None:
        plan = compute_pair_execution_plan(
            up_ask=0.55,
            down_ask=0.48,
            stake_usdc=100.0,
            fee_bps=0.0,
            slippage_bps=0.0,
            max_effective_pair_sum=1.0,
        )
        self.assertIsNone(plan)

    def test_market_family_from_text(self) -> None:
        self.assertEqual(_market_family_from_text("Ethereum Up or Down - March 25"), "eth")
        self.assertEqual(_market_family_from_text("xrp-updown-15m-1774459800"), "xrp")

    def test_market_duration_bucket_from_text(self) -> None:
        self.assertEqual(
            _market_duration_bucket_from_text("Ethereum Up or Down - March 25, 2:15PM-2:30PM ET"),
            "15m",
        )
        self.assertEqual(
            _market_duration_bucket_from_text("Bitcoin Up or Down - March 25, 3PM ET"),
            "hourly",
        )

    def test_compute_opposite_leg_price_limit(self) -> None:
        limit = compute_opposite_leg_price_limit(
            first_leg_price=0.47,
            fee_bps=10.0,
            slippage_bps=50.0,
            max_effective_pair_sum=0.995,
        )
        self.assertAlmostEqual(limit, 0.519066, places=5)

    def test_select_first_leg_candidate_prefers_requested_outcome(self) -> None:
        candidate = select_first_leg_candidate(
            up_ask=0.51,
            down_ask=0.49,
            up_asset="up-token",
            down_asset="down-token",
            up_outcome="Up",
            down_outcome="Down",
            up_book_source="market_ws",
            down_book_source="market_ws",
            preferred_outcome="Down",
            price_floor=0.4,
            price_ceiling=0.6,
        )
        assert candidate is not None
        self.assertEqual(candidate.outcome, "Down")
        self.assertEqual(candidate.asset, "down-token")

    def test_select_first_leg_candidate_falls_back_to_nearest_mid(self) -> None:
        candidate = select_first_leg_candidate(
            up_ask=0.53,
            down_ask=0.46,
            up_asset="up-token",
            down_asset="down-token",
            up_outcome="Up",
            down_outcome="Down",
            up_book_source="rest_book",
            down_book_source="market_ws",
            preferred_outcome=None,
            price_floor=0.4,
            price_ceiling=0.6,
        )
        assert candidate is not None
        self.assertEqual(candidate.outcome, "Up")
        self.assertEqual(candidate.book_source, "rest_book")

    def test_compute_rebalanced_effective_pair_sum(self) -> None:
        new_sum = compute_rebalanced_effective_pair_sum(
            current_cost_usdc=100.0,
            current_share_count=103.0,
            additional_cost_usdc=100.0,
            additional_share_count=110.0,
        )
        self.assertAlmostEqual(new_sum or 0.0, 0.938967, places=5)


if __name__ == "__main__":
    unittest.main()
