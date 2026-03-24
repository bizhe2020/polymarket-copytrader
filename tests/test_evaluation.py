import json
import tempfile
import unittest
from dataclasses import asdict
from pathlib import Path

from polymarket_copytrader.evaluation import (
    EvaluationApp,
    _infer_resolution_timestamp_seconds,
    simulate_minute_portfolio,
)
from polymarket_copytrader.models import (
    EvalConfig,
    EvalDataConfig,
    EvalOutputConfig,
    EvalPortfolioConfig,
    EvalRuntimeConfig,
    EvalSignal,
    EvalStrategyConfig,
    EvalTargetConfig,
    PriceHistoryPoint,
    ResolvedTarget,
    TradeActivity,
)


class EvaluationTests(unittest.TestCase):
    def test_minute_portfolio_handles_buy_and_sell(self) -> None:
        buy_trade = TradeActivity(
            proxy_wallet="0x1",
            timestamp_ms=120,
            condition_id="c1",
            activity_type="TRADE",
            size=100.0,
            usdc_size=50.0,
            transaction_hash="0xbuy",
            price=0.5,
            asset="asset-1",
            side="BUY",
            outcome_index=0,
            title="Title",
            slug="slug",
            event_slug="slug",
            outcome="YES",
        )
        sell_trade = TradeActivity(
            proxy_wallet="0x1",
            timestamp_ms=180,
            condition_id="c1",
            activity_type="TRADE",
            size=100.0,
            usdc_size=60.0,
            transaction_hash="0xsell",
            price=0.6,
            asset="asset-1",
            side="SELL",
            outcome_index=0,
            title="Title",
            slug="slug",
            event_slug="slug",
            outcome="YES",
        )
        signals = [
            EvalSignal(
                scenario_name="target_reference",
                target_name="alpha",
                wallet="0x1",
                trade=buy_trade,
                execution_timestamp_seconds=180,
            ),
            EvalSignal(
                scenario_name="target_reference",
                target_name="alpha",
                wallet="0x1",
                trade=sell_trade,
                execution_timestamp_seconds=240,
            ),
        ]
        history = {
            "asset-1": [
                PriceHistoryPoint(timestamp_seconds=180, price=0.5),
                PriceHistoryPoint(timestamp_seconds=240, price=0.6),
            ]
        }

        result = simulate_minute_portfolio(
            scenario_name="target_reference",
            portfolio_name="standalone_alpha",
            initial_cash_by_target={"alpha": 1000.0},
            signals=signals,
            price_histories=history,
            follow_fraction=1.0,
            max_order_usdc=100.0,
            min_target_usdc_size=5.0,
            min_order_usdc=5.0,
            max_position_per_asset_usdc=500.0,
            allow_sell=True,
            exit_mode="mark_to_market",
            extra_slippage_bps=0.0,
            execution_policy="FOK",
            synthetic_levels=5,
            synthetic_depth_multiplier=5.0,
            use_target_trade_price=True,
        )

        self.assertEqual(result.summary.fills, 2)
        self.assertGreater(result.summary.final_equity_usdc, 1000.0)
        self.assertEqual(result.summary.scenario_name, "target_reference")

    def test_minute_portfolio_counts_ioc_partial_fill(self) -> None:
        buy_trade = TradeActivity(
            proxy_wallet="0x1",
            timestamp_ms=120,
            condition_id="c1",
            activity_type="TRADE",
            size=10.0,
            usdc_size=50.0,
            transaction_hash="0xbuy",
            price=0.5,
            asset="asset-1",
            side="BUY",
            outcome_index=0,
            title="Title",
            slug="slug",
            event_slug="slug",
            outcome="YES",
        )
        signals = [
            EvalSignal(
                scenario_name="cn_vpn_base",
                target_name="alpha",
                wallet="0x1",
                trade=buy_trade,
                execution_timestamp_seconds=180,
            )
        ]
        history = {"asset-1": [PriceHistoryPoint(timestamp_seconds=180, price=0.5)]}
        result = simulate_minute_portfolio(
            scenario_name="cn_vpn_base",
            portfolio_name="standalone_alpha",
            initial_cash_by_target={"alpha": 1000.0},
            signals=signals,
            price_histories=history,
            follow_fraction=1.0,
            max_order_usdc=100.0,
            min_target_usdc_size=5.0,
            min_order_usdc=5.0,
            max_position_per_asset_usdc=500.0,
            allow_sell=True,
            exit_mode="mark_to_market",
            extra_slippage_bps=0.0,
            execution_policy="IOC",
            synthetic_levels=1,
            synthetic_depth_multiplier=0.2,
            use_target_trade_price=False,
        )
        self.assertEqual(result.summary.partial_fills, 1)

    def test_minute_portfolio_redeems_winning_token(self) -> None:
        buy_trade = TradeActivity(
            proxy_wallet="0x1",
            timestamp_ms=1774158210,
            condition_id="c1",
            activity_type="TRADE",
            size=10.0,
            usdc_size=4.0,
            transaction_hash="0xbuy",
            price=0.4,
            asset="asset-up",
            side="BUY",
            outcome_index=0,
            title="Ethereum Up or Down - March 22, 1:30AM-1:45AM ET",
            slug="eth-updown-15m-1774157400",
            event_slug="eth-updown-15m-1774157400",
            outcome="Up",
        )
        signals = [
            EvalSignal(
                scenario_name="redeem_case",
                target_name="alpha",
                wallet="0x1",
                trade=buy_trade,
                execution_timestamp_seconds=1774158210,
            )
        ]
        history = {
            "asset-up": [
                PriceHistoryPoint(timestamp_seconds=1774158200, price=0.4),
                PriceHistoryPoint(timestamp_seconds=1774158300, price=0.45),
                PriceHistoryPoint(timestamp_seconds=1774158360, price=0.99),
            ]
        }
        result = simulate_minute_portfolio(
            scenario_name="redeem_case",
            portfolio_name="standalone_alpha",
            initial_cash_by_target={"alpha": 1000.0},
            signals=signals,
            price_histories=history,
            follow_fraction=1.0,
            max_order_usdc=100.0,
            min_target_usdc_size=1.0,
            min_order_usdc=1.0,
            max_position_per_asset_usdc=500.0,
            allow_sell=True,
            exit_mode="redeem",
            extra_slippage_bps=0.0,
            execution_policy="FOK",
            synthetic_levels=5,
            synthetic_depth_multiplier=5.0,
            use_target_trade_price=True,
        )

        self.assertAlmostEqual(result.summary.final_equity_usdc, 1005.997001, places=6)

    def test_infer_resolution_timestamp_for_15m_market(self) -> None:
        trade = TradeActivity(
            proxy_wallet="0x1",
            timestamp_ms=1774158210,
            condition_id="c1",
            activity_type="TRADE",
            size=10.0,
            usdc_size=4.0,
            transaction_hash="0xbuy",
            price=0.4,
            asset="asset-up",
            side="BUY",
            outcome_index=0,
            title="Ethereum Up or Down - March 22, 1:30AM-1:45AM ET",
            slug="eth-updown-15m-1774157400",
            event_slug="eth-updown-15m-1774157400",
            outcome="Up",
        )
        self.assertEqual(_infer_resolution_timestamp_seconds(trade), 1774158300)

    def test_infer_resolution_timestamp_for_calendar_slug(self) -> None:
        trade = TradeActivity(
            proxy_wallet="0x1",
            timestamp_ms=1774200000,
            condition_id="c1",
            activity_type="TRADE",
            size=10.0,
            usdc_size=4.0,
            transaction_hash="0xbuy",
            price=0.59,
            asset="asset-up",
            side="BUY",
            outcome_index=0,
            title="Bitcoin Up or Down - March 22, 2PM ET",
            slug="bitcoin-up-or-down-march-22-2026-2pm-et",
            event_slug="bitcoin-up-or-down-march-22-2026-2pm-et",
            outcome="Up",
        )

        self.assertEqual(_infer_resolution_timestamp_seconds(trade), 1774206000)

    def test_infer_resolution_timestamp_for_calendar_slug_without_year(self) -> None:
        trade = TradeActivity(
            proxy_wallet="0x1",
            timestamp_ms=1769806824,
            condition_id="c1",
            activity_type="TRADE",
            size=10.0,
            usdc_size=5.3,
            transaction_hash="0xbuy",
            price=0.53,
            asset="asset-up",
            side="BUY",
            outcome_index=0,
            title="Bitcoin Up or Down - January 30, 4PM ET",
            slug="bitcoin-up-or-down-january-30-4pm-et",
            event_slug="bitcoin-up-or-down-january-30-4pm-et",
            outcome="Up",
        )

        self.assertEqual(_infer_resolution_timestamp_seconds(trade), 1769810400)

    def test_fetch_recent_trades_falls_back_to_cache(self) -> None:
        trade = TradeActivity(
            proxy_wallet="0x1",
            timestamp_ms=180,
            condition_id="c1",
            activity_type="TRADE",
            size=10.0,
            usdc_size=5.0,
            transaction_hash="0xcache",
            price=0.5,
            asset="asset-1",
            side="BUY",
            outcome_index=0,
            title="Title",
            slug="slug",
            event_slug="slug",
            outcome="YES",
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            trades_dir = cache_dir / "trades"
            trades_dir.mkdir(parents=True, exist_ok=True)
            with (trades_dir / "alpha.jsonl").open("w", encoding="utf-8") as handle:
                handle.write(json.dumps(asdict(trade), ensure_ascii=False) + "\n")

            app = EvaluationApp(
                EvalConfig(
                    targets=[
                        EvalTargetConfig(
                            name="alpha",
                            profile=None,
                            wallet="0x1",
                            weight=1.0,
                        )
                    ],
                    runtime=EvalRuntimeConfig(
                        request_timeout_seconds=0.1,
                        cache_dir=str(cache_dir),
                    ),
                    portfolio=EvalPortfolioConfig(
                        initial_capital_usdc=1000.0,
                        standalone_initial_capital_usdc=1000.0,
                    ),
                    data=EvalDataConfig(
                        recent_days=7,
                        activity_page_size=100,
                        price_fidelity_minutes=1,
                    ),
                    strategy=EvalStrategyConfig(
                        follow_fraction=1.0,
                        max_order_usdc=100.0,
                        min_target_usdc_size=5.0,
                        min_order_usdc=5.0,
                        max_position_per_asset_usdc=500.0,
                        allow_sell=True,
                        exit_mode="redeem",
                    ),
                    scenarios=[],
                    output=EvalOutputConfig(
                        summary_path=str(cache_dir / "summary.json"),
                        curve_path=str(cache_dir / "curve.csv"),
                        fills_path=str(cache_dir / "fills.jsonl"),
                    ),
                )
            )

            def fail_get_activity(**_: object) -> list[TradeActivity]:
                raise RuntimeError("offline")

            app.api.get_activity = fail_get_activity  # type: ignore[method-assign]
            targets = [
                ResolvedTarget(
                    name="alpha",
                    profile=None,
                    wallet="0x1",
                    weight=1.0,
                    details={},
                )
            ]

            trades_by_target = app._fetch_recent_trades(
                targets=targets,
                cutoff_seconds=100,
                now_seconds=300,
            )

            self.assertEqual(len(trades_by_target["alpha"]), 1)
            self.assertEqual(trades_by_target["alpha"][0].transaction_hash, "0xcache")
            self.assertTrue(
                any(note["source"] == "cache_fallback" for note in app.runtime_notes)
            )

    def test_fetch_recent_trades_applies_max_trade_cap(self) -> None:
        trades = [
            TradeActivity(
                proxy_wallet="0x1",
                timestamp_ms=100 + idx,
                condition_id="c1",
                activity_type="TRADE",
                size=10.0,
                usdc_size=5.0,
                transaction_hash=f"0x{idx}",
                price=0.5,
                asset="asset-1",
                side="BUY",
                outcome_index=0,
                title="Title",
                slug="slug",
                event_slug="slug",
                outcome="YES",
            )
            for idx in range(5)
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            app = EvaluationApp(
                EvalConfig(
                    targets=[
                        EvalTargetConfig(
                            name="alpha",
                            profile=None,
                            wallet="0x1",
                            weight=1.0,
                        )
                    ],
                    runtime=EvalRuntimeConfig(
                        request_timeout_seconds=0.1,
                        cache_dir=str(Path(tmpdir) / "cache"),
                    ),
                    portfolio=EvalPortfolioConfig(
                        initial_capital_usdc=1000.0,
                        standalone_initial_capital_usdc=1000.0,
                    ),
                    data=EvalDataConfig(
                        recent_days=7,
                        activity_page_size=100,
                        price_fidelity_minutes=1,
                        max_trades_per_target=3,
                    ),
                    strategy=EvalStrategyConfig(
                        follow_fraction=1.0,
                        max_order_usdc=100.0,
                        min_target_usdc_size=5.0,
                        min_order_usdc=5.0,
                        max_position_per_asset_usdc=500.0,
                        allow_sell=False,
                        exit_mode="redeem",
                    ),
                    scenarios=[],
                    output=EvalOutputConfig(
                        summary_path=str(Path(tmpdir) / "summary.json"),
                        curve_path=str(Path(tmpdir) / "curve.csv"),
                        fills_path=str(Path(tmpdir) / "fills.jsonl"),
                    ),
                )
            )
            app._fetch_recent_trades_for_wallet = (  # type: ignore[method-assign]
                lambda target_name, wallet, cutoff_seconds, now_seconds, cache_path, progress_path: trades
            )
            targets = [
                ResolvedTarget(
                    name="alpha",
                    profile=None,
                    wallet="0x1",
                    weight=1.0,
                    details={},
                )
            ]
            got = app._fetch_recent_trades(targets=targets, cutoff_seconds=0, now_seconds=999)
            self.assertEqual([t.transaction_hash for t in got["alpha"]], ["0x2", "0x3", "0x4"])

    def test_trade_fetch_resume_uses_progress_and_partial_cache(self) -> None:
        page_one = [
            TradeActivity(
                proxy_wallet="0x1",
                timestamp_ms=100,
                condition_id="c1",
                activity_type="TRADE",
                size=10.0,
                usdc_size=5.0,
                transaction_hash="0x1",
                price=0.5,
                asset="asset-1",
                side="BUY",
                outcome_index=0,
                title="Title",
                slug="slug",
                event_slug="slug",
                outcome="YES",
            ),
            TradeActivity(
                proxy_wallet="0x1",
                timestamp_ms=101,
                condition_id="c1",
                activity_type="TRADE",
                size=10.0,
                usdc_size=5.0,
                transaction_hash="0x2",
                price=0.5,
                asset="asset-1",
                side="BUY",
                outcome_index=0,
                title="Title",
                slug="slug",
                event_slug="slug",
                outcome="YES",
            ),
        ]
        page_two = [
            TradeActivity(
                proxy_wallet="0x1",
                timestamp_ms=102,
                condition_id="c1",
                activity_type="TRADE",
                size=10.0,
                usdc_size=5.0,
                transaction_hash="0x3",
                price=0.5,
                asset="asset-1",
                side="BUY",
                outcome_index=0,
                title="Title",
                slug="slug",
                event_slug="slug",
                outcome="YES",
            )
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache"
            app = EvaluationApp(
                EvalConfig(
                    targets=[
                        EvalTargetConfig(
                            name="alpha",
                            profile=None,
                            wallet="0x1",
                            weight=1.0,
                        )
                    ],
                    runtime=EvalRuntimeConfig(
                        request_timeout_seconds=0.1,
                        cache_dir=str(cache_dir),
                    ),
                    portfolio=EvalPortfolioConfig(
                        initial_capital_usdc=1000.0,
                        standalone_initial_capital_usdc=1000.0,
                    ),
                    data=EvalDataConfig(
                        recent_days=7,
                        activity_page_size=2,
                        price_fidelity_minutes=1,
                    ),
                    strategy=EvalStrategyConfig(
                        follow_fraction=1.0,
                        max_order_usdc=100.0,
                        min_target_usdc_size=5.0,
                        min_order_usdc=5.0,
                        max_position_per_asset_usdc=500.0,
                        allow_sell=False,
                        exit_mode="redeem",
                    ),
                    scenarios=[],
                    output=EvalOutputConfig(
                        summary_path=str(cache_dir / "summary.json"),
                        curve_path=str(cache_dir / "curve.csv"),
                        fills_path=str(cache_dir / "fills.jsonl"),
                    ),
                )
            )
            calls = {"count": 0}

            def first_pass(**_: object) -> list[TradeActivity]:
                calls["count"] += 1
                if calls["count"] == 1:
                    return page_one
                raise RuntimeError("interrupted")

            app.api.get_activity = first_pass  # type: ignore[method-assign]
            targets = [
                ResolvedTarget(
                    name="alpha",
                    profile=None,
                    wallet="0x1",
                    weight=1.0,
                    details={},
                )
            ]

            partial = app._fetch_recent_trades(targets=targets, cutoff_seconds=0, now_seconds=200)
            self.assertEqual([t.transaction_hash for t in partial["alpha"]], ["0x1", "0x2"])

            progress_path = cache_dir / "progress" / "trades" / "alpha.json"
            self.assertTrue(progress_path.exists())
            with progress_path.open("r", encoding="utf-8") as handle:
                progress = json.load(handle)
            self.assertEqual(progress["status"], "running")

            def second_pass(**kwargs: object) -> list[TradeActivity]:
                if int(kwargs["start_ms"]) >= 102:
                    return page_two
                return []

            app.api.get_activity = second_pass  # type: ignore[method-assign]
            resumed = app._fetch_recent_trades(targets=targets, cutoff_seconds=0, now_seconds=200)
            self.assertEqual([t.transaction_hash for t in resumed["alpha"]], ["0x1", "0x2", "0x3"])
            with progress_path.open("r", encoding="utf-8") as handle:
                progress = json.load(handle)
            self.assertEqual(progress["status"], "completed")


if __name__ == "__main__":
    unittest.main()
