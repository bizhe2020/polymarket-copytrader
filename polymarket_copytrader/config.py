from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from .models import (
    AppConfig,
    EvalConfig,
    EvalDataConfig,
    EvalOutputConfig,
    EvalPortfolioConfig,
    EvalRuntimeConfig,
    EvalScenarioConfig,
    EvalStrategyConfig,
    EvalTargetConfig,
    ExecutionConfig,
    RuntimeConfig,
    StrategyConfig,
    TargetConfig,
)


def _read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_config(path: str) -> AppConfig:
    raw = _read_json(Path(path))
    return AppConfig(
        target=TargetConfig(
            profile=raw["target"].get("profile"),
            wallet=raw["target"].get("wallet"),
        ),
        runtime=RuntimeConfig(
            poll_interval_seconds=float(raw["runtime"]["poll_interval_seconds"]),
            request_timeout_seconds=float(raw["runtime"]["request_timeout_seconds"]),
            activity_limit=int(raw["runtime"]["activity_limit"]),
            lookback_seconds_on_start=int(raw["runtime"]["lookback_seconds_on_start"]),
            requery_overlap_seconds=int(raw["runtime"]["requery_overlap_seconds"]),
            state_path=str(raw["runtime"]["state_path"]),
            event_log_path=str(raw["runtime"]["event_log_path"]),
            market_websocket_enabled=bool(raw["runtime"].get("market_websocket_enabled", True)),
            market_websocket_warmup_ms=int(raw["runtime"].get("market_websocket_warmup_ms", 350)),
        ),
        strategy=StrategyConfig(
            follow_fraction=float(raw["strategy"]["follow_fraction"]),
            fixed_order_usdc=(
                None
                if raw["strategy"].get("fixed_order_usdc") is None
                else float(raw["strategy"]["fixed_order_usdc"])
            ),
            min_target_usdc_size=float(raw["strategy"]["min_target_usdc_size"]),
            max_follow_price=float(raw["strategy"]["max_follow_price"]),
            max_slippage_bps=float(raw["strategy"]["max_slippage_bps"]),
            max_position_per_asset_usdc=float(
                raw["strategy"]["max_position_per_asset_usdc"]
            ),
            min_order_usdc=float(raw["strategy"]["min_order_usdc"]),
            skip_outcomes=list(raw["strategy"].get("skip_outcomes", [])),
            skip_market_slugs=list(raw["strategy"].get("skip_market_slugs", [])),
            execution_policy=str(raw["strategy"].get("execution_policy", "IOC")),
            aggressive_price_enabled=bool(raw["strategy"].get("aggressive_price_enabled", False)),
            aggressive_price_max_ticks=int(raw["strategy"].get("aggressive_price_max_ticks", 0)),
            max_trade_age_seconds=(
                None
                if raw["strategy"].get("max_trade_age_seconds") is None
                else float(raw["strategy"]["max_trade_age_seconds"])
            ),
            short_market_max_trade_age_seconds=(
                None
                if raw["strategy"].get("short_market_max_trade_age_seconds") is None
                else float(raw["strategy"]["short_market_max_trade_age_seconds"])
            ),
        ),
        execution=ExecutionConfig(
            mode=str(raw["execution"]["mode"]),
            host=str(raw["execution"]["host"]),
            chain_id=int(raw["execution"]["chain_id"]),
            signature_type=int(raw["execution"]["signature_type"]),
            private_key_env=str(raw["execution"]["private_key_env"]),
            funder_env=str(raw["execution"]["funder_env"]),
        ),
    )


def load_eval_config(path: str) -> EvalConfig:
    raw = _read_json(Path(path))
    return EvalConfig(
        targets=[
            EvalTargetConfig(
                name=str(item["name"]),
                profile=item.get("profile"),
                wallet=item.get("wallet"),
                weight=float(item.get("weight", 1.0)),
            )
            for item in raw["targets"]
        ],
        runtime=EvalRuntimeConfig(
            request_timeout_seconds=float(raw["runtime"]["request_timeout_seconds"]),
            cache_dir=str(raw["runtime"]["cache_dir"]),
        ),
        portfolio=EvalPortfolioConfig(
            initial_capital_usdc=float(raw["portfolio"]["initial_capital_usdc"]),
            standalone_initial_capital_usdc=float(
                raw["portfolio"]["standalone_initial_capital_usdc"]
            ),
        ),
        data=EvalDataConfig(
            recent_days=int(raw["data"]["recent_days"]),
            activity_page_size=int(raw["data"]["activity_page_size"]),
            price_fidelity_minutes=int(raw["data"]["price_fidelity_minutes"]),
            max_trades_per_target=(
                None
                if raw["data"].get("max_trades_per_target") is None
                else int(raw["data"]["max_trades_per_target"])
            ),
        ),
        strategy=EvalStrategyConfig(
            follow_fraction=float(raw["strategy"]["follow_fraction"]),
            max_order_usdc=float(raw["strategy"]["max_order_usdc"]),
            min_target_usdc_size=float(raw["strategy"]["min_target_usdc_size"]),
            min_order_usdc=float(raw["strategy"]["min_order_usdc"]),
            max_position_per_asset_usdc=float(
                raw["strategy"]["max_position_per_asset_usdc"]
            ),
            allow_sell=bool(raw["strategy"]["allow_sell"]),
            exit_mode=str(raw["strategy"].get("exit_mode", "redeem")),
        ),
        scenarios=[
            EvalScenarioConfig(
                name=str(item["name"]),
                observation_delay_seconds=int(item["observation_delay_seconds"]),
                extra_slippage_bps=float(item["extra_slippage_bps"]),
                execution_policy=str(item.get("execution_policy", "FOK")),
                synthetic_levels=int(item.get("synthetic_levels", 5)),
                synthetic_depth_multiplier=float(item.get("synthetic_depth_multiplier", 1.0)),
                use_target_trade_price=bool(item.get("use_target_trade_price", False)),
            )
            for item in raw["scenarios"]
        ],
        output=EvalOutputConfig(
            summary_path=str(raw["output"]["summary_path"]),
            curve_path=str(raw["output"]["curve_path"]),
            fills_path=str(raw["output"]["fills_path"]),
        ),
    )
