from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, List

from .alpha_baseline import (
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
from .bridge_cycle_analysis import BridgeCycleAnalysisConfig, build_bridge_cycle_analysis
from .bridge_cycle_trigger_analysis import (
    BridgeCycleTriggerAnalysisConfig,
    build_bridge_cycle_trigger_analysis,
)
from .btc_family_default_strategy_v0 import (
    BtcFamilyDefaultStrategyV0Config,
    build_btc_family_default_strategy_v0,
)
from .alpha_features import AlphaFeatureConfig, build_alpha_feature_dataset
from .alpha_outcome import (
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
from .evaluation import build_evaluation_app
from .entry_ruleset_v1 import EntryRulesetV1Config, build_entry_ruleset_v1
from .entry_hard_lock_explanation import (
    EntryHardLockExplanationConfig,
    build_entry_hard_lock_explanation,
)
from .entry_wait_budget_mapping import (
    EntryWaitBudgetMappingConfig,
    build_entry_wait_budget_mapping,
)
from .eth_sol_override_strategies_v0 import (
    EthSolOverrideStrategiesV0Config,
    build_eth_sol_override_strategies_v0,
)
from .first_leg_followup import FirstLegFollowupConfig, build_first_leg_followup_dataset
from .first_leg_completion_baseline import (
    FirstLegCompletionBaselineConfig,
    run_first_leg_completion_baseline,
)
from .first_leg_family_hazard import (
    FirstLegFamilyHazardConfig,
    build_first_leg_family_hazard_analysis,
)
from .first_leg_policy_hypothesis import (
    FirstLegPolicyHypothesisConfig,
    build_first_leg_policy_hypothesis,
)
from .first_cycle_outcome_analysis import (
    FirstCycleOutcomeAnalysisConfig,
    build_first_cycle_outcome_analysis,
)
from .first_leg_trigger_rule import FirstLegTriggerRuleConfig, build_first_leg_trigger_rule_summary
from .first_leg_ruleset import FirstLegRulesetConfig, build_first_leg_ruleset
from .first_leg_wait_budget import FirstLegWaitBudgetConfig, build_first_leg_wait_budget
from .first_leg_regime_analysis import FirstLegRegimeAnalysisConfig, build_first_leg_regime_analysis
from .follower import build_app
from .live_paper import build_live_paper_app
from .market_open_snapshot import MarketOpenSnapshotConfig, build_market_open_snapshot_dataset
from .models import TradeActivity
from .pair_analysis import (
    PairAnalysisConfig,
    PairPaperReplayConfig,
    PairSequenceAnalysisConfig,
    run_pair_analysis,
    run_pair_paper_replay,
    run_pair_sequence_analysis,
)
from .pair_live_paper import build_pair_live_paper_app
from .post_pair_cycle_loop import PostPairCycleLoopConfig, build_post_pair_cycle_loop_dataset
from .post_pair_cycle_strict import PostPairCycleStrictConfig, build_post_pair_cycle_strict_analysis
from .second_leg_hazard import SecondLegHazardConfig, build_second_leg_hazard_analysis
from .second_leg_acceptance_frontier import (
    SecondLegAcceptanceFrontierConfig,
    build_second_leg_acceptance_frontier,
)
from .second_leg_policy_ruleset import (
    SecondLegPolicyRulesetConfig,
    build_second_leg_policy_ruleset,
)
from .second_leg_wait_budget_stability import (
    SecondLegWaitBudgetStabilityConfig,
    build_second_leg_wait_budget_stability,
)
from .strict_cycle_start_baseline import (
    StrictCycleStartBaselineConfig,
    run_strict_cycle_start_baseline,
)
from .strategy_blueprint_v0 import (
    StrategyBlueprintV0Config,
    build_strategy_blueprint_v0,
)
from .strategy_config_skeleton_v0 import (
    StrategyConfigSkeletonV0Config,
    build_strategy_config_skeleton_v0,
)
from .strategy_playbook_v0 import (
    StrategyPlaybookV0Config,
    build_strategy_playbook_v0,
)
from .strategy_rollout_bundle_v0 import (
    StrategyRolloutBundleV0Config,
    build_strategy_rollout_bundle_v0,
)
from .unit_accounting import UnitAccountingConfig, build_unit_accounting_dataset
from .signal_price_cache import SignalPriceCacheConfig, fetch_signal_price_cache


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Polymarket research/runtime CLI. Plan B main runtime: `pair-live-paper --config ...`."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ["resolve-target", "doctor"]:
        cmd = subparsers.add_parser(name)
        cmd.add_argument("--config", required=True)

    run_cmd = subparsers.add_parser("run", help="Legacy copytrader runtime")
    run_cmd.add_argument("--config", required=True)
    run_cmd.add_argument("--once", action="store_true")

    backfill_cmd = subparsers.add_parser("backfill")
    backfill_cmd.add_argument("--config", required=True)
    backfill_cmd.add_argument("--limit", type=int, default=200)
    backfill_cmd.add_argument("--output", default="var/backfill.jsonl")

    replay_cmd = subparsers.add_parser("replay")
    replay_cmd.add_argument("--config", required=True)
    replay_cmd.add_argument("--input", required=True)

    eval_cmd = subparsers.add_parser("evaluate")
    eval_cmd.add_argument("--config", required=True)

    alpha_cmd = subparsers.add_parser("alpha-features")
    alpha_cmd.add_argument("--input-trades", required=True)
    alpha_cmd.add_argument("--output-csv", required=True)
    alpha_cmd.add_argument("--summary-json", required=True)
    alpha_cmd.add_argument("--price-cache-dir")
    alpha_cmd.add_argument("--external-market-data-dir")
    alpha_cmd.add_argument("--families", default="btc,eth")
    alpha_cmd.add_argument("--durations", default="5m,15m")
    alpha_cmd.add_argument("--negative-window-seconds", type=int, default=60)
    alpha_cmd.add_argument("--no-negative-samples", action="store_true")

    baseline_cmd = subparsers.add_parser("alpha-baseline")
    baseline_cmd.add_argument("--input-csv", required=True)
    baseline_cmd.add_argument("--output-json", required=True)
    baseline_cmd.add_argument("--predictions-csv")
    baseline_cmd.add_argument("--max-rows", type=int)
    baseline_cmd.add_argument("--test-fraction", type=float, default=0.2)

    two_stage_cmd = subparsers.add_parser("alpha-two-stage-baseline")
    two_stage_cmd.add_argument("--input-csv", required=True)
    two_stage_cmd.add_argument("--output-json", required=True)
    two_stage_cmd.add_argument("--predictions-csv")
    two_stage_cmd.add_argument("--max-rows", type=int)
    two_stage_cmd.add_argument("--test-fraction", type=float, default=0.2)
    two_stage_cmd.add_argument("--strict-negative-min-recent-condition-count-60s", type=int, default=2)
    two_stage_cmd.add_argument("--strict-negative-min-recent-same-market-count-60s", type=int, default=2)
    two_stage_cmd.add_argument("--strict-negative-max-candidate-abs-price-distance-from-mid", type=float, default=0.25)
    two_stage_cmd.add_argument("--buy-stage-max-positive-negative-ratio", type=float, default=1.0)

    scorer_cmd = subparsers.add_parser("alpha-signal-scorer")
    scorer_cmd.add_argument("--input-csv", required=True)
    scorer_cmd.add_argument("--output-json", required=True)
    scorer_cmd.add_argument("--signals-csv", required=True)
    scorer_cmd.add_argument("--max-rows", type=int)
    scorer_cmd.add_argument("--test-fraction", type=float, default=0.2)
    scorer_cmd.add_argument("--strict-negative-min-recent-condition-count-60s", type=int, default=2)
    scorer_cmd.add_argument("--strict-negative-min-recent-same-market-count-60s", type=int, default=2)
    scorer_cmd.add_argument("--strict-negative-max-candidate-abs-price-distance-from-mid", type=float, default=0.25)
    scorer_cmd.add_argument("--buy-stage-max-positive-negative-ratio", type=float, default=1.0)
    scorer_cmd.add_argument("--buy-threshold", type=float, default=0.5)
    scorer_cmd.add_argument("--final-threshold", type=float, default=0.5)

    replay_cmd = subparsers.add_parser("alpha-signal-replay")
    replay_cmd.add_argument("--input-csv", required=True)
    replay_cmd.add_argument("--output-json", required=True)
    replay_cmd.add_argument("--deduped-signals-csv")
    replay_cmd.add_argument("--bucket-metrics-csv")
    replay_cmd.add_argument("--time-bucket-seconds", type=int, default=15)
    replay_cmd.add_argument("--min-buy-score", type=float, default=0.0)
    replay_cmd.add_argument("--min-final-score", type=float, default=0.0)
    replay_cmd.add_argument("--skip-threshold-flags", action="store_true")

    paper_replay_cmd = subparsers.add_parser("alpha-paper-replay")
    paper_replay_cmd.add_argument("--input-csv", required=True)
    paper_replay_cmd.add_argument("--output-json", required=True)
    paper_replay_cmd.add_argument("--price-cache-dir", required=True)
    paper_replay_cmd.add_argument("--trades-csv")
    paper_replay_cmd.add_argument("--initial-capital-usdc", type=float, default=10000.0)
    paper_replay_cmd.add_argument("--fixed-order-usdc", type=float, default=100.0)
    paper_replay_cmd.add_argument("--max-concurrent-positions", type=int)
    paper_replay_cmd.add_argument("--settle-price-upper-threshold", type=float, default=0.95)
    paper_replay_cmd.add_argument("--settle-price-lower-threshold", type=float, default=0.05)

    price_cache_cmd = subparsers.add_parser("fetch-signal-price-cache")
    price_cache_cmd.add_argument("--input-csv", required=True)
    price_cache_cmd.add_argument("--output-dir", required=True)
    price_cache_cmd.add_argument("--output-json", required=True)
    price_cache_cmd.add_argument("--asset-column", default="candidate_asset")
    price_cache_cmd.add_argument("--timestamp-column", default="timestamp_seconds")
    price_cache_cmd.add_argument("--resolution-column", default="resolution_timestamp_seconds")
    price_cache_cmd.add_argument("--seconds-to-resolution-column", default="seconds_to_resolution")
    price_cache_cmd.add_argument("--request-timeout-seconds", type=float, default=20.0)
    price_cache_cmd.add_argument("--fidelity-minutes", type=int, default=1)
    price_cache_cmd.add_argument("--lookback-padding-seconds", type=int, default=300)
    price_cache_cmd.add_argument("--forward-padding-seconds", type=int, default=300)

    outcome_label_cmd = subparsers.add_parser("alpha-outcome-labels")
    outcome_label_cmd.add_argument("--input-csv", required=True)
    outcome_label_cmd.add_argument("--output-csv", required=True)
    outcome_label_cmd.add_argument("--output-json", required=True)
    outcome_label_cmd.add_argument("--price-cache-dir", required=True)
    outcome_label_cmd.add_argument("--candidate-asset-column", default="candidate_asset")
    outcome_label_cmd.add_argument("--candidate-price-column", default="candidate_price")
    outcome_label_cmd.add_argument("--timestamp-column", default="timestamp_seconds")
    outcome_label_cmd.add_argument("--resolution-column", default="resolution_timestamp_seconds")
    outcome_label_cmd.add_argument("--seconds-to-resolution-column", default="seconds_to_resolution")
    outcome_label_cmd.add_argument("--payout-upper-threshold", type=float, default=0.95)
    outcome_label_cmd.add_argument("--payout-lower-threshold", type=float, default=0.05)
    outcome_label_cmd.add_argument("--stake-usdc", type=float, default=100.0)

    outcome_baseline_cmd = subparsers.add_parser("alpha-outcome-baseline")
    outcome_baseline_cmd.add_argument("--input-csv", required=True)
    outcome_baseline_cmd.add_argument("--output-json", required=True)
    outcome_baseline_cmd.add_argument("--predictions-csv")
    outcome_baseline_cmd.add_argument("--test-fraction", type=float, default=0.2)
    outcome_baseline_cmd.add_argument("--max-rows", type=int)
    outcome_baseline_cmd.add_argument("--positive-label-column", default="label_profit_positive")

    outcome_regression_cmd = subparsers.add_parser("alpha-outcome-regression")
    outcome_regression_cmd.add_argument("--input-csv", required=True)
    outcome_regression_cmd.add_argument("--output-json", required=True)
    outcome_regression_cmd.add_argument("--predictions-csv")
    outcome_regression_cmd.add_argument("--test-fraction", type=float, default=0.2)
    outcome_regression_cmd.add_argument("--max-rows", type=int)
    outcome_regression_cmd.add_argument("--target-column", default="pnl_per_stake_usdc")

    topk_strategy_cmd = subparsers.add_parser("alpha-topk-paper-strategy")
    topk_strategy_cmd.add_argument("--input-csv", required=True)
    topk_strategy_cmd.add_argument("--output-json", required=True)
    topk_strategy_cmd.add_argument("--curve-csv")
    topk_strategy_cmd.add_argument("--trades-csv")
    topk_strategy_cmd.add_argument("--initial-capital-usdc", type=float, default=10000.0)
    topk_strategy_cmd.add_argument("--stake-usdc", type=float, default=100.0)
    topk_strategy_cmd.add_argument("--label-stake-usdc", type=float, default=100.0)
    topk_strategy_cmd.add_argument("--max-concurrent-positions", type=int)
    topk_strategy_cmd.add_argument("--dedupe-mode", choices=["one_market_total", "one_market_side"], default="one_market_total")
    topk_strategy_cmd.add_argument("--selection-mode", choices=["top_fraction", "predicted_positive"], default="top_fraction")
    topk_strategy_cmd.add_argument("--top-fraction", type=float, default=0.1)
    topk_strategy_cmd.add_argument("--min-predicted-pnl", type=float, default=0.0)

    walkforward_cmd = subparsers.add_parser("alpha-topk-walkforward")
    walkforward_cmd.add_argument("--input-csv", required=True)
    walkforward_cmd.add_argument("--output-json", required=True)
    walkforward_cmd.add_argument("--folds-csv")
    walkforward_cmd.add_argument("--curve-csv")
    walkforward_cmd.add_argument("--trades-csv")
    walkforward_cmd.add_argument("--predictions-csv")
    walkforward_cmd.add_argument("--initial-train-fraction", type=float, default=0.5)
    walkforward_cmd.add_argument("--n-folds", type=int, default=5)
    walkforward_cmd.add_argument("--max-rows", type=int)
    walkforward_cmd.add_argument("--initial-capital-usdc", type=float, default=10000.0)
    walkforward_cmd.add_argument("--stake-usdc", type=float, default=100.0)
    walkforward_cmd.add_argument("--label-stake-usdc", type=float, default=100.0)
    walkforward_cmd.add_argument("--max-concurrent-positions", type=int)
    walkforward_cmd.add_argument("--dedupe-mode", choices=["one_market_total", "one_market_side"], default="one_market_total")
    walkforward_cmd.add_argument("--selection-mode", choices=["top_fraction", "predicted_positive"], default="top_fraction")
    walkforward_cmd.add_argument("--top-fraction", type=float, default=0.1)
    walkforward_cmd.add_argument("--min-predicted-pnl", type=float, default=0.0)

    topk_replay_cmd = subparsers.add_parser("alpha-topk-paper-replay")
    topk_replay_cmd.add_argument("--input-csv", required=True)
    topk_replay_cmd.add_argument("--output-json", required=True)
    topk_replay_cmd.add_argument("--source-csv")
    topk_replay_cmd.add_argument("--curve-csv")
    topk_replay_cmd.add_argument("--trades-csv")
    topk_replay_cmd.add_argument("--initial-capital-usdc", type=float, default=10000.0)
    topk_replay_cmd.add_argument("--stake-usdc", type=float, default=100.0)
    topk_replay_cmd.add_argument("--max-concurrent-positions", type=int)
    topk_replay_cmd.add_argument("--dedupe-mode", choices=["one_market_total", "one_market_side"], default="one_market_total")
    topk_replay_cmd.add_argument("--selection-mode", choices=["top_fraction", "predicted_positive"], default="top_fraction")
    topk_replay_cmd.add_argument("--top-fraction", type=float, default=0.1)
    topk_replay_cmd.add_argument("--min-predicted-pnl", type=float, default=0.0)
    topk_replay_cmd.add_argument("--entry-slippage-bps", type=float, default=0.0)
    topk_replay_cmd.add_argument("--fee-bps", type=float, default=0.0)
    topk_replay_cmd.add_argument("--max-entry-price", type=float, default=0.95)
    topk_replay_cmd.add_argument("--min-seconds-to-resolution", type=float, default=0.0)

    market_data_cmd = subparsers.add_parser("fetch-external-market-data")
    market_data_cmd.add_argument("--output-dir", required=True)
    market_data_cmd.add_argument("--exchange-id", default="binance")
    market_data_cmd.add_argument("--timeframe", default="1m")
    market_data_cmd.add_argument("--start-timestamp-seconds", type=int)
    market_data_cmd.add_argument("--end-timestamp-seconds", type=int)
    market_data_cmd.add_argument("--families", default="btc,eth")

    pair_cmd = subparsers.add_parser("pair-analysis")
    pair_cmd.add_argument("--input-trades", required=True)
    pair_cmd.add_argument("--output-csv", required=True)
    pair_cmd.add_argument("--summary-json", required=True)
    pair_cmd.add_argument("--recent-buy-limit", type=int, default=5000)
    pair_cmd.add_argument("--families", default="btc,eth,sol,xrp")
    pair_cmd.add_argument("--durations", default="15m,hourly,other")

    pair_replay_cmd = subparsers.add_parser("pair-paper-replay")
    pair_replay_cmd.add_argument("--input-csv", required=True)
    pair_replay_cmd.add_argument("--output-json", required=True)
    pair_replay_cmd.add_argument("--curve-csv")
    pair_replay_cmd.add_argument("--trades-csv")
    pair_replay_cmd.add_argument("--initial-capital-usdc", type=float, default=10000.0)
    pair_replay_cmd.add_argument("--stake-usdc", type=float, default=100.0)
    pair_replay_cmd.add_argument("--max-effective-pair-sum", type=float, default=1.0)
    pair_replay_cmd.add_argument("--fee-bps", type=float, default=0.0)
    pair_replay_cmd.add_argument("--slippage-bps", type=float, default=0.0)
    pair_replay_cmd.add_argument("--max-pair-completion-seconds", type=int)
    pair_replay_cmd.add_argument("--max-imbalance-ratio", type=float)
    pair_replay_cmd.add_argument("--top-fraction", type=float)
    pair_replay_cmd.add_argument("--pair-sum-column", default="pair_sum")
    pair_replay_cmd.add_argument("--pair-gap-column", default="pair_gap_to_parity")
    pair_replay_cmd.add_argument("--timestamp-column", default="pair_end_timestamp_seconds")
    pair_replay_cmd.add_argument("--seconds-to-resolution-column")
    pair_replay_cmd.add_argument("--min-seconds-to-resolution", type=int)
    pair_replay_cmd.add_argument("--max-seconds-to-resolution", type=int)

    pair_seq_cmd = subparsers.add_parser("pair-sequence-analysis")
    pair_seq_cmd.add_argument("--input-trades", required=True)
    pair_seq_cmd.add_argument("--output-csv", required=True)
    pair_seq_cmd.add_argument("--summary-json", required=True)
    pair_seq_cmd.add_argument("--recent-buy-limit", type=int, default=50000)
    pair_seq_cmd.add_argument("--families", default="btc,eth,sol,xrp")
    pair_seq_cmd.add_argument("--durations", default="15m,hourly")

    market_open_cmd = subparsers.add_parser("market-open-snapshot")
    market_open_cmd.add_argument("--input-trades", required=True)
    market_open_cmd.add_argument("--output-csv", required=True)
    market_open_cmd.add_argument("--summary-json", required=True)
    market_open_cmd.add_argument("--market-universe")
    market_open_cmd.add_argument("--families", default="btc,eth,sol,xrp")
    market_open_cmd.add_argument("--durations", default="hourly")
    market_open_cmd.add_argument("--snapshot-offsets", default="0,1,3,5,10,30,60")
    market_open_cmd.add_argument("--entry-horizon-seconds", type=int, default=60)

    first_leg_followup_cmd = subparsers.add_parser("first-leg-followup")
    first_leg_followup_cmd.add_argument("--input-trades", required=True)
    first_leg_followup_cmd.add_argument("--output-csv", required=True)
    first_leg_followup_cmd.add_argument("--summary-json", required=True)
    first_leg_followup_cmd.add_argument("--external-market-data-dir")
    first_leg_followup_cmd.add_argument("--families", default="btc,eth,sol,xrp")
    first_leg_followup_cmd.add_argument("--durations", default="hourly")
    first_leg_followup_cmd.add_argument("--snapshot-offsets", default="0,1,3,5,10,30,60")
    first_leg_followup_cmd.add_argument("--future-horizons", default="1,5,10,30,60")

    cycle_loop_cmd = subparsers.add_parser("post-pair-cycle-loop")
    cycle_loop_cmd.add_argument("--input-trades", required=True)
    cycle_loop_cmd.add_argument("--output-csv", required=True)
    cycle_loop_cmd.add_argument("--summary-json", required=True)
    cycle_loop_cmd.add_argument("--families", default="btc,eth,sol,xrp")
    cycle_loop_cmd.add_argument("--durations", default="hourly")
    cycle_loop_cmd.add_argument("--cycle-start-horizon-seconds", type=int, default=180)
    cycle_loop_cmd.add_argument("--cycle-complete-horizon-seconds", type=int, default=60)

    cycle_strict_cmd = subparsers.add_parser("post-pair-cycle-strict")
    cycle_strict_cmd.add_argument("--input-csv", required=True)
    cycle_strict_cmd.add_argument("--output-csv", required=True)
    cycle_strict_cmd.add_argument("--output-json", required=True)
    cycle_strict_cmd.add_argument("--min-balance-ratio", type=float, default=0.7)
    cycle_strict_cmd.add_argument("--max-completion-delay-seconds", type=float, default=60.0)
    cycle_strict_cmd.add_argument("--min-locked-edge", type=float, default=0.01)
    cycle_strict_cmd.add_argument("--max-same-side-trade-count", type=int, default=10)

    unit_accounting_cmd = subparsers.add_parser("unit-accounting")
    unit_accounting_cmd.add_argument("--input-trades", required=True)
    unit_accounting_cmd.add_argument("--output-csv", required=True)
    unit_accounting_cmd.add_argument("--summary-json", required=True)
    unit_accounting_cmd.add_argument("--families", default="btc,eth,sol,xrp")
    unit_accounting_cmd.add_argument("--durations", default="hourly")

    bridge_cycle_cmd = subparsers.add_parser("bridge-cycle-analysis")
    bridge_cycle_cmd.add_argument("--input-csv", required=True)
    bridge_cycle_cmd.add_argument("--output-json", required=True)
    bridge_cycle_cmd.add_argument("--output-csv", required=True)

    bridge_cycle_trigger_cmd = subparsers.add_parser("bridge-cycle-trigger-analysis")
    bridge_cycle_trigger_cmd.add_argument("--input-csv", required=True)
    bridge_cycle_trigger_cmd.add_argument("--output-json", required=True)
    bridge_cycle_trigger_cmd.add_argument("--output-csv", required=True)

    hazard_cmd = subparsers.add_parser("second-leg-hazard")
    hazard_cmd.add_argument("--input-csv", required=True)
    hazard_cmd.add_argument("--output-json", required=True)
    hazard_cmd.add_argument("--curve-csv", required=True)
    hazard_cmd.add_argument("--max-seconds", type=int, default=60)

    acceptance_frontier_cmd = subparsers.add_parser("second-leg-acceptance-frontier")
    acceptance_frontier_cmd.add_argument("--input-csv", required=True)
    acceptance_frontier_cmd.add_argument("--output-json", required=True)
    acceptance_frontier_cmd.add_argument("--output-csv", required=True)
    acceptance_frontier_cmd.add_argument("--followup-offset-seconds", type=int, default=0)

    second_leg_policy_ruleset_cmd = subparsers.add_parser("second-leg-policy-ruleset")
    second_leg_policy_ruleset_cmd.add_argument("--wait-budget-json", required=True)
    second_leg_policy_ruleset_cmd.add_argument("--policy-hypothesis-json", required=True)
    second_leg_policy_ruleset_cmd.add_argument("--acceptance-frontier-json", required=True)
    second_leg_policy_ruleset_cmd.add_argument("--output-json", required=True)
    second_leg_policy_ruleset_cmd.add_argument("--output-csv", required=True)

    second_leg_wait_budget_stability_cmd = subparsers.add_parser("second-leg-wait-budget-stability")
    second_leg_wait_budget_stability_cmd.add_argument("--wait-budget-json", required=True)
    second_leg_wait_budget_stability_cmd.add_argument("--policy-hypothesis-json", required=True)
    second_leg_wait_budget_stability_cmd.add_argument("--output-json", required=True)
    second_leg_wait_budget_stability_cmd.add_argument("--output-csv", required=True)

    strategy_blueprint_v0_cmd = subparsers.add_parser("strategy-blueprint-v0")
    strategy_blueprint_v0_cmd.add_argument("--entry-ruleset-json", required=True)
    strategy_blueprint_v0_cmd.add_argument("--second-leg-policy-json", required=True)
    strategy_blueprint_v0_cmd.add_argument("--wait-budget-stability-json", required=True)
    strategy_blueprint_v0_cmd.add_argument("--output-json", required=True)
    strategy_blueprint_v0_cmd.add_argument("--output-csv", required=True)

    strategy_playbook_v0_cmd = subparsers.add_parser("strategy-playbook-v0")
    strategy_playbook_v0_cmd.add_argument("--strategy-blueprint-json", required=True)
    strategy_playbook_v0_cmd.add_argument("--output-json", required=True)
    strategy_playbook_v0_cmd.add_argument("--output-csv", required=True)

    btc_family_default_strategy_v0_cmd = subparsers.add_parser("btc-family-default-strategy-v0")
    btc_family_default_strategy_v0_cmd.add_argument("--strategy-blueprint-json", required=True)
    btc_family_default_strategy_v0_cmd.add_argument("--strategy-playbook-json", required=True)
    btc_family_default_strategy_v0_cmd.add_argument("--output-json", required=True)
    btc_family_default_strategy_v0_cmd.add_argument("--output-csv", required=True)

    eth_sol_override_strategies_v0_cmd = subparsers.add_parser("eth-sol-override-strategies-v0")
    eth_sol_override_strategies_v0_cmd.add_argument("--strategy-blueprint-json", required=True)
    eth_sol_override_strategies_v0_cmd.add_argument("--strategy-playbook-json", required=True)
    eth_sol_override_strategies_v0_cmd.add_argument("--output-json", required=True)
    eth_sol_override_strategies_v0_cmd.add_argument("--output-csv", required=True)

    strategy_rollout_bundle_v0_cmd = subparsers.add_parser("strategy-rollout-bundle-v0")
    strategy_rollout_bundle_v0_cmd.add_argument("--strategy-playbook-json", required=True)
    strategy_rollout_bundle_v0_cmd.add_argument("--btc-strategy-json", required=True)
    strategy_rollout_bundle_v0_cmd.add_argument("--eth-sol-override-json", required=True)
    strategy_rollout_bundle_v0_cmd.add_argument("--output-json", required=True)
    strategy_rollout_bundle_v0_cmd.add_argument("--output-csv", required=True)

    strategy_config_skeleton_v0_cmd = subparsers.add_parser("strategy-config-skeleton-v0")
    strategy_config_skeleton_v0_cmd.add_argument("--strategy-rollout-bundle-json", required=True)
    strategy_config_skeleton_v0_cmd.add_argument("--btc-strategy-json", required=True)
    strategy_config_skeleton_v0_cmd.add_argument("--eth-sol-override-json", required=True)
    strategy_config_skeleton_v0_cmd.add_argument("--output-json", required=True)
    strategy_config_skeleton_v0_cmd.add_argument("--output-csv", required=True)

    regime_cmd = subparsers.add_parser("first-leg-regime-analysis")
    regime_cmd.add_argument("--input-csv", required=True)
    regime_cmd.add_argument("--output-json", required=True)
    regime_cmd.add_argument("--output-csv", required=True)
    regime_cmd.add_argument("--followup-offset-seconds", type=int, default=0)

    first_leg_completion_cmd = subparsers.add_parser("first-leg-completion-baseline")
    first_leg_completion_cmd.add_argument("--input-csv", required=True)
    first_leg_completion_cmd.add_argument("--output-json", required=True)
    first_leg_completion_cmd.add_argument("--followup-offset-seconds", type=int, default=0)
    first_leg_completion_cmd.add_argument("--target-columns", default="label_complete_pair_10s,label_complete_pair_60s")
    first_leg_completion_cmd.add_argument(
        "--feature-columns",
        default="market_family,first_leg_outcome,first_leg_price,first_leg_usdc_size,seconds_to_resolution,realized_first_cross_pair_sum,family_return_60s,family_return_300s,family_return_900s,family_realized_vol_60s,family_realized_vol_300s,family_realized_vol_900s,family_volume_60s,family_volume_300s,family_volume_900s,signed_return_60s,signed_return_300s,signed_return_900s,abs_return_60s,abs_return_300s,abs_return_900s,signed_first_leg_price_distance_from_mid,first_leg_abs_price_distance_from_mid",
    )
    first_leg_completion_cmd.add_argument("--families", default="btc,eth,sol,xrp")

    first_leg_family_hazard_cmd = subparsers.add_parser("first-leg-family-hazard")
    first_leg_family_hazard_cmd.add_argument("--input-csv", required=True)
    first_leg_family_hazard_cmd.add_argument("--output-json", required=True)
    first_leg_family_hazard_cmd.add_argument("--curve-csv", required=True)
    first_leg_family_hazard_cmd.add_argument("--followup-offset-seconds", type=int, default=0)
    first_leg_family_hazard_cmd.add_argument("--max-seconds", type=int, default=60)

    first_leg_wait_budget_cmd = subparsers.add_parser("first-leg-wait-budget")
    first_leg_wait_budget_cmd.add_argument("--input-curve-csv", required=True)
    first_leg_wait_budget_cmd.add_argument("--output-json", required=True)
    first_leg_wait_budget_cmd.add_argument("--coverage-thresholds", default="0.7,0.8")

    first_leg_policy_cmd = subparsers.add_parser("first-leg-policy-hypothesis")
    first_leg_policy_cmd.add_argument("--wait-budget-json", required=True)
    first_leg_policy_cmd.add_argument("--family-hazard-json", required=True)
    first_leg_policy_cmd.add_argument("--completion-baseline-json", required=True)
    first_leg_policy_cmd.add_argument("--output-json", required=True)
    first_leg_policy_cmd.add_argument("--output-csv", required=True)

    first_leg_trigger_cmd = subparsers.add_parser("first-leg-trigger-rule")
    first_leg_trigger_cmd.add_argument("--market-open-csv", required=True)
    first_leg_trigger_cmd.add_argument("--first-leg-followup-csv", required=True)
    first_leg_trigger_cmd.add_argument("--output-json", required=True)
    first_leg_trigger_cmd.add_argument("--min-band-coverage", type=float, default=0.25)

    first_leg_ruleset_cmd = subparsers.add_parser("first-leg-ruleset")
    first_leg_ruleset_cmd.add_argument("--trigger-rule-json", required=True)
    first_leg_ruleset_cmd.add_argument("--output-json", required=True)
    first_leg_ruleset_cmd.add_argument("--output-csv", required=True)

    first_cycle_outcome_cmd = subparsers.add_parser("first-cycle-outcome-analysis")
    first_cycle_outcome_cmd.add_argument("--unit-accounting-csv", required=True)
    first_cycle_outcome_cmd.add_argument("--market-open-csv", required=True)
    first_cycle_outcome_cmd.add_argument("--ruleset-json", required=True)
    first_cycle_outcome_cmd.add_argument("--output-json", required=True)
    first_cycle_outcome_cmd.add_argument("--output-csv", required=True)

    entry_ruleset_v1_cmd = subparsers.add_parser("entry-ruleset-v1")
    entry_ruleset_v1_cmd.add_argument("--first-leg-ruleset-json", required=True)
    entry_ruleset_v1_cmd.add_argument("--first-cycle-outcome-json", required=True)
    entry_ruleset_v1_cmd.add_argument("--output-json", required=True)
    entry_ruleset_v1_cmd.add_argument("--output-csv", required=True)

    entry_wait_budget_mapping_cmd = subparsers.add_parser("entry-wait-budget-mapping")
    entry_wait_budget_mapping_cmd.add_argument("--first-leg-followup-csv", required=True)
    entry_wait_budget_mapping_cmd.add_argument("--market-open-csv", required=True)
    entry_wait_budget_mapping_cmd.add_argument("--entry-ruleset-json", required=True)
    entry_wait_budget_mapping_cmd.add_argument("--policy-hypothesis-json", required=True)
    entry_wait_budget_mapping_cmd.add_argument("--output-json", required=True)
    entry_wait_budget_mapping_cmd.add_argument("--output-csv", required=True)

    entry_hard_lock_explanation_cmd = subparsers.add_parser("entry-hard-lock-explanation")
    entry_hard_lock_explanation_cmd.add_argument("--unit-accounting-csv", required=True)
    entry_hard_lock_explanation_cmd.add_argument("--market-open-csv", required=True)
    entry_hard_lock_explanation_cmd.add_argument("--entry-ruleset-json", required=True)
    entry_hard_lock_explanation_cmd.add_argument("--output-json", required=True)
    entry_hard_lock_explanation_cmd.add_argument("--output-csv", required=True)

    strict_cycle_start_cmd = subparsers.add_parser("strict-cycle-start-baseline")
    strict_cycle_start_cmd.add_argument("--input-csv", required=True)
    strict_cycle_start_cmd.add_argument("--output-json", required=True)
    strict_cycle_start_cmd.add_argument("--predictions-csv")
    strict_cycle_start_cmd.add_argument("--enriched-csv")
    strict_cycle_start_cmd.add_argument("--external-market-data-dir")
    strict_cycle_start_cmd.add_argument(
        "--feature-columns",
        default="market_family,market_duration_bucket,locked_unit_index,seconds_to_resolution,current_locked_pair_sum,current_locked_edge,cumulative_up_count,cumulative_down_count,cumulative_up_usdc,cumulative_down_usdc,label_next_cycle_start_delay_seconds,label_next_cycle_first_leg_outcome,label_next_cycle_first_leg_price,label_next_cycle_first_leg_usdc_size,external_market_return_60s,external_market_return_300s,external_market_return_900s,external_market_volume_60s,external_market_volume_300s,external_market_trade_count_60s,external_market_trade_count_300s,external_market_realized_vol_300s,external_market_realized_vol_900s,signed_external_market_return_60s,signed_external_market_return_300s,signed_external_market_return_900s",
    )
    strict_cycle_start_cmd.add_argument("--test-fraction", type=float, default=0.2)
    strict_cycle_start_cmd.add_argument("--max-rows", type=int)
    strict_cycle_start_cmd.add_argument("--topk-fractions", default="0.02,0.1,0.2")

    pair_live_cmd = subparsers.add_parser(
        "pair-live-paper",
        help="Recommended Plan B runtime: skeleton-driven first-leg scanner + live paper execution",
    )
    pair_live_cmd.add_argument("--config", required=True)
    pair_live_cmd.add_argument("--once", action="store_true")

    live_paper_cmd = subparsers.add_parser("live-paper")
    live_paper_cmd.add_argument("--config", required=True)
    live_paper_cmd.add_argument("--once", action="store_true")
    return parser


def _load_jsonl(path: str) -> Iterable[TradeActivity]:
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = json.loads(line)
            payload = raw["payload"] if "payload" in raw else raw
            yield TradeActivity(
                proxy_wallet=str(payload["proxy_wallet"]),
                timestamp_ms=int(payload["timestamp_ms"]),
                condition_id=str(payload["condition_id"]),
                activity_type=str(payload["activity_type"]),
                size=float(payload["size"]),
                usdc_size=float(payload["usdc_size"]),
                transaction_hash=str(payload["transaction_hash"]),
                price=float(payload["price"]),
                asset=str(payload["asset"]),
                side=str(payload["side"]),
                outcome_index=int(payload["outcome_index"]),
                title=str(payload["title"]),
                slug=str(payload["slug"]),
                event_slug=str(payload["event_slug"]),
                outcome=str(payload["outcome"]),
                name=str(payload.get("name") or ""),
                pseudonym=str(payload.get("pseudonym") or ""),
            )


def main(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "fetch-external-market-data":
            from .external_market_data import ExternalMarketFetchConfig, fetch_external_market_data

            summary = fetch_external_market_data(
                ExternalMarketFetchConfig(
                    output_dir=args.output_dir,
                    exchange_id=args.exchange_id,
                    timeframe=args.timeframe,
                    start_timestamp_seconds=args.start_timestamp_seconds,
                    end_timestamp_seconds=args.end_timestamp_seconds,
                    families=tuple(item.strip() for item in args.families.split(",") if item.strip()),
                )
            )
            print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "pair-analysis":
            summary = run_pair_analysis(
                PairAnalysisConfig(
                    input_trades_path=args.input_trades,
                    output_csv_path=args.output_csv,
                    output_json_path=args.summary_json,
                    recent_buy_limit=args.recent_buy_limit,
                    families=tuple(item.strip() for item in args.families.split(",") if item.strip()),
                    durations=tuple(item.strip() for item in args.durations.split(",") if item.strip()),
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "pair-paper-replay":
            summary = run_pair_paper_replay(
                PairPaperReplayConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    curve_csv_path=args.curve_csv,
                    trades_csv_path=args.trades_csv,
                    initial_capital_usdc=args.initial_capital_usdc,
                    stake_usdc=args.stake_usdc,
                    max_effective_pair_sum=args.max_effective_pair_sum,
                    fee_bps=args.fee_bps,
                    slippage_bps=args.slippage_bps,
                    max_pair_completion_seconds=args.max_pair_completion_seconds,
                    max_imbalance_ratio=args.max_imbalance_ratio,
                    top_fraction=args.top_fraction,
                    pair_sum_column=args.pair_sum_column,
                    pair_gap_column=args.pair_gap_column,
                    timestamp_column=args.timestamp_column,
                    seconds_to_resolution_column=args.seconds_to_resolution_column,
                    min_seconds_to_resolution=args.min_seconds_to_resolution,
                    max_seconds_to_resolution=args.max_seconds_to_resolution,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "pair-sequence-analysis":
            summary = run_pair_sequence_analysis(
                PairSequenceAnalysisConfig(
                    input_trades_path=args.input_trades,
                    output_csv_path=args.output_csv,
                    output_json_path=args.summary_json,
                    recent_buy_limit=args.recent_buy_limit,
                    families=tuple(item.strip() for item in args.families.split(",") if item.strip()),
                    durations=tuple(item.strip() for item in args.durations.split(",") if item.strip()),
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "market-open-snapshot":
            summary = build_market_open_snapshot_dataset(
                MarketOpenSnapshotConfig(
                    input_trades_path=args.input_trades,
                    output_csv_path=args.output_csv,
                    summary_json_path=args.summary_json,
                    market_universe_path=args.market_universe,
                    families=tuple(item.strip() for item in args.families.split(",") if item.strip()),
                    durations=tuple(item.strip() for item in args.durations.split(",") if item.strip()),
                    snapshot_offsets_seconds=tuple(
                        int(item.strip()) for item in args.snapshot_offsets.split(",") if item.strip()
                    ),
                    entry_horizon_seconds=args.entry_horizon_seconds,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "first-leg-followup":
            summary = build_first_leg_followup_dataset(
                FirstLegFollowupConfig(
                    input_trades_path=args.input_trades,
                    output_csv_path=args.output_csv,
                    summary_json_path=args.summary_json,
                    external_market_data_dir=args.external_market_data_dir,
                    families=tuple(item.strip() for item in args.families.split(",") if item.strip()),
                    durations=tuple(item.strip() for item in args.durations.split(",") if item.strip()),
                    snapshot_offsets_seconds=tuple(
                        int(item.strip()) for item in args.snapshot_offsets.split(",") if item.strip()
                    ),
                    future_horizons_seconds=tuple(
                        int(item.strip()) for item in args.future_horizons.split(",") if item.strip()
                    ),
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "post-pair-cycle-loop":
            summary = build_post_pair_cycle_loop_dataset(
                PostPairCycleLoopConfig(
                    input_trades_path=args.input_trades,
                    output_csv_path=args.output_csv,
                    summary_json_path=args.summary_json,
                    families=tuple(item.strip() for item in args.families.split(",") if item.strip()),
                    durations=tuple(item.strip() for item in args.durations.split(",") if item.strip()),
                    cycle_start_horizon_seconds=args.cycle_start_horizon_seconds,
                    cycle_complete_horizon_seconds=args.cycle_complete_horizon_seconds,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "second-leg-hazard":
            summary = build_second_leg_hazard_analysis(
                SecondLegHazardConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    curve_csv_path=args.curve_csv,
                    max_seconds=args.max_seconds,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "first-leg-regime-analysis":
            summary = build_first_leg_regime_analysis(
                FirstLegRegimeAnalysisConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                    followup_offset_seconds=args.followup_offset_seconds,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "second-leg-acceptance-frontier":
            summary = build_second_leg_acceptance_frontier(
                SecondLegAcceptanceFrontierConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                    followup_offset_seconds=args.followup_offset_seconds,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "second-leg-policy-ruleset":
            summary = build_second_leg_policy_ruleset(
                SecondLegPolicyRulesetConfig(
                    wait_budget_json_path=args.wait_budget_json,
                    policy_hypothesis_json_path=args.policy_hypothesis_json,
                    acceptance_frontier_json_path=args.acceptance_frontier_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "second-leg-wait-budget-stability":
            summary = build_second_leg_wait_budget_stability(
                SecondLegWaitBudgetStabilityConfig(
                    wait_budget_json_path=args.wait_budget_json,
                    policy_hypothesis_json_path=args.policy_hypothesis_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "strategy-blueprint-v0":
            summary = build_strategy_blueprint_v0(
                StrategyBlueprintV0Config(
                    entry_ruleset_json_path=args.entry_ruleset_json,
                    second_leg_policy_ruleset_json_path=args.second_leg_policy_json,
                    second_leg_wait_budget_stability_json_path=args.wait_budget_stability_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "strategy-playbook-v0":
            summary = build_strategy_playbook_v0(
                StrategyPlaybookV0Config(
                    strategy_blueprint_json_path=args.strategy_blueprint_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "btc-family-default-strategy-v0":
            summary = build_btc_family_default_strategy_v0(
                BtcFamilyDefaultStrategyV0Config(
                    strategy_blueprint_json_path=args.strategy_blueprint_json,
                    strategy_playbook_json_path=args.strategy_playbook_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "eth-sol-override-strategies-v0":
            summary = build_eth_sol_override_strategies_v0(
                EthSolOverrideStrategiesV0Config(
                    strategy_blueprint_json_path=args.strategy_blueprint_json,
                    strategy_playbook_json_path=args.strategy_playbook_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "strategy-rollout-bundle-v0":
            summary = build_strategy_rollout_bundle_v0(
                StrategyRolloutBundleV0Config(
                    strategy_playbook_json_path=args.strategy_playbook_json,
                    btc_strategy_json_path=args.btc_strategy_json,
                    eth_sol_override_json_path=args.eth_sol_override_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "strategy-config-skeleton-v0":
            summary = build_strategy_config_skeleton_v0(
                StrategyConfigSkeletonV0Config(
                    strategy_rollout_bundle_json_path=args.strategy_rollout_bundle_json,
                    btc_strategy_json_path=args.btc_strategy_json,
                    eth_sol_override_json_path=args.eth_sol_override_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "first-leg-completion-baseline":
            summary = run_first_leg_completion_baseline(
                FirstLegCompletionBaselineConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    followup_offset_seconds=args.followup_offset_seconds,
                    target_columns=tuple(item.strip() for item in args.target_columns.split(",") if item.strip()),
                    feature_columns=tuple(item.strip() for item in args.feature_columns.split(",") if item.strip()),
                    families=tuple(item.strip() for item in args.families.split(",") if item.strip()),
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "first-leg-family-hazard":
            summary = build_first_leg_family_hazard_analysis(
                FirstLegFamilyHazardConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    curve_csv_path=args.curve_csv,
                    followup_offset_seconds=args.followup_offset_seconds,
                    max_seconds=args.max_seconds,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "first-leg-wait-budget":
            summary = build_first_leg_wait_budget(
                FirstLegWaitBudgetConfig(
                    input_curve_csv_path=args.input_curve_csv,
                    output_json_path=args.output_json,
                    coverage_thresholds=tuple(
                        float(item.strip()) for item in args.coverage_thresholds.split(",") if item.strip()
                    ),
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "first-leg-policy-hypothesis":
            summary = build_first_leg_policy_hypothesis(
                FirstLegPolicyHypothesisConfig(
                    wait_budget_json_path=args.wait_budget_json,
                    family_hazard_json_path=args.family_hazard_json,
                    completion_baseline_json_path=args.completion_baseline_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "first-leg-trigger-rule":
            summary = build_first_leg_trigger_rule_summary(
                FirstLegTriggerRuleConfig(
                    market_open_csv_path=args.market_open_csv,
                    first_leg_followup_csv_path=args.first_leg_followup_csv,
                    output_json_path=args.output_json,
                    min_band_coverage=args.min_band_coverage,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "first-leg-ruleset":
            summary = build_first_leg_ruleset(
                FirstLegRulesetConfig(
                    trigger_rule_json_path=args.trigger_rule_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "first-cycle-outcome-analysis":
            summary = build_first_cycle_outcome_analysis(
                FirstCycleOutcomeAnalysisConfig(
                    unit_accounting_csv_path=args.unit_accounting_csv,
                    market_open_csv_path=args.market_open_csv,
                    ruleset_json_path=args.ruleset_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "entry-ruleset-v1":
            summary = build_entry_ruleset_v1(
                EntryRulesetV1Config(
                    first_leg_ruleset_json_path=args.first_leg_ruleset_json,
                    first_cycle_outcome_json_path=args.first_cycle_outcome_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "entry-wait-budget-mapping":
            summary = build_entry_wait_budget_mapping(
                EntryWaitBudgetMappingConfig(
                    first_leg_followup_csv_path=args.first_leg_followup_csv,
                    market_open_csv_path=args.market_open_csv,
                    entry_ruleset_json_path=args.entry_ruleset_json,
                    policy_hypothesis_json_path=args.policy_hypothesis_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "entry-hard-lock-explanation":
            summary = build_entry_hard_lock_explanation(
                EntryHardLockExplanationConfig(
                    unit_accounting_csv_path=args.unit_accounting_csv,
                    market_open_csv_path=args.market_open_csv,
                    entry_ruleset_json_path=args.entry_ruleset_json,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "strict-cycle-start-baseline":
            summary = run_strict_cycle_start_baseline(
                StrictCycleStartBaselineConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    predictions_csv_path=args.predictions_csv,
                    enriched_csv_path=args.enriched_csv,
                    external_market_data_dir=args.external_market_data_dir,
                    feature_columns=tuple(item.strip() for item in args.feature_columns.split(",") if item.strip()),
                    test_fraction=args.test_fraction,
                    max_rows=args.max_rows,
                    topk_fractions=tuple(float(item.strip()) for item in args.topk_fractions.split(",") if item.strip()),
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "post-pair-cycle-strict":
            summary = build_post_pair_cycle_strict_analysis(
                PostPairCycleStrictConfig(
                    input_csv_path=args.input_csv,
                    output_csv_path=args.output_csv,
                    output_json_path=args.output_json,
                    min_balance_ratio=args.min_balance_ratio,
                    max_completion_delay_seconds=args.max_completion_delay_seconds,
                    min_locked_edge=args.min_locked_edge,
                    max_same_side_trade_count=args.max_same_side_trade_count,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "unit-accounting":
            summary = build_unit_accounting_dataset(
                UnitAccountingConfig(
                    input_trades_path=args.input_trades,
                    output_csv_path=args.output_csv,
                    summary_json_path=args.summary_json,
                    families=tuple(item.strip() for item in args.families.split(",") if item.strip()),
                    durations=tuple(item.strip() for item in args.durations.split(",") if item.strip()),
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "bridge-cycle-analysis":
            summary = build_bridge_cycle_analysis(
                BridgeCycleAnalysisConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "bridge-cycle-trigger-analysis":
            summary = build_bridge_cycle_trigger_analysis(
                BridgeCycleTriggerAnalysisConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    output_csv_path=args.output_csv,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "pair-live-paper":
            app = build_pair_live_paper_app(args.config)
            if args.once:
                for line in app.doctor():
                    print(line)
                app.run(once=True)
                return 0
            for line in app.doctor():
                print(line)
            app.run(once=False)
            return 0

        if args.command == "alpha-baseline":
            summary = run_alpha_baseline(
                AlphaBaselineConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    predictions_csv_path=args.predictions_csv,
                    max_rows=args.max_rows,
                    test_fraction=args.test_fraction,
                )
            )
            print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-two-stage-baseline":
            summary = run_alpha_two_stage_baseline(
                AlphaTwoStageConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    predictions_csv_path=args.predictions_csv,
                    max_rows=args.max_rows,
                    test_fraction=args.test_fraction,
                    strict_negative_min_recent_condition_count_60s=args.strict_negative_min_recent_condition_count_60s,
                    strict_negative_min_recent_same_market_count_60s=args.strict_negative_min_recent_same_market_count_60s,
                    strict_negative_max_candidate_abs_price_distance_from_mid=args.strict_negative_max_candidate_abs_price_distance_from_mid,
                    buy_stage_max_positive_negative_ratio=args.buy_stage_max_positive_negative_ratio,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-signal-scorer":
            summary = run_alpha_signal_scorer(
                AlphaSignalScorerConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    signals_csv_path=args.signals_csv,
                    max_rows=args.max_rows,
                    test_fraction=args.test_fraction,
                    strict_negative_min_recent_condition_count_60s=args.strict_negative_min_recent_condition_count_60s,
                    strict_negative_min_recent_same_market_count_60s=args.strict_negative_min_recent_same_market_count_60s,
                    strict_negative_max_candidate_abs_price_distance_from_mid=args.strict_negative_max_candidate_abs_price_distance_from_mid,
                    buy_stage_max_positive_negative_ratio=args.buy_stage_max_positive_negative_ratio,
                    buy_threshold=args.buy_threshold,
                    final_threshold=args.final_threshold,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-signal-replay":
            summary = run_alpha_signal_replay(
                AlphaSignalReplayConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    deduped_signals_csv_path=args.deduped_signals_csv,
                    bucket_metrics_csv_path=args.bucket_metrics_csv,
                    require_threshold_flags=not args.skip_threshold_flags,
                    min_buy_score=args.min_buy_score,
                    min_final_score=args.min_final_score,
                    time_bucket_seconds=args.time_bucket_seconds,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-paper-replay":
            summary = run_alpha_paper_replay(
                AlphaPaperReplayConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    price_cache_dir=args.price_cache_dir,
                    trades_csv_path=args.trades_csv,
                    initial_capital_usdc=args.initial_capital_usdc,
                    fixed_order_usdc=args.fixed_order_usdc,
                    max_concurrent_positions=args.max_concurrent_positions,
                    settle_price_upper_threshold=args.settle_price_upper_threshold,
                    settle_price_lower_threshold=args.settle_price_lower_threshold,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "fetch-signal-price-cache":
            summary = fetch_signal_price_cache(
                SignalPriceCacheConfig(
                    input_csv_path=args.input_csv,
                    output_dir=args.output_dir,
                    output_json_path=args.output_json,
                    asset_column=args.asset_column,
                    timestamp_column=args.timestamp_column,
                    resolution_column=args.resolution_column,
                    seconds_to_resolution_column=args.seconds_to_resolution_column,
                    request_timeout_seconds=args.request_timeout_seconds,
                    fidelity_minutes=args.fidelity_minutes,
                    lookback_padding_seconds=args.lookback_padding_seconds,
                    forward_padding_seconds=args.forward_padding_seconds,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-outcome-labels":
            summary = run_alpha_outcome_labels(
                AlphaOutcomeLabelConfig(
                    input_csv_path=args.input_csv,
                    output_csv_path=args.output_csv,
                    output_json_path=args.output_json,
                    price_cache_dir=args.price_cache_dir,
                    candidate_asset_column=args.candidate_asset_column,
                    candidate_price_column=args.candidate_price_column,
                    timestamp_column=args.timestamp_column,
                    resolution_column=args.resolution_column,
                    seconds_to_resolution_column=args.seconds_to_resolution_column,
                    payout_upper_threshold=args.payout_upper_threshold,
                    payout_lower_threshold=args.payout_lower_threshold,
                    stake_usdc=args.stake_usdc,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-outcome-baseline":
            summary = run_alpha_outcome_baseline(
                AlphaOutcomeBaselineConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    predictions_csv_path=args.predictions_csv,
                    test_fraction=args.test_fraction,
                    max_rows=args.max_rows,
                    positive_label_column=args.positive_label_column,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-outcome-regression":
            summary = run_alpha_outcome_regression(
                AlphaOutcomeRegressionConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    predictions_csv_path=args.predictions_csv,
                    test_fraction=args.test_fraction,
                    max_rows=args.max_rows,
                    target_column=args.target_column,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-topk-paper-strategy":
            summary = run_alpha_topk_paper_strategy(
                AlphaTopKPaperStrategyConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    curve_csv_path=args.curve_csv,
                    trades_csv_path=args.trades_csv,
                    initial_capital_usdc=args.initial_capital_usdc,
                    stake_usdc=args.stake_usdc,
                    label_stake_usdc=args.label_stake_usdc,
                    max_concurrent_positions=args.max_concurrent_positions,
                    dedupe_mode=args.dedupe_mode,
                    selection_mode=args.selection_mode,
                    top_fraction=args.top_fraction,
                    min_predicted_pnl=args.min_predicted_pnl,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-topk-walkforward":
            summary = run_alpha_topk_walkforward(
                AlphaTopKWalkForwardConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    folds_csv_path=args.folds_csv,
                    curve_csv_path=args.curve_csv,
                    trades_csv_path=args.trades_csv,
                    predictions_csv_path=args.predictions_csv,
                    initial_train_fraction=args.initial_train_fraction,
                    n_folds=args.n_folds,
                    max_rows=args.max_rows,
                    initial_capital_usdc=args.initial_capital_usdc,
                    stake_usdc=args.stake_usdc,
                    label_stake_usdc=args.label_stake_usdc,
                    max_concurrent_positions=args.max_concurrent_positions,
                    dedupe_mode=args.dedupe_mode,
                    selection_mode=args.selection_mode,
                    top_fraction=args.top_fraction,
                    min_predicted_pnl=args.min_predicted_pnl,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-topk-paper-replay":
            summary = run_alpha_topk_paper_replay(
                AlphaTopKPaperReplayConfig(
                    input_csv_path=args.input_csv,
                    output_json_path=args.output_json,
                    source_csv_path=args.source_csv,
                    curve_csv_path=args.curve_csv,
                    trades_csv_path=args.trades_csv,
                    initial_capital_usdc=args.initial_capital_usdc,
                    stake_usdc=args.stake_usdc,
                    max_concurrent_positions=args.max_concurrent_positions,
                    dedupe_mode=args.dedupe_mode,
                    selection_mode=args.selection_mode,
                    top_fraction=args.top_fraction,
                    min_predicted_pnl=args.min_predicted_pnl,
                    entry_slippage_bps=args.entry_slippage_bps,
                    fee_bps=args.fee_bps,
                    max_entry_price=args.max_entry_price,
                    min_seconds_to_resolution=args.min_seconds_to_resolution,
                )
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "alpha-features":
            summary = build_alpha_feature_dataset(
                AlphaFeatureConfig(
                    input_trades_path=args.input_trades,
                    output_csv_path=args.output_csv,
                    summary_json_path=args.summary_json,
                    price_cache_dir=args.price_cache_dir,
                    external_market_data_dir=args.external_market_data_dir,
                    families=tuple(item.strip() for item in args.families.split(",") if item.strip()),
                    durations=tuple(item.strip() for item in args.durations.split(",") if item.strip()),
                    include_negative_samples=not args.no_negative_samples,
                    negative_window_seconds=args.negative_window_seconds,
                )
            )
            print(json.dumps(summary.__dict__, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "evaluate":
            summary = build_evaluation_app(args.config).run()
            print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
            return 0

        if args.command == "live-paper":
            app = build_live_paper_app(args.config)
            if args.once:
                for line in app.doctor():
                    print(line)
                app.run(once=True)
                return 0
            for line in app.doctor():
                print(line)
            app.run(once=False)
            return 0

        app = build_app(args.config)

        if args.command == "resolve-target":
            wallet = app.resolve_target()
            print(wallet)
            return 0

        if args.command == "doctor":
            for line in app.doctor():
                print(line)
            return 0

        if args.command == "run":
            app.run(once=args.once)
            return 0

        if args.command == "backfill":
            count = app.backfill(limit=args.limit, output_path=args.output)
            print(f"backfilled={count}")
            return 0

        if args.command == "replay":
            count = app.replay(_load_jsonl(args.input))
            print(f"replayed={count}")
            return 0

    except Exception as exc:
        print(f"error: {exc}")
        return 1

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
