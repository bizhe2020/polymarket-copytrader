from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .alpha_features import PriceHistoryLookup


_DEFAULT_FEATURE_COLUMNS: Tuple[str, ...] = (
    "market_family",
    "market_duration_bucket",
    "candidate_outcome",
    "candidate_abs_price_distance_from_mid",
    "candidate_price_distance_from_mid",
    "time_since_prev_same_market_trade_seconds",
    "time_since_prev_same_condition_trade_seconds",
    "time_since_prev_same_market_outcome_trade_seconds",
    "seconds_to_resolution",
    "asset_return_60s",
    "asset_return_300s",
    "recent_condition_count_60s",
    "recent_same_outcome_count_60s",
    "recent_opposite_outcome_count_60s",
    "recent_same_market_count_60s",
    "recent_same_market_outcome_count_60s",
    "recent_condition_usdc_60s",
    "recent_same_outcome_usdc_60s",
    "recent_opposite_outcome_usdc_60s",
    "recent_outcome_count_imbalance_60s",
    "recent_outcome_usdc_imbalance_60s",
    "recent_outcome_count_share_60s",
    "recent_outcome_usdc_share_60s",
    "recent_market_outcome_count_share_60s",
    "recent_market_outcome_usdc_share_60s",
    "recent_market_outcome_count_imbalance_60s",
    "recent_market_outcome_usdc_imbalance_60s",
    "external_market_return_60s",
    "external_market_return_300s",
    "external_market_return_900s",
    "external_market_volume_60s",
    "external_market_trade_count_300s",
    "external_market_realized_vol_300s",
    "candidate_signed_external_return_60s",
    "candidate_signed_external_return_300s",
)

_CATEGORICAL_COLUMNS: Tuple[str, ...] = (
    "market_family",
    "market_duration_bucket",
    "candidate_outcome",
)


@dataclass
class AlphaBaselineConfig:
    input_csv_path: str
    output_json_path: str
    predictions_csv_path: Optional[str] = None
    feature_columns: Tuple[str, ...] = _DEFAULT_FEATURE_COLUMNS
    test_fraction: float = 0.2
    max_rows: Optional[int] = None
    topk_fractions: Tuple[float, ...] = (0.01, 0.05, 0.1)


@dataclass
class AlphaBaselineSummary:
    total_rows: int
    train_rows: int
    test_rows: int
    positive_rate_train: float
    positive_rate_test: float
    roc_auc: Optional[float]
    threshold: float
    precision: float
    recall: float
    f1: float
    topk_precision: Dict[str, float]
    feature_columns: List[str]


@dataclass
class AlphaTwoStageConfig:
    input_csv_path: str
    output_json_path: str
    predictions_csv_path: Optional[str] = None
    feature_columns: Tuple[str, ...] = _DEFAULT_FEATURE_COLUMNS
    test_fraction: float = 0.2
    max_rows: Optional[int] = None
    topk_fractions: Tuple[float, ...] = (0.01, 0.05, 0.1)
    strict_negative_min_recent_condition_count_60s: int = 2
    strict_negative_min_recent_same_market_count_60s: int = 2
    strict_negative_max_candidate_abs_price_distance_from_mid: float = 0.25
    buy_stage_max_positive_negative_ratio: Optional[float] = 1.0


@dataclass
class AlphaDirectionSummary:
    total_rows: int
    train_rows: int
    test_rows: int
    accuracy: float
    precision_up: float
    recall_up: float
    f1_up: float
    positive_rate_up_train: float
    positive_rate_up_test: float


@dataclass
class AlphaTwoStageSummary:
    total_rows: int
    strict_rows: int
    strict_negative_rows: int
    feature_columns: List[str]
    buy_stage: AlphaBaselineSummary
    direction_stage: AlphaDirectionSummary


@dataclass
class AlphaSignalScorerConfig:
    input_csv_path: str
    output_json_path: str
    signals_csv_path: str
    feature_columns: Tuple[str, ...] = _DEFAULT_FEATURE_COLUMNS
    test_fraction: float = 0.2
    max_rows: Optional[int] = None
    strict_negative_min_recent_condition_count_60s: int = 2
    strict_negative_min_recent_same_market_count_60s: int = 2
    strict_negative_max_candidate_abs_price_distance_from_mid: float = 0.25
    buy_stage_max_positive_negative_ratio: Optional[float] = 1.0
    buy_threshold: float = 0.5
    final_threshold: float = 0.5


@dataclass
class AlphaSignalScorerSummary:
    total_rows: int
    scored_rows: int
    emitted_signals: int
    buy_threshold: float
    final_threshold: float
    average_buy_score: float
    average_final_score: float
    predicted_up_signal_ratio: float


@dataclass
class AlphaSignalReplayConfig:
    input_csv_path: str
    output_json_path: str
    deduped_signals_csv_path: Optional[str] = None
    bucket_metrics_csv_path: Optional[str] = None
    require_threshold_flags: bool = True
    min_buy_score: float = 0.0
    min_final_score: float = 0.0
    time_bucket_seconds: int = 15
    score_bucket_thresholds: Tuple[float, ...] = (0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9)


@dataclass
class AlphaSignalReplaySummary:
    total_rows: int
    filtered_rows: int
    outcome_deduped_rows: int
    condition_deduped_rows: int
    time_bucket_seconds: int
    average_buy_score: float
    average_final_score: float
    buy_precision: float
    direction_match_on_buys: float
    joint_precision: float
    predicted_up_ratio: float


@dataclass
class AlphaPaperReplayConfig:
    input_csv_path: str
    output_json_path: str
    price_cache_dir: str
    trades_csv_path: Optional[str] = None
    initial_capital_usdc: float = 10000.0
    fixed_order_usdc: float = 100.0
    max_concurrent_positions: Optional[int] = None
    settle_price_upper_threshold: float = 0.95
    settle_price_lower_threshold: float = 0.05


@dataclass
class AlphaPaperReplaySummary:
    total_signals: int
    executed_signals: int
    skipped_signals: int
    resolved_trades: int
    unresolved_trades: int
    wins: int
    losses: int
    initial_capital_usdc: float
    final_cash_usdc: float
    total_pnl_usdc: float
    total_return_pct: float
    average_trade_pnl_usdc: float
    average_win_pnl_usdc: float
    average_loss_pnl_usdc: float


def run_alpha_baseline(config: AlphaBaselineConfig) -> AlphaBaselineSummary:
    frame = pd.read_csv(config.input_csv_path)
    if config.max_rows:
        frame = frame.iloc[: config.max_rows].copy()
    frame = _augment_directional_features(frame)
    frame = frame.sort_values(["timestamp_seconds", "sample_id"]).reset_index(drop=True)
    if frame.empty:
        raise ValueError("input feature dataset is empty")

    if "label_buy" not in frame.columns:
        raise ValueError("missing required label column: label_buy")

    feature_columns = [column for column in config.feature_columns if column in frame.columns]
    if not feature_columns:
        raise ValueError("no requested feature columns found in input dataset")

    split_index = max(1, min(len(frame) - 1, int(len(frame) * (1.0 - config.test_fraction))))
    train = frame.iloc[:split_index].copy()
    test = frame.iloc[split_index:].copy()

    if train.empty or test.empty:
        raise ValueError("need both train and test rows after split")

    categorical_columns = [column for column in _CATEGORICAL_COLUMNS if column in feature_columns]
    numeric_columns = [column for column in feature_columns if column not in categorical_columns]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_columns,
            ),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_columns,
            ),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            (
                "classifier",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )

    X_train = train[feature_columns]
    y_train = train["label_buy"].astype(int)
    X_test = test[feature_columns]
    y_test = test["label_buy"].astype(int)

    model.fit(X_train, y_train)
    probabilities = model.predict_proba(X_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test,
        predictions,
        average="binary",
        zero_division=0,
    )

    roc_auc: Optional[float]
    if len(set(y_test.tolist())) < 2:
        roc_auc = None
    else:
        roc_auc = float(roc_auc_score(y_test, probabilities))

    topk_precision = _compute_topk_precision(probabilities, y_test.tolist(), config.topk_fractions)

    if config.predictions_csv_path:
        predictions_frame = test[
            [
                "sample_id",
                "timestamp_seconds",
                "market_slug",
                "candidate_outcome",
                "label_buy",
            ]
        ].copy()
        predictions_frame["predicted_probability"] = probabilities
        predictions_frame["predicted_label"] = predictions
        output_path = Path(config.predictions_csv_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        predictions_frame.to_csv(output_path, index=False)

    summary = AlphaBaselineSummary(
        total_rows=int(len(frame)),
        train_rows=int(len(train)),
        test_rows=int(len(test)),
        positive_rate_train=float(y_train.mean()),
        positive_rate_test=float(y_test.mean()),
        roc_auc=roc_auc,
        threshold=0.5,
        precision=float(precision),
        recall=float(recall),
        f1=float(f1),
        topk_precision=topk_precision,
        feature_columns=feature_columns,
    )
    output_path = Path(config.output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_alpha_two_stage_baseline(config: AlphaTwoStageConfig) -> AlphaTwoStageSummary:
    frame = pd.read_csv(config.input_csv_path)
    if config.max_rows:
        frame = frame.iloc[: config.max_rows].copy()
    frame = _augment_directional_features(frame)
    frame = frame.sort_values(["timestamp_seconds", "sample_id"]).reset_index(drop=True)
    if frame.empty:
        raise ValueError("input feature dataset is empty")

    feature_columns = [column for column in config.feature_columns if column in frame.columns]
    if not feature_columns:
        raise ValueError("no requested feature columns found in input dataset")

    strict_frame = _build_strict_negative_frame(
        frame,
        min_recent_condition_count=config.strict_negative_min_recent_condition_count_60s,
        min_recent_same_market_count=config.strict_negative_min_recent_same_market_count_60s,
        max_candidate_abs_price_distance=config.strict_negative_max_candidate_abs_price_distance_from_mid,
    )

    buy_summary, buy_predictions = _fit_buy_stage(
        frame=strict_frame,
        feature_columns=feature_columns,
        test_fraction=config.test_fraction,
        max_positive_negative_ratio=config.buy_stage_max_positive_negative_ratio,
    )
    direction_summary, direction_predictions = _fit_direction_stage(
        frame=frame,
        feature_columns=feature_columns,
        test_fraction=config.test_fraction,
    )

    if config.predictions_csv_path:
        output_path = Path(config.predictions_csv_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined = buy_predictions.merge(
            direction_predictions,
            how="outer",
            on=["sample_id", "timestamp_seconds", "market_slug"],
        )
        combined.to_csv(output_path, index=False)

    summary = AlphaTwoStageSummary(
        total_rows=int(len(frame)),
        strict_rows=int(len(strict_frame)),
        strict_negative_rows=int((strict_frame["label_buy"].astype(int) == 0).sum()),
        feature_columns=feature_columns,
        buy_stage=buy_summary,
        direction_stage=direction_summary,
    )
    output_path = Path(config.output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_alpha_signal_scorer(config: AlphaSignalScorerConfig) -> AlphaSignalScorerSummary:
    frame = pd.read_csv(config.input_csv_path)
    if config.max_rows:
        frame = frame.iloc[: config.max_rows].copy()
    frame = _augment_directional_features(frame)
    frame = frame.sort_values(["timestamp_seconds", "sample_id"]).reset_index(drop=True)
    if frame.empty:
        raise ValueError("input feature dataset is empty")

    feature_columns = [column for column in config.feature_columns if column in frame.columns]
    if not feature_columns:
        raise ValueError("no requested feature columns found in input dataset")

    strict_frame = _build_strict_negative_frame(
        frame,
        min_recent_condition_count=config.strict_negative_min_recent_condition_count_60s,
        min_recent_same_market_count=config.strict_negative_min_recent_same_market_count_60s,
        max_candidate_abs_price_distance=config.strict_negative_max_candidate_abs_price_distance_from_mid,
    )
    train_buy, test_buy = _time_split(strict_frame, config.test_fraction)
    train_buy = _rebalance_binary_frame(
        train_buy,
        label_column="label_buy",
        positive_value=1,
        negative_value=0,
        max_positive_negative_ratio=config.buy_stage_max_positive_negative_ratio,
        group_columns=("market_family", "market_duration_bucket", "candidate_outcome"),
    )

    buy_model = _build_classifier_pipeline(feature_columns)
    buy_model.fit(train_buy[list(feature_columns)], train_buy["label_buy"].astype(int))

    positive_frame = frame[frame["label_buy"].astype(int) == 1].copy()
    positive_frame = positive_frame[
        positive_frame["candidate_outcome"].astype(str).str.lower().isin({"up", "down"})
    ].copy()
    positive_frame["label_up"] = (
        positive_frame["candidate_outcome"].astype(str).str.lower() == "up"
    ).astype(int)
    train_dir, _ = _time_split(positive_frame, config.test_fraction)
    direction_feature_columns = [column for column in feature_columns if column != "candidate_outcome"]
    direction_model = _build_classifier_pipeline(direction_feature_columns)
    direction_model.fit(train_dir[list(direction_feature_columns)], train_dir["label_up"].astype(int))

    signals = test_buy.copy()
    signals["buy_score"] = buy_model.predict_proba(signals[list(feature_columns)])[:, 1]
    signals["up_score"] = direction_model.predict_proba(signals[list(direction_feature_columns)])[:, 1]
    outcome_is_up = signals["candidate_outcome"].astype(str).str.lower().eq("up")
    signals["outcome_probability"] = signals["up_score"].where(outcome_is_up, 1.0 - signals["up_score"])
    signals["final_score"] = signals["buy_score"] * signals["outcome_probability"]
    signals["passes_buy_threshold"] = signals["buy_score"] >= config.buy_threshold
    signals["passes_final_threshold"] = signals["final_score"] >= config.final_threshold
    signals["predicted_up"] = signals["up_score"] >= 0.5
    signals["predicted_outcome_match"] = signals["predicted_up"].where(outcome_is_up, ~signals["predicted_up"])

    output_columns = [
        "sample_id",
        "timestamp_seconds",
        "timestamp_iso",
        "market_slug",
        "condition_id",
        "candidate_outcome",
        "candidate_asset",
        "reference_trade_asset",
        "label_buy",
        "buy_score",
        "up_score",
        "outcome_probability",
        "final_score",
        "passes_buy_threshold",
        "passes_final_threshold",
        "predicted_up",
        "predicted_outcome_match",
        "market_family",
        "market_duration_bucket",
        "candidate_price",
        "seconds_to_resolution",
        "candidate_abs_price_distance_from_mid",
        "recent_outcome_count_imbalance_60s",
        "recent_outcome_usdc_imbalance_60s",
        "candidate_signed_external_return_60s",
        "candidate_signed_external_return_300s",
    ]
    existing_columns = [column for column in output_columns if column in signals.columns]
    output_path = Path(config.signals_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    signals[existing_columns].to_csv(output_path, index=False)

    emitted = signals[
        signals["passes_buy_threshold"] & signals["passes_final_threshold"]
    ]
    summary = AlphaSignalScorerSummary(
        total_rows=int(len(frame)),
        scored_rows=int(len(signals)),
        emitted_signals=int(len(emitted)),
        buy_threshold=float(config.buy_threshold),
        final_threshold=float(config.final_threshold),
        average_buy_score=float(signals["buy_score"].mean()),
        average_final_score=float(signals["final_score"].mean()),
        predicted_up_signal_ratio=float(emitted["predicted_up"].mean()) if not emitted.empty else 0.0,
    )
    summary_path = Path(config.output_json_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_alpha_signal_replay(config: AlphaSignalReplayConfig) -> AlphaSignalReplaySummary:
    frame = pd.read_csv(config.input_csv_path)
    if frame.empty:
        raise ValueError("input signal dataset is empty")

    required_columns = {"timestamp_seconds", "condition_id", "candidate_outcome", "buy_score", "final_score"}
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"missing required signal columns: {', '.join(missing_columns)}")

    working = frame.copy()
    if config.require_threshold_flags:
        if "passes_buy_threshold" in working.columns:
            working = working[working["passes_buy_threshold"].astype(bool)].copy()
        if "passes_final_threshold" in working.columns:
            working = working[working["passes_final_threshold"].astype(bool)].copy()

    working = working[
        (working["buy_score"].astype(float) >= config.min_buy_score)
        & (working["final_score"].astype(float) >= config.min_final_score)
    ].copy()

    if working.empty:
        summary = AlphaSignalReplaySummary(
            total_rows=int(len(frame)),
            filtered_rows=0,
            outcome_deduped_rows=0,
            condition_deduped_rows=0,
            time_bucket_seconds=int(config.time_bucket_seconds),
            average_buy_score=0.0,
            average_final_score=0.0,
            buy_precision=0.0,
            direction_match_on_buys=0.0,
            joint_precision=0.0,
            predicted_up_ratio=0.0,
        )
        summary_path = Path(config.output_json_path)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
        if config.bucket_metrics_csv_path:
            bucket_path = Path(config.bucket_metrics_csv_path)
            bucket_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                columns=[
                    "threshold",
                    "signal_count",
                    "buy_precision",
                    "direction_match_on_buys",
                    "joint_precision",
                    "predicted_up_ratio",
                ]
            ).to_csv(bucket_path, index=False)
        return summary

    bucket_seconds = max(1, int(config.time_bucket_seconds))
    working["timestamp_seconds"] = working["timestamp_seconds"].astype(int)
    working["time_bucket_start"] = (working["timestamp_seconds"] // bucket_seconds) * bucket_seconds
    if "predicted_up" not in working.columns:
        working["predicted_up"] = False
    if "predicted_outcome_match" not in working.columns:
        working["predicted_outcome_match"] = False
    if "label_buy" not in working.columns:
        working["label_buy"] = 0
    working["predicted_up"] = working["predicted_up"].astype(bool)
    working["predicted_outcome_match"] = working["predicted_outcome_match"].astype(bool)
    working["label_buy"] = working["label_buy"].astype(int)

    sort_columns = [column for column in ("final_score", "buy_score", "outcome_probability", "timestamp_seconds") if column in working.columns]
    working = working.sort_values(sort_columns, ascending=[False] * len(sort_columns))

    outcome_deduped = working.drop_duplicates(
        subset=["condition_id", "candidate_outcome", "time_bucket_start"],
        keep="first",
    ).copy()
    condition_deduped = outcome_deduped.drop_duplicates(
        subset=["condition_id", "time_bucket_start"],
        keep="first",
    ).copy()
    condition_deduped = condition_deduped.sort_values(["timestamp_seconds", "final_score"], ascending=[True, False])

    if config.deduped_signals_csv_path:
        output_path = Path(config.deduped_signals_csv_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        condition_deduped.to_csv(output_path, index=False)

    bucket_rows = []
    for threshold in config.score_bucket_thresholds:
        subset = condition_deduped[condition_deduped["final_score"].astype(float) >= float(threshold)].copy()
        bucket_rows.append(
            {
                "threshold": float(threshold),
                "signal_count": int(len(subset)),
                "buy_precision": _safe_mean(subset["label_buy"]),
                "direction_match_on_buys": _safe_mean(
                    subset.loc[subset["label_buy"].astype(int) == 1, "predicted_outcome_match"]
                ),
                "joint_precision": _safe_mean(
                    (
                        subset["label_buy"].astype(int).eq(1)
                        & subset["predicted_outcome_match"].astype(bool)
                    ).astype(int)
                ),
                "predicted_up_ratio": _safe_mean(subset["predicted_up"]),
            }
        )
    bucket_frame = pd.DataFrame(bucket_rows)
    if config.bucket_metrics_csv_path:
        bucket_path = Path(config.bucket_metrics_csv_path)
        bucket_path.parent.mkdir(parents=True, exist_ok=True)
        bucket_frame.to_csv(bucket_path, index=False)

    buy_mask = condition_deduped["label_buy"].astype(int) == 1
    summary = AlphaSignalReplaySummary(
        total_rows=int(len(frame)),
        filtered_rows=int(len(working)),
        outcome_deduped_rows=int(len(outcome_deduped)),
        condition_deduped_rows=int(len(condition_deduped)),
        time_bucket_seconds=bucket_seconds,
        average_buy_score=float(condition_deduped["buy_score"].astype(float).mean()),
        average_final_score=float(condition_deduped["final_score"].astype(float).mean()),
        buy_precision=_safe_mean(condition_deduped["label_buy"]),
        direction_match_on_buys=_safe_mean(condition_deduped.loc[buy_mask, "predicted_outcome_match"]),
        joint_precision=_safe_mean(
            (condition_deduped["label_buy"].astype(int).eq(1) & condition_deduped["predicted_outcome_match"].astype(bool)).astype(int)
        ),
        predicted_up_ratio=_safe_mean(condition_deduped["predicted_up"]),
    )
    summary_path = Path(config.output_json_path)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_alpha_paper_replay(config: AlphaPaperReplayConfig) -> AlphaPaperReplaySummary:
    frame = pd.read_csv(config.input_csv_path)
    if frame.empty:
        raise ValueError("input signal dataset is empty")

    required_columns = {"timestamp_seconds", "market_slug", "condition_id", "candidate_outcome", "candidate_price"}
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"missing required replay columns: {', '.join(missing_columns)}")

    working = frame.sort_values(["timestamp_seconds", "final_score"], ascending=[True, False]).reset_index(drop=True)
    price_lookup = PriceHistoryLookup(config.price_cache_dir)
    cash = float(config.initial_capital_usdc)
    open_positions: List[Dict[str, object]] = []
    executed_rows: List[Dict[str, object]] = []
    skipped = 0
    max_positions = config.max_concurrent_positions

    for row in working.to_dict(orient="records"):
        timestamp_seconds = int(row["timestamp_seconds"])
        open_positions, cash = _settle_positions_up_to_timestamp(
            open_positions=open_positions,
            timestamp_seconds=timestamp_seconds,
            price_lookup=price_lookup,
            cash=cash,
            upper_threshold=float(config.settle_price_upper_threshold),
            lower_threshold=float(config.settle_price_lower_threshold),
        )
        if cash < config.fixed_order_usdc - 1e-9:
            skipped += 1
            continue
        if max_positions is not None and len(open_positions) >= max_positions:
            skipped += 1
            continue

        candidate_price = float(row["candidate_price"])
        if candidate_price <= 0:
            skipped += 1
            continue

        resolution_ts = _signal_resolution_timestamp_seconds(row)
        size = float(config.fixed_order_usdc) / candidate_price
        trade_row = dict(row)
        trade_row.update(
            {
                "entry_timestamp_seconds": timestamp_seconds,
                "entry_price": candidate_price,
                "entry_usdc": float(config.fixed_order_usdc),
                "entry_size": size,
                "resolution_timestamp_seconds": resolution_ts,
                "status": "open",
                "exit_price": None,
                "payout_price": None,
                "exit_usdc": None,
                "pnl_usdc": None,
            }
        )
        open_positions.append(trade_row)
        executed_rows.append(trade_row)
        cash -= float(config.fixed_order_usdc)

    open_positions, cash = _settle_positions_up_to_timestamp(
        open_positions=open_positions,
        timestamp_seconds=10**12,
        price_lookup=price_lookup,
        cash=cash,
        upper_threshold=float(config.settle_price_upper_threshold),
        lower_threshold=float(config.settle_price_lower_threshold),
    )

    unresolved_count = 0
    wins = 0
    losses = 0
    total_pnl = 0.0
    win_pnls: List[float] = []
    loss_pnls: List[float] = []
    for trade in executed_rows:
        if trade["status"] != "resolved":
            unresolved_count += 1
            trade["pnl_usdc"] = 0.0 if trade["pnl_usdc"] is None else trade["pnl_usdc"]
            continue
        pnl = float(trade["pnl_usdc"])
        total_pnl += pnl
        if pnl >= 0:
            wins += 1
            win_pnls.append(pnl)
        else:
            losses += 1
            loss_pnls.append(pnl)

    if config.trades_csv_path:
        trades_path = Path(config.trades_csv_path)
        trades_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(executed_rows).to_csv(trades_path, index=False)

    final_cash = cash
    summary = AlphaPaperReplaySummary(
        total_signals=int(len(working)),
        executed_signals=int(len(executed_rows)),
        skipped_signals=int(skipped),
        resolved_trades=int(len(executed_rows) - unresolved_count),
        unresolved_trades=int(unresolved_count),
        wins=int(wins),
        losses=int(losses),
        initial_capital_usdc=float(config.initial_capital_usdc),
        final_cash_usdc=float(round(final_cash, 6)),
        total_pnl_usdc=float(round(final_cash - config.initial_capital_usdc, 6)),
        total_return_pct=float(round(((final_cash / config.initial_capital_usdc) - 1.0) * 100.0, 6)),
        average_trade_pnl_usdc=float(round(total_pnl / len(executed_rows), 6)) if executed_rows else 0.0,
        average_win_pnl_usdc=float(round(sum(win_pnls) / len(win_pnls), 6)) if win_pnls else 0.0,
        average_loss_pnl_usdc=float(round(sum(loss_pnls) / len(loss_pnls), 6)) if loss_pnls else 0.0,
    )
    output_path = Path(config.output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _compute_topk_precision(
    probabilities: Sequence[float],
    labels: Sequence[int],
    fractions: Sequence[float],
) -> Dict[str, float]:
    rows = sorted(zip(probabilities, labels), key=lambda item: item[0], reverse=True)
    results: Dict[str, float] = {}
    for fraction in fractions:
        key = f"top_{int(fraction * 100)}pct"
        count = max(1, int(len(rows) * fraction))
        subset = rows[:count]
        positives = sum(label for _, label in subset)
        results[key] = float(positives / count) if count else 0.0
    return results


def _augment_directional_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "recent_same_outcome_count_60s" in result.columns and "recent_opposite_outcome_count_60s" in result.columns:
        same = result["recent_same_outcome_count_60s"].fillna(0)
        opp = result["recent_opposite_outcome_count_60s"].fillna(0)
        denom = (same + opp).replace(0, float("nan"))
        result["recent_outcome_count_imbalance_60s"] = same - opp
        result["recent_outcome_count_share_60s"] = same / denom
    if "recent_same_outcome_usdc_60s" in result.columns and "recent_opposite_outcome_usdc_60s" in result.columns:
        same_usdc = result["recent_same_outcome_usdc_60s"].fillna(0)
        opp_usdc = result["recent_opposite_outcome_usdc_60s"].fillna(0)
        denom_usdc = (same_usdc + opp_usdc).replace(0, float("nan"))
        result["recent_outcome_usdc_imbalance_60s"] = same_usdc - opp_usdc
        result["recent_outcome_usdc_share_60s"] = same_usdc / denom_usdc
    if "recent_same_market_count_60s" in result.columns and "recent_same_market_outcome_count_60s" in result.columns:
        market_total = result["recent_same_market_count_60s"].fillna(0)
        market_same = result["recent_same_market_outcome_count_60s"].fillna(0)
        market_denom = market_total.replace(0, float("nan"))
        result["recent_market_outcome_count_share_60s"] = market_same / market_denom
        result["recent_market_outcome_count_imbalance_60s"] = (2 * market_same) - market_total
    if "recent_same_market_usdc_60s" in result.columns and "recent_same_market_outcome_usdc_60s" in result.columns:
        market_usdc_total = result["recent_same_market_usdc_60s"].fillna(0)
        market_usdc_same = result["recent_same_market_outcome_usdc_60s"].fillna(0)
        market_usdc_denom = market_usdc_total.replace(0, float("nan"))
        result["recent_market_outcome_usdc_share_60s"] = market_usdc_same / market_usdc_denom
        result["recent_market_outcome_usdc_imbalance_60s"] = (2 * market_usdc_same) - market_usdc_total
    if "candidate_price_distance_from_mid" in result.columns and "external_market_return_60s" in result.columns:
        sign = result["candidate_price_distance_from_mid"].fillna(0).apply(lambda x: 1.0 if x >= 0 else -1.0)
        result["candidate_signed_external_return_60s"] = sign * result["external_market_return_60s"].fillna(0)
    if "candidate_price_distance_from_mid" in result.columns and "external_market_return_300s" in result.columns:
        sign = result["candidate_price_distance_from_mid"].fillna(0).apply(lambda x: 1.0 if x >= 0 else -1.0)
        result["candidate_signed_external_return_300s"] = sign * result["external_market_return_300s"].fillna(0)
    return result


def _build_preprocessor(feature_columns: Sequence[str]) -> ColumnTransformer:
    categorical_columns = [column for column in _CATEGORICAL_COLUMNS if column in feature_columns]
    numeric_columns = [column for column in feature_columns if column not in categorical_columns]
    return ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_columns,
            ),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("onehot", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_columns,
            ),
        ]
    )


def _build_classifier_pipeline(feature_columns: Sequence[str]) -> Pipeline:
    return Pipeline(
        steps=[
            ("preprocessor", _build_preprocessor(feature_columns)),
            (
                "classifier",
                LogisticRegression(
                    max_iter=1000,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )


def _fit_buy_stage(
    frame: pd.DataFrame,
    feature_columns: Sequence[str],
    test_fraction: float,
    max_positive_negative_ratio: Optional[float],
) -> Tuple[AlphaBaselineSummary, pd.DataFrame]:
    train, test = _time_split(frame, test_fraction)
    train = _rebalance_binary_frame(
        train,
        label_column="label_buy",
        positive_value=1,
        negative_value=0,
        max_positive_negative_ratio=max_positive_negative_ratio,
        group_columns=("market_family", "market_duration_bucket", "candidate_outcome"),
    )
    test = _rebalance_binary_frame(
        test,
        label_column="label_buy",
        positive_value=1,
        negative_value=0,
        max_positive_negative_ratio=max_positive_negative_ratio,
        group_columns=("market_family", "market_duration_bucket", "candidate_outcome"),
    )
    model = _build_classifier_pipeline(feature_columns)

    X_train = train[list(feature_columns)]
    y_train = train["label_buy"].astype(int)
    X_test = test[list(feature_columns)]
    y_test = test["label_buy"].astype(int)

    model.fit(X_train, y_train)
    probabilities = model.predict_proba(X_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test,
        predictions,
        average="binary",
        zero_division=0,
    )
    roc_auc = None if len(set(y_test.tolist())) < 2 else float(roc_auc_score(y_test, probabilities))
    summary = AlphaBaselineSummary(
        total_rows=int(len(frame)),
        train_rows=int(len(train)),
        test_rows=int(len(test)),
        positive_rate_train=float(y_train.mean()),
        positive_rate_test=float(y_test.mean()),
        roc_auc=roc_auc,
        threshold=0.5,
        precision=float(precision),
        recall=float(recall),
        f1=float(f1),
        topk_precision=_compute_topk_precision(probabilities, y_test.tolist(), (0.01, 0.05, 0.1)),
        feature_columns=list(feature_columns),
    )
    predictions_frame = test[
        ["sample_id", "timestamp_seconds", "market_slug", "candidate_outcome", "label_buy"]
    ].copy()
    predictions_frame["buy_probability"] = probabilities
    predictions_frame["buy_predicted_label"] = predictions
    return summary, predictions_frame


def _fit_direction_stage(
    frame: pd.DataFrame,
    feature_columns: Sequence[str],
    test_fraction: float,
) -> Tuple[AlphaDirectionSummary, pd.DataFrame]:
    direction_frame = frame[frame["label_buy"].astype(int) == 1].copy()
    direction_frame = direction_frame[
        direction_frame["candidate_outcome"].astype(str).str.lower().isin({"up", "down"})
    ].copy()
    if direction_frame.empty:
        raise ValueError("no positive up/down rows available for direction stage")

    direction_frame["label_up"] = (
        direction_frame["candidate_outcome"].astype(str).str.lower() == "up"
    ).astype(int)
    train, test = _time_split(direction_frame, test_fraction)
    direction_feature_columns = [column for column in feature_columns if column != "candidate_outcome"]
    model = _build_classifier_pipeline(direction_feature_columns)
    X_train = train[list(direction_feature_columns)]
    y_train = train["label_up"].astype(int)
    X_test = test[list(direction_feature_columns)]
    y_test = test["label_up"].astype(int)

    model.fit(X_train, y_train)
    probabilities = model.predict_proba(X_test)[:, 1]
    predictions = (probabilities >= 0.5).astype(int)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test,
        predictions,
        average="binary",
        zero_division=0,
    )
    summary = AlphaDirectionSummary(
        total_rows=int(len(direction_frame)),
        train_rows=int(len(train)),
        test_rows=int(len(test)),
        accuracy=float(accuracy_score(y_test, predictions)),
        precision_up=float(precision),
        recall_up=float(recall),
        f1_up=float(f1),
        positive_rate_up_train=float(y_train.mean()),
        positive_rate_up_test=float(y_test.mean()),
    )
    predictions_frame = test[
        ["sample_id", "timestamp_seconds", "market_slug", "candidate_outcome"]
    ].copy()
    predictions_frame["direction_probability_up"] = probabilities
    predictions_frame["direction_predicted_up"] = predictions
    predictions_frame["direction_label_up"] = y_test.values
    return summary, predictions_frame


def _build_strict_negative_frame(
    frame: pd.DataFrame,
    min_recent_condition_count: int,
    min_recent_same_market_count: int,
    max_candidate_abs_price_distance: float,
) -> pd.DataFrame:
    positives = frame[frame["label_buy"].astype(int) == 1].copy()
    negatives = frame[frame["label_buy"].astype(int) == 0].copy()
    if negatives.empty:
        return frame.copy()
    strict_negatives = negatives[
        (negatives["recent_condition_count_60s"].fillna(0) >= min_recent_condition_count)
        & (negatives["recent_same_market_count_60s"].fillna(0) >= min_recent_same_market_count)
        & (negatives["candidate_abs_price_distance_from_mid"].fillna(1.0) <= max_candidate_abs_price_distance)
    ].copy()
    combined = pd.concat([positives, strict_negatives], ignore_index=True)
    return combined.sort_values(["timestamp_seconds", "sample_id"]).reset_index(drop=True)


def _time_split(frame: pd.DataFrame, test_fraction: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    split_index = max(1, min(len(frame) - 1, int(len(frame) * (1.0 - test_fraction))))
    train = frame.iloc[:split_index].copy()
    test = frame.iloc[split_index:].copy()
    if train.empty or test.empty:
        raise ValueError("need both train and test rows after split")
    return train, test


def _rebalance_binary_frame(
    frame: pd.DataFrame,
    label_column: str,
    positive_value: int,
    negative_value: int,
    max_positive_negative_ratio: Optional[float],
    group_columns: Sequence[str],
) -> pd.DataFrame:
    if max_positive_negative_ratio is None:
        return frame.copy()
    groups: List[pd.DataFrame] = []
    for _, group in frame.groupby(list(group_columns), dropna=False, sort=False):
        positives = group[group[label_column].astype(int) == positive_value]
        negatives = group[group[label_column].astype(int) == negative_value]
        if negatives.empty or positives.empty:
            groups.append(group)
            continue
        max_positives = int(max(1, len(negatives) * max_positive_negative_ratio))
        if len(positives) > max_positives:
            positives = positives.sample(n=max_positives, random_state=42)
        groups.append(pd.concat([positives, negatives], ignore_index=False))
    if not groups:
        return frame.copy()
    combined = pd.concat(groups, ignore_index=False)
    return combined.sort_values(["timestamp_seconds", "sample_id"]).reset_index(drop=True)


def _safe_mean(series: pd.Series) -> float:
    if series.empty:
        return 0.0
    return float(series.astype(float).mean())


def _signal_resolution_timestamp_seconds(row: Dict[str, object]) -> Optional[int]:
    seconds_to_resolution = row.get("seconds_to_resolution")
    timestamp_seconds = int(row.get("timestamp_seconds") or 0)
    if seconds_to_resolution is not None and pd.notna(seconds_to_resolution):
        try:
            value = int(float(seconds_to_resolution))
        except (TypeError, ValueError):
            value = None
        if value is not None and value >= 0:
            return timestamp_seconds + value
    market_slug = str(row.get("market_slug") or "")
    duration_match = re.search(r"-(\d+)([mh])-([0-9]{10})$", market_slug)
    if duration_match:
        duration = int(duration_match.group(1))
        multiplier = 60 if duration_match.group(2) == "m" else 3600
        start_ts = int(duration_match.group(3))
        return start_ts + duration * multiplier
    return None


def _settle_positions_up_to_timestamp(
    open_positions: List[Dict[str, object]],
    timestamp_seconds: int,
    price_lookup: PriceHistoryLookup,
    cash: float,
    upper_threshold: float,
    lower_threshold: float,
) -> Tuple[List[Dict[str, object]], float]:
    remaining_positions: List[Dict[str, object]] = []
    for position in open_positions:
        resolution_ts = position.get("resolution_timestamp_seconds")
        if resolution_ts is None or int(resolution_ts) > timestamp_seconds:
            remaining_positions.append(position)
            continue
        asset_id = str(position.get("candidate_asset") or "")
        if not asset_id:
            remaining_positions.append(position)
            continue
        settle_price = price_lookup.get_price(asset_id, int(resolution_ts))
        if settle_price is None:
            remaining_positions.append(position)
            continue
        payout_price = settle_price
        if settle_price >= upper_threshold:
            payout_price = 1.0
        elif settle_price <= lower_threshold:
            payout_price = 0.0
        entry_size = float(position.get("entry_size") or 0.0)
        exit_usdc = entry_size * payout_price
        pnl = exit_usdc - float(position.get("entry_usdc") or 0.0)
        cash += exit_usdc
        position["status"] = "resolved"
        position["exit_price"] = float(settle_price)
        position["payout_price"] = float(payout_price)
        position["exit_usdc"] = float(exit_usdc)
        position["pnl_usdc"] = float(pnl)
    return remaining_positions, cash
