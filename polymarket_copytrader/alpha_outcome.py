from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, precision_recall_fscore_support, r2_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .alpha_baseline import _DEFAULT_FEATURE_COLUMNS
from .alpha_features import PriceHistoryLookup

_CATEGORICAL_COLUMNS: Tuple[str, ...] = (
    "market_family",
    "market_duration_bucket",
    "candidate_outcome",
)


@dataclass
class AlphaOutcomeLabelConfig:
    input_csv_path: str
    output_csv_path: str
    output_json_path: str
    price_cache_dir: str
    candidate_asset_column: str = "candidate_asset"
    candidate_price_column: str = "candidate_price"
    timestamp_column: str = "timestamp_seconds"
    resolution_column: str = "resolution_timestamp_seconds"
    seconds_to_resolution_column: str = "seconds_to_resolution"
    payout_upper_threshold: float = 0.95
    payout_lower_threshold: float = 0.05
    stake_usdc: float = 100.0


@dataclass
class AlphaOutcomeLabelSummary:
    total_rows: int
    labeled_rows: int
    unresolved_rows: int
    profitable_rows: int
    unprofitable_rows: int
    average_payout_price: float
    average_pnl_per_stake_usdc: float


@dataclass
class AlphaOutcomeBaselineConfig:
    input_csv_path: str
    output_json_path: str
    predictions_csv_path: Optional[str] = None
    feature_columns: Tuple[str, ...] = _DEFAULT_FEATURE_COLUMNS
    test_fraction: float = 0.2
    max_rows: Optional[int] = None
    positive_label_column: str = "label_profit_positive"


@dataclass
class AlphaOutcomeBaselineSummary:
    total_rows: int
    resolved_rows: int
    train_rows: int
    test_rows: int
    positive_rate_train: float
    positive_rate_test: float
    roc_auc: Optional[float]
    precision: float
    recall: float
    f1: float
    average_realized_pnl_test: float
    average_predicted_positive_pnl_test: float
    feature_columns: List[str]


@dataclass
class AlphaOutcomeRegressionConfig:
    input_csv_path: str
    output_json_path: str
    predictions_csv_path: Optional[str] = None
    feature_columns: Tuple[str, ...] = _DEFAULT_FEATURE_COLUMNS
    test_fraction: float = 0.2
    max_rows: Optional[int] = None
    target_column: str = "pnl_per_stake_usdc"
    topk_fractions: Tuple[float, ...] = (0.01, 0.05, 0.1, 0.2)


@dataclass
class AlphaOutcomeRegressionSummary:
    total_rows: int
    resolved_rows: int
    train_rows: int
    test_rows: int
    average_realized_pnl_test: float
    average_predicted_pnl_test: float
    mae: float
    rmse: float
    r2: float
    sign_accuracy: float
    topk_realized_pnl: dict
    feature_columns: List[str]


@dataclass
class AlphaTopKPaperStrategyConfig:
    input_csv_path: str
    output_json_path: str
    curve_csv_path: Optional[str] = None
    trades_csv_path: Optional[str] = None
    initial_capital_usdc: float = 10000.0
    stake_usdc: float = 100.0
    label_stake_usdc: float = 100.0
    max_concurrent_positions: Optional[int] = None
    dedupe_mode: str = "one_market_total"
    selection_mode: str = "top_fraction"
    top_fraction: float = 0.1
    min_predicted_pnl: float = 0.0


@dataclass
class AlphaTopKPaperStrategySummary:
    total_rows: int
    deduped_rows: int
    selected_rows: int
    executed_trades: int
    skipped_trades: int
    wins: int
    losses: int
    initial_capital_usdc: float
    final_equity_usdc: float
    total_pnl_usdc: float
    total_return_pct: float
    max_drawdown_pct: float
    average_trade_pnl_usdc: float
    average_win_pnl_usdc: float
    average_loss_pnl_usdc: float


@dataclass
class AlphaTopKWalkForwardConfig:
    input_csv_path: str
    output_json_path: str
    folds_csv_path: Optional[str] = None
    curve_csv_path: Optional[str] = None
    trades_csv_path: Optional[str] = None
    predictions_csv_path: Optional[str] = None
    feature_columns: Tuple[str, ...] = _DEFAULT_FEATURE_COLUMNS
    target_column: str = "pnl_per_stake_usdc"
    initial_train_fraction: float = 0.5
    n_folds: int = 5
    max_rows: Optional[int] = None
    initial_capital_usdc: float = 10000.0
    stake_usdc: float = 100.0
    label_stake_usdc: float = 100.0
    max_concurrent_positions: Optional[int] = None
    dedupe_mode: str = "one_market_total"
    selection_mode: str = "top_fraction"
    top_fraction: float = 0.1
    min_predicted_pnl: float = 0.0


@dataclass
class AlphaTopKWalkForwardSummary:
    total_rows: int
    resolved_rows: int
    initial_train_rows: int
    folds: int
    combined_test_rows: int
    average_fold_realized_pnl: float
    average_fold_predicted_pnl: float
    average_fold_mae: float
    average_fold_rmse: float
    average_fold_r2: float
    average_fold_sign_accuracy: float
    final_equity_usdc: float
    total_pnl_usdc: float
    total_return_pct: float
    max_drawdown_pct: float
    executed_trades: int
    wins: int
    losses: int


@dataclass
class AlphaTopKPaperReplayConfig:
    input_csv_path: str
    output_json_path: str
    source_csv_path: Optional[str] = None
    curve_csv_path: Optional[str] = None
    trades_csv_path: Optional[str] = None
    initial_capital_usdc: float = 10000.0
    stake_usdc: float = 100.0
    max_concurrent_positions: Optional[int] = None
    dedupe_mode: str = "one_market_total"
    selection_mode: str = "top_fraction"
    top_fraction: float = 0.1
    min_predicted_pnl: float = 0.0
    entry_slippage_bps: float = 0.0
    fee_bps: float = 0.0
    max_entry_price: float = 0.95
    min_seconds_to_resolution: float = 0.0


@dataclass
class AlphaTopKPaperReplaySummary:
    total_rows: int
    enriched_rows: int
    deduped_rows: int
    selected_rows: int
    executed_trades: int
    skipped_trades: int
    skipped_for_price: int
    skipped_for_resolution: int
    wins: int
    losses: int
    initial_capital_usdc: float
    final_equity_usdc: float
    total_pnl_usdc: float
    total_return_pct: float
    max_drawdown_pct: float
    average_trade_pnl_usdc: float
    average_win_pnl_usdc: float
    average_loss_pnl_usdc: float


def run_alpha_outcome_labels(config: AlphaOutcomeLabelConfig) -> AlphaOutcomeLabelSummary:
    frame = pd.read_csv(config.input_csv_path)
    if frame.empty:
        raise ValueError("input feature dataset is empty")

    required_columns = {
        config.candidate_asset_column,
        config.candidate_price_column,
        config.timestamp_column,
    }
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"missing required columns: {', '.join(missing_columns)}")

    price_lookup = PriceHistoryLookup(config.price_cache_dir)
    payout_prices: List[Optional[float]] = []
    resolution_timestamps: List[Optional[int]] = []
    settle_mark_prices: List[Optional[float]] = []
    pnl_values: List[Optional[float]] = []
    profitable_labels: List[Optional[int]] = []
    resolved_flags: List[int] = []

    stake = float(config.stake_usdc)
    for row in frame.to_dict(orient="records"):
        resolution_ts = _row_resolution_timestamp_seconds(
            row=row,
            timestamp_column=config.timestamp_column,
            resolution_column=config.resolution_column,
            seconds_to_resolution_column=config.seconds_to_resolution_column,
        )
        resolution_timestamps.append(resolution_ts)

        asset_id = str(row.get(config.candidate_asset_column) or "").strip()
        candidate_price_raw = row.get(config.candidate_price_column)
        try:
            candidate_price = float(candidate_price_raw)
        except (TypeError, ValueError):
            candidate_price = 0.0
        if not asset_id or candidate_price <= 0 or resolution_ts is None:
            settle_mark_prices.append(None)
            payout_prices.append(None)
            pnl_values.append(None)
            profitable_labels.append(None)
            resolved_flags.append(0)
            continue

        settle_mark = price_lookup.get_price(asset_id, int(resolution_ts))
        settle_mark_prices.append(settle_mark)
        if settle_mark is None:
            payout_prices.append(None)
            pnl_values.append(None)
            profitable_labels.append(None)
            resolved_flags.append(0)
            continue

        if settle_mark >= config.payout_upper_threshold:
            payout_price = 1.0
        elif settle_mark <= config.payout_lower_threshold:
            payout_price = 0.0
        else:
            payout_price = float(settle_mark)
        pnl = stake * ((payout_price / candidate_price) - 1.0)
        payout_prices.append(float(payout_price))
        pnl_values.append(float(pnl))
        profitable_labels.append(1 if pnl > 0 else 0)
        resolved_flags.append(1)

    labeled = frame.copy()
    labeled["resolution_timestamp_seconds"] = resolution_timestamps
    labeled["settle_mark_price"] = settle_mark_prices
    labeled["payout_price"] = payout_prices
    labeled["label_resolved"] = resolved_flags
    labeled["label_profit_positive"] = profitable_labels
    labeled["pnl_per_stake_usdc"] = pnl_values
    output_csv = Path(config.output_csv_path)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    labeled.to_csv(output_csv, index=False)

    resolved_frame = labeled[labeled["label_resolved"].astype(int) == 1].copy()
    pnl_series = resolved_frame["pnl_per_stake_usdc"].astype(float) if not resolved_frame.empty else pd.Series(dtype=float)
    payout_series = resolved_frame["payout_price"].astype(float) if not resolved_frame.empty else pd.Series(dtype=float)
    summary = AlphaOutcomeLabelSummary(
        total_rows=int(len(labeled)),
        labeled_rows=int(len(resolved_frame)),
        unresolved_rows=int(len(labeled) - len(resolved_frame)),
        profitable_rows=int((resolved_frame["label_profit_positive"].astype(int) == 1).sum()) if not resolved_frame.empty else 0,
        unprofitable_rows=int((resolved_frame["label_profit_positive"].astype(int) == 0).sum()) if not resolved_frame.empty else 0,
        average_payout_price=float(round(payout_series.mean(), 6)) if not payout_series.empty else 0.0,
        average_pnl_per_stake_usdc=float(round(pnl_series.mean(), 6)) if not pnl_series.empty else 0.0,
    )
    output_json = Path(config.output_json_path)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_alpha_outcome_baseline(config: AlphaOutcomeBaselineConfig) -> AlphaOutcomeBaselineSummary:
    frame = pd.read_csv(config.input_csv_path)
    if config.max_rows:
        frame = frame.iloc[: config.max_rows].copy()
    if frame.empty:
        raise ValueError("input labeled outcome dataset is empty")
    if "label_resolved" not in frame.columns:
        raise ValueError("missing required label column: label_resolved")
    if config.positive_label_column not in frame.columns:
        raise ValueError(f"missing required label column: {config.positive_label_column}")
    if "pnl_per_stake_usdc" not in frame.columns:
        raise ValueError("missing required label column: pnl_per_stake_usdc")

    frame = frame[frame["label_resolved"].astype(int) == 1].copy()
    frame = frame.sort_values(["timestamp_seconds", "sample_id"]).reset_index(drop=True)
    if frame.empty:
        raise ValueError("no resolved rows available for outcome baseline")

    feature_columns = [column for column in config.feature_columns if column in frame.columns]
    if not feature_columns:
        raise ValueError("no requested feature columns found in labeled outcome dataset")

    train, test = _time_split(frame, config.test_fraction)
    model = _build_classifier_pipeline(feature_columns)

    X_train = train[feature_columns]
    y_train = train[config.positive_label_column].astype(int)
    X_test = test[feature_columns]
    y_test = test[config.positive_label_column].astype(int)

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

    pnl_test = test["pnl_per_stake_usdc"].astype(float)
    positive_predicate = predictions == 1
    positive_pnl_mean = float(pnl_test[positive_predicate].mean()) if positive_predicate.any() else 0.0

    if config.predictions_csv_path:
        predictions_frame = test[
            ["sample_id", "timestamp_seconds", "market_slug", "candidate_outcome", config.positive_label_column, "pnl_per_stake_usdc"]
        ].copy()
        predictions_frame["predicted_probability"] = probabilities
        predictions_frame["predicted_label"] = predictions
        output_path = Path(config.predictions_csv_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        predictions_frame.to_csv(output_path, index=False)

    summary = AlphaOutcomeBaselineSummary(
        total_rows=int(len(pd.read_csv(config.input_csv_path))),
        resolved_rows=int(len(frame)),
        train_rows=int(len(train)),
        test_rows=int(len(test)),
        positive_rate_train=float(y_train.mean()),
        positive_rate_test=float(y_test.mean()),
        roc_auc=roc_auc,
        precision=float(precision),
        recall=float(recall),
        f1=float(f1),
        average_realized_pnl_test=float(round(pnl_test.mean(), 6)) if not pnl_test.empty else 0.0,
        average_predicted_positive_pnl_test=float(round(positive_pnl_mean, 6)),
        feature_columns=feature_columns,
    )
    output_json = Path(config.output_json_path)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_alpha_outcome_regression(config: AlphaOutcomeRegressionConfig) -> AlphaOutcomeRegressionSummary:
    frame, feature_columns, total_rows = _load_regression_frame(
        input_csv_path=config.input_csv_path,
        max_rows=config.max_rows,
        feature_columns=config.feature_columns,
        target_column=config.target_column,
    )
    train, test = _time_split(frame, config.test_fraction)
    predictions, regression_metrics = _fit_predict_regression(
        train=train,
        test=test,
        feature_columns=feature_columns,
        target_column=config.target_column,
    )
    y_test = test[config.target_column].astype(float)
    topk_realized_pnl = _compute_topk_realized_pnl(predictions, y_test.tolist(), config.topk_fractions)

    if config.predictions_csv_path:
        predictions_frame = _build_outcome_prediction_frame(test, config.target_column, predictions)
        output_path = Path(config.predictions_csv_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        predictions_frame.to_csv(output_path, index=False)

    summary = AlphaOutcomeRegressionSummary(
        total_rows=total_rows,
        resolved_rows=int(len(frame)),
        train_rows=int(len(train)),
        test_rows=int(len(test)),
        average_realized_pnl_test=float(round(y_test.mean(), 6)) if not y_test.empty else 0.0,
        average_predicted_pnl_test=float(round(float(pd.Series(predictions).mean()), 6)) if len(predictions) else 0.0,
        mae=regression_metrics["mae"],
        rmse=regression_metrics["rmse"],
        r2=regression_metrics["r2"],
        sign_accuracy=regression_metrics["sign_accuracy"],
        topk_realized_pnl=topk_realized_pnl,
        feature_columns=feature_columns,
    )
    output_json = Path(config.output_json_path)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_alpha_topk_paper_strategy(config: AlphaTopKPaperStrategyConfig) -> AlphaTopKPaperStrategySummary:
    frame = pd.read_csv(config.input_csv_path)
    if frame.empty:
        raise ValueError("input prediction dataset is empty")
    summary, curve, executed = _run_topk_paper_strategy_frame(frame, config)

    if config.curve_csv_path:
        curve_path = Path(config.curve_csv_path)
        curve_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(curve).to_csv(curve_path, index=False)
    if config.trades_csv_path:
        trades_path = Path(config.trades_csv_path)
        trades_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(executed).to_csv(trades_path, index=False)

    output_json = Path(config.output_json_path)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_alpha_topk_walkforward(config: AlphaTopKWalkForwardConfig) -> AlphaTopKWalkForwardSummary:
    frame, feature_columns, total_rows = _load_regression_frame(
        input_csv_path=config.input_csv_path,
        max_rows=config.max_rows,
        feature_columns=config.feature_columns,
        target_column=config.target_column,
    )
    initial_train_rows = max(1, min(len(frame) - 1, int(len(frame) * config.initial_train_fraction)))
    remaining_rows = len(frame) - initial_train_rows
    if remaining_rows < config.n_folds:
        raise ValueError("not enough rows for requested number of folds")

    fold_size = max(1, remaining_rows // config.n_folds)
    fold_summaries: List[dict] = []
    prediction_frames: List[pd.DataFrame] = []
    train_end = initial_train_rows

    for fold_index in range(config.n_folds):
        test_start = train_end
        test_end = len(frame) if fold_index == config.n_folds - 1 else min(len(frame), test_start + fold_size)
        train = frame.iloc[:test_start].copy()
        test = frame.iloc[test_start:test_end].copy()
        if train.empty or test.empty:
            continue
        predictions, metrics = _fit_predict_regression(
            train=train,
            test=test,
            feature_columns=feature_columns,
            target_column=config.target_column,
        )
        prediction_frame = _build_outcome_prediction_frame(test, config.target_column, predictions)
        prediction_frame["fold_index"] = fold_index
        prediction_frames.append(prediction_frame)
        fold_summaries.append(
            {
                "fold_index": int(fold_index),
                "train_rows": int(len(train)),
                "test_rows": int(len(test)),
                "train_end_timestamp_seconds": int(train["timestamp_seconds"].max()),
                "test_start_timestamp_seconds": int(test["timestamp_seconds"].min()),
                "test_end_timestamp_seconds": int(test["timestamp_seconds"].max()),
                "average_realized_pnl_test": float(round(test[config.target_column].astype(float).mean(), 6)),
                "average_predicted_pnl_test": float(round(float(pd.Series(predictions).mean()), 6)),
                **metrics,
            }
        )
        train_end = test_end

    if not prediction_frames:
        raise ValueError("walk-forward produced no out-of-sample predictions")

    combined_predictions = pd.concat(prediction_frames, ignore_index=True)
    combined_predictions = combined_predictions.sort_values(["timestamp_seconds", "sample_id"]).reset_index(drop=True)

    strategy_summary, curve, trades = _run_topk_paper_strategy_frame(
        combined_predictions,
        AlphaTopKPaperStrategyConfig(
            input_csv_path=config.input_csv_path,
            output_json_path=config.output_json_path,
            initial_capital_usdc=config.initial_capital_usdc,
            stake_usdc=config.stake_usdc,
            label_stake_usdc=config.label_stake_usdc,
            max_concurrent_positions=config.max_concurrent_positions,
            dedupe_mode=config.dedupe_mode,
            selection_mode=config.selection_mode,
            top_fraction=config.top_fraction,
            min_predicted_pnl=config.min_predicted_pnl,
        ),
    )

    summary = AlphaTopKWalkForwardSummary(
        total_rows=total_rows,
        resolved_rows=int(len(frame)),
        initial_train_rows=int(initial_train_rows),
        folds=int(len(fold_summaries)),
        combined_test_rows=int(len(combined_predictions)),
        average_fold_realized_pnl=float(round(pd.DataFrame(fold_summaries)["average_realized_pnl_test"].mean(), 6)),
        average_fold_predicted_pnl=float(round(pd.DataFrame(fold_summaries)["average_predicted_pnl_test"].mean(), 6)),
        average_fold_mae=float(round(pd.DataFrame(fold_summaries)["mae"].mean(), 6)),
        average_fold_rmse=float(round(pd.DataFrame(fold_summaries)["rmse"].mean(), 6)),
        average_fold_r2=float(round(pd.DataFrame(fold_summaries)["r2"].mean(), 6)),
        average_fold_sign_accuracy=float(round(pd.DataFrame(fold_summaries)["sign_accuracy"].mean(), 6)),
        final_equity_usdc=strategy_summary.final_equity_usdc,
        total_pnl_usdc=strategy_summary.total_pnl_usdc,
        total_return_pct=strategy_summary.total_return_pct,
        max_drawdown_pct=strategy_summary.max_drawdown_pct,
        executed_trades=strategy_summary.executed_trades,
        wins=strategy_summary.wins,
        losses=strategy_summary.losses,
    )

    if config.folds_csv_path:
        folds_path = Path(config.folds_csv_path)
        folds_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(fold_summaries).to_csv(folds_path, index=False)
    if config.curve_csv_path:
        curve_path = Path(config.curve_csv_path)
        curve_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(curve).to_csv(curve_path, index=False)
    if config.trades_csv_path:
        trades_path = Path(config.trades_csv_path)
        trades_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(trades).to_csv(trades_path, index=False)
    if config.predictions_csv_path:
        predictions_path = Path(config.predictions_csv_path)
        predictions_path.parent.mkdir(parents=True, exist_ok=True)
        combined_predictions.to_csv(predictions_path, index=False)

    output_json = Path(config.output_json_path)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_alpha_topk_paper_replay(config: AlphaTopKPaperReplayConfig) -> AlphaTopKPaperReplaySummary:
    frame = pd.read_csv(config.input_csv_path)
    if frame.empty:
        raise ValueError("input prediction dataset is empty")

    working = frame.copy()
    source_frame: Optional[pd.DataFrame] = None
    if config.source_csv_path:
        source_frame = pd.read_csv(config.source_csv_path)
    working = _enrich_prediction_rows(working, source_frame)

    required_columns = {
        "sample_id",
        "timestamp_seconds",
        "market_slug",
        "candidate_outcome",
        "candidate_price",
        "payout_price",
        "predicted_pnl_per_stake_usdc",
        "resolution_timestamp_seconds",
    }
    missing_columns = sorted(required_columns.difference(working.columns))
    if missing_columns:
        raise ValueError(f"missing required replay columns: {', '.join(missing_columns)}")

    if "condition_id" not in working.columns:
        working["condition_id"] = working["market_slug"]
    if "seconds_to_resolution" not in working.columns:
        working["seconds_to_resolution"] = (
            pd.to_numeric(working["resolution_timestamp_seconds"], errors="coerce").fillna(0)
            - pd.to_numeric(working["timestamp_seconds"], errors="coerce").fillna(0)
        )

    deduped = _dedupe_strategy_candidates(working, config.dedupe_mode)
    selected = _select_strategy_candidates(deduped, config.selection_mode, config.top_fraction, config.min_predicted_pnl)
    selected = selected.sort_values(["timestamp_seconds", "predicted_pnl_per_stake_usdc"], ascending=[True, False]).reset_index(drop=True)

    cash = float(config.initial_capital_usdc)
    open_positions: List[dict] = []
    executed: List[dict] = []
    curve: List[dict] = []
    skipped = 0
    skipped_for_price = 0
    skipped_for_resolution = 0
    event_timestamps = sorted(
        set(selected["timestamp_seconds"].astype(int).tolist())
        | set(selected["resolution_timestamp_seconds"].astype(int).tolist())
    )
    selected_by_entry = {}
    for row in selected.to_dict(orient="records"):
        selected_by_entry.setdefault(int(row["timestamp_seconds"]), []).append(row)

    fee_multiplier = 1.0 + (float(config.fee_bps) / 10000.0)
    slippage_multiplier = 1.0 + (float(config.entry_slippage_bps) / 10000.0)

    for timestamp_seconds in event_timestamps:
        open_positions, cash, settled_now = _settle_replay_positions(open_positions, timestamp_seconds, cash)
        for trade in settled_now:
            executed.append(trade)
        for row in selected_by_entry.get(timestamp_seconds, []):
            if cash < config.stake_usdc - 1e-9:
                skipped += 1
                continue
            if config.max_concurrent_positions is not None and len(open_positions) >= config.max_concurrent_positions:
                skipped += 1
                continue
            seconds_to_resolution = float(row.get("seconds_to_resolution") or 0.0)
            if seconds_to_resolution < float(config.min_seconds_to_resolution):
                skipped += 1
                skipped_for_resolution += 1
                continue
            candidate_price = float(row["candidate_price"])
            if candidate_price <= 0:
                skipped += 1
                skipped_for_price += 1
                continue
            entry_price = candidate_price * slippage_multiplier
            if entry_price <= 0 or entry_price >= 1.0 or entry_price > float(config.max_entry_price):
                skipped += 1
                skipped_for_price += 1
                continue
            gross_entry_usdc = float(config.stake_usdc)
            net_buying_power = gross_entry_usdc / fee_multiplier
            size = net_buying_power / entry_price
            payout_price = float(row["payout_price"])
            exit_usdc = size * payout_price
            pnl = exit_usdc - gross_entry_usdc
            position = dict(row)
            position["entry_price"] = float(round(entry_price, 6))
            position["stake_usdc"] = float(gross_entry_usdc)
            position["entry_size"] = float(size)
            position["exit_cash_usdc"] = float(exit_usdc)
            position["scaled_pnl_usdc"] = float(pnl)
            open_positions.append(position)
            cash -= gross_entry_usdc
        equity = cash + sum(float(item["exit_cash_usdc"]) for item in open_positions)
        curve.append(
            {
                "timestamp_seconds": int(timestamp_seconds),
                "cash_usdc": round(float(cash), 6),
                "equity_usdc": round(float(equity), 6),
                "open_positions": int(len(open_positions)),
            }
        )

    if open_positions:
        final_timestamp = max(int(item["resolution_timestamp_seconds"]) for item in open_positions)
        open_positions, cash, settled_now = _settle_replay_positions(open_positions, final_timestamp, cash)
        for trade in settled_now:
            executed.append(trade)
        curve.append(
            {
                "timestamp_seconds": int(final_timestamp),
                "cash_usdc": round(float(cash), 6),
                "equity_usdc": round(float(cash), 6),
                "open_positions": 0,
            }
        )

    wins = [trade for trade in executed if float(trade["scaled_pnl_usdc"]) > 0]
    losses = [trade for trade in executed if float(trade["scaled_pnl_usdc"]) <= 0]
    final_equity = float(cash)
    total_pnl = final_equity - float(config.initial_capital_usdc)
    summary = AlphaTopKPaperReplaySummary(
        total_rows=int(len(frame)),
        enriched_rows=int(len(working)),
        deduped_rows=int(len(deduped)),
        selected_rows=int(len(selected)),
        executed_trades=int(len(executed)),
        skipped_trades=int(skipped),
        skipped_for_price=int(skipped_for_price),
        skipped_for_resolution=int(skipped_for_resolution),
        wins=int(len(wins)),
        losses=int(len(losses)),
        initial_capital_usdc=float(config.initial_capital_usdc),
        final_equity_usdc=float(round(final_equity, 6)),
        total_pnl_usdc=float(round(total_pnl, 6)),
        total_return_pct=float(round(((final_equity / config.initial_capital_usdc) - 1.0) * 100.0, 6)),
        max_drawdown_pct=float(round(_max_drawdown_pct([row["equity_usdc"] for row in curve]), 6)),
        average_trade_pnl_usdc=float(round(sum(float(trade["scaled_pnl_usdc"]) for trade in executed) / len(executed), 6))
        if executed
        else 0.0,
        average_win_pnl_usdc=float(round(sum(float(trade["scaled_pnl_usdc"]) for trade in wins) / len(wins), 6))
        if wins
        else 0.0,
        average_loss_pnl_usdc=float(round(sum(float(trade["scaled_pnl_usdc"]) for trade in losses) / len(losses), 6))
        if losses
        else 0.0,
    )

    if config.curve_csv_path:
        curve_path = Path(config.curve_csv_path)
        curve_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(curve).to_csv(curve_path, index=False)
    if config.trades_csv_path:
        trades_path = Path(config.trades_csv_path)
        trades_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(executed).to_csv(trades_path, index=False)

    output_json = Path(config.output_json_path)
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _run_topk_paper_strategy_frame(
    frame: pd.DataFrame,
    config: AlphaTopKPaperStrategyConfig,
) -> Tuple[AlphaTopKPaperStrategySummary, List[dict], List[dict]]:
    required_columns = {
        "timestamp_seconds",
        "market_slug",
        "candidate_outcome",
        "predicted_pnl_per_stake_usdc",
        "pnl_per_stake_usdc",
    }
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"missing required prediction columns: {', '.join(missing_columns)}")

    working = frame.copy()
    if "condition_id" not in working.columns:
        working["condition_id"] = working["market_slug"]
    if "resolution_timestamp_seconds" not in working.columns:
        if "seconds_to_resolution" in working.columns:
            seconds_to_resolution = pd.to_numeric(working["seconds_to_resolution"], errors="coerce").fillna(0).astype(int)
        else:
            seconds_to_resolution = pd.Series([0] * len(working), index=working.index, dtype="int64")
        working["resolution_timestamp_seconds"] = (
            working["timestamp_seconds"].astype(int)
            + seconds_to_resolution
        )

    deduped = _dedupe_strategy_candidates(working, config.dedupe_mode)
    selected = _select_strategy_candidates(deduped, config.selection_mode, config.top_fraction, config.min_predicted_pnl)
    selected = selected.sort_values(["timestamp_seconds", "predicted_pnl_per_stake_usdc"], ascending=[True, False]).reset_index(drop=True)

    cash = float(config.initial_capital_usdc)
    open_positions: List[dict] = []
    executed: List[dict] = []
    skipped = 0
    curve: List[dict] = []
    event_timestamps = sorted(
        set(selected["timestamp_seconds"].astype(int).tolist())
        | set(selected["resolution_timestamp_seconds"].astype(int).tolist())
    )
    selected_by_entry = {}
    for row in selected.to_dict(orient="records"):
        selected_by_entry.setdefault(int(row["timestamp_seconds"]), []).append(row)

    for timestamp_seconds in event_timestamps:
        open_positions, cash, settled_now = _settle_strategy_positions(open_positions, timestamp_seconds, cash)
        for trade in settled_now:
            executed.append(trade)
        for row in selected_by_entry.get(timestamp_seconds, []):
            if cash < config.stake_usdc - 1e-9:
                skipped += 1
                continue
            if config.max_concurrent_positions is not None and len(open_positions) >= config.max_concurrent_positions:
                skipped += 1
                continue
            scaled_pnl = float(row["pnl_per_stake_usdc"]) * float(config.stake_usdc / config.label_stake_usdc)
            position = dict(row)
            position["stake_usdc"] = float(config.stake_usdc)
            position["scaled_pnl_usdc"] = float(scaled_pnl)
            position["exit_cash_usdc"] = float(config.stake_usdc + scaled_pnl)
            open_positions.append(position)
            cash -= float(config.stake_usdc)
        equity = cash + sum(float(item["exit_cash_usdc"]) for item in open_positions)
        curve.append(
            {
                "timestamp_seconds": int(timestamp_seconds),
                "cash_usdc": round(float(cash), 6),
                "equity_usdc": round(float(equity), 6),
                "open_positions": int(len(open_positions)),
            }
        )

    if open_positions:
        final_timestamp = max(int(item["resolution_timestamp_seconds"]) for item in open_positions)
        open_positions, cash, settled_now = _settle_strategy_positions(open_positions, final_timestamp, cash)
        for trade in settled_now:
            executed.append(trade)
        curve.append(
            {
                "timestamp_seconds": int(final_timestamp),
                "cash_usdc": round(float(cash), 6),
                "equity_usdc": round(float(cash), 6),
                "open_positions": 0,
            }
        )

    wins = [trade for trade in executed if float(trade["scaled_pnl_usdc"]) > 0]
    losses = [trade for trade in executed if float(trade["scaled_pnl_usdc"]) <= 0]
    final_equity = float(cash)
    total_pnl = final_equity - float(config.initial_capital_usdc)
    summary = AlphaTopKPaperStrategySummary(
        total_rows=int(len(frame)),
        deduped_rows=int(len(deduped)),
        selected_rows=int(len(selected)),
        executed_trades=int(len(executed)),
        skipped_trades=int(skipped),
        wins=int(len(wins)),
        losses=int(len(losses)),
        initial_capital_usdc=float(config.initial_capital_usdc),
        final_equity_usdc=float(round(final_equity, 6)),
        total_pnl_usdc=float(round(total_pnl, 6)),
        total_return_pct=float(round(((final_equity / config.initial_capital_usdc) - 1.0) * 100.0, 6)),
        max_drawdown_pct=float(round(_max_drawdown_pct([row["equity_usdc"] for row in curve]), 6)),
        average_trade_pnl_usdc=float(round(sum(float(trade["scaled_pnl_usdc"]) for trade in executed) / len(executed), 6))
        if executed
        else 0.0,
        average_win_pnl_usdc=float(round(sum(float(trade["scaled_pnl_usdc"]) for trade in wins) / len(wins), 6))
        if wins
        else 0.0,
        average_loss_pnl_usdc=float(round(sum(float(trade["scaled_pnl_usdc"]) for trade in losses) / len(losses), 6))
        if losses
        else 0.0,
    )
    return summary, curve, executed


def _enrich_prediction_rows(frame: pd.DataFrame, source_frame: Optional[pd.DataFrame]) -> pd.DataFrame:
    working = frame.copy()
    if source_frame is None or "sample_id" not in working.columns or "sample_id" not in source_frame.columns:
        return working
    enrich_columns = [
        "sample_id",
        "candidate_asset",
        "candidate_price",
        "payout_price",
        "settle_mark_price",
        "seconds_to_resolution",
        "resolution_timestamp_seconds",
        "condition_id",
        "event_slug",
        "title",
    ]
    available_columns = [column for column in enrich_columns if column in source_frame.columns]
    if len(available_columns) <= 1:
        return working
    source_subset = source_frame[available_columns].drop_duplicates(subset=["sample_id"], keep="last")
    merged = working.merge(source_subset, on="sample_id", how="left", suffixes=("", "_source"))
    for column in available_columns:
        if column == "sample_id":
            continue
        source_column = f"{column}_source"
        if source_column in merged.columns:
            if column not in merged.columns:
                merged[column] = merged[source_column]
            else:
                merged[column] = merged[column].where(~merged[column].isna(), merged[source_column])
            merged = merged.drop(columns=[source_column])
    return merged


def _settle_replay_positions(open_positions: List[dict], timestamp_seconds: int, cash: float) -> Tuple[List[dict], float, List[dict]]:
    remaining: List[dict] = []
    settled: List[dict] = []
    for position in open_positions:
        resolution_ts = int(position["resolution_timestamp_seconds"])
        if resolution_ts > timestamp_seconds:
            remaining.append(position)
            continue
        exit_cash = float(position["exit_cash_usdc"])
        position["status"] = "resolved"
        position["pnl_usdc"] = float(position["scaled_pnl_usdc"])
        cash += exit_cash
        settled.append(position)
    return remaining, cash, settled


def _build_classifier_pipeline(feature_columns: Sequence[str]) -> Pipeline:
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
    return Pipeline(
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


def _build_regression_pipeline(feature_columns: Sequence[str]) -> Pipeline:
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
    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("regressor", Ridge(alpha=1.0, random_state=42)),
        ]
    )


def _load_regression_frame(
    input_csv_path: str,
    max_rows: Optional[int],
    feature_columns: Sequence[str],
    target_column: str,
) -> Tuple[pd.DataFrame, List[str], int]:
    original = pd.read_csv(input_csv_path)
    total_rows = int(len(original))
    frame = original.iloc[:max_rows].copy() if max_rows else original.copy()
    if frame.empty:
        raise ValueError("input labeled outcome dataset is empty")
    if "label_resolved" not in frame.columns:
        raise ValueError("missing required label column: label_resolved")
    if target_column not in frame.columns:
        raise ValueError(f"missing required target column: {target_column}")
    frame = frame[frame["label_resolved"].astype(int) == 1].copy()
    frame = frame.sort_values(["timestamp_seconds", "sample_id"]).reset_index(drop=True)
    if frame.empty:
        raise ValueError("no resolved rows available for outcome regression")
    usable_feature_columns = [column for column in feature_columns if column in frame.columns]
    if not usable_feature_columns:
        raise ValueError("no requested feature columns found in labeled outcome dataset")
    return frame, usable_feature_columns, total_rows


def _fit_predict_regression(
    train: pd.DataFrame,
    test: pd.DataFrame,
    feature_columns: Sequence[str],
    target_column: str,
) -> Tuple[Sequence[float], dict]:
    model = _build_regression_pipeline(feature_columns)
    X_train = train[feature_columns]
    y_train = train[target_column].astype(float)
    X_test = test[feature_columns]
    y_test = test[target_column].astype(float)
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    metrics = {
        "mae": float(round(float(mean_absolute_error(y_test, predictions)), 6)),
        "rmse": float(round(float(mean_squared_error(y_test, predictions) ** 0.5), 6)),
        "r2": float(round(float(r2_score(y_test, predictions)), 6)),
        "sign_accuracy": float(round(float(((predictions > 0) == (y_test > 0)).mean()), 6)),
    }
    return predictions, metrics


def _build_outcome_prediction_frame(
    test: pd.DataFrame,
    target_column: str,
    predictions: Sequence[float],
) -> pd.DataFrame:
    output_columns = [
        "sample_id",
        "timestamp_seconds",
        "market_slug",
        "condition_id",
        "candidate_outcome",
        "seconds_to_resolution",
        "resolution_timestamp_seconds",
        target_column,
    ]
    existing_columns = [column for column in output_columns if column in test.columns]
    predictions_frame = test[existing_columns].copy()
    predictions_frame["predicted_pnl_per_stake_usdc"] = predictions
    return predictions_frame


def _time_split(frame: pd.DataFrame, test_fraction: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    split_index = max(1, min(len(frame) - 1, int(len(frame) * (1.0 - test_fraction))))
    train = frame.iloc[:split_index].copy()
    test = frame.iloc[split_index:].copy()
    if train.empty or test.empty:
        raise ValueError("need both train and test rows after split")
    return train, test


def _compute_topk_realized_pnl(
    predicted_values: Sequence[float],
    realized_values: Sequence[float],
    fractions: Sequence[float],
) -> dict:
    rows = sorted(zip(predicted_values, realized_values), key=lambda item: item[0], reverse=True)
    results = {}
    for fraction in fractions:
        key = f"top_{int(fraction * 100)}pct"
        count = max(1, int(len(rows) * fraction))
        subset = rows[:count]
        realized_mean = sum(realized for _, realized in subset) / count if count else 0.0
        results[key] = float(round(realized_mean, 6))
    return results


def _dedupe_strategy_candidates(frame: pd.DataFrame, dedupe_mode: str) -> pd.DataFrame:
    ranked = frame.sort_values(["predicted_pnl_per_stake_usdc", "timestamp_seconds"], ascending=[False, True]).copy()
    mode = dedupe_mode.lower()
    if mode == "one_market_total":
        return ranked.drop_duplicates(["market_slug"], keep="first").copy()
    if mode == "one_market_side":
        return ranked.drop_duplicates(["market_slug", "candidate_outcome"], keep="first").copy()
    raise ValueError(f"unsupported dedupe_mode: {dedupe_mode}")


def _select_strategy_candidates(
    frame: pd.DataFrame,
    selection_mode: str,
    top_fraction: float,
    min_predicted_pnl: float,
) -> pd.DataFrame:
    ranked = frame.sort_values(["predicted_pnl_per_stake_usdc", "timestamp_seconds"], ascending=[False, True]).copy()
    mode = selection_mode.lower()
    if mode == "predicted_positive":
        return ranked[ranked["predicted_pnl_per_stake_usdc"].astype(float) >= float(min_predicted_pnl)].copy()
    if mode == "top_fraction":
        fraction = float(top_fraction)
        if fraction <= 0 or fraction > 1:
            raise ValueError("top_fraction must be within (0, 1]")
        count = max(1, int(len(ranked) * fraction))
        return ranked.head(count).copy()
    raise ValueError(f"unsupported selection_mode: {selection_mode}")


def _settle_strategy_positions(
    open_positions: List[dict],
    timestamp_seconds: int,
    cash: float,
) -> Tuple[List[dict], float, List[dict]]:
    remaining = []
    settled = []
    for position in open_positions:
        if int(position["resolution_timestamp_seconds"]) > int(timestamp_seconds):
            remaining.append(position)
            continue
        cash += float(position["exit_cash_usdc"])
        position["entry_timestamp_seconds"] = int(position["timestamp_seconds"])
        position["exit_timestamp_seconds"] = int(position["resolution_timestamp_seconds"])
        settled.append(position)
    return remaining, cash, settled


def _max_drawdown_pct(equity_curve: Sequence[float]) -> float:
    peak = None
    max_drawdown = 0.0
    for value in equity_curve:
        value = float(value)
        if peak is None or value > peak:
            peak = value
        if peak and peak > 0:
            drawdown = (peak - value) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown
    return max_drawdown * 100.0


def _row_resolution_timestamp_seconds(
    row: dict,
    timestamp_column: str,
    resolution_column: str,
    seconds_to_resolution_column: str,
) -> Optional[int]:
    raw_resolution = row.get(resolution_column)
    if raw_resolution is not None and pd.notna(raw_resolution):
        try:
            return int(float(raw_resolution))
        except (TypeError, ValueError):
            pass
    raw_timestamp = row.get(timestamp_column)
    raw_delta = row.get(seconds_to_resolution_column)
    if raw_timestamp is None or raw_delta is None or not pd.notna(raw_delta):
        return None
    try:
        return int(float(raw_timestamp)) + int(float(raw_delta))
    except (TypeError, ValueError):
        return None
