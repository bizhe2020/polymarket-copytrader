from __future__ import annotations

import csv
import json
import re
from bisect import bisect_right
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

from .api import PolymarketPublicApi
from .config import load_eval_config
from .matching import build_synthetic_order_book, simulate_book_execution
from .models import (
    EquityPoint,
    EvalConfig,
    EvalScenarioConfig,
    EvalSignal,
    EvalSummary,
    OrderBook,
    PriceHistoryPoint,
    ResolvedTarget,
    SimulatedFill,
    TradeActivity,
)
from .resolve import resolve_target_wallet


@dataclass
class _SleeveState:
    name: str
    initial_cash: float
    cash: float
    holdings: Dict[str, float] = field(default_factory=dict)
    last_prices: Dict[str, float] = field(default_factory=dict)
    fills: List[SimulatedFill] = field(default_factory=list)


@dataclass
class _SimulationResult:
    scenario_name: str
    portfolio_name: str
    initial_capital_usdc: float
    curve: List[EquityPoint]
    fills: List[SimulatedFill]
    summary: EvalSummary


@dataclass
class _RedeemSettlement:
    condition_id: str
    market_slug: str
    resolution_timestamp_seconds: int
    payout_by_asset: Dict[str, float]


class MinutePriceLookup:
    def __init__(self, histories: Dict[str, List[PriceHistoryPoint]]) -> None:
        self._histories = histories
        self._timestamps = {
            asset: [point.timestamp_seconds for point in points]
            for asset, points in histories.items()
        }

    def price_at(
        self, asset_id: str, timestamp_seconds: int, fallback: float | None
    ) -> float | None:
        points = self._histories.get(asset_id, [])
        if not points:
            return fallback
        timestamps = self._timestamps[asset_id]
        index = bisect_right(timestamps, timestamp_seconds) - 1
        if index >= 0:
            return points[index].price
        return points[0].price if points else fallback

    def price_at_or_after(
        self, asset_id: str, timestamp_seconds: int, fallback: float | None
    ) -> float | None:
        points = self._histories.get(asset_id, [])
        if not points:
            return fallback
        timestamps = self._timestamps[asset_id]
        index = bisect_right(timestamps, timestamp_seconds - 1)
        if index < len(points):
            return points[index].price
        return fallback


class EvaluationApp:
    def __init__(self, config: EvalConfig) -> None:
        self.config = config
        self.api = PolymarketPublicApi(timeout_seconds=config.runtime.request_timeout_seconds)
        self.cache_dir = Path(config.runtime.cache_dir)
        self.runtime_notes: List[Dict[str, object]] = []

    def run(self) -> Dict[str, object]:
        now_seconds = int(datetime.now(timezone.utc).timestamp())
        cutoff_seconds = now_seconds - self.config.data.recent_days * 86400

        resolved_targets = self._resolve_targets()
        trades_by_target = self._fetch_recent_trades(
            targets=resolved_targets,
            cutoff_seconds=cutoff_seconds,
            now_seconds=now_seconds,
        )
        price_histories = self._fetch_price_histories(
            trades_by_target=trades_by_target,
            scenarios=self.config.scenarios,
        )
        scenario_results = self._run_scenarios(
            targets=resolved_targets,
            trades_by_target=trades_by_target,
            price_histories=price_histories,
        )
        return self._write_outputs(
            resolved_targets=resolved_targets,
            trades_by_target=trades_by_target,
            cutoff_seconds=cutoff_seconds,
            now_seconds=now_seconds,
            scenario_results=scenario_results,
        )

    def _resolve_targets(self) -> List[ResolvedTarget]:
        cache_path = self.cache_dir / "resolved_targets.json"
        resolved = []
        for item in self.config.targets:
            try:
                wallet, details = resolve_target_wallet(self.api, item.profile, item.wallet)
            except RuntimeError:
                cached_target = self._read_cached_resolved_target(cache_path, item.name)
                if cached_target is None:
                    raise
                wallet = cached_target.wallet
                details = dict(cached_target.details)
                details["source"] = "cache.resolved_targets"
                self.runtime_notes.append(
                    {
                        "kind": "resolved_target",
                        "name": item.name,
                        "source": "cache_fallback",
                        "path": str(cache_path),
                    }
                )
            resolved.append(
                ResolvedTarget(
                    name=item.name,
                    profile=item.profile,
                    wallet=wallet,
                    weight=item.weight,
                    details=details,
                )
            )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._write_json(
            cache_path,
            [asdict(item) for item in resolved],
        )
        return resolved

    def _fetch_recent_trades(
        self,
        targets: Sequence[ResolvedTarget],
        cutoff_seconds: int,
        now_seconds: int,
    ) -> Dict[str, List[TradeActivity]]:
        trades_dir = self.cache_dir / "trades"
        trades_dir.mkdir(parents=True, exist_ok=True)
        result: Dict[str, List[TradeActivity]] = {}
        for target in targets:
            cache_path = trades_dir / f"{target.name}.jsonl"
            progress_path = self.cache_dir / "progress" / "trades" / f"{target.name}.json"
            try:
                result[target.name] = self._fetch_recent_trades_for_wallet(
                    target_name=target.name,
                    wallet=target.wallet,
                    cutoff_seconds=cutoff_seconds,
                    now_seconds=now_seconds,
                    cache_path=cache_path,
                    progress_path=progress_path,
                )
            except RuntimeError:
                if not cache_path.exists():
                    raise
                result[target.name] = self._read_trade_jsonl(
                    cache_path,
                    cutoff_seconds=cutoff_seconds,
                    now_seconds=now_seconds,
                )
                self.runtime_notes.append(
                    {
                        "kind": "trades",
                        "name": target.name,
                        "source": "cache_fallback",
                        "path": str(cache_path),
                        "progress_path": str(progress_path),
                    }
                )
            max_trades = self.config.data.max_trades_per_target
            if max_trades is not None and max_trades > 0:
                result[target.name] = result[target.name][-max_trades:]
        return result

    def _fetch_recent_trades_for_wallet(
        self,
        target_name: str,
        wallet: str,
        cutoff_seconds: int,
        now_seconds: int,
        cache_path: Path,
        progress_path: Path,
    ) -> List[TradeActivity]:
        page_size = self.config.data.activity_page_size
        trades = self._read_trade_jsonl(cache_path, cutoff_seconds, now_seconds)
        seen = {trade.dedupe_key for trade in trades}
        progress = self._read_progress(progress_path)
        cursor_start = cutoff_seconds
        offset = 0

        if progress and progress.get("wallet") == wallet:
            cursor_start = max(int(progress.get("cursor_start", cutoff_seconds)), cutoff_seconds)
            offset = max(int(progress.get("offset", 0)), 0)
            self.runtime_notes.append(
                {
                    "kind": "trade_fetch_resume",
                    "name": target_name,
                    "source": "progress_file",
                    "path": str(progress_path),
                    "cursor_start": cursor_start,
                    "offset": offset,
                }
            )
        elif trades:
            cursor_start = max(max(trade.timestamp_seconds for trade in trades) + 1, cutoff_seconds)
            self.runtime_notes.append(
                {
                    "kind": "trade_fetch_resume",
                    "name": target_name,
                    "source": "cache_tail",
                    "path": str(cache_path),
                    "cursor_start": cursor_start,
                    "offset": 0,
                }
            )

        while True:
            self._write_progress(
                progress_path,
                {
                    "target_name": target_name,
                    "wallet": wallet,
                    "cutoff_seconds": cutoff_seconds,
                    "now_seconds": now_seconds,
                    "cursor_start": cursor_start,
                    "offset": offset,
                    "status": "running",
                    "cached_trades": len(trades),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            page = self.api.get_activity(
                wallet=wallet,
                limit=page_size,
                start_ms=cursor_start,
                end_ms=now_seconds,
                side=None,
                offset=offset,
                sort_direction="ASC",
            )
            if not page:
                break
            fresh = []
            for trade in page:
                if trade.timestamp_seconds < cutoff_seconds:
                    continue
                if trade.dedupe_key in seen:
                    continue
                seen.add(trade.dedupe_key)
                fresh.append(trade)
            trades.extend(fresh)
            if fresh:
                self._append_jsonl(cache_path, [asdict(item) for item in fresh])
            if len(page) < page_size:
                break
            next_cursor = max(trade.timestamp_seconds for trade in page) + 1
            if next_cursor <= cursor_start:
                offset += page_size
                if offset > 3000:
                    raise RuntimeError("activity pagination stalled at max offset")
            else:
                cursor_start = next_cursor
                offset = 0
            self._write_progress(
                progress_path,
                {
                    "target_name": target_name,
                    "wallet": wallet,
                    "cutoff_seconds": cutoff_seconds,
                    "now_seconds": now_seconds,
                    "cursor_start": cursor_start,
                    "offset": offset,
                    "status": "running",
                    "cached_trades": len(trades),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            if cursor_start > now_seconds:
                break

        ordered = sorted(trades, key=lambda item: (item.timestamp_seconds, item.transaction_hash))
        self._write_progress(
            progress_path,
            {
                "target_name": target_name,
                "wallet": wallet,
                "cutoff_seconds": cutoff_seconds,
                "now_seconds": now_seconds,
                "cursor_start": max(cursor_start, now_seconds),
                "offset": 0,
                "status": "completed",
                "cached_trades": len(ordered),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        return ordered

    def _fetch_price_histories(
        self,
        trades_by_target: Dict[str, List[TradeActivity]],
        scenarios: Sequence[EvalScenarioConfig],
    ) -> Dict[str, List[PriceHistoryPoint]]:
        all_trades = [trade for trades in trades_by_target.values() for trade in trades]
        if not all_trades:
            return {}

        assets = sorted({trade.asset for trade in all_trades})
        max_delay_seconds = max(scenario.observation_delay_seconds for scenario in scenarios)
        start_ts = min(trade.timestamp_seconds for trade in all_trades) - 120
        end_ts = max(trade.timestamp_seconds for trade in all_trades) + max_delay_seconds + 120

        prices_dir = self.cache_dir / "prices"
        prices_dir.mkdir(parents=True, exist_ok=True)
        histories: Dict[str, List[PriceHistoryPoint]] = {}
        for asset_id in assets:
            cache_path = prices_dir / f"{asset_id}.json"
            if cache_path.exists():
                points = self._read_price_history(cache_path)
            else:
                points = self.api.get_price_history(
                    asset_id=asset_id,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    fidelity_minutes=self.config.data.price_fidelity_minutes,
                )
                self._write_json(cache_path, [asdict(point) for point in points])
            histories[asset_id] = points
        return histories

    def _run_scenarios(
        self,
        targets: Sequence[ResolvedTarget],
        trades_by_target: Dict[str, List[TradeActivity]],
        price_histories: Dict[str, List[PriceHistoryPoint]],
    ) -> List[_SimulationResult]:
        results: List[_SimulationResult] = []
        for scenario in self.config.scenarios:
            signals = self._build_signals(targets, trades_by_target, scenario)
            combined_result = self._simulate_combined(targets, signals, price_histories, scenario)
            standalone_results = [
                self._simulate_standalone(
                    target=target,
                    trades=trades_by_target[target.name],
                    price_histories=price_histories,
                    scenario=scenario,
                )
                for target in targets
            ]
            results.extend([combined_result, *standalone_results])
        return self._attach_benchmark_metrics(results)

    def _build_signals(
        self,
        targets: Sequence[ResolvedTarget],
        trades_by_target: Dict[str, List[TradeActivity]],
        scenario: EvalScenarioConfig,
    ) -> List[EvalSignal]:
        wallets = {target.name: target.wallet for target in targets}
        signals: List[EvalSignal] = []
        for target in targets:
            for trade in trades_by_target[target.name]:
                execution_timestamp_seconds = trade.timestamp_seconds + scenario.observation_delay_seconds
                signals.append(
                    EvalSignal(
                        scenario_name=scenario.name,
                        target_name=target.name,
                        wallet=wallets[target.name],
                        trade=trade,
                        execution_timestamp_seconds=execution_timestamp_seconds,
                    )
                )
        return sorted(
            signals,
            key=lambda item: (
                item.execution_timestamp_seconds,
                item.trade.timestamp_seconds,
                item.trade.transaction_hash,
            ),
        )

    def _simulate_combined(
        self,
        targets: Sequence[ResolvedTarget],
        signals: Sequence[EvalSignal],
        price_histories: Dict[str, List[PriceHistoryPoint]],
        scenario: EvalScenarioConfig,
    ) -> _SimulationResult:
        weight_sum = sum(max(target.weight, 0.0) for target in targets) or 1.0
        initial_cash_by_target = {
            target.name: self.config.portfolio.initial_capital_usdc * (target.weight / weight_sum)
            for target in targets
        }
        return simulate_minute_portfolio(
            scenario_name=scenario.name,
            portfolio_name="combined_equal_weight",
            initial_cash_by_target=initial_cash_by_target,
            signals=signals,
            price_histories=price_histories,
            follow_fraction=self.config.strategy.follow_fraction,
            max_order_usdc=self.config.strategy.max_order_usdc,
            min_target_usdc_size=self.config.strategy.min_target_usdc_size,
            min_order_usdc=self.config.strategy.min_order_usdc,
            max_position_per_asset_usdc=self.config.strategy.max_position_per_asset_usdc,
            allow_sell=self.config.strategy.allow_sell,
            exit_mode=self.config.strategy.exit_mode,
            extra_slippage_bps=scenario.extra_slippage_bps,
            execution_policy=scenario.execution_policy,
            synthetic_levels=scenario.synthetic_levels,
            synthetic_depth_multiplier=scenario.synthetic_depth_multiplier,
            use_target_trade_price=scenario.use_target_trade_price,
        )

    def _simulate_standalone(
        self,
        target: ResolvedTarget,
        trades: Sequence[TradeActivity],
        price_histories: Dict[str, List[PriceHistoryPoint]],
        scenario: EvalScenarioConfig,
    ) -> _SimulationResult:
        signals = [
            EvalSignal(
                scenario_name=scenario.name,
                target_name=target.name,
                wallet=target.wallet,
                trade=trade,
                execution_timestamp_seconds=trade.timestamp_seconds + scenario.observation_delay_seconds,
            )
            for trade in trades
        ]
        return simulate_minute_portfolio(
            scenario_name=scenario.name,
            portfolio_name=f"standalone_{target.name}",
            initial_cash_by_target={target.name: self.config.portfolio.standalone_initial_capital_usdc},
            signals=signals,
            price_histories=price_histories,
            follow_fraction=self.config.strategy.follow_fraction,
            max_order_usdc=self.config.strategy.max_order_usdc,
            min_target_usdc_size=self.config.strategy.min_target_usdc_size,
            min_order_usdc=self.config.strategy.min_order_usdc,
            max_position_per_asset_usdc=self.config.strategy.max_position_per_asset_usdc,
            allow_sell=self.config.strategy.allow_sell,
            exit_mode=self.config.strategy.exit_mode,
            extra_slippage_bps=scenario.extra_slippage_bps,
            execution_policy=scenario.execution_policy,
            synthetic_levels=scenario.synthetic_levels,
            synthetic_depth_multiplier=scenario.synthetic_depth_multiplier,
            use_target_trade_price=scenario.use_target_trade_price,
        )

    def _attach_benchmark_metrics(
        self, results: Sequence[_SimulationResult]
    ) -> List[_SimulationResult]:
        benchmark_by_key: Dict[Tuple[str, str], _SimulationResult] = {}
        for result in results:
            benchmark_by_key[(result.summary.scenario_name, result.portfolio_name)] = result

        enriched: List[_SimulationResult] = []
        for result in results:
            benchmark_scenario_name = _benchmark_scenario_for(result.summary.scenario_name)
            benchmark = (
                benchmark_by_key.get((benchmark_scenario_name, result.portfolio_name))
                if benchmark_scenario_name
                else None
            )
            if benchmark:
                benchmark_pnl = benchmark.summary.final_equity_usdc - benchmark.summary.initial_capital_usdc
                pnl = result.summary.final_equity_usdc - result.summary.initial_capital_usdc
                result.summary.benchmark_scenario_name = benchmark.summary.scenario_name
                result.summary.benchmark_return_pct = benchmark.summary.total_return_pct
                if abs(benchmark_pnl) > 1e-12:
                    result.summary.pnl_capture_ratio = round((pnl / benchmark_pnl) * 100.0, 6)
            enriched.append(result)
        return enriched

    def _write_outputs(
        self,
        resolved_targets: Sequence[ResolvedTarget],
        trades_by_target: Dict[str, List[TradeActivity]],
        cutoff_seconds: int,
        now_seconds: int,
        scenario_results: Sequence[_SimulationResult],
    ) -> Dict[str, object]:
        summary_payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window": {
                "recent_days": self.config.data.recent_days,
                "start": _isoformat(cutoff_seconds),
                "end": _isoformat(now_seconds),
            },
            "runtime_notes": self.runtime_notes,
            "resolved_targets": [asdict(item) for item in resolved_targets],
            "target_stats": self._target_stats(trades_by_target),
            "portfolios": [asdict(item.summary) for item in scenario_results],
        }
        self._write_json(Path(self.config.output.summary_path), summary_payload)

        all_fills = []
        for result in scenario_results:
            for fill in result.fills:
                payload = asdict(fill)
                payload["portfolio_name"] = result.portfolio_name
                all_fills.append(payload)
        self._write_jsonl(Path(self.config.output.fills_path), all_fills)
        self._write_curve_csv(Path(self.config.output.curve_path), scenario_results)
        return summary_payload

    def _target_stats(self, trades_by_target: Dict[str, List[TradeActivity]]) -> List[Dict[str, object]]:
        rows = []
        for name, trades in trades_by_target.items():
            buys = sum(1 for trade in trades if trade.side.upper() == "BUY")
            sells = sum(1 for trade in trades if trade.side.upper() == "SELL")
            rows.append(
                {
                    "name": name,
                    "trades": len(trades),
                    "buys": buys,
                    "sells": sells,
                    "start": _isoformat(min(trade.timestamp_seconds for trade in trades)) if trades else None,
                    "end": _isoformat(max(trade.timestamp_seconds for trade in trades)) if trades else None,
                }
            )
        return rows

    @staticmethod
    def _write_json(path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)

    @staticmethod
    def _write_jsonl(path: Path, rows: Iterable[object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    @staticmethod
    def _append_jsonl(path: Path, rows: Iterable[object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    @staticmethod
    def _read_price_history(path: Path) -> List[PriceHistoryPoint]:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        return [
            PriceHistoryPoint(
                timestamp_seconds=int(item["timestamp_seconds"]),
                price=float(item["price"]),
            )
            for item in raw
        ]

    @staticmethod
    def _read_trade_jsonl(
        path: Path, cutoff_seconds: int, now_seconds: int
    ) -> List[TradeActivity]:
        if not path.exists():
            return []
        trades: List[TradeActivity] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                raw = json.loads(line)
                trade = TradeActivity(**raw)
                if cutoff_seconds <= trade.timestamp_seconds <= now_seconds:
                    trades.append(trade)
        return sorted(trades, key=lambda item: (item.timestamp_seconds, item.transaction_hash))

    @staticmethod
    def _read_cached_resolved_target(path: Path, name: str) -> Optional[ResolvedTarget]:
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
        for item in raw:
            if str(item.get("name")) != name:
                continue
            return ResolvedTarget(
                name=str(item["name"]),
                profile=item.get("profile"),
                wallet=str(item["wallet"]),
                weight=float(item.get("weight", 1.0)),
                details=dict(item.get("details") or {}),
            )
        return None

    @staticmethod
    def _read_progress(path: Path) -> Optional[Dict[str, object]]:
        if not path.exists():
            return None
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    @staticmethod
    def _write_progress(path: Path, payload: Dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)

    def _write_curve_csv(self, path: Path, scenario_results: Sequence[_SimulationResult]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        point_maps = {
            (result.summary.scenario_name, result.portfolio_name): {
                point.timestamp_seconds: point for point in result.curve
            }
            for result in scenario_results
        }
        timestamps = sorted(
            {
                timestamp
                for point_map in point_maps.values()
                for timestamp in point_map.keys()
            }
        )
        fieldnames = ["timestamp_seconds", "timestamp_iso"]
        ordered_keys = sorted(point_maps.keys())
        for scenario_name, portfolio_name in ordered_keys:
            fieldnames.append(f"{scenario_name}__{portfolio_name}__equity")
            fieldnames.append(f"{scenario_name}__{portfolio_name}__return_pct")

        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            last_values = {
                key: next(iter(point_maps[key].values())).combined_equity
                for key in ordered_keys
                if point_maps[key]
            }
            initial_capitals = {
                (result.summary.scenario_name, result.portfolio_name): result.initial_capital_usdc
                for result in scenario_results
            }
            for timestamp_seconds in timestamps:
                row = {
                    "timestamp_seconds": timestamp_seconds,
                    "timestamp_iso": _isoformat(timestamp_seconds),
                }
                for key in ordered_keys:
                    point = point_maps[key].get(timestamp_seconds)
                    if point is not None:
                        last_values[key] = point.combined_equity
                    equity = last_values.get(key, initial_capitals[key])
                    row[f"{key[0]}__{key[1]}__equity"] = round(equity, 6)
                    row[f"{key[0]}__{key[1]}__return_pct"] = round(
                        ((equity / initial_capitals[key]) - 1.0) * 100.0,
                        6,
                    )
                writer.writerow(row)


def simulate_minute_portfolio(
    scenario_name: str,
    portfolio_name: str,
    initial_cash_by_target: Dict[str, float],
    signals: Sequence[EvalSignal],
    price_histories: Dict[str, List[PriceHistoryPoint]],
    follow_fraction: float,
    max_order_usdc: float,
    min_target_usdc_size: float,
    min_order_usdc: float,
    max_position_per_asset_usdc: float,
    allow_sell: bool,
    exit_mode: str,
    extra_slippage_bps: float,
    execution_policy: str,
    synthetic_levels: int,
    synthetic_depth_multiplier: float,
    use_target_trade_price: bool,
) -> _SimulationResult:
    initial_capital = round(sum(initial_cash_by_target.values()), 6)
    if not signals:
        summary = EvalSummary(
            scenario_name=scenario_name,
            portfolio_name=portfolio_name,
            initial_capital_usdc=initial_capital,
            final_equity_usdc=initial_capital,
            total_return_pct=0.0,
            max_drawdown_pct=0.0,
            fills=0,
            buys=0,
            sells=0,
            skipped=0,
            avg_execution_slippage_bps=0.0,
        )
        return _SimulationResult(
            scenario_name=scenario_name,
            portfolio_name=portfolio_name,
            initial_capital_usdc=initial_capital,
            curve=[],
            fills=[],
            summary=summary,
        )

    lookup = MinutePriceLookup(price_histories)
    settlements = _build_redeem_settlements(signals, lookup, exit_mode)
    sleeves = {
        target_name: _SleeveState(
            name=target_name,
            initial_cash=initial_cash,
            cash=initial_cash,
        )
        for target_name, initial_cash in initial_cash_by_target.items()
    }
    signals_by_time: Dict[int, List[EvalSignal]] = {}
    for signal in signals:
        signals_by_time.setdefault(signal.execution_timestamp_seconds, []).append(signal)

    settlements_by_time: Dict[int, List[_RedeemSettlement]] = {}
    for settlement in settlements:
        settlements_by_time.setdefault(settlement.resolution_timestamp_seconds, []).append(
            settlement
        )

    start_ts = min(signals_by_time)
    end_ts = max(
        [max(signals_by_time)]
        + [settlement.resolution_timestamp_seconds for settlement in settlements]
    )
    curve: List[EquityPoint] = []
    minute_marks = set(range(_floor_minute(start_ts), _ceil_minute(end_ts) + 60, 60))
    timeline = sorted(minute_marks | set(signals_by_time) | set(settlements_by_time))

    for timestamp_seconds in timeline:
        for signal in signals_by_time.get(timestamp_seconds, []):
            sleeve = sleeves[signal.target_name]
            fill = _execute_signal(
                sleeve=sleeve,
                signal=signal,
                lookup=lookup,
                follow_fraction=follow_fraction,
                max_order_usdc=max_order_usdc,
                min_target_usdc_size=min_target_usdc_size,
                min_order_usdc=min_order_usdc,
                max_position_per_asset_usdc=max_position_per_asset_usdc,
                allow_sell=allow_sell,
                extra_slippage_bps=extra_slippage_bps,
                execution_policy=execution_policy,
                synthetic_levels=synthetic_levels,
                synthetic_depth_multiplier=synthetic_depth_multiplier,
                use_target_trade_price=use_target_trade_price,
            )
            sleeve.fills.append(fill)

        for settlement in settlements_by_time.get(timestamp_seconds, []):
            _apply_redeem_settlement(sleeves, settlement)

        per_target_equity = {}
        combined_cash = 0.0
        for sleeve in sleeves.values():
            equity = _equity_of_sleeve(sleeve, timestamp_seconds, lookup)
            per_target_equity[sleeve.name] = round(equity, 6)
            combined_cash += sleeve.cash
        combined_equity = round(sum(per_target_equity.values()), 6)
        curve.append(
            EquityPoint(
                timestamp_seconds=timestamp_seconds,
                combined_equity=combined_equity,
                combined_cash=round(combined_cash, 6),
                per_target_equity=per_target_equity,
            )
        )

    fills = [fill for sleeve in sleeves.values() for fill in sleeve.fills]
    executed = [fill for fill in fills if fill.status in {"filled", "partial"}]
    final_equity = curve[-1].combined_equity if curve else initial_capital
    total_return_pct = ((final_equity / initial_capital) - 1.0) * 100.0 if initial_capital else 0.0
    max_drawdown_pct = _max_drawdown_pct([point.combined_equity for point in curve])
    avg_execution_slippage = (
        sum(fill.execution_slippage_bps for fill in executed) / len(executed)
        if executed
        else 0.0
    )

    summary = EvalSummary(
        scenario_name=scenario_name,
        portfolio_name=portfolio_name,
        initial_capital_usdc=initial_capital,
        final_equity_usdc=round(final_equity, 6),
        total_return_pct=round(total_return_pct, 6),
        max_drawdown_pct=round(max_drawdown_pct, 6),
        fills=len(executed),
        buys=sum(1 for fill in executed if fill.side == "BUY"),
        sells=sum(1 for fill in executed if fill.side == "SELL"),
        skipped=sum(1 for fill in fills if fill.status == "skipped"),
        partial_fills=sum(1 for fill in fills if fill.status == "partial"),
        avg_execution_slippage_bps=round(avg_execution_slippage, 6),
    )
    return _SimulationResult(
        scenario_name=scenario_name,
        portfolio_name=portfolio_name,
        initial_capital_usdc=initial_capital,
        curve=curve,
        fills=fills,
        summary=summary,
    )


def _execute_signal(
    sleeve: _SleeveState,
    signal: EvalSignal,
    lookup: MinutePriceLookup,
    follow_fraction: float,
    max_order_usdc: float,
    min_target_usdc_size: float,
    min_order_usdc: float,
    max_position_per_asset_usdc: float,
    allow_sell: bool,
    extra_slippage_bps: float,
    execution_policy: str,
    synthetic_levels: int,
    synthetic_depth_multiplier: float,
    use_target_trade_price: bool,
) -> SimulatedFill:
    trade = signal.trade
    price = trade.price if use_target_trade_price else lookup.price_at(
        trade.asset, signal.execution_timestamp_seconds, trade.price
    )
    if price is None or price <= 0:
        return _skipped_fill(signal, 0.0, 0.0, "skip_no_price")

    side = trade.side.upper()
    book = build_synthetic_order_book(
        asset_id=trade.asset,
        market=trade.slug,
        reference_price=price,
        trade_size=trade.size,
        extra_slippage_bps=extra_slippage_bps,
        levels=synthetic_levels,
        depth_multiplier=synthetic_depth_multiplier,
    )
    sleeve.last_prices[trade.asset] = book.last_trade_price

    if trade.usdc_size < min_target_usdc_size:
        return _skipped_fill(signal, book.last_trade_price, 0.0, "skip_small_target_trade")

    desired_usdc = min(trade.usdc_size * follow_fraction, max_order_usdc)
    if desired_usdc < min_order_usdc:
        return _skipped_fill(signal, book.last_trade_price, 0.0, "skip_below_min_order_usdc")

    current_value = sleeve.holdings.get(trade.asset, 0.0) * book.last_trade_price
    if side == "BUY":
        remaining_position = max_position_per_asset_usdc - current_value
        requested_usdc = min(desired_usdc, sleeve.cash, remaining_position)
        if requested_usdc < min_order_usdc:
            reason = "skip_position_limit_reached" if remaining_position < min_order_usdc else "skip_no_cash"
            return _skipped_fill(signal, book.last_trade_price, requested_usdc, reason)
        match = simulate_book_execution(
            order_book=book,
            side="BUY",
            requested_usdc=requested_usdc,
            requested_size=0.0,
            execution_policy=execution_policy,
        )
        if match.status == "unfilled":
            return _skipped_fill(signal, book.best_ask or book.last_trade_price, requested_usdc, match.reason)
        copied_usdc = match.filled_usdc
        copied_size = match.filled_size
        execution_price = match.avg_price or book.best_ask or book.last_trade_price
        sleeve.cash -= copied_usdc
        sleeve.holdings[trade.asset] = sleeve.holdings.get(trade.asset, 0.0) + copied_size
        return SimulatedFill(
            scenario_name=signal.scenario_name,
            target_name=signal.target_name,
            side="BUY",
            asset_id=trade.asset,
            market_slug=trade.slug,
            outcome=trade.outcome,
            signal_timestamp_seconds=trade.timestamp_seconds,
            execution_timestamp_seconds=signal.execution_timestamp_seconds,
            signal_price=trade.price,
            execution_price=execution_price,
            target_usdc_size=round(trade.usdc_size, 6),
            copied_usdc=round(copied_usdc, 6),
            copied_size=round(copied_size, 6),
            status=match.status,
            reason=match.reason,
            execution_slippage_bps=round(_execution_slippage_bps(trade.price, execution_price, "BUY"), 6),
            requested_usdc=round(requested_usdc, 6),
            requested_size=round(requested_usdc / max(book.best_ask or execution_price, 1e-9), 6),
            fill_ratio=round(match.fill_ratio, 6),
        )

    if side == "SELL":
        if not allow_sell:
            return _skipped_fill(signal, book.last_trade_price, 0.0, "skip_sell_disabled")
        available_size = sleeve.holdings.get(trade.asset, 0.0)
        if available_size <= 0:
            return _skipped_fill(signal, book.last_trade_price, 0.0, "skip_no_inventory")
        requested_size = min(desired_usdc / max(book.best_bid or book.last_trade_price, 1e-9), available_size)
        requested_usdc = requested_size * (book.best_bid or book.last_trade_price)
        if requested_usdc < min_order_usdc:
            return _skipped_fill(signal, book.last_trade_price, requested_usdc, "skip_no_inventory")
        match = simulate_book_execution(
            order_book=book,
            side="SELL",
            requested_usdc=0.0,
            requested_size=requested_size,
            execution_policy=execution_policy,
        )
        if match.status == "unfilled":
            return _skipped_fill(signal, book.best_bid or book.last_trade_price, requested_usdc, match.reason)
        copied_usdc = match.filled_usdc
        copied_size = match.filled_size
        execution_price = match.avg_price or book.best_bid or book.last_trade_price
        sleeve.cash += copied_usdc
        remaining_size = available_size - copied_size
        if remaining_size <= 1e-12:
            sleeve.holdings.pop(trade.asset, None)
        else:
            sleeve.holdings[trade.asset] = remaining_size
        return SimulatedFill(
            scenario_name=signal.scenario_name,
            target_name=signal.target_name,
            side="SELL",
            asset_id=trade.asset,
            market_slug=trade.slug,
            outcome=trade.outcome,
            signal_timestamp_seconds=trade.timestamp_seconds,
            execution_timestamp_seconds=signal.execution_timestamp_seconds,
            signal_price=trade.price,
            execution_price=execution_price,
            target_usdc_size=round(trade.usdc_size, 6),
            copied_usdc=round(copied_usdc, 6),
            copied_size=round(copied_size, 6),
            status=match.status,
            reason=match.reason,
            execution_slippage_bps=round(_execution_slippage_bps(trade.price, execution_price, "SELL"), 6),
            requested_usdc=round(requested_usdc, 6),
            requested_size=round(requested_size, 6),
            fill_ratio=round(match.fill_ratio, 6),
        )

    return _skipped_fill(signal, book.last_trade_price, 0.0, "skip_unknown_side")


_DURATION_RE = re.compile(r"-(\d+)([mh])-([0-9]{10})$")
_MONTH_NAME_TO_NUMBER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}
_CALENDAR_RESOLUTION_RE = re.compile(
    r"-(january|february|march|april|may|june|july|august|september|october|november|december)-"
    r"([0-9]{1,2})(?:-([0-9]{4}))?-([0-9]{1,4})(am|pm)-et$"
)
_ET_ZONE = ZoneInfo("America/New_York")


def _build_redeem_settlements(
    signals: Sequence[EvalSignal],
    lookup: MinutePriceLookup,
    exit_mode: str,
) -> List[_RedeemSettlement]:
    if exit_mode.lower() != "redeem":
        return []

    by_condition: Dict[str, List[TradeActivity]] = {}
    for signal in signals:
        by_condition.setdefault(signal.trade.condition_id, []).append(signal.trade)

    settlements: List[_RedeemSettlement] = []
    for condition_id, trades in by_condition.items():
        first_trade = trades[0]
        resolution_ts = _infer_resolution_timestamp_seconds(first_trade)
        if resolution_ts is None:
            continue
        latest_prices: Dict[str, float] = {}
        for trade in trades:
            latest_price = lookup.price_at_or_after(trade.asset, resolution_ts, None)
            if latest_price is None:
                latest_price = lookup.price_at(trade.asset, resolution_ts, None)
            if latest_price is not None:
                latest_prices[trade.asset] = latest_price
        if not latest_prices:
            continue
        winning_asset = max(
            latest_prices.items(),
            key=lambda item: (item[1], item[0]),
        )[0]
        settlements.append(
            _RedeemSettlement(
                condition_id=condition_id,
                market_slug=first_trade.slug,
                resolution_timestamp_seconds=resolution_ts,
                payout_by_asset={
                    asset_id: 1.0 if asset_id == winning_asset else 0.0
                    for asset_id in latest_prices
                },
            )
        )
    return sorted(settlements, key=lambda item: item.resolution_timestamp_seconds)


def _infer_resolution_timestamp_seconds(trade: TradeActivity) -> Optional[int]:
    for candidate in [trade.event_slug, trade.slug]:
        if not candidate:
            continue
        match = _DURATION_RE.search(candidate)
        if match:
            duration = int(match.group(1))
            unit = match.group(2)
            start_ts = int(match.group(3))
            multiplier = 60 if unit == "m" else 3600
            return start_ts + duration * multiplier
        calendar_match = _CALENDAR_RESOLUTION_RE.search(candidate)
        if not calendar_match:
            continue
        month = _MONTH_NAME_TO_NUMBER[calendar_match.group(1)]
        day = int(calendar_match.group(2))
        year = _infer_calendar_year(calendar_match.group(3), trade.timestamp_seconds)
        parsed_time = _parse_calendar_time_token(calendar_match.group(4), calendar_match.group(5))
        if parsed_time is None:
            continue
        hour, minute = parsed_time
        # Calendar slugs like "...-2pm-et" represent the hourly market bucket.
        # They settle at the end of that labeled hour.
        return int(datetime(year, month, day, hour, minute, tzinfo=_ET_ZONE).timestamp()) + 3600
    return None


def _infer_calendar_year(raw_year: str | None, reference_timestamp_seconds: int) -> int:
    if raw_year:
        return int(raw_year)
    return datetime.fromtimestamp(reference_timestamp_seconds, tz=_ET_ZONE).year


def _parse_calendar_time_token(time_token: str, meridiem: str) -> Optional[Tuple[int, int]]:
    if not time_token.isdigit():
        return None
    if len(time_token) <= 2:
        hour = int(time_token)
        minute = 0
    elif len(time_token) == 3:
        hour = int(time_token[0])
        minute = int(time_token[1:])
    elif len(time_token) == 4:
        hour = int(time_token[:2])
        minute = int(time_token[2:])
    else:
        return None
    if hour < 1 or hour > 12 or minute < 0 or minute >= 60:
        return None
    if meridiem == "am":
        hour = 0 if hour == 12 else hour
    else:
        hour = 12 if hour == 12 else hour + 12
    return hour, minute


def _apply_redeem_settlement(
    sleeves: Dict[str, _SleeveState], settlement: _RedeemSettlement
) -> None:
    for sleeve in sleeves.values():
        for asset_id, payout in settlement.payout_by_asset.items():
            size = sleeve.holdings.get(asset_id, 0.0)
            if size <= 1e-12:
                continue
            sleeve.cash += size * payout
            sleeve.holdings.pop(asset_id, None)
            sleeve.last_prices.pop(asset_id, None)


def _equity_of_sleeve(
    sleeve: _SleeveState, timestamp_seconds: int, lookup: MinutePriceLookup
) -> float:
    equity = sleeve.cash
    for asset_id, size in sleeve.holdings.items():
        fallback = sleeve.last_prices.get(asset_id)
        price = lookup.price_at(asset_id, timestamp_seconds, fallback)
        if price is None:
            continue
        sleeve.last_prices[asset_id] = price
        equity += size * price
    return equity


def _skipped_fill(
    signal: EvalSignal, price: float, copied_usdc: float, reason: str
) -> SimulatedFill:
    trade = signal.trade
    copied_size = (copied_usdc / price) if price > 0 and copied_usdc > 0 else 0.0
    return SimulatedFill(
        scenario_name=signal.scenario_name,
        target_name=signal.target_name,
        side=trade.side.upper(),
        asset_id=trade.asset,
        market_slug=trade.slug,
        outcome=trade.outcome,
        signal_timestamp_seconds=trade.timestamp_seconds,
        execution_timestamp_seconds=signal.execution_timestamp_seconds,
        signal_price=trade.price,
        execution_price=price,
        target_usdc_size=round(trade.usdc_size, 6),
        copied_usdc=round(copied_usdc, 6),
        copied_size=round(copied_size, 6),
        status="skipped",
        reason=reason,
        execution_slippage_bps=round(_execution_slippage_bps(trade.price, price, trade.side.upper()), 6),
        requested_usdc=round(copied_usdc, 6),
        requested_size=round(copied_size, 6),
        fill_ratio=0.0,
    )


def _execution_slippage_bps(signal_price: float, execution_price: float, side: str) -> float:
    if signal_price <= 0 or execution_price <= 0:
        return 0.0
    if side == "BUY":
        return ((execution_price - signal_price) / signal_price) * 10000.0
    return ((signal_price - execution_price) / signal_price) * 10000.0


def _max_drawdown_pct(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    peak = values[0]
    max_drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        if peak <= 0:
            continue
        drawdown = (peak - value) / peak
        max_drawdown = max(max_drawdown, drawdown)
    return max_drawdown * 100.0


def _floor_minute(timestamp_seconds: int) -> int:
    return (timestamp_seconds // 60) * 60


def _ceil_minute(timestamp_seconds: int) -> int:
    floored = _floor_minute(timestamp_seconds)
    return floored if floored == timestamp_seconds else floored + 60


def _isoformat(timestamp_seconds: int) -> str:
    return datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc).isoformat()


def _benchmark_scenario_for(scenario_name: str) -> Optional[str]:
    if scenario_name == "target_reference":
        return None
    if scenario_name == "copy_reference_0m":
        return "target_reference"
    return "copy_reference_0m"


def build_evaluation_app(config_path: str) -> EvaluationApp:
    return EvaluationApp(load_eval_config(config_path))
