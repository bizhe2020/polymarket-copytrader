from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List


@dataclass
class StrategyConfigSkeletonV0Config:
    strategy_rollout_bundle_json_path: str
    btc_strategy_json_path: str
    eth_sol_override_json_path: str
    output_json_path: str
    output_csv_path: str


@dataclass
class StrategyConfigSkeletonV0Summary:
    config_id: str
    active_families: List[str]
    deferred_families: List[str]
    config_payload: Dict[str, object]


def build_strategy_config_skeleton_v0(
    config: StrategyConfigSkeletonV0Config,
) -> StrategyConfigSkeletonV0Summary:
    bundle = json.loads(Path(config.strategy_rollout_bundle_json_path).read_text(encoding="utf-8"))
    btc = json.loads(Path(config.btc_strategy_json_path).read_text(encoding="utf-8"))
    eth_sol = json.loads(Path(config.eth_sol_override_json_path).read_text(encoding="utf-8"))

    active_families = list(bundle["active_families"])
    deferred_families = list(bundle["deferred_families"])

    family_configs: Dict[str, object] = {
        "btc": {
            "enabled": True,
            "strategy_type": "family_default_state_machine",
            "strategy_id": btc["strategy_id"],
            "entry": {
                "window_seconds": btc["state_machine"]["observe_open"]["entry_window_seconds"],
                "price_band_lower": btc["state_machine"]["enter_first_leg"]["price_band_lower"],
                "price_band_upper": btc["state_machine"]["enter_first_leg"]["price_band_upper"],
                "size_anchor_usdc": btc["state_machine"]["enter_first_leg"]["size_anchor_usdc"],
                "size_policy": btc["state_machine"]["enter_first_leg"]["size_policy"],
                "optional_soft_size_max_distance_pct": btc["state_machine"]["enter_first_leg"]["optional_soft_size_max_distance_pct"],
                "min_seconds_to_resolution": btc["state_machine"]["enter_first_leg"]["min_seconds_to_resolution"],
            },
            "second_leg": {
                "default_route_class": btc["state_machine"]["monitor_second_leg"]["default_route_class"],
                "wait_budget_70pct": btc["state_machine"]["monitor_second_leg"]["default_wait_budget_70pct"],
                "wait_budget_80pct": btc["state_machine"]["monitor_second_leg"]["default_wait_budget_80pct"],
                "acceptance_fast_bands": btc["state_machine"]["monitor_second_leg"]["acceptance_fast_bands"],
                "acceptance_delayed_tail_bands": btc["state_machine"]["monitor_second_leg"]["acceptance_delayed_tail_bands"],
                "acceptance_hard_lock_bands": btc["state_machine"]["monitor_second_leg"]["acceptance_hard_lock_bands"],
                "timeout_seconds": btc["state_machine"]["complete_or_timeout"]["default_timeout_seconds"],
            },
            "regime_overrides": btc["regime_overrides"],
        },
    }

    for family in ("eth", "sol"):
        strategy = eth_sol["strategies"][family]
        family_configs[family] = {
            "enabled": True,
            "strategy_type": "default_plus_override_state_machine",
            "strategy_id": strategy["strategy_id"],
            "entry": {
                "window_seconds": strategy["state_machine"]["observe_open"]["entry_window_seconds"],
                "price_band_lower": strategy["state_machine"]["enter_first_leg"]["price_band_lower"],
                "price_band_upper": strategy["state_machine"]["enter_first_leg"]["price_band_upper"],
                "size_anchor_usdc": strategy["state_machine"]["enter_first_leg"]["size_anchor_usdc"],
                "size_policy": strategy["state_machine"]["enter_first_leg"]["size_policy"],
                "optional_soft_size_max_distance_pct": strategy["state_machine"]["enter_first_leg"]["optional_soft_size_max_distance_pct"],
                "min_seconds_to_resolution": strategy["state_machine"]["enter_first_leg"]["min_seconds_to_resolution"],
            },
            "second_leg": {
                "default_route_class": strategy["default_route"]["route_class"],
                "wait_budget_70pct": strategy["default_route"]["wait_budget_70pct"],
                "wait_budget_80pct": strategy["default_route"]["wait_budget_80pct"],
                "acceptance_fast_bands": strategy["state_machine"]["monitor_second_leg"]["acceptance_fast_bands"],
                "acceptance_delayed_tail_bands": strategy["state_machine"]["monitor_second_leg"]["acceptance_delayed_tail_bands"],
                "acceptance_hard_lock_bands": strategy["state_machine"]["monitor_second_leg"]["acceptance_hard_lock_bands"],
                "timeout_seconds": strategy["state_machine"]["complete_or_timeout"]["default_timeout_seconds"],
            },
            "regime_overrides": strategy["regime_routes"],
        }

    for family in deferred_families:
        family_configs[family] = {
            "enabled": False,
            "strategy_type": "deferred",
            "reason": "regime_aware_required",
        }

    config_payload: Dict[str, object] = {
        "config_id": "strategy_config_skeleton_v0",
        "bundle_id": bundle["bundle_id"],
        "mode": "skeleton",
        "runtime": {
            "market_duration_bucket": "hourly",
            "poll_interval_seconds": 1.0,
            "market_websocket_enabled": True,
            "require_real_order_book": False,
            "notes": "Skeleton only. Fill execution and market-data wiring before running.",
        },
        "rollout": {
            "active_families": active_families,
            "deferred_families": deferred_families,
            "phase_1": ["btc"],
            "phase_2": ["eth", "sol"],
        },
        "families": family_configs,
    }

    csv_rows: List[Dict[str, object]] = []
    for family in active_families + deferred_families:
        row = family_configs[family]
        csv_rows.append(
            {
                "family": family,
                "enabled": row["enabled"],
                "strategy_type": row["strategy_type"],
                "phase": (
                    "phase_1"
                    if family == "btc"
                    else "phase_2"
                    if family in {"eth", "sol"}
                    else "deferred"
                ),
                "strategy_id": row.get("strategy_id", ""),
                "min_seconds_to_resolution": row.get("entry", {}).get("min_seconds_to_resolution", ""),
                "default_route_class": row.get("second_leg", {}).get("default_route_class", ""),
                "wait_budget_70pct": row.get("second_leg", {}).get("wait_budget_70pct", ""),
                "wait_budget_80pct": row.get("second_leg", {}).get("wait_budget_80pct", ""),
            }
        )

    _write_csv(config.output_csv_path, csv_rows)
    output_path = Path(config.output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(config_payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return StrategyConfigSkeletonV0Summary(
        config_id="strategy_config_skeleton_v0",
        active_families=active_families,
        deferred_families=deferred_families,
        config_payload=config_payload,
    )


def _write_csv(path: str, rows: List[Dict[str, object]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return
    fieldnames: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
