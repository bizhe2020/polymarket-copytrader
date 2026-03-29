from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List


@dataclass
class EthSolOverrideStrategiesV0Config:
    strategy_blueprint_json_path: str
    strategy_playbook_json_path: str
    output_json_path: str
    output_csv_path: str


@dataclass
class EthSolOverrideStrategiesV0Summary:
    strategies: Dict[str, Dict[str, object]]


def build_eth_sol_override_strategies_v0(
    config: EthSolOverrideStrategiesV0Config,
) -> EthSolOverrideStrategiesV0Summary:
    blueprint = json.loads(Path(config.strategy_blueprint_json_path).read_text(encoding="utf-8"))
    playbook = json.loads(Path(config.strategy_playbook_json_path).read_text(encoding="utf-8"))

    strategies: Dict[str, Dict[str, object]] = {}
    csv_rows: List[Dict[str, object]] = []

    for family in ("eth", "sol"):
        family_blueprint = blueprint["family_blueprints"][family]
        family_playbook = playbook["family_playbooks"][family]
        entry = family_blueprint["entry"]
        second_leg = family_blueprint["second_leg"]
        overrides = family_blueprint["regime_overrides"]

        state_machine = {
            "observe_open": {
                "goal": f"Wait for a fresh hourly {family.upper()} market and track the default open window.",
                "entry_window_seconds": entry["open_window_seconds_default"],
                "transition_on": "market_open_detected",
            },
            "enter_first_leg": {
                "goal": f"Open the first leg only inside the {family.upper()} entry band.",
                "price_band_lower": entry["price_band_lower"],
                "price_band_upper": entry["price_band_upper"],
                "size_anchor_usdc": entry["size_anchor_usdc"],
                "size_policy": entry["size_policy"],
                "optional_soft_size_max_distance_pct": entry["optional_soft_size_max_distance_pct"],
                "min_seconds_to_resolution": entry["min_seconds_to_resolution"],
                "transition_on": "first_leg_filled",
            },
            "select_second_leg_route": {
                "goal": f"Start from the {family.upper()} default second-leg path, then switch when an override regime is detected.",
                "default_route_class": second_leg["default_route_class"],
                "default_wait_budget_70pct": second_leg["default_wait_budget_70pct"],
                "default_wait_budget_80pct": second_leg["default_wait_budget_80pct"],
                "override_regimes": sorted(overrides.keys()),
                "transition_on": "regime_resolved",
            },
            "monitor_second_leg": {
                "goal": f"Monitor the opposite leg using the selected {family.upper()} route.",
                "acceptance_fast_bands": second_leg["acceptance_fast_bands"],
                "acceptance_delayed_tail_bands": second_leg["acceptance_delayed_tail_bands"],
                "acceptance_hard_lock_bands": second_leg["acceptance_hard_lock_bands"],
                "transition_on": "second_leg_price_seen",
            },
            "complete_or_timeout": {
                "goal": f"Complete the pair inside the preferred band or stop at the selected {family.upper()} timeout.",
                "preferred_hard_lock_band": second_leg["acceptance_hard_lock_bands"],
                "default_timeout_seconds": second_leg["default_wait_budget_80pct"],
                "fallback_action": "timeout_without_forcing_far_tail_price",
                "transition_on": "pair_completed_or_timeout",
            },
        }

        regime_routes = {
            regime: {
                "route_class": row["route_class"],
                "wait_budget_70pct": row["wait_budget_70pct"],
                "wait_budget_80pct": row["wait_budget_80pct"],
                "median_completion_seconds": row["median_completion_seconds"],
                "trigger_note": row["policy_note"],
            }
            for regime, row in sorted(overrides.items())
        }

        strategy_id = f"{family}_override_strategy_v0"
        strategy = {
            "strategy_id": strategy_id,
            "family": family,
            "deployment_mode": family_playbook["deployment_mode"],
            "default_route": {
                "route_class": second_leg["default_route_class"],
                "wait_budget_70pct": second_leg["default_wait_budget_70pct"],
                "wait_budget_80pct": second_leg["default_wait_budget_80pct"],
            },
            "state_machine": state_machine,
            "regime_routes": regime_routes,
        }
        strategies[family] = strategy

        csv_rows.append(
            {
                "row_type": "strategy_meta",
                "strategy_id": strategy_id,
                "family": family,
                "deployment_mode": family_playbook["deployment_mode"],
                "entry_window_seconds": entry["open_window_seconds_default"],
                "entry_price_band": f"{entry['price_band_lower']}-{entry['price_band_upper']}",
                "min_seconds_to_resolution": entry["min_seconds_to_resolution"],
                "size_policy": entry["size_policy"],
                "default_route_class": second_leg["default_route_class"],
                "default_wait_budget_70pct": second_leg["default_wait_budget_70pct"],
                "default_wait_budget_80pct": second_leg["default_wait_budget_80pct"],
                "override_regimes": ",".join(sorted(regime_routes.keys())),
            }
        )
        for state_name, state in state_machine.items():
            csv_rows.append(
                {
                    "row_type": "state",
                    "strategy_id": strategy_id,
                    "family": family,
                    "state_name": state_name,
                    "goal": state["goal"],
                    "transition_on": state["transition_on"],
                }
            )
        for regime, row in regime_routes.items():
            csv_rows.append(
                {
                    "row_type": "regime_route",
                    "strategy_id": strategy_id,
                    "family": family,
                    "regime": regime,
                    "route_class": row["route_class"],
                    "wait_budget_70pct": row["wait_budget_70pct"],
                    "wait_budget_80pct": row["wait_budget_80pct"],
                    "median_completion_seconds": row["median_completion_seconds"],
                    "trigger_note": row["trigger_note"],
                }
            )

    _write_csv(config.output_csv_path, csv_rows)
    summary = EthSolOverrideStrategiesV0Summary(strategies=strategies)
    output_path = Path(config.output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return summary


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
