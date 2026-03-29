from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List


@dataclass
class StrategyPlaybookV0Config:
    strategy_blueprint_json_path: str
    output_json_path: str
    output_csv_path: str


@dataclass
class StrategyPlaybookV0Summary:
    rollout_order: List[str]
    deferred_families: List[str]
    family_playbooks: Dict[str, Dict[str, object]]


def build_strategy_playbook_v0(config: StrategyPlaybookV0Config) -> StrategyPlaybookV0Summary:
    blueprint = json.loads(Path(config.strategy_blueprint_json_path).read_text(encoding="utf-8"))
    family_blueprints = blueprint["family_blueprints"]

    rollout_order = _rollout_order(family_blueprints)
    deferred_families = [family for family, row in family_blueprints.items() if row["incubation_stage"] == "regime_aware_required"]

    family_playbooks: Dict[str, Dict[str, object]] = {}
    csv_rows: List[Dict[str, object]] = []
    for family in rollout_order + [family for family in sorted(family_blueprints.keys()) if family not in rollout_order]:
        row = family_blueprints[family]
        deployment_mode = _deployment_mode(row["incubation_stage"])
        playbook = {
            "deployment_mode": deployment_mode,
            "entry": row["entry"],
            "second_leg": row["second_leg"],
            "override_regimes": sorted(row["regime_overrides"].keys()),
            "execution_notes": _execution_notes(
                family=family,
                deployment_mode=deployment_mode,
                row=row,
            ),
        }
        family_playbooks[family] = playbook
        csv_rows.append(
            {
                "family": family,
                "deployment_mode": deployment_mode,
                "entry_window_default": row["entry"]["open_window_seconds_default"],
                "entry_price_band": f"{row['entry']['price_band_lower']}-{row['entry']['price_band_upper']}",
                "size_policy": row["entry"]["size_policy"],
                "default_route_class": row["second_leg"]["default_route_class"],
                "default_wait_budget_70pct": row["second_leg"]["default_wait_budget_70pct"],
                "default_wait_budget_80pct": row["second_leg"]["default_wait_budget_80pct"],
                "override_regimes": ",".join(sorted(row["regime_overrides"].keys())),
                "execution_notes": playbook["execution_notes"],
            }
        )

    _write_csv(config.output_csv_path, csv_rows)
    summary = StrategyPlaybookV0Summary(
        rollout_order=rollout_order,
        deferred_families=deferred_families,
        family_playbooks=family_playbooks,
    )
    output_path = Path(config.output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _rollout_order(family_blueprints: Dict[str, Dict[str, object]]) -> List[str]:
    priority = {
        "default_ready": 0,
        "default_plus_exceptions": 1,
        "regime_aware_required": 2,
    }
    ordered = sorted(
        family_blueprints.items(),
        key=lambda item: (
            priority.get(str(item[1]["incubation_stage"]), 99),
            -float(item[1]["second_leg"]["route_confidence_pct"]),
            item[0],
        ),
    )
    return [family for family, row in ordered if row["incubation_stage"] != "regime_aware_required"]


def _deployment_mode(incubation_stage: str) -> str:
    if incubation_stage == "default_ready":
        return "ship_family_default"
    if incubation_stage == "default_plus_exceptions":
        return "ship_default_with_overrides"
    return "defer_or_isolate"


def _execution_notes(*, family: str, deployment_mode: str, row: Dict[str, object]) -> str:
    entry = row["entry"]
    second_leg = row["second_leg"]
    overrides = sorted(row["regime_overrides"].keys())
    if deployment_mode == "ship_family_default":
        return (
            f"{family}: start directly from the family default. Entry around {entry['price_band_lower']}-{entry['price_band_upper']} "
            f"within {entry['open_window_seconds_default']}s, then use {second_leg['default_wait_budget_70pct']}s/"
            f"{second_leg['default_wait_budget_80pct']}s second-leg budgets."
        )
    if deployment_mode == "ship_default_with_overrides":
        return (
            f"{family}: ship the family default first, but explicitly branch on regime overrides ({', '.join(overrides)})."
        )
    return (
        f"{family}: do not include in the default rollout. Treat as a separate regime-aware branch if pursued."
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
