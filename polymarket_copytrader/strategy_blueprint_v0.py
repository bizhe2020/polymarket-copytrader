from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List


@dataclass
class StrategyBlueprintV0Config:
    entry_ruleset_json_path: str
    second_leg_policy_ruleset_json_path: str
    second_leg_wait_budget_stability_json_path: str
    output_json_path: str
    output_csv_path: str


@dataclass
class StrategyBlueprintV0Summary:
    global_strategy: Dict[str, object]
    family_blueprints: Dict[str, Dict[str, object]]


def build_strategy_blueprint_v0(config: StrategyBlueprintV0Config) -> StrategyBlueprintV0Summary:
    entry = json.loads(Path(config.entry_ruleset_json_path).read_text(encoding="utf-8"))
    second_leg = json.loads(Path(config.second_leg_policy_ruleset_json_path).read_text(encoding="utf-8"))
    stability = json.loads(Path(config.second_leg_wait_budget_stability_json_path).read_text(encoding="utf-8"))

    global_strategy = {
        "strategy_class": "pair_unit_builder_v0",
        "entry_rule_class": "entry_ruleset_v1",
        "second_leg_rule_class": "second_leg_policy_ruleset_v0",
        "operating_principle": (
            "Open a first leg early near the family-specific mid band, then complete the pair using "
            "family/regime-aware wait budgets and parity-anchor acceptance bands."
        ),
        "global_acceptance_fast_bands": second_leg["global_acceptance_policy"]["fast_accept_bands"],
        "global_acceptance_delayed_tail_bands": second_leg["global_acceptance_policy"]["delayed_tail_bands"],
        "global_hard_lock_bands": second_leg["global_acceptance_policy"]["hard_lock_bands"],
    }

    family_blueprints: Dict[str, Dict[str, object]] = {}
    csv_rows: List[Dict[str, object]] = []

    for family in sorted(entry["family_rules"].keys()):
        entry_rule = entry["family_rules"][family]
        second_leg_rule = second_leg["family_rules"][family]
        stability_rule = stability["family_summary"][family]
        incubation_stage = _incubation_stage(
            stability_tier=str(stability_rule["stability_tier"]),
            route_confidence_pct=float(second_leg_rule["route_confidence_pct"]),
        )
        regime_overrides = {
            regime: route
            for regime, route in second_leg_rule["regime_routes"].items()
            if (
                str(route["route_class"]) != str(second_leg_rule["default_route_class"])
                or int(route["wait_budget_70pct"]) != int(second_leg_rule["default_wait_budget_70pct"])
                or int(route["wait_budget_80pct"]) != int(second_leg_rule["default_wait_budget_80pct"])
            )
        }
        blueprint = {
            "incubation_stage": incubation_stage,
            "entry": {
                "open_window_seconds_default": entry_rule["open_window_seconds_default"],
                "open_window_seconds_aggressive": entry_rule["open_window_seconds_aggressive"],
                "min_seconds_to_resolution": entry_rule["min_seconds_to_resolution"],
                "price_band_lower": entry_rule["price_band_lower"],
                "price_band_upper": entry_rule["price_band_upper"],
                "size_anchor_usdc": entry_rule["size_anchor_usdc"],
                "size_policy": entry_rule["size_policy"],
                "optional_soft_size_max_distance_pct": entry_rule["optional_soft_size_max_distance_pct"],
            },
            "second_leg": {
                "default_route_class": second_leg_rule["default_route_class"],
                "default_wait_budget_70pct": second_leg_rule["default_wait_budget_70pct"],
                "default_wait_budget_80pct": second_leg_rule["default_wait_budget_80pct"],
                "acceptance_fast_bands": second_leg_rule["acceptance_fast_bands"],
                "acceptance_delayed_tail_bands": second_leg_rule["acceptance_delayed_tail_bands"],
                "acceptance_hard_lock_bands": second_leg_rule["acceptance_hard_lock_bands"],
                "route_confidence_pct": second_leg_rule["route_confidence_pct"],
                "stability_tier": stability_rule["stability_tier"],
            },
            "regime_overrides": regime_overrides,
            "blueprint_note": _blueprint_note(
                family=family,
                incubation_stage=incubation_stage,
                second_leg_rule=second_leg_rule,
                stability_rule=stability_rule,
                regime_overrides=regime_overrides,
            ),
        }
        family_blueprints[family] = blueprint
        csv_rows.append(
            {
                "family": family,
                "incubation_stage": incubation_stage,
                "entry_window_default": entry_rule["open_window_seconds_default"],
                "min_seconds_to_resolution": entry_rule["min_seconds_to_resolution"],
                "entry_price_band": f"{entry_rule['price_band_lower']}-{entry_rule['price_band_upper']}",
                "size_policy": entry_rule["size_policy"],
                "default_route_class": second_leg_rule["default_route_class"],
                "default_wait_budget_70pct": second_leg_rule["default_wait_budget_70pct"],
                "default_wait_budget_80pct": second_leg_rule["default_wait_budget_80pct"],
                "route_confidence_pct": second_leg_rule["route_confidence_pct"],
                "stability_tier": stability_rule["stability_tier"],
                "acceptance_fast_bands": ",".join(second_leg_rule["acceptance_fast_bands"]),
                "acceptance_delayed_tail_bands": ",".join(second_leg_rule["acceptance_delayed_tail_bands"]),
                "acceptance_hard_lock_bands": ",".join(second_leg_rule["acceptance_hard_lock_bands"]),
                "override_count": len(regime_overrides),
                "override_regimes": ",".join(sorted(regime_overrides.keys())),
                "blueprint_note": blueprint["blueprint_note"],
            }
        )

    _write_csv(config.output_csv_path, csv_rows)
    summary = StrategyBlueprintV0Summary(
        global_strategy=global_strategy,
        family_blueprints=family_blueprints,
    )
    output_path = Path(config.output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _incubation_stage(*, stability_tier: str, route_confidence_pct: float) -> str:
    if stability_tier == "stable_default" and route_confidence_pct >= 75.0:
        return "default_ready"
    if stability_tier in {"stable_default", "mixed_default"}:
        return "default_plus_exceptions"
    return "regime_aware_required"


def _blueprint_note(
    *,
    family: str,
    incubation_stage: str,
    second_leg_rule: Dict[str, object],
    stability_rule: Dict[str, object],
    regime_overrides: Dict[str, object],
) -> str:
    route = second_leg_rule["default_route_class"]
    budget70 = second_leg_rule["default_wait_budget_70pct"]
    budget80 = second_leg_rule["default_wait_budget_80pct"]
    stability_tier = stability_rule["stability_tier"]
    if incubation_stage == "default_ready":
        return (
            f"{family}: the family default is already strong enough to incubate directly "
            f"({route}, {budget70}s/{budget80}s, {stability_tier})."
        )
    if incubation_stage == "default_plus_exceptions":
        return (
            f"{family}: start from the family default ({route}, {budget70}s/{budget80}s) "
            f"and layer {len(regime_overrides)} regime override(s) where needed."
        )
    return (
        f"{family}: do not incubate from a single family default; route second-leg behavior through regime-aware "
        f"overrides first."
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
