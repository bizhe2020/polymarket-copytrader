from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List

DEFAULT_MIN_SECONDS_TO_RESOLUTION = 300


@dataclass
class EntryRulesetV1Config:
    first_leg_ruleset_json_path: str
    first_cycle_outcome_json_path: str
    output_json_path: str
    output_csv_path: str


@dataclass
class EntryRulesetV1Summary:
    overall_baseline: Dict[str, float]
    family_rules: Dict[str, Dict[str, object]]


def build_entry_ruleset_v1(config: EntryRulesetV1Config) -> EntryRulesetV1Summary:
    base_rules = json.loads(Path(config.first_leg_ruleset_json_path).read_text(encoding="utf-8"))["family_rules"]
    outcome = json.loads(Path(config.first_cycle_outcome_json_path).read_text(encoding="utf-8"))

    family_rules: Dict[str, Dict[str, object]] = {}
    csv_rows: List[Dict[str, object]] = []

    for family in sorted(base_rules.keys()):
        base_rule = base_rules[family]
        family_outcome = outcome["family_component_summary"][family]
        time_price = family_outcome["time_price"]
        soft_100 = family_outcome["time_price_size_soft_100"]
        size_policy = _size_policy_for_family(time_price=time_price, soft_100=soft_100)

        row = {
            "family": family,
            "entry_rule_class": "time_price_v1",
            "open_window_seconds_default": int(base_rule["open_window_seconds_default"]),
            "open_window_seconds_aggressive": int(base_rule["open_window_seconds_aggressive"]),
            "min_seconds_to_resolution": int(DEFAULT_MIN_SECONDS_TO_RESOLUTION),
            "price_band_lower": float(base_rule["price_band_lower"]),
            "price_band_upper": float(base_rule["price_band_upper"]),
            "size_anchor_usdc": float(base_rule["size_anchor_usdc"]),
            "size_policy": size_policy["size_policy"],
            "optional_soft_size_max_distance_pct": size_policy["optional_soft_size_max_distance_pct"],
            "time_price_coverage_pct": float(time_price["coverage_pct"]),
            "time_price_hard_lock_pct": float(time_price["hard_lock_pct"]),
            "time_price_bridge_pct": float(time_price["bridge_cycle_pct"]),
            "time_price_soft100_coverage_pct": float(soft_100["coverage_pct"]),
            "time_price_soft100_hard_lock_pct": float(soft_100["hard_lock_pct"]),
            "time_price_soft100_bridge_pct": float(soft_100["bridge_cycle_pct"]),
            "rule_note": _rule_note(
                family=family,
                base_rule=base_rule,
                time_price=time_price,
                size_policy=size_policy,
            ),
        }
        family_rules[family] = row
        csv_rows.append(row)

    _write_csv(config.output_csv_path, csv_rows)
    summary = EntryRulesetV1Summary(
        overall_baseline=outcome["overall_baseline"],
        family_rules=family_rules,
    )
    output_path = Path(config.output_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def _size_policy_for_family(*, time_price: Dict[str, object], soft_100: Dict[str, object]) -> Dict[str, object]:
    time_price_hard = float(time_price["hard_lock_pct"])
    time_price_cov = float(time_price["coverage_pct"])
    soft_hard = float(soft_100["hard_lock_pct"])
    soft_cov = float(soft_100["coverage_pct"])

    if soft_hard >= time_price_hard + 1.0 and soft_cov >= time_price_cov * 0.55:
        return {
            "size_policy": "optional_soft_100",
            "optional_soft_size_max_distance_pct": 100.0,
        }
    return {
        "size_policy": "no_hard_size_gate",
        "optional_soft_size_max_distance_pct": None,
    }


def _rule_note(
    *,
    family: str,
    base_rule: Dict[str, object],
    time_price: Dict[str, object],
    size_policy: Dict[str, object],
) -> str:
    window = int(base_rule["open_window_seconds_default"])
    low = float(base_rule["price_band_lower"])
    high = float(base_rule["price_band_upper"])
    hard_lock = float(time_price["hard_lock_pct"])
    coverage = float(time_price["coverage_pct"])
    if size_policy["size_policy"] == "optional_soft_100":
        return (
            f"{family}: use time+price as the core entry rule "
            f"(<= {window}s, price in [{low}, {high}], >= {DEFAULT_MIN_SECONDS_TO_RESOLUTION}s to resolution); "
            f"optionally reject size farther than 100% from the family anchor."
        )
    return (
        f"{family}: use time+price as the core entry rule "
        f"(<= {window}s, price in [{low}, {high}], >= {DEFAULT_MIN_SECONDS_TO_RESOLUTION}s to resolution); "
        f"do not hard-gate on size at v1. "
        f"This structure covers {coverage:.2f}% with {hard_lock:.2f}% hard-lock."
    )


def _write_csv(path: str, rows: List[Dict[str, object]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        output_path.write_text("", encoding="utf-8")
        return
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
