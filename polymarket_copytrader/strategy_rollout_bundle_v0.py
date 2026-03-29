from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List


@dataclass
class StrategyRolloutBundleV0Config:
    strategy_playbook_json_path: str
    btc_strategy_json_path: str
    eth_sol_override_json_path: str
    output_json_path: str
    output_csv_path: str


@dataclass
class StrategyRolloutBundleV0Summary:
    bundle_id: str
    active_families: List[str]
    deferred_families: List[str]
    family_strategies: Dict[str, Dict[str, object]]


def build_strategy_rollout_bundle_v0(
    config: StrategyRolloutBundleV0Config,
) -> StrategyRolloutBundleV0Summary:
    playbook = json.loads(Path(config.strategy_playbook_json_path).read_text(encoding="utf-8"))
    btc_strategy = json.loads(Path(config.btc_strategy_json_path).read_text(encoding="utf-8"))
    eth_sol = json.loads(Path(config.eth_sol_override_json_path).read_text(encoding="utf-8"))

    active_families = list(playbook["rollout_order"])
    deferred_families = list(playbook["deferred_families"])
    family_strategies = {
        "btc": {
            "strategy_id": btc_strategy["strategy_id"],
            "deployment_mode": btc_strategy["deployment_mode"],
            "strategy_type": "family_default_state_machine",
        },
        "eth": {
            "strategy_id": eth_sol["strategies"]["eth"]["strategy_id"],
            "deployment_mode": eth_sol["strategies"]["eth"]["deployment_mode"],
            "strategy_type": "default_plus_override_state_machine",
        },
        "sol": {
            "strategy_id": eth_sol["strategies"]["sol"]["strategy_id"],
            "deployment_mode": eth_sol["strategies"]["sol"]["deployment_mode"],
            "strategy_type": "default_plus_override_state_machine",
        },
    }

    csv_rows: List[Dict[str, object]] = []
    for family in active_families:
        csv_rows.append(
            {
                "family": family,
                "status": "active",
                "strategy_id": family_strategies[family]["strategy_id"],
                "deployment_mode": family_strategies[family]["deployment_mode"],
                "strategy_type": family_strategies[family]["strategy_type"],
            }
        )
    for family in deferred_families:
        csv_rows.append(
            {
                "family": family,
                "status": "deferred",
                "strategy_id": "",
                "deployment_mode": "defer_or_isolate",
                "strategy_type": "regime_aware_required",
            }
        )

    _write_csv(config.output_csv_path, csv_rows)
    summary = StrategyRolloutBundleV0Summary(
        bundle_id="strategy_rollout_bundle_v0",
        active_families=active_families,
        deferred_families=deferred_families,
        family_strategies=family_strategies,
    )
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
