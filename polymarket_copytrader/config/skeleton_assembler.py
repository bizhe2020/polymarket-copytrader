"""
Skeleton assembler for strategy_config_skeleton_v0.

Reads var/research_blue_walnut/strategy_config_skeleton_v0.json,
normalizes and validates each family config, and returns a dict
 keyed by family name -> assembled family strategy dict.

Usage
-----
    from polymarket_copytrader.config.skeleton_assembler import load_skeleton

    bundle = load_skeleton()
    btc_strategy = bundle["families"]["btc"]
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SKELETON_PATH = Path(__file__).parent.parent.parent / "var" / "research_blue_walnut" / "strategy_config_skeleton_v0.json"

# Required top-level keys
_REQUIRED_TOP_KEYS = {"bundle_id", "config_id", "mode", "rollout", "families"}

# Required per-family keys (for enabled families)
_REQUIRED_FAMILY_KEYS = {"enabled", "strategy_type", "strategy_id", "entry", "second_leg"}

# Required entry sub-keys
_REQUIRED_ENTRY_KEYS = {"price_band_lower", "price_band_upper", "window_seconds", "size_policy"}

# Required second_leg sub-keys
_REQUIRED_SECOND_LEG_KEYS = {
    "default_route_class",
    "timeout_seconds",
    "wait_budget_70pct",
    "wait_budget_80pct",
    "acceptance_fast_bands",
    "acceptance_delayed_tail_bands",
    "acceptance_hard_lock_bands",
}

# Valid strategy_type values
_VALID_STRATEGY_TYPES = {
    "family_default_state_machine",
    "default_plus_override_state_machine",
    "deferred",
}

# Valid size_policy values observed across families
_VALID_SIZE_POLICIES = {
    "optional_soft_100",
    "no_hard_size_gate",
    # add new policies here as they appear
}

# Valid route_class values
_VALID_ROUTE_CLASSES = {
    "fast_20",
    "fast_20_30",
    "slow_30",
    "slow_30_60",
    "mixed_20_60",
}


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

class SkeletonValidationError(ValueError):
    """Raised when strategy_config_skeleton_v0 fails validation."""
    pass


def _check_keys(present: str, actual: set, required: set) -> None:
    missing = required - actual
    if missing:
        raise SkeletonValidationError(
            f"[{present}] Missing required key(s): {sorted(missing)}"
        )


def _validate_entry(entry: Dict[str, Any], family: str) -> Dict[str, Any]:
    _check_keys(f"family={family} entry", set(entry.keys()), _REQUIRED_ENTRY_KEYS)

    # Normalize numeric fields
    normalized = dict(entry)
    normalized["price_band_lower"] = float(entry["price_band_lower"])
    normalized["price_band_upper"] = float(entry["price_band_upper"])
    normalized["window_seconds"] = int(entry["window_seconds"])
    normalized["min_seconds_to_resolution"] = int(entry.get("min_seconds_to_resolution", 300))

    # Optional fields with defaults
    normalized.setdefault("size_anchor_usdc", None)
    if normalized["size_anchor_usdc"] is not None:
        normalized["size_anchor_usdc"] = float(normalized["size_anchor_usdc"])

    normalized.setdefault("optional_soft_size_max_distance_pct", None)
    if normalized["optional_soft_size_max_distance_pct"] is not None:
        normalized["optional_soft_size_max_distance_pct"] = float(
            normalized["optional_soft_size_max_distance_pct"]
        )

    # Validate price band sanity
    if normalized["price_band_lower"] >= normalized["price_band_upper"]:
        raise SkeletonValidationError(
            f"[family={family}] entry.price_band_lower "
            f"({normalized['price_band_lower']}) must be < price_band_upper "
            f"({normalized['price_band_upper']})"
        )

    # Validate window
    if normalized["window_seconds"] <= 0:
        raise SkeletonValidationError(
            f"[family={family}] entry.window_seconds must be positive, "
            f"got {normalized['window_seconds']}"
        )

    # Validate min_seconds_to_resolution
    if normalized["min_seconds_to_resolution"] < 0:
        raise SkeletonValidationError(
            f"[family={family}] entry.min_seconds_to_resolution must be >= 0, "
            f"got {normalized['min_seconds_to_resolution']}"
        )

    return normalized


def _validate_second_leg(second_leg: Dict[str, Any], family: str) -> Dict[str, Any]:
    _check_keys(f"family={family} second_leg", set(second_leg.keys()), _REQUIRED_SECOND_LEG_KEYS)

    normalized = dict(second_leg)
    normalized["timeout_seconds"] = int(second_leg["timeout_seconds"])
    normalized["wait_budget_70pct"] = int(second_leg["wait_budget_70pct"])
    normalized["wait_budget_80pct"] = int(second_leg["wait_budget_80pct"])

    # Validate bands are non-empty lists
    for band_key in (
        "acceptance_fast_bands",
        "acceptance_delayed_tail_bands",
        "acceptance_hard_lock_bands",
    ):
        bands = second_leg.get(band_key, [])
        if not isinstance(bands, list):
            raise SkeletonValidationError(
                f"[family={family}] second_leg.{band_key} must be a list, "
                f"got {type(bands).__name__}"
            )
        normalized[band_key] = bands

    # Validate route_class
    route_class = normalized["default_route_class"]
    if route_class not in _VALID_ROUTE_CLASSES:
        raise SkeletonValidationError(
            f"[family={family}] second_leg.default_route_class "
            f"'{route_class}' is not in known route classes: {sorted(_VALID_ROUTE_CLASSES)}"
        )

    # Validate timeout / budgets are positive
    if normalized["timeout_seconds"] <= 0:
        raise SkeletonValidationError(
            f"[family={family}] second_leg.timeout_seconds must be positive, "
            f"got {normalized['timeout_seconds']}"
        )

    return normalized


def _validate_regime_overrides(
    overrides: Optional[Dict[str, Any]], family: str
) -> Optional[Dict[str, Any]]:
    """
    Validate and normalize regime overrides.

    Each override entry should have:
      - route_class
      - wait_budget_70pct
      - wait_budget_80pct
      - median_completion_seconds (informational)
      - trigger_note (informational)
    """
    if overrides is None:
        return None

    if not isinstance(overrides, dict):
        raise SkeletonValidationError(
            f"[family={family}] regime_overrides must be a dict, "
            f"got {type(overrides).__name__}"
        )

    normalized = {}
    for regime, cfg in overrides.items():
        if not isinstance(cfg, dict):
            raise SkeletonValidationError(
                f"[family={family}] regime_overrides['{regime}'] "
                f"must be a dict, got {type(cfg).__name__}"
            )

        entry: Dict[str, Any] = dict(cfg)
        for int_key in ("wait_budget_70pct", "wait_budget_80pct", "median_completion_seconds"):
            if int_key in entry and entry[int_key] is not None:
                entry[int_key] = int(entry[int_key])

        if "route_class" not in entry:
            raise SkeletonValidationError(
                f"[family={family}] regime_overrides['{regime}'] "
                f"missing required field 'route_class'"
            )

        if entry["route_class"] not in _VALID_ROUTE_CLASSES:
            raise SkeletonValidationError(
                f"[family={family}] regime_overrides['{regime}'].route_class "
                f"'{entry['route_class']}' not in {sorted(_VALID_ROUTE_CLASSES)}"
            )

        normalized[regime] = entry

    return normalized


def _assemble_family(family: str, raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize and validate a single family config block."""
    # Every family must have at least enabled + strategy_type
    _check_keys(f"family={family}", set(raw.keys()), {"enabled", "strategy_type"})
    strategy_type = str(raw["strategy_type"])

    if strategy_type not in _VALID_STRATEGY_TYPES:
        raise SkeletonValidationError(
            f"[family={family}] strategy_type '{strategy_type}' "
            f"not in {sorted(_VALID_STRATEGY_TYPES)}"
        )

    enabled = bool(raw["enabled"])
    result: Dict[str, Any] = {
        "family": family,
        "enabled": enabled,
        "strategy_type": strategy_type,
    }

    if strategy_type == "deferred":
        # Deferred families carry enabled + reason only
        result["strategy_id"] = str(raw.get("strategy_id", ""))
        result["reason"] = raw.get("reason", "unknown")
        return result

    # Non-deferred families require full execution config
    actual_keys = set(raw.keys())
    for required in _REQUIRED_FAMILY_KEYS - {"enabled", "strategy_type"}:
        if required not in actual_keys:
            raise SkeletonValidationError(
                f"[family={family}] Missing required key(s): [{required}]"
            )

    result["strategy_id"] = str(raw["strategy_id"])

    # --- entry ---
    result["entry"] = _validate_entry(raw.get("entry", {}), family)

    # --- second_leg ---
    result["second_leg"] = _validate_second_leg(raw.get("second_leg", {}), family)

    # --- regime_overrides ---
    result["regime_overrides"] = _validate_regime_overrides(
        raw.get("regime_overrides"), family
    )

    return result


# ---------------------------------------------------------------------------
# Main public API
# ---------------------------------------------------------------------------

def load_skeleton(path: Optional[str | Path] = None) -> Dict[str, Any]:
    """
    Load and validate strategy_config_skeleton_v0.json.

    Returns
    -------
    Dict with top-level keys:
      - config_id, bundle_id, mode, runtime, rollout
      - families: Dict[family_name -> assembled_family_config]

    Raises
    ------
    SkeletonValidationError
        If the skeleton is missing required fields or contains invalid values.
    FileNotFoundError
        If the skeleton file cannot be found.
    """
    skeleton_path = Path(path) if path else SKELETON_PATH

    with skeleton_path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)

    # --- top-level sanity ---
    _check_keys("root", set(raw.keys()), _REQUIRED_TOP_KEYS)

    if raw.get("mode") != "skeleton":
        raise SkeletonValidationError(
            f"Expected mode='skeleton', got mode='{raw.get('mode')}'. "
            "This loader only handles skeleton-mode configs."
        )

    # --- assemble families ---
    families_raw = raw.get("families", {})
    if not families_raw:
        raise SkeletonValidationError("No families defined in skeleton.")

    assembled_families: Dict[str, Any] = {}
    for family_name, family_cfg in families_raw.items():
        assembled_families[family_name] = _assemble_family(family_name, family_cfg)

    return {
        "config_id": str(raw["config_id"]),
        "bundle_id": str(raw["bundle_id"]),
        "mode": str(raw["mode"]),
        "runtime": dict(raw.get("runtime", {})),
        "rollout": dict(raw.get("rollout", {})),
        "families": assembled_families,
    }


def get_enabled_families(bundle: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return only the enabled family configs from an assembled bundle."""
    return {
        name: cfg
        for name, cfg in bundle["families"].items()
        if cfg.get("enabled", False)
    }


def get_active_families(bundle: Dict[str, Any]) -> List[str]:
    """Return rollout-ordered list of active family names (excludes deferred)."""
    rollout = bundle.get("rollout", {})
    phase_1 = list(rollout.get("phase_1", []))
    phase_2 = list(rollout.get("phase_2", []))
    return phase_1 + phase_2


# ---------------------------------------------------------------------------
# CLI smoke-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import pprint

    print(f"Loading skeleton from: {SKELETON_PATH}")
    bundle = load_skeleton()
    print("\n=== Loaded families ===")
    pprint.pprint({k: v.get("strategy_type") for k, v in bundle["families"].items()})
    print("\n=== Enabled families ===")
    pprint.pprint(list(get_enabled_families(bundle).keys()))
    print("\n=== Rollout order ===")
    pprint.pprint(get_active_families(bundle))
