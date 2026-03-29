"""
PairUnitStrategy — scanner-facing rule engine for blue-walnut pair units.

Reads the assembled skeleton config and evaluates the family-specific state machine:

    observe_open -> enter_first_leg -> monitor_second_leg -> complete_or_timeout

The intended caller is a local market scanner. The scanner is responsible for:

1. registering market open time via `on_market_open()`
2. passing local book candidates into `evaluate_market_candidate()`
3. opening a tracked first leg via `open_market_candidate()`
4. polling `evaluate_second_leg_completion()` on the opposite-leg price

Legacy wrappers (`evaluate_first_leg_entry`, `enter_first_leg`) remain for compatibility,
but the preferred API no longer assumes a target-wallet trade as the trigger source.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .market_time import infer_resolution_timestamp_seconds_from_slug

# ---------------------------------------------------------------------------
# Acceptance band parsing
# ---------------------------------------------------------------------------

# Band notation (c = cent = $0.01):
#   "-2c~-1c"  -> two-part range: lower=-0.02, upper=-0.01
#   "0~1c"     -> left side bare 0, right side 1c  -> lower=0.00, upper=0.01
#   "-1c~0"    -> left has c, right bare 0            -> lower=-0.01, upper=0.00
#   "<=-2c"    -> upper-bound only: lower=None, upper=-0.02
#   ">-1c"     -> lower-bound only: lower=-0.01, upper=None
#   "2c"       -> bare upper-bound: lower=None, upper=0.02
#
# 'c' is matched OUTSIDE capture groups so _parse_c always receives a pure number.
# 'c?' on both sides of '~' handles asymmetric bands like "0~1c" and "-1c~0".
_BAND_RE = re.compile(
    r"^(<=|>=|<|>)?(-?[\d.]+)c?~\~?(-?[\d.]+)c?$|"
    r"^(<=|>=|<|>)?(-?[\d.]+)c$"
)


def parse_band(band: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Parse a compact band string into (lower, upper) float bounds.
    Uses cents (c) as the unit: "1c" means $0.01.

    Returns (lower, upper) where None means unbounded in that direction.
    Handles asymmetric bands like "0~1c" where only the right side has 'c'.
    """
    band = band.strip()
    m = _BAND_RE.match(band)
    if not m:
        # Fallback: simple "N~M" without 'c' suffix
        if "~" in band:
            parts = band.split("~")
            if len(parts) == 2:
                try:
                    return float(parts[0]), float(parts[1])
                except ValueError:
                    pass
        return None, None

    # Two-part range: "-2c~-1c", "0~1c", "-1c~0", "<=-2c~-1c"
    if m.group(2) is not None:
        lower = _parse_c(m.group(2))
        upper = _parse_c(m.group(3))
        prefix = m.group(1)
        if prefix == "<=":
            # e.g. "<=-2c" encodes upper-bound only (no '~' in this branch)
            return None, upper
        if prefix == ">=":
            return lower, None
        return lower, upper

    # Single bound: "<=-2c", ">-1c", "2c"
    prefix = m.group(4)
    value = _parse_c(m.group(5))
    if prefix == "<=":
        return None, value
    if prefix == ">=":
        return value, None
    if prefix == "<":
        return None, value
    if prefix == ">":
        return value, None
    # Bare "2c" — treat as upper-bound single value
    return None, value


def _parse_c(token: str) -> float:
    """
    Parse a cent token string to a dollar float.
    The 'c' suffix has already been stripped by the regex (c is outside capture groups).

    Args:
        token: A string like '-2', '1', '0' (already stripped of any 'c').
    Returns:
        Dollar equivalent, e.g. '-2' -> -0.02, '1' -> 0.01
    """
    token = token.strip()
    # Guard: strip any残留 'c' that may have slipped through
    if token.endswith("c"):
        token = token[:-1]
    return float(token) / 100.0


def band_contains(band: str, value: float) -> bool:
    """Return True if value falls within the parsed band."""
    lower, upper = parse_band(band)
    if lower is not None and value < lower:
        return False
    if upper is not None and value > upper:
        return False
    return True


# ---------------------------------------------------------------------------
# Route class resolution
# ---------------------------------------------------------------------------

# Maps compact route_class names to (wait_budget_70pct, wait_budget_80pct, timeout_seconds)
# These defaults are loaded from the skeleton per-family, but this dict
# provides a fallback baseline for validation.
DEFAULT_ROUTE_PARAMS: Dict[str, Tuple[int, int, int]] = {
    "fast_20":       (20,  20,  20),
    "fast_20_30":     (20,  30,  30),
    "slow_30":       (30,  30,  30),
    "slow_30_60":    (30,  60,  60),
    "mixed_20_60":   (20,  60,  60),
}


# ---------------------------------------------------------------------------
# Internal data models
# ---------------------------------------------------------------------------


@dataclass
class FirstLegEntry:
    """Record of an opened first leg."""
    market_slug: str
    family: str
    open_timestamp: float          # wall-clock seconds when market opened
    entry_timestamp: float        # wall-clock seconds when we entered
    price: float                  # fill price
    usdc_size: float
    condition_id: str
    side: str                     # BUY or SELL (the side we took)


@dataclass
class PendingPairUnit:
    """An open pair unit waiting for second-leg completion."""
    unit_id: str
    first_leg: FirstLegEntry
    route_class: str
    wait_budget_70pct: int        # seconds
    wait_budget_80pct: int        # seconds
    timeout_seconds: int
    acceptance_fast_bands: List[str]
    acceptance_delayed_tail_bands: List[str]
    acceptance_hard_lock_bands: List[str]
    created_at: float             # wall-clock seconds when unit was opened
    budget_expiry_70: float       # wall-clock seconds at 70pct budget
    budget_expiry_80: float       # wall-clock seconds at 80pct budget
    completed: bool = False
    completion_price: Optional[float] = None
    completion_timestamp: Optional[float] = None
    completion_reason: Optional[str] = None   # "fast_accept" | "delayed_accept" | "hard_lock" | "timeout"


# ---------------------------------------------------------------------------
# Regime detector (simplified — replace with real signal)
# ---------------------------------------------------------------------------

def detect_regime(family: str, current_time: float, open_time: float) -> str:
    """
    Very rough heuristic for regime detection.
    In production this would look at real external market data.
    Returns one of: "strong_aligned", "strong_opp", "default"
    """
    elapsed = current_time - open_time
    if elapsed < 5:
        return "strong_aligned"
    if elapsed > 40:
        return "strong_opp"
    return "default"


# ---------------------------------------------------------------------------
# FamilyStrategy — per-family entry / second-leg config
# ---------------------------------------------------------------------------


@dataclass
class FamilyEntryConfig:
    window_seconds: int
    min_seconds_to_resolution: int
    price_band_lower: float
    price_band_upper: float
    size_anchor_usdc: Optional[float]
    size_policy: str              # "optional_soft_100" | "no_hard_size_gate"
    optional_soft_size_max_distance_pct: Optional[float]


@dataclass
class FamilySecondLegConfig:
    default_route_class: str
    timeout_seconds: int
    wait_budget_70pct: int
    wait_budget_80pct: int
    acceptance_fast_bands: List[str]
    acceptance_delayed_tail_bands: List[str]
    acceptance_hard_lock_bands: List[str]


@dataclass
class FamilyConfig:
    family: str
    enabled: bool
    strategy_type: str
    entry: FamilyEntryConfig
    second_leg: FamilySecondLegConfig
    regime_overrides: Dict[str, Dict[str, Any]]   # regime_name -> {route_class, wait_budget_70pct, ...}
    wait_budget_70pct_default: int
    wait_budget_80pct_default: int


# ---------------------------------------------------------------------------
# Decision output
# ---------------------------------------------------------------------------


@dataclass
class PairUnitDecision:
    action: str          # "enter_first_leg" | "complete_second_leg" | "skip" | "timeout"
    reason: str
    unit_id: Optional[str] = None
    market_slug: Optional[str] = None
    price: Optional[float] = None
    usdc_size: Optional[float] = None
    side: Optional[str] = None
    wait_budget_seconds: Optional[int] = None
    acceptance_bands: Optional[List[str]] = None
    slippage_bps: Optional[float] = None
    details: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# PairUnitStrategy — main runtime class
# ---------------------------------------------------------------------------

_COUNTER = 0


def _next_unit_id() -> str:
    global _COUNTER
    _COUNTER += 1
    return f"pu-{_COUNTER:06d}"


class PairUnitStrategy:
    """
    Runtime strategy runner that executes the blue-walnut pair-unit building
    state machine using the assembled skeleton config.

    Parameters
    ----------
    bundle : dict
        Output of skeleton_assembler.load_skeleton().
        Contains per-family entry / second-leg / override configs.
    """

    def __init__(self, bundle: Dict[str, Any]) -> None:
        self.bundle = bundle
        self.families: Dict[str, FamilyConfig] = {}
        self._pending_units: Dict[str, PendingPairUnit] = {}  # unit_id -> PendingPairUnit
        # Index by market_slug for fast lookup
        self._market_units: Dict[str, List[str]] = {}          # market_slug -> [unit_id, ...]
        # Track market open times
        self._market_open_times: Dict[str, float] = {}          # market_slug -> open_timestamp

        self._build_family_configs()

    # ------------------------------------------------------------------
    # Config building
    # ------------------------------------------------------------------

    def _build_family_configs(self) -> None:
        for family_name, cfg in self.bundle.get("families", {}).items():
            if not cfg.get("enabled", False):
                continue
            strategy_type = cfg.get("strategy_type", "")
            if strategy_type == "deferred":
                continue

            entry_raw = cfg.get("entry", {})
            sl_raw = cfg.get("second_leg", {})
            overrides = cfg.get("regime_overrides") or {}

            entry = FamilyEntryConfig(
                window_seconds=int(entry_raw.get("window_seconds", 12)),
                min_seconds_to_resolution=int(entry_raw.get("min_seconds_to_resolution", 300)),
                price_band_lower=float(entry_raw.get("price_band_lower", 0.48)),
                price_band_upper=float(entry_raw.get("price_band_upper", 0.50)),
                size_anchor_usdc=entry_raw.get("size_anchor_usdc"),
                size_policy=str(entry_raw.get("size_policy", "no_hard_size_gate")),
                optional_soft_size_max_distance_pct=entry_raw.get(
                    "optional_soft_size_max_distance_pct"
                ),
            )

            sl = FamilySecondLegConfig(
                default_route_class=str(sl_raw.get("default_route_class", "fast_20_30")),
                timeout_seconds=int(sl_raw.get("timeout_seconds", 30)),
                wait_budget_70pct=int(sl_raw.get("wait_budget_70pct", 20)),
                wait_budget_80pct=int(sl_raw.get("wait_budget_80pct", 30)),
                acceptance_fast_bands=list(sl_raw.get("acceptance_fast_bands", [])),
                acceptance_delayed_tail_bands=list(sl_raw.get("acceptance_delayed_tail_bands", [])),
                acceptance_hard_lock_bands=list(sl_raw.get("acceptance_hard_lock_bands", [])),
            )

            # Normalise override route params
            norm_overrides: Dict[str, Dict[str, Any]] = {}
            for regime, row in overrides.items():
                norm_overrides[regime] = {
                    "route_class": str(row.get("route_class", sl.default_route_class)),
                    "wait_budget_70pct": int(row.get("wait_budget_70pct", sl.wait_budget_70pct)),
                    "wait_budget_80pct": int(row.get("wait_budget_80pct", sl.wait_budget_80pct)),
                    "median_completion_seconds": float(row.get("median_completion_seconds", 0)),
                    "trigger_note": str(row.get("trigger_note", "")),
                }

            self.families[family_name] = FamilyConfig(
                family=family_name,
                enabled=True,
                strategy_type=strategy_type,
                entry=entry,
                second_leg=sl,
                regime_overrides=norm_overrides,
                wait_budget_70pct_default=sl.wait_budget_70pct,
                wait_budget_80pct_default=sl.wait_budget_80pct,
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def families_configured(self) -> List[str]:
        """Return list of enabled family names."""
        return sorted(self.families.keys())

    def pending_unit_count(self) -> int:
        return len(self._pending_units)

    def get_pending_units(self) -> List[PendingPairUnit]:
        return list(self._pending_units.values())

    def on_market_open(self, market_slug: str, family: str, open_timestamp: float) -> None:
        """Register a market open timestamp so scanner candidates can be window-checked."""
        self._market_open_times[market_slug] = open_timestamp
        # Clean up any stale units for this market
        self._market_units[market_slug] = []

    def on_market_close(self, market_slug: str, settlement_price: Optional[float] = None) -> List[PairUnitDecision]:
        """
        Call when a market settles / closes.
        Forces completion of any pending units at settlement price or timeout.
        """
        decisions = []
        for unit_id in list(self._market_units.get(market_slug, [])):
            unit = self._pending_units.pop(unit_id, None)
            if unit is None:
                continue
            decisions.append(PairUnitDecision(
                action="timeout",
                reason=f"market_closed",
                unit_id=unit_id,
                market_slug=market_slug,
                price=settlement_price,
                details={"settlement_price": settlement_price},
            ))
        self._market_units.pop(market_slug, None)
        return decisions

    def tick(self, market_slug: str, current_time: float) -> List[PairUnitDecision]:
        """
        Called periodically (e.g. every second). Checks pending units for
        timeout or expiry events.
        """
        decisions = []
        for unit_id in list(self._pending_units.keys()):
            unit = self._pending_units[unit_id]
            if unit.completed or unit.first_leg.market_slug != market_slug:
                continue

            # Check hard timeout
            if current_time >= unit.created_at + unit.timeout_seconds:
                decisions.append(self._timeout_unit(unit, "hard_timeout"))
                continue

            # Check 80% budget expiry -> record but don't force-complete yet
            if current_time >= unit.budget_expiry_80 and not unit.completed:
                # Close to budget boundary — mark decision but let tick()
                # or the next trade complete it
                pass  # passive monitoring, no forced action

        return decisions

    def evaluate_market_candidate(
        self,
        market_slug: str,
        family: str,
        candidate_price: float,
        candidate_usdc_size: float,
        current_time: Optional[float] = None,
    ) -> PairUnitDecision:
        """
        Evaluate whether a scanner-observed candidate should become the first leg.

        Returns a PairUnitDecision with action="enter_first_leg" or "skip".
        """
        now = current_time or time.time()
        family_cfg = self.families.get(family)
        if family_cfg is None:
            return PairUnitDecision("skip", f"family_{family}_not_enabled")

        open_time = self._market_open_times.get(market_slug)
        if open_time is None:
            # No recorded open time — try to infer from current time (first observation)
            open_time = now - family_cfg.entry.window_seconds
            self._market_open_times[market_slug] = open_time

        entry = family_cfg.entry

        # Pre-compute resolution time (needed for both skip and success paths)
        resolution_timestamp = infer_resolution_timestamp_seconds_from_slug(
            market_slug,
            reference_timestamp_seconds=int(now),
        )

        # Window check
        elapsed = now - open_time
        if elapsed > entry.window_seconds:
            return PairUnitDecision("skip", f"entry_window_closed_{family}", market_slug=market_slug)

        # Resolution time hard gate: skip if market resolves within min_seconds_to_resolution
        if resolution_timestamp is not None:
            seconds_to_resolution = int(resolution_timestamp - int(now))
            if seconds_to_resolution < entry.min_seconds_to_resolution:
                return PairUnitDecision(
                    "skip",
                    f"entry_min_seconds_to_resolution_{family}_{seconds_to_resolution}s",
                    market_slug=market_slug,
                    details={
                        "seconds_to_resolution": seconds_to_resolution,
                        "min_seconds_to_resolution": entry.min_seconds_to_resolution,
                    },
                )

        # Price band check
        if not (entry.price_band_lower <= candidate_price <= entry.price_band_upper):
            return PairUnitDecision(
                "skip",
                f"price_out_of_band_{family}_{candidate_price:.4f}",
                market_slug=market_slug,
            )

        # Size policy check
        size_reason, target_usdc = self._evaluate_size_policy(
            candidate_usdc_size, entry, family
        )
        if size_reason != "size_ok":
            return PairUnitDecision(
                "skip",
                size_reason,
                market_slug=market_slug,
            )

        # Determine route (regime override or default)
        regime = detect_regime(family, now, open_time)
        route_class, wait_70, wait_80, timeout = self._resolve_route(family_cfg, regime)
        acceptance_bands = family_cfg.second_leg.acceptance_fast_bands

        return PairUnitDecision(
            action="enter_first_leg",
            reason=f"entry_ok_{family}_regime_{regime}",
            market_slug=market_slug,
            price=candidate_price,
            usdc_size=target_usdc,
            side="BUY",
            wait_budget_seconds=wait_80,
            acceptance_bands=acceptance_bands,
            details={
                "family": family,
                "regime": regime,
                "route_class": route_class,
                "wait_budget_70pct": wait_70,
                "wait_budget_80pct": wait_80,
                "entry_window_elapsed_s": round(elapsed, 2),
                "price_band": [entry.price_band_lower, entry.price_band_upper],
                "size_anchor_usdc": entry.size_anchor_usdc,
                "min_seconds_to_resolution": entry.min_seconds_to_resolution,
                "seconds_to_resolution": (
                    int(resolution_timestamp - int(now))
                    if resolution_timestamp is not None
                    else None
                ),
            },
        )

    def evaluate_first_leg_entry(
        self,
        market_slug: str,
        family: str,
        trade_price: float,
        trade_usdc_size: float,
        current_time: Optional[float] = None,
    ) -> PairUnitDecision:
        """Compatibility wrapper for legacy callers. Prefer evaluate_market_candidate()."""
        return self.evaluate_market_candidate(
            market_slug=market_slug,
            family=family,
            candidate_price=trade_price,
            candidate_usdc_size=trade_usdc_size,
            current_time=current_time,
        )

    def open_market_candidate(
        self,
        market_slug: str,
        family: str,
        candidate_price: float,
        candidate_usdc_size: float,
        current_time: Optional[float] = None,
    ) -> PairUnitDecision:
        """
        Commit to opening a scanner-selected first leg and create a PendingPairUnit.
        """
        decision = self.evaluate_market_candidate(
            market_slug, family, candidate_price, candidate_usdc_size, current_time
        )
        if decision.action != "enter_first_leg":
            return decision

        now = current_time or time.time()
        unit_id = _next_unit_id()
        open_time = self._market_open_times.get(market_slug, now)
        regime = detect_regime(family, now, open_time)
        family_cfg = self.families[family]
        route_class, wait_70, wait_80, timeout = self._resolve_route(family_cfg, regime)
        sl = family_cfg.second_leg

        unit = PendingPairUnit(
            unit_id=unit_id,
            first_leg=FirstLegEntry(
                market_slug=market_slug,
                family=family,
                open_timestamp=open_time,
                entry_timestamp=now,
                price=candidate_price,
                usdc_size=decision.usdc_size or candidate_usdc_size,
                condition_id="",          # filled by caller
                side="BUY",
            ),
            route_class=route_class,
            wait_budget_70pct=wait_70,
            wait_budget_80pct=wait_80,
            timeout_seconds=timeout,
            acceptance_fast_bands=sl.acceptance_fast_bands,
            acceptance_delayed_tail_bands=sl.acceptance_delayed_tail_bands,
            acceptance_hard_lock_bands=sl.acceptance_hard_lock_bands,
            created_at=now,
            budget_expiry_70=now + wait_70,
            budget_expiry_80=now + wait_80,
        )
        self._pending_units[unit_id] = unit
        self._market_units.setdefault(market_slug, []).append(unit_id)

        decision.unit_id = unit_id
        decision.details["unit_id"] = unit_id
        return decision

    def enter_first_leg(
        self,
        market_slug: str,
        family: str,
        trade_price: float,
        trade_usdc_size: float,
        current_time: Optional[float] = None,
    ) -> PairUnitDecision:
        """Compatibility wrapper for legacy callers. Prefer open_market_candidate()."""
        return self.open_market_candidate(
            market_slug=market_slug,
            family=family,
            candidate_price=trade_price,
            candidate_usdc_size=trade_usdc_size,
            current_time=current_time,
        )

    def evaluate_second_leg_completion(
        self,
        unit_id: str,
        market_slug: str,
        current_price: float,
        current_time: Optional[float] = None,
    ) -> PairUnitDecision:
        """
        Evaluate whether a pending unit's second leg should be completed
        at the current opposite-leg price.
        """
        now = current_time or time.time()
        unit = self._pending_units.get(unit_id)
        if unit is None:
            return PairUnitDecision("skip", f"unit_{unit_id}_not_found")
        if unit.completed:
            return PairUnitDecision("skip", f"unit_{unit_id}_already_completed", unit_id=unit_id)
        if unit.first_leg.market_slug != market_slug:
            return PairUnitDecision("skip", f"unit_market_mismatch", unit_id=unit_id)

        family_cfg = self.families.get(unit.first_leg.family)
        if family_cfg is None:
            return PairUnitDecision("skip", f"family_not_enabled_{unit.first_leg.family}")

        # Compute price distance from parity (first_leg price = mid)
        first_leg_price = unit.first_leg.price
        if first_leg_price <= 0:
            return PairUnitDecision("skip", "invalid_first_leg_price", unit_id=unit_id)

        # Price distance in cents from mid (parity = 0)
        distance_from_parity = (current_price - 0.50) * 2  # normalise to [-1, 1] range around 0.50 mid

        # Acceptance bands use mid-relative notation: "-2c" means 2 cents below mid
        mid_price = 0.50  # assumption for hourly markets
        price_delta = current_price - mid_price  # negative = opposite leg below mid

        sl = unit.acceptance_fast_bands
        delayed = unit.acceptance_delayed_tail_bands
        hard_lock = unit.acceptance_hard_lock_bands

        # Try fast bands first
        for band in sl:
            if band_contains(band, price_delta):
                unit.completed = True
                unit.completion_price = current_price
                unit.completion_timestamp = now
                unit.completion_reason = "fast_accept"
                return PairUnitDecision(
                    action="complete_second_leg",
                    reason=f"fast_accept_{band}",
                    unit_id=unit_id,
                    market_slug=market_slug,
                    price=current_price,
                    usdc_size=unit.first_leg.usdc_size,
                    side="SELL",
                    details={
                        "price_delta_from_mid": round(price_delta, 6),
                        "band_matched": band,
                        "completion_elapsed_s": round(now - unit.created_at, 2),
                    },
                )

        # Try delayed tail bands
        for band in delayed:
            if band_contains(band, price_delta):
                unit.completed = True
                unit.completion_price = current_price
                unit.completion_timestamp = now
                unit.completion_reason = "delayed_accept"
                return PairUnitDecision(
                    action="complete_second_leg",
                    reason=f"delayed_accept_{band}",
                    unit_id=unit_id,
                    market_slug=market_slug,
                    price=current_price,
                    usdc_size=unit.first_leg.usdc_size,
                    side="SELL",
                    details={
                        "price_delta_from_mid": round(price_delta, 6),
                        "band_matched": band,
                        "completion_elapsed_s": round(now - unit.created_at, 2),
                    },
                )

        # Hard-lock bands — highest priority but may not be reached
        for band in hard_lock:
            if band_contains(band, price_delta):
                # Mark completed but flag as hard-lock
                unit.completed = True
                unit.completion_price = current_price
                unit.completion_timestamp = now
                unit.completion_reason = "hard_lock"
                return PairUnitDecision(
                    action="complete_second_leg",
                    reason=f"hard_lock_{band}",
                    unit_id=unit_id,
                    market_slug=market_slug,
                    price=current_price,
                    usdc_size=unit.first_leg.usdc_size,
                    side="SELL",
                    details={
                        "price_delta_from_mid": round(price_delta, 6),
                        "band_matched": band,
                        "completion_elapsed_s": round(now - unit.created_at, 2),
                    },
                )

        # Check timeout
        if now >= unit.created_at + unit.timeout_seconds:
            return self._timeout_unit(unit, "timeout_at_boundary")

        return PairUnitDecision(
            "skip",
            f"no_band_match_delta_{price_delta:.4f}",
            unit_id=unit_id,
            market_slug=market_slug,
            price=current_price,
            details={
                "price_delta_from_mid": round(price_delta, 6),
                "fast_bands": sl,
                "delayed_bands": delayed,
                "hard_lock_bands": hard_lock,
                "elapsed_s": round(now - unit.created_at, 2),
                "budget_70_expiry_s": round(unit.budget_expiry_70 - unit.created_at, 2),
                "budget_80_expiry_s": round(unit.budget_expiry_80 - unit.created_at, 2),
            },
        )

    def _timeout_unit(self, unit: PendingPairUnit, reason: str) -> PairUnitDecision:
        unit.completed = True
        unit.completion_reason = "timeout"
        return PairUnitDecision(
            action="timeout",
            reason=reason,
            unit_id=unit.unit_id,
            market_slug=unit.first_leg.market_slug,
            details={
                "elapsed_s": round(
                    (unit.completion_timestamp or time.time()) - unit.created_at, 2
                ),
                "route_class": unit.route_class,
                "wait_budget_80pct": unit.wait_budget_80pct,
            },
        )

    # ------------------------------------------------------------------
    # Size policy helpers
    # ------------------------------------------------------------------

    def _evaluate_size_policy(
        self, trade_usdc_size: float, entry: FamilyEntryConfig, family: str
    ) -> Tuple[str, Optional[float]]:
        policy = entry.size_policy
        anchor = entry.size_anchor_usdc
        max_distance_pct = entry.optional_soft_size_max_distance_pct

        if policy == "no_hard_size_gate":
            return "size_ok", trade_usdc_size

        if policy == "optional_soft_100":
            if anchor is None:
                return "size_ok", trade_usdc_size
            # Soft gate: warn if size deviates > 100% from anchor
            if max_distance_pct is not None:
                distance_pct = abs(trade_usdc_size - anchor) / anchor * 100.0
                if distance_pct > max_distance_pct:
                    return f"size_too_far_from_anchor_{family}_{distance_pct:.1f}pct", trade_usdc_size
            return "size_ok", trade_usdc_size

        # Unknown policy — allow but log
        return "size_ok", trade_usdc_size

    # ------------------------------------------------------------------
    # Route resolution
    # ------------------------------------------------------------------

    def _resolve_route(
        self, family_cfg: FamilyConfig, regime: str
    ) -> Tuple[str, int, int, int]:
        """Return (route_class, wait_70, wait_80, timeout)."""
        override = family_cfg.regime_overrides.get(regime)
        if override is not None:
            rc = override["route_class"]
            w70 = override["wait_budget_70pct"]
            w80 = override["wait_budget_80pct"]
        else:
            rc = family_cfg.second_leg.default_route_class
            w70 = family_cfg.second_leg.wait_budget_70pct
            w80 = family_cfg.second_leg.wait_budget_80pct

        # Resolve timeout from route_class defaults
        default_t = DEFAULT_ROUTE_PARAMS.get(rc)
        if default_t is not None:
            _, _, route_timeout = default_t
        else:
            route_timeout = family_cfg.second_leg.timeout_seconds

        return rc, w70, w80, route_timeout

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------

    def describe(self) -> Dict[str, Any]:
        """Return a human-readable description of current strategy state."""
        return {
            "families": {
                name: {
                    "enabled": cfg.enabled,
                    "entry_window_s": cfg.entry.window_seconds,
                    "min_seconds_to_resolution_s": cfg.entry.min_seconds_to_resolution,
                    "entry_price_band": [cfg.entry.price_band_lower, cfg.entry.price_band_upper],
                    "size_policy": cfg.entry.size_policy,
                    "default_route": cfg.second_leg.default_route_class,
                    "wait_budget_70pct_s": cfg.second_leg.wait_budget_70pct,
                    "wait_budget_80pct_s": cfg.second_leg.wait_budget_80pct,
                    "acceptance_fast_bands": cfg.second_leg.acceptance_fast_bands,
                    "regime_overrides": list(cfg.regime_overrides.keys()),
                }
                for name, cfg in self.families.items()
            },
            "pending_units": len(self._pending_units),
            "markets_with_open_units": list(self._market_units.keys()),
        }
