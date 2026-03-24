from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional


@dataclass
class TradeActivity:
    proxy_wallet: str
    timestamp_ms: int
    condition_id: str
    activity_type: str
    size: float
    usdc_size: float
    transaction_hash: str
    price: float
    asset: str
    side: str
    outcome_index: int
    title: str
    slug: str
    event_slug: str
    outcome: str
    name: str = ""
    pseudonym: str = ""

    @property
    def timestamp_seconds(self) -> int:
        return int(self.timestamp_ms // 1000) if self.timestamp_ms > 100_000_000_000 else int(self.timestamp_ms)

    @property
    def timestamp_minute(self) -> int:
        return (self.timestamp_seconds // 60) * 60

    @property
    def market_family(self) -> str:
        haystack = f"{self.slug} {self.event_slug} {self.title}".lower()
        if "bitcoin" in haystack or "btc" in haystack:
            return "btc"
        if "ethereum" in haystack or re.search(r"\beth\b", haystack):
            return "eth"
        if "solana" in haystack or re.search(r"\bsol\b", haystack):
            return "sol"
        if re.search(r"\bxrp\b", haystack):
            return "xrp"
        return "other"

    @property
    def market_duration_bucket(self) -> str:
        haystack = f"{self.slug} {self.event_slug} {self.title}".lower()
        if "15m" in haystack or "15min" in haystack:
            return "15m"
        if "5m" in haystack or "5min" in haystack:
            return "5m"
        if re.search(r"\b(1|2|3|4|5|6|7|8|9|10|11|12)(am|pm) et\b", haystack):
            return "hourly"
        return "other"

    @property
    def is_short_duration_market(self) -> bool:
        return self.market_duration_bucket in {"5m", "15m"}

    @property
    def is_priority_short_market(self) -> bool:
        return self.is_short_duration_market and self.market_family in {"btc", "eth"}

    @property
    def dedupe_key(self) -> str:
        return ":".join(
            [
                self.transaction_hash or "nohash",
                self.asset,
                self.side,
                str(self.timestamp_ms),
                f"{self.price:.8f}",
                f"{self.size:.8f}",
            ]
        )


@dataclass
class PriceLevel:
    price: float
    size: float


@dataclass
class PriceHistoryPoint:
    timestamp_seconds: int
    price: float


@dataclass
class OrderBook:
    market: str
    asset_id: str
    timestamp: str
    bids: List[PriceLevel]
    asks: List[PriceLevel]
    min_order_size: float
    tick_size: float
    neg_risk: bool
    last_trade_price: float

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0].price if self.asks else None


@dataclass
class FollowDecision:
    should_follow: bool
    reason: str
    target_trade: TradeActivity
    follow_side: Optional[str] = None
    follow_price: Optional[float] = None
    follow_usdc: Optional[float] = None
    follow_size: Optional[float] = None
    slippage_bps: Optional[float] = None

    def to_dict(self) -> Dict[str, object]:
        payload = asdict(self)
        payload["target_trade"] = asdict(self.target_trade)
        return payload


@dataclass
class ExecutionReport:
    ok: bool
    mode: str
    status: str
    asset_id: str
    requested_usdc: float
    requested_price: Optional[float]
    requested_size: Optional[float]
    details: Dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, object]:
        return asdict(self)


@dataclass
class TargetConfig:
    profile: Optional[str]
    wallet: Optional[str]


@dataclass
class RuntimeConfig:
    poll_interval_seconds: float
    request_timeout_seconds: float
    activity_limit: int
    lookback_seconds_on_start: int
    requery_overlap_seconds: int
    state_path: str
    event_log_path: str
    market_websocket_enabled: bool = True
    market_websocket_warmup_ms: int = 350


@dataclass
class StrategyConfig:
    follow_fraction: float
    fixed_order_usdc: Optional[float]
    min_target_usdc_size: float
    max_follow_price: float
    max_slippage_bps: float
    max_position_per_asset_usdc: float
    min_order_usdc: float
    skip_outcomes: List[str]
    skip_market_slugs: List[str]
    execution_policy: str = "IOC"
    aggressive_price_enabled: bool = False
    aggressive_price_max_ticks: int = 0
    max_trade_age_seconds: Optional[float] = None
    short_market_max_trade_age_seconds: Optional[float] = None


@dataclass
class ExecutionConfig:
    mode: str
    host: str
    chain_id: int
    signature_type: int
    private_key_env: str
    funder_env: str


@dataclass
class AppConfig:
    target: TargetConfig
    runtime: RuntimeConfig
    strategy: StrategyConfig
    execution: ExecutionConfig


@dataclass
class EvalTargetConfig:
    name: str
    profile: Optional[str]
    wallet: Optional[str]
    weight: float


@dataclass
class EvalRuntimeConfig:
    request_timeout_seconds: float
    cache_dir: str


@dataclass
class EvalPortfolioConfig:
    initial_capital_usdc: float
    standalone_initial_capital_usdc: float


@dataclass
class EvalDataConfig:
    recent_days: int
    activity_page_size: int
    price_fidelity_minutes: int
    max_trades_per_target: Optional[int] = None


@dataclass
class EvalStrategyConfig:
    follow_fraction: float
    max_order_usdc: float
    min_target_usdc_size: float
    min_order_usdc: float
    max_position_per_asset_usdc: float
    allow_sell: bool
    exit_mode: str = "redeem"


@dataclass
class EvalScenarioConfig:
    name: str
    observation_delay_seconds: int
    extra_slippage_bps: float
    execution_policy: str = "FOK"
    synthetic_levels: int = 5
    synthetic_depth_multiplier: float = 1.0
    use_target_trade_price: bool = False


@dataclass
class EvalOutputConfig:
    summary_path: str
    curve_path: str
    fills_path: str


@dataclass
class EvalConfig:
    targets: List[EvalTargetConfig]
    runtime: EvalRuntimeConfig
    portfolio: EvalPortfolioConfig
    data: EvalDataConfig
    strategy: EvalStrategyConfig
    scenarios: List[EvalScenarioConfig]
    output: EvalOutputConfig


@dataclass
class ResolvedTarget:
    name: str
    profile: Optional[str]
    wallet: str
    weight: float
    details: Dict[str, object]


@dataclass
class EvalSignal:
    scenario_name: str
    target_name: str
    wallet: str
    trade: TradeActivity
    execution_timestamp_seconds: int


@dataclass
class SimulatedFill:
    scenario_name: str
    target_name: str
    side: str
    asset_id: str
    market_slug: str
    outcome: str
    signal_timestamp_seconds: int
    execution_timestamp_seconds: int
    signal_price: float
    execution_price: float
    target_usdc_size: float
    copied_usdc: float
    copied_size: float
    status: str
    reason: str
    execution_slippage_bps: float
    requested_usdc: float = 0.0
    requested_size: float = 0.0
    fill_ratio: float = 0.0


@dataclass
class EquityPoint:
    timestamp_seconds: int
    combined_equity: float
    combined_cash: float
    per_target_equity: Dict[str, float]


@dataclass
class EvalSummary:
    scenario_name: str
    portfolio_name: str
    initial_capital_usdc: float
    final_equity_usdc: float
    total_return_pct: float
    max_drawdown_pct: float
    fills: int
    buys: int
    sells: int
    skipped: int
    partial_fills: int = 0
    avg_execution_slippage_bps: float = 0.0
    benchmark_scenario_name: Optional[str] = None
    benchmark_return_pct: Optional[float] = None
    pnl_capture_ratio: Optional[float] = None


@dataclass
class StateSnapshot:
    target_wallet: Optional[str] = None
    last_event_timestamp_ms: int = 0
    seen_keys: List[str] = field(default_factory=list)
    asset_exposures_usdc: Dict[str, float] = field(default_factory=dict)
    asset_positions_size: Dict[str, float] = field(default_factory=dict)

    def remember(self, key: str, max_size: int = 5000) -> None:
        self.seen_keys.append(key)
        if len(self.seen_keys) > max_size:
            self.seen_keys = self.seen_keys[-max_size:]
