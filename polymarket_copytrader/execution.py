from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Dict, Optional

from .matching import simulate_book_execution
from .models import ExecutionConfig, ExecutionReport, FollowDecision, OrderBook, StateSnapshot


class ExecutionClient(ABC):
    def __init__(self, config: ExecutionConfig, state: StateSnapshot) -> None:
        self.config = config
        self.state = state

    def current_exposure(self, asset_id: str) -> float:
        return float(self.state.asset_exposures_usdc.get(asset_id, 0.0))

    def current_position_size(self, asset_id: str) -> float:
        return float(self.state.asset_positions_size.get(asset_id, 0.0))

    def register_buy_fill(self, asset_id: str, usdc_size: float, size: float) -> None:
        self.state.asset_exposures_usdc[asset_id] = self.current_exposure(asset_id) + usdc_size
        self.state.asset_positions_size[asset_id] = self.current_position_size(asset_id) + size

    def register_sell_fill(self, asset_id: str, usdc_size: float, size: float) -> None:
        next_exposure = max(self.current_exposure(asset_id) - usdc_size, 0.0)
        next_size = max(self.current_position_size(asset_id) - size, 0.0)
        self.state.asset_exposures_usdc[asset_id] = next_exposure
        if next_size <= 1e-12:
            self.state.asset_positions_size.pop(asset_id, None)
        else:
            self.state.asset_positions_size[asset_id] = next_size

    @abstractmethod
    def place_follow_trade(
        self,
        decision: FollowDecision,
        order_book: Optional[OrderBook] = None,
        execution_policy: str = "FOK",
    ) -> ExecutionReport:
        raise NotImplementedError


class PaperExecutionClient(ExecutionClient):
    def place_follow_trade(
        self,
        decision: FollowDecision,
        order_book: Optional[OrderBook] = None,
        execution_policy: str = "FOK",
    ) -> ExecutionReport:
        asset_id = decision.target_trade.asset
        side = str(decision.follow_side or decision.target_trade.side).upper()
        requested_usdc = float(decision.follow_usdc or 0.0)
        requested_size = float(decision.follow_size or 0.0)
        if order_book is None:
            status = "paper_unfilled"
            details = {
                "side": side,
                "reason": "missing_order_book",
                "execution_policy": execution_policy.upper(),
            }
            return ExecutionReport(
                ok=False,
                mode="paper",
                status=status,
                asset_id=asset_id,
                requested_usdc=requested_usdc,
                requested_price=decision.follow_price,
                requested_size=requested_size,
                details=details,
            )

        match = simulate_book_execution(
            order_book=order_book,
            side=side,
            requested_usdc=requested_usdc,
            requested_size=requested_size,
            execution_policy=execution_policy,
            limit_price=decision.follow_price,
        )
        filled_usdc = match.filled_usdc
        filled_size = match.filled_size
        avg_price = match.avg_price or decision.follow_price
        if match.status in {"filled", "partial"}:
            if side == "BUY":
                self.register_buy_fill(asset_id, filled_usdc, filled_size)
            else:
                self.register_sell_fill(asset_id, filled_usdc, filled_size)
        status = (
            "paper_filled"
            if match.status == "filled"
            else "paper_partial"
            if match.status == "partial"
            else "paper_unfilled"
        )
        return ExecutionReport(
            ok=match.status in {"filled", "partial"},
            mode="paper",
            status=status,
            asset_id=asset_id,
            requested_usdc=filled_usdc if match.status in {"filled", "partial"} else requested_usdc,
            requested_price=avg_price,
            requested_size=filled_size if match.status in {"filled", "partial"} else requested_size,
            details={
                "side": side,
                "reason": match.reason,
                "execution_policy": execution_policy.upper(),
                "fill_ratio": round(match.fill_ratio, 6),
                "requested_usdc": requested_usdc,
                "requested_size": requested_size,
                "market_slug": decision.target_trade.slug,
                "outcome": decision.target_trade.outcome,
                "copied_from_tx": decision.target_trade.transaction_hash,
            },
        )


class LiveExecutionClient(ExecutionClient):
    def __init__(self, config: ExecutionConfig, state: StateSnapshot) -> None:
        super().__init__(config, state)
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL
        except ImportError as exc:
            raise RuntimeError(
                "真实下单需要先安装 py-clob-client。"
            ) from exc

        private_key = os.environ.get(config.private_key_env)
        funder = os.environ.get(config.funder_env)
        if not private_key or not funder:
            raise RuntimeError(
                f"缺少环境变量 {config.private_key_env} 或 {config.funder_env}。"
            )

        self._clob_client_cls = ClobClient
        self._market_order_args_cls = MarketOrderArgs
        self._order_type = OrderType
        self._buy = BUY
        self._sell = SELL
        self._client = ClobClient(
            config.host,
            key=private_key,
            chain_id=config.chain_id,
            signature_type=config.signature_type,
            funder=funder,
        )
        self._client.set_api_creds(self._client.create_or_derive_api_creds())

    def place_follow_trade(
        self,
        decision: FollowDecision,
        order_book: Optional[OrderBook] = None,
        execution_policy: str = "FOK",
    ) -> ExecutionReport:
        normalized_policy = str(execution_policy or "FOK").upper()
        if normalized_policy not in {"IOC", "FOK"}:
            raise ValueError(f"unsupported execution policy: {execution_policy}")
        order_type = getattr(self._order_type, normalized_policy, None)
        if order_type is None:
            raise RuntimeError(f"live execution policy not supported by client: {normalized_policy}")
        usdc_size = float(decision.follow_usdc or 0.0)
        size = float(decision.follow_size or 0.0)
        side = str(decision.follow_side or decision.target_trade.side).upper()
        side_const = self._buy if side == "BUY" else self._sell
        amount = usdc_size if side == "BUY" else size
        try:
            market_order = self._market_order_args_cls(
                token_id=decision.target_trade.asset,
                amount=amount,
                side=side_const,
                price=decision.follow_price,
                order_type=order_type,
            )
        except TypeError:
            market_order = self._market_order_args_cls(
                token_id=decision.target_trade.asset,
                amount=amount,
                side=side_const,
            )
        signed_order = self._client.create_market_order(market_order)
        response = self._client.post_order(signed_order, order_type)
        if side == "BUY":
            self.register_buy_fill(decision.target_trade.asset, usdc_size, size)
        else:
            self.register_sell_fill(decision.target_trade.asset, usdc_size, size)
        details: Dict[str, object] = dict(response) if isinstance(response, dict) else {"raw": str(response)}
        details["side"] = side
        details["execution_policy"] = normalized_policy
        return ExecutionReport(
            ok=True,
            mode="live",
            status=str(details.get("status", "submitted")),
            asset_id=decision.target_trade.asset,
            requested_usdc=usdc_size,
            requested_price=decision.follow_price,
            requested_size=decision.follow_size,
            details=details,
        )


def build_execution_client(config: ExecutionConfig, state: StateSnapshot) -> ExecutionClient:
    mode = config.mode.lower()
    if mode == "paper":
        return PaperExecutionClient(config, state)
    if mode == "live":
        return LiveExecutionClient(config, state)
    raise ValueError(f"未知 execution.mode: {config.mode}")
