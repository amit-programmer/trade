"""
engine_risk_budget.py - Dynamic Risk and Position Budget Calculator.

Calculates exact coin quantities and capital allocation based on strict
account risk limits (e.g. $20 max loss per trade) and minimum Risk-to-Reward rules.
"""
from __future__ import annotations

import os
from typing import Any, Dict, Optional


class RiskBudgetManager:
    def __init__(
        self,
        account_balance: float = 1000.0,
        max_loss_per_trade_usd: float = 20.0,
        max_daily_trades: int = 5,
        max_concurrent_trades: int = 3,
        max_daily_loss_usd: float = 50.0,
    ):
        self.account_balance = float(account_balance)
        self.max_loss_usd = float(max_loss_per_trade_usd)
        self.max_daily_trades = int(max_daily_trades)
        self.max_concurrent_trades = int(max_concurrent_trades)
        self.max_daily_loss_usd = float(max_daily_loss_usd)
        self.trades_today = 0

    @classmethod
    def from_env(cls) -> "RiskBudgetManager":
        """Instantiate RiskBudgetManager from environment variables or defaults."""
        bal = float(os.getenv("ACCOUNT_BALANCE", "1000.0"))
        max_loss = float(os.getenv("MAX_LOSS_PER_TRADE_USD", "20.0"))
        daily_limit = int(os.getenv("MAX_DAILY_TRADES", "5"))
        max_concurrent = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))
        max_daily_loss = float(os.getenv("MAX_DAILY_LOSS_USD", "50.0"))
        return cls(
            account_balance=bal,
            max_loss_per_trade_usd=max_loss,
            max_daily_trades=daily_limit,
            max_concurrent_trades=max_concurrent,
            max_daily_loss_usd=max_daily_loss,
        )

    def can_open_trade(
        self,
        current_open_count: int,
        today_realized_loss: float = 0.0,
    ) -> tuple[bool, str]:
        """
        Validates risk governance limits before opening or approving a trade:
        1. Max Concurrent Open Trades Guard (default: 3)
        2. Daily Max Drawdown Hard-Stop Guard (default: $50)
        3. Daily Trade Count limit (default: 5)
        """
        self.max_concurrent_trades = int(os.getenv("MAX_CONCURRENT_TRADES", str(self.max_concurrent_trades)))
        self.max_daily_loss_usd = float(os.getenv("MAX_DAILY_LOSS_USD", str(self.max_daily_loss_usd)))

        if current_open_count >= self.max_concurrent_trades:
            return False, f"BLOCKED: MAX_CONCURRENT_TRADES_REACHED ({current_open_count}/{self.max_concurrent_trades} active)"

        if today_realized_loss >= self.max_daily_loss_usd:
            return False, f"STATUS: DAILY_LOSS_LIMIT_REACHED (${today_realized_loss:,.2f} >= max ${self.max_daily_loss_usd:,.2f})"

        if self.trades_today >= self.max_daily_trades:
            return False, f"BLOCKED: MAX_DAILY_TRADES_REACHED ({self.trades_today}/{self.max_daily_trades} today)"

        return True, "OK"

    def calculate_position(
        self,
        entry: float,
        stop_loss: float,
        target: float,
        symbol: str = "",
        current_open_count: Optional[int] = None,
        today_realized_loss: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Calculates position size based on dollar risk budget.
        Quantity = Max Loss ($) / Risk Per Coin ($)
        """
        self.max_concurrent_trades = int(os.getenv("MAX_CONCURRENT_TRADES", str(self.max_concurrent_trades)))
        self.max_daily_loss_usd = float(os.getenv("MAX_DAILY_LOSS_USD", str(self.max_daily_loss_usd)))

        if current_open_count is not None and current_open_count >= self.max_concurrent_trades:
            return {
                "status": "BLOCK",
                "reason": f"BLOCKED: MAX_CONCURRENT_TRADES_REACHED ({current_open_count}/{self.max_concurrent_trades} active)",
            }

        if today_realized_loss is not None and today_realized_loss >= self.max_daily_loss_usd:
            return {
                "status": "BLOCK",
                "reason": f"STATUS: DAILY_LOSS_LIMIT_REACHED (${today_realized_loss:,.2f} >= max ${self.max_daily_loss_usd:,.2f})",
            }

        entry = float(entry)
        stop_loss = float(stop_loss)
        target = float(target)

        risk_per_unit = abs(entry - stop_loss)
        reward_per_unit = abs(target - entry)

        if risk_per_unit <= 0:
            return {"status": "BLOCK", "reason": "Invalid Stop Loss (equal to Entry)"}

        # Size = Risk Amount / Risk Per Share or Coin
        quantity = self.max_loss_usd / risk_per_unit
        total_trade_capital = quantity * entry
        risk_reward_ratio = round(reward_per_unit / risk_per_unit, 2)

        # Risk validation rules
        if risk_reward_ratio < 1.4:
            return {
                "status": "BLOCK",
                "reason": f"Risk-Reward ({risk_reward_ratio}R) is below minimum 1.4R",
                "risk_reward_ratio": f"1:{risk_reward_ratio}",
            }

        if self.trades_today >= self.max_daily_trades:
            return {
                "status": "BLOCK",
                "reason": f"Max daily trade limit reached ({self.max_daily_trades} trades)",
                "risk_reward_ratio": f"1:{risk_reward_ratio}",
            }

        # Format coin quantity appropriately based on price scale
        if entry > 10000:       # e.g. BTC
            qty_rounded = round(quantity, 4)
        elif entry > 100:       # e.g. ETH, SOL
            qty_rounded = round(quantity, 3)
        elif entry > 1:         # e.g. XRP
            qty_rounded = round(quantity, 2)
        else:                   # e.g. DOGE
            qty_rounded = round(quantity, 1)

        if qty_rounded == 0:
            qty_rounded = round(quantity, 6)

        return {
            "status": "PASS",
            "quantity": qty_rounded,
            "quantity_str": f"{qty_rounded:g}",
            "capital_required_usd": round(total_trade_capital, 2),
            "max_loss_usd": self.max_loss_usd,
            "risk_per_unit": round(risk_per_unit, 4),
            "reward_per_unit": round(reward_per_unit, 4),
            "risk_reward_ratio": f"1:{risk_reward_ratio}",
            "rr_num": risk_reward_ratio,
            "bracket": {
                "entry": entry,
                "stop_loss": stop_loss,
                "take_profit": target,
            },
        }

    def record_trade(self) -> None:
        self.trades_today += 1

    def reset_daily_trades(self) -> None:
        self.trades_today = 0
