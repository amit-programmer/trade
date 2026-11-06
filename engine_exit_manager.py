"""
engine_exit_manager.py - Multi-Target Dynamic Exit & Breakeven Manager.

Implements institutional 3-tier scaling exits:
  - TP1 (1.5R): 50% locked + Stop-Loss moved to Breakeven (entry price)
  - TP2 (2.5R): 30% locked at opposing swing pool
  - TP3 (Runner): 20% trailed with swing structure
  - Stale Trade Rule: Exit alert if chop persists after 4 hours without reaching 0.5R
"""
from __future__ import annotations

from typing import Any, Dict

try:
    from engine_institutional_guards import calculate_fee_adjusted_breakeven
except ImportError:
    def calculate_fee_adjusted_breakeven(entry: float, direction: str, fee_buffer_pct: float = 0.0012) -> float:
        is_long = str(direction).upper() in ("LONG", "BUY")
        return round(entry * (1.0 + fee_buffer_pct if is_long else 1.0 - fee_buffer_pct), 4)


def generate_exit_plan(
    entry: float,
    stop_loss: float,
    direction: str = "LONG",
    target: float = 0.0,
) -> Dict[str, Any]:
    """
    Creates a 3-Tier dynamic exit plan with exact price levels and position allocations.
    """
    entry = float(entry)
    stop_loss = float(stop_loss)
    is_long = direction.upper() in ("LONG", "BUY")
    risk_r = abs(entry - stop_loss)

    if risk_r <= 0:
        risk_r = entry * 0.01  # safe fallback 1%

    # 1. TP1 = 1.5R
    # 2. TP2 = 2.5R (or given target if higher)
    # 3. TP3 = 3.5R (Runner)
    if is_long:
        tp1 = entry + 1.5 * risk_r
        tp2_ref = entry + 2.5 * risk_r
        tp2 = max(target, tp2_ref) if target > entry else tp2_ref
        tp3 = entry + 3.5 * risk_r
    else:
        tp1 = entry - 1.5 * risk_r
        tp2_ref = entry - 2.5 * risk_r
        tp2 = min(target, tp2_ref) if (target > 0 and target < entry) else tp2_ref
        tp3 = entry - 3.5 * risk_r

    be_price = calculate_fee_adjusted_breakeven(entry, direction)

    return {
        "entry": entry,
        "stop_loss": stop_loss,
        "breakeven_trigger": "TP1_HIT",
        "breakeven_price": be_price,
        "tp1": {
            "price": round(tp1, 6),
            "allocation_pct": 50,
            "target_r": 1.5,
            "action": "Close 50% & Move Stop to Breakeven",
        },
        "tp2": {
            "price": round(tp2, 6),
            "allocation_pct": 30,
            "target_r": round(abs(tp2 - entry) / risk_r, 2),
            "action": "Close 30% at Opposing Liquidity",
        },
        "tp3": {
            "price": round(tp3, 6),
            "allocation_pct": 20,
            "target_r": round(abs(tp3 - entry) / risk_r, 2),
            "action": "Trail remaining 20% runner with candle lows/highs",
        },
        "stale_timeout_hours": 4.0,
        "stale_min_r": 0.5,
    }


def check_trade_stage(
    position: Dict[str, Any],
    current_price: float,
    elapsed_seconds: float = 0.0,
) -> Dict[str, Any]:
    """
    Evaluates current price against position brackets.
    Returns any state changes: 'STOP_HIT', 'TP1_HIT', 'TP2_HIT', 'TP3_HIT', 'STALE_ALERT', or 'HOLD'.
    """
    direction = str(position.get("direction", "LONG")).upper()
    is_long = direction in ("LONG", "BUY")
    entry = float(position["entry_price"])
    current_sl = float(position.get("stop_loss", position.get("initial_stop_loss", 0.0)))
    tp1 = float(position.get("tp1", 0.0))
    tp2 = float(position.get("tp2", 0.0))
    tp3 = float(position.get("tp3", 0.0))
    stage = position.get("stage", "OPEN")
    risk_r = abs(entry - float(position.get("initial_stop_loss", current_sl))) or (entry * 0.01)

    # 1. Stop Loss Check
    if is_long and current_price <= current_sl:
        is_be = abs(current_sl - entry) < (risk_r * 0.1)
        return {
            "event": "BE_HIT" if is_be else "STOP_HIT",
            "exit_price": current_price,
            "realized_r": 0.0 if is_be else -1.0,
            "description": "Stop Loss Hit at Breakeven" if is_be else "Initial Stop Loss Hit",
        }
    elif (not is_long) and current_price >= current_sl:
        is_be = abs(current_sl - entry) < (risk_r * 0.1)
        return {
            "event": "BE_HIT" if is_be else "STOP_HIT",
            "exit_price": current_price,
            "realized_r": 0.0 if is_be else -1.0,
            "description": "Stop Loss Hit at Breakeven" if is_be else "Initial Stop Loss Hit",
        }

    # 2. TP3 Check
    if tp3 > 0:
        if (is_long and current_price >= tp3) or ((not is_long) and current_price <= tp3):
            return {
                "event": "TP3_HIT",
                "exit_price": current_price,
                "realized_r": 3.5,
                "description": "Final Target (TP3) Reached - 100% position closed",
            }

    # 3. TP2 Check
    if stage in ("OPEN", "TP1_HIT") and tp2 > 0:
        if (is_long and current_price >= tp2) or ((not is_long) and current_price <= tp2):
            return {
                "event": "TP2_HIT",
                "exit_price": current_price,
                "realized_r": round(abs(tp2 - entry) / risk_r, 2),
                "description": "TP2 Hit (80% total booked) - Runner remaining",
            }

    # 4. TP1 Check (triggers Breakeven move)
    if stage == "OPEN" and tp1 > 0:
        if (is_long and current_price >= tp1) or ((not is_long) and current_price <= tp1):
            be_price = calculate_fee_adjusted_breakeven(entry, direction)
            return {
                "event": "TP1_HIT",
                "exit_price": current_price,
                "new_stop_loss": be_price,
                "realized_r": 1.5,
                "description": f"TP1 Hit (50% booked) - Stop Loss moved to Fee-Adjusted Breakeven (${be_price:,.4f})",
            }

    # 5. Stale Trade Rule (4-hour chop without reaching 0.5R)
    if stage == "OPEN" and elapsed_seconds > 4 * 3600:
        current_r = ((current_price - entry) if is_long else (entry - current_price)) / risk_r
        if current_r < 0.5:
            return {
                "event": "STALE_ALERT",
                "exit_price": current_price,
                "realized_r": round(current_r, 2),
                "description": f"Trade stagnant for >4h with only {current_r:+.2f}R - Capital protection recommended",
            }

    return {"event": "HOLD"}
