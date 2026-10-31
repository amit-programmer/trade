"""
engine_timemachine.py - Quantitative Monte Carlo Simulator for Trade Setups.

Replays historical candle price move distributions over 10,000 simulations
to calculate empirical statistical odds of hitting Take Profit vs Stop Loss.
"""
from __future__ import annotations

import random
from typing import Any, Dict, List, Union
import pandas as pd


def run_monte_carlo_simulation(
    recent_candles: Union[List[Dict[str, float]], pd.DataFrame],
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    direction: str = "BUY",
    simulations: int = 10000,
    max_steps: int = 50,
) -> Dict[str, Any]:
    """
    recent_candles: list of dicts [{'open': ..., 'high': ..., 'low': ..., 'close': ...}]
                    OR a pandas DataFrame with open, high, low, close columns.
    Replays price distribution over 10,000 permutations to determine
    the likelihood of hitting take profit before stop loss.
    """
    # 1. Normalize input to list of candle dicts
    candles: List[Dict[str, float]] = []
    if isinstance(recent_candles, pd.DataFrame):
        sub_df = recent_candles.tail(100)
        for _, row in sub_df.iterrows():
            candles.append({
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            })
    elif isinstance(recent_candles, list):
        candles = recent_candles[-100:]

    if len(candles) < 20:
        return {
            "simulations": 0,
            "target_win_prob": "50.0%",
            "stop_loss_prob": "50.0%",
            "unresolved_prob": "0.0%",
            "target_pct_raw": 50.0,
            "stop_pct_raw": 50.0,
            "edge": "NEUTRAL",
            "verdict": "INSUFFICIENT DATA",
            "error": "Not enough candle data for simulation (min 20 required)",
        }

    # 2. Extract relative price moves (high, low, close relative to open)
    moves = []
    for c in candles[-60:]:
        o = float(c["open"])
        if o <= 0:
            continue
        h_diff = float(c["high"]) - o
        l_diff = o - float(c["low"])
        c_diff = float(c["close"]) - o
        moves.append((h_diff, l_diff, c_diff))

    if not moves:
        return {
            "simulations": 0,
            "target_win_prob": "50.0%",
            "stop_loss_prob": "50.0%",
            "unresolved_prob": "0.0%",
            "target_pct_raw": 50.0,
            "stop_pct_raw": 50.0,
            "edge": "NEUTRAL",
            "verdict": "INSUFFICIENT DATA",
        }

    hits_target = 0
    hits_stop = 0
    remained_open = 0
    is_long = direction.upper() in ("BUY", "LONG")

    # 3. Monte Carlo Loop
    for _ in range(simulations):
        current_price = entry_price
        resolved = False

        for _ in range(max_steps):
            h_diff, l_diff, c_diff = random.choice(moves)

            if is_long:
                sim_high = current_price + h_diff
                sim_low = current_price - l_diff

                if sim_low <= stop_loss:
                    hits_stop += 1
                    resolved = True
                    break
                elif sim_high >= take_profit:
                    hits_target += 1
                    resolved = True
                    break
            else:  # SHORT
                sim_high = current_price + h_diff
                sim_low = current_price - l_diff

                if sim_high >= stop_loss:
                    hits_stop += 1
                    resolved = True
                    break
                elif sim_low <= take_profit:
                    hits_target += 1
                    resolved = True
                    break

            current_price += c_diff

        if not resolved:
            remained_open += 1

    target_pct = round((hits_target / simulations) * 100, 2)
    stop_pct = round((hits_stop / simulations) * 100, 2)
    open_pct = round((remained_open / simulations) * 100, 2)

    if target_pct >= 55.0:
        verdict = "STRONG PASS"
    elif target_pct >= 48.0:
        verdict = "PASS"
    elif target_pct >= 40.0:
        verdict = "NEUTRAL"
    else:
        verdict = "WEAK / HIGH RISK"

    edge = "POSITIVE" if target_pct > stop_pct else ("NEGATIVE" if target_pct < stop_pct else "NEUTRAL")

    return {
        "simulations": simulations,
        "target_win_prob": f"{target_pct:.1f}%",
        "stop_loss_prob": f"{stop_pct:.1f}%",
        "unresolved_prob": f"{open_pct:.1f}%",
        "target_pct_raw": target_pct,
        "stop_pct_raw": stop_pct,
        "open_pct_raw": open_pct,
        "edge": edge,
        "verdict": verdict,
    }
