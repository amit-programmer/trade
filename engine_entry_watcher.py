"""
engine_entry_watcher.py - Micro-Timeframe (1m) Bar-by-Bar Entry Watcher.

Monitors Order Block zones on micro-timeframes (1m) to catch precision rejection wicks
and optimal Risk-to-Reward mitigation entries before candles close on higher timeframes.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Union
import pandas as pd


def check_micro_entry(
    current_1m_candle: Union[Dict[str, float], pd.Series],
    ob_low: float,
    ob_high: float,
    direction: str = "LONG",
) -> Dict[str, Any]:
    """
    Detects if a 1m candle mitigates inside the Order Block zone
    and produces an institutional rejection wick.
    """
    try:
        if isinstance(current_1m_candle, pd.Series):
            price = float(current_1m_candle["close"])
            low = float(current_1m_candle["low"])
            high = float(current_1m_candle["high"])
            open_p = float(current_1m_candle["open"])
        else:
            price = float(current_1m_candle.get("close", 0.0))
            low = float(current_1m_candle.get("low", 0.0))
            high = float(current_1m_candle.get("high", 0.0))
            open_p = float(current_1m_candle.get("open", 0.0))
    except Exception as exc:
        return {"status": "ERROR", "reason": str(exc)}

    c_range = high - low
    if c_range <= 0:
        return {"status": "WAITING"}

    is_long = direction.upper() in ("LONG", "BUY", "BULLISH")

    if is_long:
        # Long: Candle dips into OB zone (low <= ob_high and high >= ob_low)
        in_zone = (low <= ob_high and high >= ob_low)
        # Rejection: Close in upper 50% of the 1m candle range
        rejection = price >= (low + c_range * 0.5)
        if in_zone and rejection:
            return {
                "status": "TRIGGERED",
                "entry_price": price,
                "rejection_wick_size": round(price - low, 4),
                "pattern": "Bullish 1m OB Tap + Upper Rejection Wick",
            }
    else:
        # Short: Candle tests OB zone (high >= ob_low and low <= ob_high)
        in_zone = (high >= ob_low and low <= ob_high)
        # Rejection: Close in lower 50% of the 1m candle range
        rejection = price <= (high - c_range * 0.5)
        if in_zone and rejection:
            return {
                "status": "TRIGGERED",
                "entry_price": price,
                "rejection_wick_size": round(high - price, 4),
                "pattern": "Bearish 1m OB Tap + Lower Rejection Wick",
            }

    return {"status": "WAITING"}
