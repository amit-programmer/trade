"""
engine_whale_tracker.py - Smart Money & Institutional Derivatives Flow Tracker.

Pulls Binance Futures public derivatives metrics (Open Interest & Top Trader Long/Short Ratio)
to give Groq AI real institutional volume and liquidation context.
"""
from __future__ import annotations

import time
from typing import Any, Dict
import requests

_CACHE: Dict[str, Any] = {}
_CACHE_TTL = 180  # 3 minutes


def get_whale_derivatives_data(symbol: str) -> Dict[str, Any]:
    """
    Fetches Binance Open Interest and Top Long/Short Position Ratio.
    Returns: {
        "open_interest": float,
        "top_traders_long_pct": str,
        "top_traders_short_pct": str,
        "flow_summary": str,
        "institutional_bias": str
    }
    """
    now = time.time()
    cache_key = f"whale_{symbol}"
    if cache_key in _CACHE and (now - _CACHE[f"{cache_key}_time"]) < _CACHE_TTL:
        return _CACHE[cache_key]

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    oi_url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={symbol}"
    ratio_url = f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={symbol}&period=15m&limit=5"

    oi_val = 0.0
    long_pct_str = "50%"
    short_pct_str = "50%"
    flow_desc = "Normal Derivatives Flow"
    inst_bias = "NEUTRAL"

    # 1. Fetch Open Interest
    try:
        r_oi = requests.get(oi_url, headers=headers, timeout=5)
        if r_oi.status_code == 200:
            oi_val = float(r_oi.json().get("openInterest", 0.0))
    except Exception:
        pass

    # 2. Fetch Top Trader Long/Short Ratio
    try:
        r_ratio = requests.get(ratio_url, headers=headers, timeout=5)
        if r_ratio.status_code == 200:
            rows = r_ratio.json()
            if isinstance(rows, list) and rows:
                latest = rows[-1]
                long_val = float(latest.get("longAccount", 0.5))
                short_val = float(latest.get("shortAccount", 0.5))
                l_pct = round(long_val * 100, 1)
                s_pct = round(short_val * 100, 1)
                long_pct_str = f"{l_pct}%"
                short_pct_str = f"{s_pct}%"

                if l_pct >= 62.0:
                    flow_desc = f"Heavy Top-Trader Long Dominance ({long_pct_str} Long)"
                    inst_bias = "INSTITUTIONAL_BULLISH"
                elif s_pct >= 62.0:
                    flow_desc = f"Heavy Top-Trader Short Dominance ({short_pct_str} Short)"
                    inst_bias = "INSTITUTIONAL_BEARISH"
                else:
                    flow_desc = f"Balanced Positioning ({long_pct_str} Long / {short_pct_str} Short)"
                    inst_bias = "BALANCED"
    except Exception:
        pass

    result = {
        "symbol": symbol,
        "open_interest": oi_val,
        "top_traders_long_pct": long_pct_str,
        "top_traders_short_pct": short_pct_str,
        "flow_summary": flow_desc,
        "institutional_bias": inst_bias,
    }

    _CACHE[cache_key] = result
    _CACHE[f"{cache_key}_time"] = now
    return result
