"""
engine_macro_filter.py - Crypto Sentiment and Macro Economic Event Filter.

Pulls real-time Fear & Greed Index and market macro conditions.
Implements the 30-Minute High-Impact Macro Economic Event Buffer:
Automatically blocks new setup executions before major volatility events (US CPI, PPI, FOMC, NFP)
to prevent high-slippage wick liquidations.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
import requests

_CACHE: Dict[str, Any] = {}
_CACHE_TTL = 900  # 15 minutes

# Major High-Impact US Economic Releases (Dates in UTC format: YYYY-MM-DD HH:MM)
# Standard releases occur at 12:30 / 13:30 UTC (CPI/PPI/NFP) and 18:00 / 19:00 UTC (FOMC)
HIGH_IMPACT_SCHEDULE: List[Dict[str, str]] = [
    # 2026/2027 FOMC, CPI, NFP Reference Schedules
    {"name": "US FOMC Rate Decision & Press Conference", "utc": "2026-10-28 18:00"},
    {"name": "US Non-Farm Payrolls (NFP) & Unemployment", "utc": "2026-11-06 13:30"},
    {"name": "US CPI (Consumer Price Index) Inflation", "utc": "2026-11-12 13:30"},
    {"name": "US PPI (Producer Price Index)", "utc": "2026-11-13 13:30"},
    {"name": "US FOMC Interest Rate Decision", "utc": "2026-12-09 18:00"},
    {"name": "US CPI Inflation Release", "utc": "2026-12-11 13:30"},
]


def get_crypto_sentiment() -> Dict[str, Any]:
    """
    Fetches the live Crypto Fear & Greed Index with in-memory caching.
    Returns: {"score": int, "classification": str, "advice": str, "timestamp": str}
    """
    now = time.time()
    if "sentiment" in _CACHE and (now - _CACHE["sentiment_time"]) < _CACHE_TTL:
        return _CACHE["sentiment"]

    url = "https://api.alternative.me/fng/?limit=1"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            if data:
                item = data[0]
                score = int(item.get("value", 50))
                classification = item.get("value_classification", "Neutral")

                if score <= 25:
                    advice = "Extreme Fear - Potential capitulation bounce; favor high-probability sweeps."
                    bias = "CAPITULATION_WATCH"
                elif score <= 45:
                    advice = "Fear - Cautious market; demand clean MSS confirmation."
                    bias = "DEFENSIVE"
                elif score <= 55:
                    advice = "Neutral - Balanced market structure; follow local OB setups."
                    bias = "BALANCED"
                elif score <= 75:
                    advice = "Greed - Bullish momentum; watch out for late-long liquidity traps."
                    bias = "MOMENTUM"
                else:
                    advice = "Extreme Greed - High distribution risk; beware of sharp long liquidations."
                    bias = "DISTRIBUTION_RISK"

                result = {
                    "score": score,
                    "classification": classification,
                    "advice": advice,
                    "bias": bias,
                    "status": "OK",
                }
                _CACHE["sentiment"] = result
                _CACHE["sentiment_time"] = now
                return result
    except Exception:
        pass

    # Safe fallback if network fails
    fallback = {
        "score": 50,
        "classification": "Neutral",
        "advice": "Balanced market structure; follow local OB setups.",
        "bias": "BALANCED",
        "status": "FALLBACK",
    }
    return _CACHE.get("sentiment", fallback)


def check_macro_event_block(buffer_pre_min: int = 30, buffer_post_min: int = 15) -> Dict[str, Any]:
    """
    Checks if current time falls within the high-impact macro economic event window.
    Blocks new setups if an event is scheduled within the next buffer_pre_min (default 30 mins)
    or just occurred within buffer_post_min (default 15 mins).
    """
    now_utc = datetime.now(timezone.utc)
    now_ts = now_utc.timestamp()

    for event in HIGH_IMPACT_SCHEDULE:
        try:
            event_dt = datetime.strptime(event["utc"], "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
            event_ts = event_dt.timestamp()

            pre_window = event_ts - (buffer_pre_min * 60)
            post_window = event_ts + (buffer_post_min * 60)

            if pre_window <= now_ts <= post_window:
                time_diff_min = int((event_ts - now_ts) / 60)
                timing_str = f"in {time_diff_min} mins" if time_diff_min > 0 else f"{abs(time_diff_min)} mins ago"
                return {
                    "is_blocked": True,
                    "event_name": event["name"],
                    "scheduled_utc": event["utc"],
                    "timing": timing_str,
                    "reason": f"High-Impact Macro Event Buffer: {event['name']} scheduled {timing_str}. Trading paused 30m prior to avoid slippage.",
                }
        except Exception:
            continue

    return {
        "is_blocked": False,
        "event_name": None,
        "timing": "Clear",
        "reason": "No high-impact macro announcements within event buffer.",
    }
