"""
engine_institutional_guards.py - Institutional-Grade Execution & Risk Shields.

Implements institutional execution safeguards and risk filters to protect real-money
capital against real-world microstructure frictions:
1. Spread & Liquidity Anomaly Guard (Max Allowable Spread % check: > 0.08% blocks setup)
2. Pair Correlation & Synthetic Over-Leverage Shield (Prevents stacking correlated positions)
3. Anti-Tilt & Consecutive Loss Cooldown (3-consecutive loss lockout for 4 hours)
4. Weekend Low-Volume & Wick Risk Multiplier (50% risk budget reduction during Fri 21:00 - Sun 22:00 UTC)
5. Fee-Adjusted Breakeven Calculator (Entry +/- 0.12% buffer covers round-trip taker fees & slippage)
6. Stale Trade TTL Decay (Chop / Capital Recycling exit for stagnant positions > 4h with < 0.5R)
7. Funding Rate Arbitrage / Squeeze Gate (Blocks entries into extreme funding rate penalties)
8. Flash Crash / Liquidity Void Circuit Breaker (Detects sudden BTC > 2.5% 1m drops and pauses entries)
"""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import requests

log = logging.getLogger("trader_bot")

# Global circuit breaker pause timestamp
_CIRCUIT_BREAKER_UNTIL: float = 0.0

# Correlated Asset Clusters (High-beta crypto pairs)
CORRELATED_CLUSTERS: List[set[str]] = [
    {"BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT", "XRPUSDT"},
]


# =============================================================================
# 1. SPREAD & LIQUIDITY ANOMALY GUARD
# =============================================================================
def check_market_spread(
    symbol: str,
    client: Optional[Any] = None,
    max_spread_pct: float = 0.08,
) -> Tuple[bool, str, float]:
    """
    Checks real-time bid-ask spread percentage.
    If spread > max_spread_pct (default 0.08%), returns False to prevent wick poisoning / adverse fills.
    """
    if not symbol:
        return True, "SPREAD_OK_DEFAULT", 0.0

    norm_sym = symbol.upper().replace("-", "").replace("/", "")

    if client is not None and hasattr(client, "_get"):
        try:
            data = client._get("/api/v3/ticker/bookTicker", {"symbol": norm_sym})
            bid = float(data.get("bidPrice", 0.0))
            ask = float(data.get("askPrice", 0.0))
            if bid > 0 and ask > 0:
                spread_pct = ((ask - bid) / ask) * 100.0
                if spread_pct > max_spread_pct:
                    return (
                        False,
                        f"SPREAD_TOO_WIDE: Spread {spread_pct:.3f}% exceeds maximum allowable {max_spread_pct:.3f}%",
                        round(spread_pct, 4),
                    )
                return True, "SPREAD_OK", round(spread_pct, 4)
        except Exception as exc:
            log.debug("Spread check client error for %s: %s", norm_sym, exc)

    return True, "SPREAD_OK_DEFAULT", 0.0


# =============================================================================
# 2. PAIR CORRELATION & SYNTHETIC OVER-LEVERAGE SHIELD
# =============================================================================
def check_correlation_risk(
    new_symbol: str,
    new_direction: str,
    open_positions: Union[Dict[str, Any], List[Dict[str, Any]]],
    max_correlated_positions: int = 1,
) -> Tuple[bool, str]:
    """
    Prevents opening multiple highly correlated positions in the same direction,
    avoiding synthetic 2x-3x BTC beta risk.
    """
    norm_sym = new_symbol.upper().replace("-", "").replace("/", "")
    norm_dir = new_direction.upper()

    positions_list: List[Dict[str, Any]] = []
    if isinstance(open_positions, dict):
        positions_list = list(open_positions.values())
    elif isinstance(open_positions, list):
        positions_list = open_positions

    for cluster in CORRELATED_CLUSTERS:
        if norm_sym in cluster:
            same_dir_count = 0
            open_correlated = []
            for pos in positions_list:
                p_sym = str(pos.get("symbol", "")).upper().replace("-", "").replace("/", "")
                p_dir = str(pos.get("direction", "")).upper()
                if p_sym in cluster and p_dir == norm_dir:
                    same_dir_count += 1
                    open_correlated.append(p_sym)

            if same_dir_count >= max_correlated_positions:
                return (
                    False,
                    f"CORRELATION_RISK_BLOCK: Already holding {', '.join(open_correlated)} in direction {norm_dir}. "
                    f"Skipping {norm_sym} to prevent synthetic portfolio over-leverage.",
                )

    return True, "CORRELATION_OK"


# =============================================================================
# 3. ANTI-TILT & CONSECUTIVE LOSS COOLDOWN
# =============================================================================
def check_anti_tilt_lockout(
    closed_trades: List[Dict[str, Any]],
    max_consecutive_losses: int = 3,
    lockout_seconds: float = 14400.0,  # 4 hours
    current_time: Optional[float] = None,
) -> Tuple[bool, str, float]:
    """
    Anti-Tilt Shield: If the bot suffers 3 consecutive realized losses,
    triggers an automatic 4-hour cooldown lockout to preserve capital and prevent revenge trading.
    Returns: (is_locked_out, reason, remaining_seconds)
    """
    if not closed_trades or len(closed_trades) < max_consecutive_losses:
        return False, "ANTI_TILT_CLEAR", 0.0

    now_t = current_time if current_time is not None else time.time()

    sorted_trades = sorted(
        closed_trades,
        key=lambda t: float(t.get("closed_at") or 0.0),
        reverse=True,
    )

    consecutive_losses = 0
    last_loss_time = 0.0

    for t in sorted_trades:
        pnl = float(t.get("total_pnl_usd", t.get("realized_pnl_usd", 0.0)))
        r = float(t.get("realized_r", 0.0))
        if pnl < -0.05 or r < -0.2:
            consecutive_losses += 1
            if last_loss_time <= 0:
                last_loss_time = float(t.get("closed_at") or now_t)
            if consecutive_losses >= max_consecutive_losses:
                break
        elif pnl > 0.05 or r > 0.2:
            break
        else:
            pass

    if consecutive_losses >= max_consecutive_losses:
        elapsed = now_t - last_loss_time
        if elapsed < lockout_seconds:
            remaining = lockout_seconds - elapsed
            mins_rem = int(remaining // 60)
            return (
                True,
                f"ANTI_TILT_LOCKOUT_ACTIVE: {consecutive_losses} consecutive realized losses. "
                f"System cooling down for capital preservation. Time remaining: {mins_rem}m.",
                round(remaining, 1),
            )

    return False, "ANTI_TILT_CLEAR", 0.0


# =============================================================================
# 4. WEEKEND LOW-VOLUME & WICK RISK MULTIPLIER
# =============================================================================
def get_weekend_risk_multiplier(utc_dt: Optional[datetime] = None) -> float:
    """
    Institutional Weekend Shield:
    Between Friday 21:00 UTC and Sunday 22:00 UTC, CME futures are closed,
    market liquidity is thin, and predatory manipulation wicks are prevalent.
    Reduces standard position risk by 50% (multiplier = 0.5).
    """
    dt = utc_dt or datetime.now(timezone.utc)
    weekday = dt.weekday()  # Monday = 0, ..., Friday = 4, Saturday = 5, Sunday = 6
    hour = dt.hour

    if weekday == 4 and hour >= 21:
        return 0.5
    if weekday == 5:
        return 0.5
    if weekday == 6 and hour < 22:
        return 0.5

    return 1.0


# =============================================================================
# 5. FEE-ADJUSTED BREAKEVEN CALCULATOR
# =============================================================================
def calculate_fee_adjusted_breakeven(
    entry: float,
    direction: str,
    fee_buffer_pct: float = 0.0012,  # 0.12% buffer covers 0.05% entry taker + 0.05% exit taker + 0.02% slippage
) -> float:
    """
    Calculates true fee-adjusted breakeven price.
    Moving SL to pure entry price results in net loss on hit due to taker fees & slippage.
    Adding a small 0.12% buffer in favor of the trade guarantees realized net PnL >= $0.00.
    """
    entry = float(entry)
    is_long = direction.upper() in ("LONG", "BUY")
    if is_long:
        be_price = entry * (1.0 + fee_buffer_pct)
    else:
        be_price = entry * (1.0 - fee_buffer_pct)

    if entry >= 1000:
        return round(be_price, 2)
    elif entry >= 1:
        return round(be_price, 4)
    else:
        return round(be_price, 6)


# =============================================================================
# 6. STALE TRADE TTL DECAY (CHOP / CAPITAL RECYCLING)
# =============================================================================
def check_stale_trade_ttl(
    position: Dict[str, Any],
    current_price: float,
    max_age_seconds: float = 14400.0,  # 4 hours
    min_r: float = 0.5,
    current_time: Optional[float] = None,
) -> Tuple[bool, str]:
    """
    Time-Decay / Stale Trade Guard:
    If a trade has been open for > 4 hours without reaching +0.5R,
    the thesis has stalled in chop.
    Signals early exit for capital recycling.
    """
    now_t = current_time if current_time is not None else time.time()
    opened_at = float(position.get("opened_at") or now_t)
    elapsed = now_t - opened_at

    if elapsed <= max_age_seconds:
        return False, "TTL_OK"

    entry = float(position.get("entry_price", current_price))
    sl = float(position.get("initial_stop_loss", position.get("stop_loss", entry * 0.99)))
    risk_r = abs(entry - sl) or (entry * 0.01)
    is_long = str(position.get("direction", "LONG")).upper() in ("LONG", "BUY")

    current_r = ((current_price - entry) if is_long else (entry - current_price)) / risk_r
    if current_r < min_r:
        hours_open = round(elapsed / 3600.0, 1)
        return (
            True,
            f"STALE_TRADE_TTL_EXPIRED: Position stagnant for {hours_open}h with only {current_r:+.2f}R (< {min_r}R). "
            f"Triggering early exit for capital recycling.",
        )

    return False, "TTL_OK"


# =============================================================================
# 7. FUNDING RATE ARBITRAGE / SQUEEZE GATE
# =============================================================================
def check_funding_rate_gate(
    symbol: str,
    direction: str,
    client: Optional[Any] = None,
    max_abs_funding: float = 0.0005,  # 0.05%
) -> Tuple[bool, str, float]:
    """
    Funding Rate Filter:
    Fetches latest funding rate for the perpetual pair.
    - If going LONG and funding > +0.05%: long positions pay high holding cost, block entry.
    - If going SHORT and funding < -0.05%: short positions pay high holding cost, block entry.
    """
    if not symbol:
        return True, "FUNDING_OK_DEFAULT", 0.0

    norm_sym = symbol.upper().replace("-", "").replace("/", "")
    try:
        url = f"https://fapi.binance.com/fapi/v1/fundingRate?symbol={norm_sym}&limit=1"
        resp = requests.get(url, timeout=3.0)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                fr = float(data[0].get("fundingRate", 0.0))
                is_long = direction.upper() in ("LONG", "BUY")
                if is_long and fr > max_abs_funding:
                    return (
                        False,
                        f"FUNDING_RATE_HIGH_LONG: Funding rate {fr*100:+.3f}% > +{max_abs_funding*100:.2f}%. "
                        f"Longs face aggressive negative carry drag.",
                        fr,
                    )
                elif (not is_long) and fr < -max_abs_funding:
                    return (
                        False,
                        f"FUNDING_RATE_HIGH_SHORT: Funding rate {fr*100:+.3f}% < -{max_abs_funding*100:.2f}%. "
                        f"Shorts face aggressive negative carry drag.",
                        fr,
                    )
                return True, "FUNDING_OK", fr
    except Exception as exc:
        log.debug("Funding rate check bypassed for %s: %s", norm_sym, exc)

    return True, "FUNDING_OK_DEFAULT", 0.0


# =============================================================================
# 8. FLASH CRASH / LIQUIDITY VOID CIRCUIT BREAKER
# =============================================================================
def check_flash_crash_circuit_breaker(
    client: Optional[Any] = None,
    max_1m_drop_pct: float = 2.5,
    cooldown_seconds: float = 900.0,  # 15 minutes
    current_time: Optional[float] = None,
) -> Tuple[bool, str]:
    """
    Emergency Circuit Breaker:
    Monitors BTC 1-minute candle. If BTC dropped or spiked > 2.5% in 1 minute,
    trips circuit breaker for 15 minutes to prevent taking falling knives during cascade liquidations.
    """
    global _CIRCUIT_BREAKER_UNTIL
    now_t = current_time if current_time is not None else time.time()

    if now_t < _CIRCUIT_BREAKER_UNTIL:
        rem = int((_CIRCUIT_BREAKER_UNTIL - now_t) // 60)
        return (
            True,
            f"CIRCUIT_BREAKER_ACTIVE: Flash crash protection active. Entries paused for {rem}m.",
        )

    if client is not None and hasattr(client, "klines"):
        try:
            df_1m = client.klines("BTCUSDT", "1m", 2)
            if df_1m is not None and len(df_1m) > 0:
                last_candle = df_1m.iloc[-1]
                c_open = float(last_candle.get("open", 0.0))
                c_low = float(last_candle.get("low", 0.0))
                c_high = float(last_candle.get("high", 0.0))
                if c_open > 0:
                    drop_pct = ((c_open - c_low) / c_open) * 100.0
                    spike_pct = ((c_high - c_open) / c_open) * 100.0
                    max_move = max(drop_pct, spike_pct)
                    if max_move >= max_1m_drop_pct:
                        _CIRCUIT_BREAKER_UNTIL = now_t + cooldown_seconds
                        return (
                            True,
                            f"CIRCUIT_BREAKER_TRIPPED: Extreme 1m BTC volatility ({max_move:.2f}% >= {max_1m_drop_pct}%). "
                            f"Emergency pause for {int(cooldown_seconds // 60)} minutes.",
                        )
        except Exception as exc:
            log.debug("Circuit breaker check bypassed: %s", exc)

    return False, "CIRCUIT_BREAKER_CLEAR"


def reset_circuit_breaker() -> None:
    """Manually clears any active flash crash circuit breaker."""
    global _CIRCUIT_BREAKER_UNTIL
    _CIRCUIT_BREAKER_UNTIL = 0.0
