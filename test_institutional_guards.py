"""
test_institutional_guards.py - Unit Test Suite for Institutional-Grade Execution & Risk Shields.

Tests:
1. Market Bid-Ask Spread Anomaly Guard
2. Multi-Pair Correlation & Synthetic Over-Leverage Shield
3. Anti-Tilt & Consecutive Loss Cooldown Lockout
4. Weekend Low-Volume & Wick Risk Multiplier
5. Fee-Adjusted Breakeven Calculator
6. Stale Trade TTL Decay (Chop / Capital Recycling)
7. Funding Rate Arbitrage / Squeeze Gate
8. Flash Crash Circuit Breaker
"""
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from engine_institutional_guards import (
    check_market_spread,
    check_correlation_risk,
    check_anti_tilt_lockout,
    get_weekend_risk_multiplier,
    calculate_fee_adjusted_breakeven,
    check_stale_trade_ttl,
    check_funding_rate_gate,
    check_flash_crash_circuit_breaker,
    reset_circuit_breaker,
)
from engine_exit_manager import generate_exit_plan, check_trade_stage
from engine_risk_budget import RiskBudgetManager


def test_market_spread_guard():
    print("Testing 1. Market Bid-Ask Spread Guard...")
    mock_client = MagicMock()

    # Case A: Normal tight spread (0.01%) -> Allowed
    mock_client._get.return_value = {"bidPrice": "99.99", "askPrice": "100.00"}
    ok, reason, spread = check_market_spread("BTCUSDT", client=mock_client, max_spread_pct=0.08)
    assert ok is True, f"Expected OK, got: {reason}"
    assert spread < 0.08

    # Case B: Wide anomalous spread (0.20%) -> Blocked
    mock_client._get.return_value = {"bidPrice": "99.80", "askPrice": "100.00"}
    ok, reason, spread = check_market_spread("BTCUSDT", client=mock_client, max_spread_pct=0.08)
    assert ok is False, "Expected wide spread to be blocked"
    assert "SPREAD_TOO_WIDE" in reason
    assert spread >= 0.08
    print("  -> PASSED: Spread guard accurately filters wicks/slippage gaps.")


def test_correlation_risk_shield():
    print("Testing 2. Multi-Pair Correlation Risk Shield...")
    open_positions = {
        "BTC_123": {"symbol": "BTCUSDT", "direction": "LONG", "entry_price": 60000.0},
    }

    # Case A: Trying to open ETH LONG while BTC LONG is already open -> Blocked (Synthetic 2x BTC leverage)
    ok, reason = check_correlation_risk("ETHUSDT", "LONG", open_positions)
    assert ok is False
    assert "CORRELATION_RISK_BLOCK" in reason

    # Case B: Trying to open SOL LONG while BTC LONG is already open -> Blocked
    ok, reason = check_correlation_risk("SOLUSDT", "LONG", open_positions)
    assert ok is False

    # Case C: Trying to open ETH SHORT (Hedged / opposing direction) -> Allowed
    ok, reason = check_correlation_risk("ETHUSDT", "SHORT", open_positions)
    assert ok is True

    # Case D: Empty open positions -> Allowed
    ok, reason = check_correlation_risk("BTCUSDT", "LONG", {})
    assert ok is True
    print("  -> PASSED: Correlation guard prevents synthetic portfolio over-leverage.")


def test_anti_tilt_lockout():
    print("Testing 3. Anti-Tilt Consecutive Loss Lockout...")
    now_t = 1000000.0

    # Case A: Only 2 losses -> Clear
    two_losses = [
        {"trade_id": "T1", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 300},
        {"trade_id": "T2", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 200},
    ]
    locked, reason, _ = check_anti_tilt_lockout(two_losses, current_time=now_t)
    assert locked is False

    # Case B: 3 consecutive losses within 4 hours -> Lockout Active!
    three_losses = [
        {"trade_id": "T1", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 300},
        {"trade_id": "T2", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 200},
        {"trade_id": "T3", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 100},
    ]
    locked, reason, rem = check_anti_tilt_lockout(three_losses, current_time=now_t)
    assert locked is True
    assert "ANTI_TILT_LOCKOUT_ACTIVE" in reason
    assert rem > 0

    # Case C: Recent Win breaks the streak -> Clear
    win_broken = [
        {"trade_id": "T0", "total_pnl_usd": 35.0, "realized_r": 1.75, "closed_at": now_t - 50},
        {"trade_id": "T1", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 300},
        {"trade_id": "T2", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 200},
        {"trade_id": "T3", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 100},
    ]
    locked, reason, _ = check_anti_tilt_lockout(win_broken, current_time=now_t)
    assert locked is False

    # Case D: 3 losses older than 4 hours (14400s) -> Lockout Expired
    old_losses = [
        {"trade_id": "T1", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 20000},
        {"trade_id": "T2", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 18000},
        {"trade_id": "T3", "total_pnl_usd": -20.0, "realized_r": -1.0, "closed_at": now_t - 15000},
    ]
    locked, reason, _ = check_anti_tilt_lockout(old_losses, current_time=now_t)
    assert locked is False
    print("  -> PASSED: Anti-tilt lockout triggers cooldown on 3 losses and clears on win/expiry.")


def test_weekend_risk_multiplier():
    print("Testing 4. Weekend Low-Volume & Wick Risk Multiplier...")
    # Friday 22:00 UTC (Weekend mode) -> 0.5
    fri_night = datetime(2026, 11, 6, 22, 0, 0, tzinfo=timezone.utc)
    assert get_weekend_risk_multiplier(fri_night) == 0.5

    # Saturday 14:00 UTC (Weekend mode) -> 0.5
    sat_mid = datetime(2026, 11, 7, 14, 0, 0, tzinfo=timezone.utc)
    assert get_weekend_risk_multiplier(sat_mid) == 0.5

    # Sunday 20:00 UTC (Weekend mode) -> 0.5
    sun_night = datetime(2026, 11, 8, 20, 0, 0, tzinfo=timezone.utc)
    assert get_weekend_risk_multiplier(sun_night) == 0.5

    # Sunday 23:00 UTC (CME open / Normal mode) -> 1.0
    sun_late = datetime(2026, 11, 8, 23, 0, 0, tzinfo=timezone.utc)
    assert get_weekend_risk_multiplier(sun_late) == 1.0

    # Tuesday 10:00 UTC (Weekday mode) -> 1.0
    tue_day = datetime(2026, 11, 10, 10, 0, 0, tzinfo=timezone.utc)
    assert get_weekend_risk_multiplier(tue_day) == 1.0
    print("  -> PASSED: Weekend risk multiplier cuts risk by 50% during thin CME weekend sessions.")


def test_fee_adjusted_breakeven():
    print("Testing 5. Fee-Adjusted Breakeven Calculator...")
    # Long @ $100.0: Buffer of +0.12% -> $100.12
    be_long = calculate_fee_adjusted_breakeven(100.0, "LONG", fee_buffer_pct=0.0012)
    assert be_long > 100.0
    assert abs(be_long - 100.12) < 0.0001

    # Short @ $100.0: Buffer of -0.12% -> $99.88
    be_short = calculate_fee_adjusted_breakeven(100.0, "SHORT", fee_buffer_pct=0.0012)
    assert be_short < 100.0
    assert abs(be_short - 99.88) < 0.0001

    # Test integration with engine_exit_manager
    plan = generate_exit_plan(entry=100.0, stop_loss=95.0, direction="LONG", target=115.0)
    assert plan["breakeven_price"] == be_long

    # Test stage transition to TP1_HIT returns fee-adjusted BE
    stage_res = check_trade_stage(
        {"direction": "LONG", "entry_price": 100.0, "stop_loss": 95.0, "tp1": 107.5, "stage": "OPEN"},
        current_price=108.0,
    )
    assert stage_res["event"] == "TP1_HIT"
    assert stage_res["new_stop_loss"] == be_long
    print("  -> PASSED: Fee-adjusted breakeven preserves positive net edge over taker fees.")


def test_stale_trade_ttl():
    print("Testing 6. Stale Trade TTL Decay...")
    now_t = 2000000.0

    pos = {
        "symbol": "BTCUSDT",
        "direction": "LONG",
        "entry_price": 50000.0,
        "initial_stop_loss": 49000.0,
        "opened_at": now_t - (5 * 3600),  # 5 hours old
    }

    # Case A: 5 hours old with only 0.2R profit -> Expired / Stale
    is_stale, reason = check_stale_trade_ttl(pos, current_price=50200.0, current_time=now_t)
    assert is_stale is True
    assert "STALE_TRADE_TTL_EXPIRED" in reason

    # Case B: 5 hours old with 0.8R profit -> Progressing fine, not stale
    is_stale, reason = check_stale_trade_ttl(pos, current_price=50800.0, current_time=now_t)
    assert is_stale is False

    # Case C: Only 1 hour old -> Within TTL window
    pos_young = dict(pos, opened_at=now_t - 3600)
    is_stale, reason = check_stale_trade_ttl(pos_young, current_price=50100.0, current_time=now_t)
    assert is_stale is False
    print("  -> PASSED: Stale trade decay identifies chop and flags capital recycling.")


def test_funding_rate_gate():
    print("Testing 7. Funding Rate Arbitrage / Squeeze Gate...")
    # Mocking requests for funding rate
    import engine_institutional_guards
    orig_get = engine_institutional_guards.requests.get

    class MockResp:
        def __init__(self, rate):
            self.status_code = 200
            self._rate = rate
        def json(self):
            return [{"fundingRate": str(self._rate)}]

    try:
        # Long into extreme positive funding (+0.08%) -> Blocked
        engine_institutional_guards.requests.get = lambda url, timeout: MockResp(0.0008)
        ok, reason, fr = check_funding_rate_gate("BTCUSDT", "LONG", max_abs_funding=0.0005)
        assert ok is False
        assert "FUNDING_RATE_HIGH_LONG" in reason

        # Short into extreme negative funding (-0.08%) -> Blocked
        engine_institutional_guards.requests.get = lambda url, timeout: MockResp(-0.0008)
        ok, reason, fr = check_funding_rate_gate("BTCUSDT", "SHORT", max_abs_funding=0.0005)
        assert ok is False
        assert "FUNDING_RATE_HIGH_SHORT" in reason

        # Long with normal funding (+0.01%) -> Allowed
        engine_institutional_guards.requests.get = lambda url, timeout: MockResp(0.0001)
        ok, reason, fr = check_funding_rate_gate("BTCUSDT", "LONG", max_abs_funding=0.0005)
        assert ok is True
    finally:
        engine_institutional_guards.requests.get = orig_get

    print("  -> PASSED: Funding rate gate protects against negative carry drag.")


def test_flash_crash_circuit_breaker():
    print("Testing 8. Flash Crash Circuit Breaker...")
    reset_circuit_breaker()
    mock_client = MagicMock()

    # Case A: Normal candle move (0.4%) -> Clear
    import pandas as pd
    df_normal = pd.DataFrame([{"open": 50000.0, "low": 49800.0, "high": 50100.0, "close": 49950.0}])
    mock_client.klines.return_value = df_normal

    tripped, reason = check_flash_crash_circuit_breaker(mock_client, max_1m_drop_pct=2.5)
    assert tripped is False

    # Case B: Sudden flash crash (3.0% drop in 1m) -> Tripped!
    df_crash = pd.DataFrame([{"open": 50000.0, "low": 48500.0, "high": 50050.0, "close": 48600.0}])
    mock_client.klines.return_value = df_crash

    now_t = 3000000.0
    tripped, reason = check_flash_crash_circuit_breaker(mock_client, max_1m_drop_pct=2.5, current_time=now_t)
    assert tripped is True
    assert "CIRCUIT_BREAKER_TRIPPED" in reason

    # Case C: Subsequent call within 15 min cooldown remains active
    tripped, reason = check_flash_crash_circuit_breaker(mock_client, current_time=now_t + 300)
    assert tripped is True
    assert "CIRCUIT_BREAKER_ACTIVE" in reason

    # Case D: Reset manually clears circuit breaker
    reset_circuit_breaker()
    tripped, reason = check_flash_crash_circuit_breaker(mock_client, current_time=now_t + 300)
    assert tripped is True  # re-evaluates df_crash
    reset_circuit_breaker()
    mock_client.klines.return_value = df_normal
    tripped, reason = check_flash_crash_circuit_breaker(mock_client, current_time=now_t + 300)
    assert tripped is False
    print("  -> PASSED: Flash crash circuit breaker emergency pause functions perfectly.")


def test_risk_budget_manager_multiplier():
    print("Testing Risk Budget Manager with Weekend Multiplier...")
    mgr = RiskBudgetManager(account_balance=1000.0, max_loss_per_trade_usd=20.0)

    # Standard (1.0x): max loss = $20.0
    res_std = mgr.calculate_position(entry=100.0, stop_loss=95.0, target=115.0, risk_multiplier=1.0)
    assert res_std["status"] == "PASS"
    assert res_std["max_loss_usd"] == 20.0
    assert res_std["quantity"] == 4.0

    # Weekend (0.5x): max loss = $10.0, quantity = 2.0
    res_wknd = mgr.calculate_position(entry=100.0, stop_loss=95.0, target=115.0, risk_multiplier=0.5)
    assert res_wknd["status"] == "PASS"
    assert res_wknd["max_loss_usd"] == 10.0
    assert res_wknd["quantity"] == 2.0
    print("  -> PASSED: Risk Budget Manager accurately scales risk with multiplier.")


if __name__ == "__main__":
    test_market_spread_guard()
    test_correlation_risk_shield()
    test_anti_tilt_lockout()
    test_weekend_risk_multiplier()
    test_fee_adjusted_breakeven()
    test_stale_trade_ttl()
    test_funding_rate_gate()
    test_flash_crash_circuit_breaker()
    test_risk_budget_manager_multiplier()
    print("\n" + "=" * 60)
    print("ALL 8 INSTITUTIONAL SHIELDS & GUARDS VERIFIED SUCCESSFULLY!")
    print("=" * 60)

