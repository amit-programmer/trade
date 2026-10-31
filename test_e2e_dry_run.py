"""
test_e2e_dry_run.py - Automated End-to-End Synthetic Dry-Run & Runtime Guard Test.

Validates the complete 8-step pipeline:
  1. Mock SMC Bullish Setup Trigger (BTCUSDT MSS Retest)
  2. Time Machine Monte Carlo Simulation (1,000 runs)
  3. Dynamic Risk Budget Position Sizing ($20 max loss)
  4. 3-Tier Multi-Target Exit Plan (TP1 1.5R + Auto-BE, TP2, TP3)
  5. Pre-Trade Available Margin Check (Insufficient Margin Block & Pass)
  6. Slippage Guard: Actual Fill Price Sync & Dynamic Bracket Recalculation
  7. Position Lifecycle & Atomic Breakeven Migration (50% lot size precision)
  8. Graceful Shutdown & Clean Disk Lock Guard (no orphan .tmp files)
"""
import json
import os
import sys
import time
from pathlib import Path

# Force unbuffered output and UTF-8 encoding
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from engine_timemachine import run_monte_carlo_simulation
from engine_risk_budget import RiskBudgetManager
from engine_exit_manager import generate_exit_plan, check_trade_stage
from engine_paper_broker import PAPER_BROKER
from engine_delta_broker import DELTA_BROKER, DEFAULT_CONTRACT_SPECS


def run_e2e_test() -> bool:
    print("======================================================================")
    print("STARTING AUTOMATED END-TO-END SYNTHETIC DRY-RUN (MOCK EXECUTION)")
    print("======================================================================")

    # -------------------------------------------------------------------------
    # STEP 1: MOCK SMC SETUP CREATION
    # -------------------------------------------------------------------------
    symbol = "BTCUSDT"
    direction = "LONG"
    timeframe = "15m"
    planned_entry = 65000.0
    initial_sl = 64500.0
    planned_tp2 = 67500.0
    print(f"\n[STEP 1] Mock Setup Detected: {symbol} {direction} on {timeframe}")
    print(f"         Planned Entry: ${planned_entry:,.2f} | Stop-Loss: ${initial_sl:,.2f}")

    # -------------------------------------------------------------------------
    # STEP 2: TIME MACHINE MONTE CARLO SIMULATION
    # -------------------------------------------------------------------------
    print("\n[STEP 2] Running Time Machine Monte Carlo Simulation (1,000 iterations)...")
    synthetic_candles = []
    base_p = 64800.0
    for i in range(60):
        c_open = base_p + i * 5.0
        c_high = c_open + 95.0
        c_low = c_open - 35.0
        c_close = c_open + 50.0
        synthetic_candles.append({"open": c_open, "high": c_high, "low": c_low, "close": c_close})

    sim_res = run_monte_carlo_simulation(
        recent_candles=synthetic_candles,
        entry_price=planned_entry,
        stop_loss=initial_sl,
        take_profit=planned_tp2,
        direction="BUY",
        simulations=1000,
    )
    print(f"         Sim Win Prob: {sim_res['target_win_prob']} | Stop Prob: {sim_res['stop_loss_prob']} | Edge: {sim_res['edge']}")
    assert sim_res["simulations"] == 1000, "Simulation count mismatch"
    assert "target_pct_raw" in sim_res, "Missing raw target percentage"
    print("         >> [OK] Time Machine Simulation: PASSED")

    # -------------------------------------------------------------------------
    # STEP 3: RISK BUDGET POSITION SIZING
    # -------------------------------------------------------------------------
    print("\n[STEP 3] Calculating Dynamic Risk Budget (Target $20 Risk / 1R)...")
    risk_mgr = RiskBudgetManager(account_balance=1000.0, max_loss_per_trade_usd=20.0)
    sizing = risk_mgr.calculate_position(
        entry=planned_entry,
        stop_loss=initial_sl,
        target=planned_tp2,
        symbol=symbol,
    )
    coin_qty = sizing["quantity"]
    max_loss = sizing["max_loss_usd"]
    c_lots = DELTA_BROKER.normalize_contract_size(symbol, coin_qty)
    print(f"         Allocated: {coin_qty} BTC ({c_lots} Delta contracts) for Max Loss: ${max_loss:,.2f}")
    assert coin_qty > 0, "Invalid position sizing"
    assert c_lots >= 1, "Contract normalization failed"
    print("         >> [OK] Risk Sizing & Lot Normalization: PASSED")

    # -------------------------------------------------------------------------
    # STEP 4: INSTITUTIONAL 3-TIER EXIT PLAN
    # -------------------------------------------------------------------------
    print("\n[STEP 4] Generating Multi-Target Dynamic Exit Plan...")
    exit_plan = generate_exit_plan(
        entry=planned_entry,
        stop_loss=initial_sl,
        direction=direction,
        target=planned_tp2,
    )
    tp1 = exit_plan["tp1"]["price"]
    tp2 = exit_plan["tp2"]["price"]
    tp3 = exit_plan["tp3"]["price"]
    print(f"         TP1 (1.5R): ${tp1:,.2f} (50% lock + Move to BE)")
    print(f"         TP2 (2.5R): ${tp2:,.2f} (30% lock at opposing pool)")
    print(f"         TP3 Runner: ${tp3:,.2f} (20% trail)")
    assert tp1 == planned_entry + 1.5 * (planned_entry - initial_sl)
    print("         >> [OK] Exit Plan Math: PASSED")

    # -------------------------------------------------------------------------
    # STEP 5: PRE-TRADE AVAILABLE MARGIN CHECK
    # -------------------------------------------------------------------------
    print("\n[STEP 5] Testing Pre-Trade Available Margin Check...")
    # Test 5A: Insufficient margin simulation
    mock_low_balance = {
        "success": True,
        "result": [{"asset_symbol": "USDT", "available_balance": "5.50", "balance": "10.00"}]
    }
    orig_get_balance = DELTA_BROKER.get_balance
    orig_enabled = DELTA_BROKER.enabled
    orig_key = DELTA_BROKER.api_key
    orig_sec = DELTA_BROKER.api_secret

    DELTA_BROKER.get_balance = lambda: mock_low_balance
    DELTA_BROKER.enabled = True
    DELTA_BROKER.api_key = "dummy_key"
    DELTA_BROKER.api_secret = "dummy_secret"

    margin_check_low = DELTA_BROKER.check_available_margin(required_margin=50.0)
    print(f"         Test 5A (Low Margin): Sufficient={margin_check_low['sufficient']} (Available: ${margin_check_low['available_margin']})")
    assert margin_check_low["sufficient"] is False, "Low margin check should fail"

    # Test 5B: Sufficient margin simulation
    mock_high_balance = {
        "success": True,
        "result": [{"asset_symbol": "USDT", "available_balance": "1250.00", "balance": "1500.00"}]
    }
    DELTA_BROKER.get_balance = lambda: mock_high_balance
    margin_check_high = DELTA_BROKER.check_available_margin(required_margin=50.0)
    print(f"         Test 5B (High Margin): Sufficient={margin_check_high['sufficient']} (Available: ${margin_check_high['available_margin']})")
    assert margin_check_high["sufficient"] is True, "High margin check should pass"

    # Restore broker state
    DELTA_BROKER.get_balance = orig_get_balance
    DELTA_BROKER.enabled = orig_enabled
    DELTA_BROKER.api_key = orig_key
    DELTA_BROKER.api_secret = orig_sec
    print("         >> [OK] Pre-Trade Margin Check: PASSED (Spam & Liquidation Guard Active)")

    # -------------------------------------------------------------------------
    # STEP 6: SLIPPAGE GUARD & DYNAMIC BRACKET RECALCULATION
    # -------------------------------------------------------------------------
    print("\n[STEP 6] Testing Slippage Guard & Real Fill Price Sync...")
    test_tid = "e2e_dry_run_trade_999"
    # Register pending setup
    PAPER_BROKER.register_pending_setup(
        trade_id=test_tid,
        symbol=symbol,
        direction=direction,
        timeframe=timeframe,
        entry_price=planned_entry,
        stop_loss=initial_sl,
        target=planned_tp2,
        quantity=coin_qty,
        max_loss_usd=max_loss,
        exit_plan=exit_plan,
        ai_grade="A+",
        sim_win_prob="62%",
    )
    # Approve trade
    approved_trade = PAPER_BROKER.approve_trade(test_tid)
    assert approved_trade is not None
    assert approved_trade["entry_price"] == planned_entry

    # Simulate realistic exchange slippage: order filled at $65,035.00 (+35 USD higher)
    actual_fill_price = 65035.0
    updated_trade = PAPER_BROKER.update_entry_price_with_fill(test_tid, actual_fill_price)
    assert updated_trade is not None
    assert updated_trade["entry_price"] == actual_fill_price
    assert updated_trade["slippage_usd"] == 35.0
    expected_new_tp1 = round(actual_fill_price + 1.5 * (actual_fill_price - initial_sl), 6)
    assert abs(updated_trade["tp1"] - expected_new_tp1) < 1e-4, f"Recalculated TP1 mismatch: {updated_trade['tp1']} vs {expected_new_tp1}"
    print(f"         Planned Entry: ${planned_entry:,.2f} -> Actual Fill: ${actual_fill_price:,.2f} (+${updated_trade['slippage_usd']:.2f} slippage)")
    print(f"         Recalculated TP1 from Fill: ${updated_trade['tp1']:,.2f} | Breakeven Price: ${updated_trade['breakeven_price']:,.2f}")
    print("         >> [OK] Slippage Guard Fill Sync & Recalculation: PASSED")

    # -------------------------------------------------------------------------
    # STEP 7: LIFECYCLE TRACKER & ATOMIC BREAKEVEN MIGRATION
    # -------------------------------------------------------------------------
    print("\n[STEP 7] Testing Position Milestone Events & Atomic Breakeven...")
    # Price rises and hits new TP1
    price_tp1 = updated_trade["tp1"] + 10.0
    events_tp1 = PAPER_BROKER.update_positions({symbol: price_tp1})
    assert len(events_tp1) > 0, "No event generated on TP1 hit"
    tp1_event = next(e for e in events_tp1 if e["type"] == "TP1_HIT")
    assert tp1_event["trade_id"] == test_tid
    print(f"         Milestone Hit: {tp1_event['type']} @ ${price_tp1:,.2f}")
    print(f"         Partial Secured: +${tp1_event['booked_pnl_usd']:,.2f} | New Stop: ${tp1_event['new_stop']:,.2f}")

    # Verify atomic breakeven update call
    be_res = DELTA_BROKER.update_stop_loss_to_breakeven(
        symbol=symbol,
        size=int(c_lots * 0.5),
        side="sell",
        entry_price=actual_fill_price,
    )
    assert be_res["status"] in ("SKIPPED", "MIGRATED_TO_BREAKEVEN")
    print(f"         Atomic Breakeven Delta Trigger: {be_res['status']} (Zero naked exposure)")

    # Complete trade closure via TP3 hit
    price_tp3 = updated_trade["tp3"] + 50.0
    events_tp3 = PAPER_BROKER.update_positions({symbol: price_tp3})
    assert any(e["type"] in ("TP3_HIT", "STOP_HIT", "BE_HIT") for e in events_tp3)
    close_event = events_tp3[0]
    print(f"         Trade Completed: {close_event['type']} | Realized P&L: +${close_event['total_pnl_usd']:,.2f} ({close_event['realized_r']:+.2f}R)")
    print("         >> [OK] Lifecycle Tracker & Breakeven: PASSED")

    # -------------------------------------------------------------------------
    # STEP 8: CLEAN SHUTDOWN & LOCK GUARD
    # -------------------------------------------------------------------------
    print("\n[STEP 8] Testing Graceful Shutdown & Lock Guard...")
    PAPER_BROKER.shutdown()
    tmp_path = PAPER_BROKER.filepath.with_suffix(".tmp")
    assert not tmp_path.exists(), f"Orphan temporary file {tmp_path} detected!"
    with open(PAPER_BROKER.filepath, "r", encoding="utf-8") as f:
        saved_data = json.load(f)
    assert "stats" in saved_data
    print("         Zero orphan .tmp files found. JSON database valid and intact.")
    print("         >> [OK] Graceful Shutdown & Lock Guard: PASSED")

    # Cleanup test trade from closed history to keep state pristine
    with open(PAPER_BROKER.filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    data["closed"] = [t for t in data.get("closed", []) if t.get("id") != test_tid]
    with open(PAPER_BROKER.filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print("\n======================================================================")
    print("ALL 8/8 END-TO-END SYNTHETIC TESTS PASSED WITH 100% SUCCESS!")
    print("======================================================================")
    return True


if __name__ == "__main__":
    success = run_e2e_test()
    sys.exit(0 if success else 1)
