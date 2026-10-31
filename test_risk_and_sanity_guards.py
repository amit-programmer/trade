"""
test_risk_and_sanity_guards.py - Unit Test Verification for Capital Safety & Data Sanity Guards.

Verifies:
  1. Max Concurrent Open Trades Guard (BLOCKED: MAX_CONCURRENT_TRADES_REACHED on 4th trade)
  2. Daily Max Drawdown Hard-Stop Guard (STATUS: DAILY_LOSS_LIMIT_REACHED at $50 net loss)
  3. Stale Data & Timestamp Sanity Guard (SKIPPED: STALE_MARKET_DATA for feed lag > 180s)
"""
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

# Force unbuffered output and UTF-8 encoding
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from engine_risk_budget import RiskBudgetManager
from engine_paper_broker import PaperBroker
from engine_exit_manager import generate_exit_plan
from trader_bot import verify_market_data_freshness, handle_setup, Setup
import pandas as pd


def test_guards() -> bool:
    print("======================================================================")
    print("RUNNING UNIT TESTS FOR CAPITAL SAFETY & DATA SANITY GUARDS")
    print("======================================================================")

    temp_dir = Path(tempfile.mkdtemp(prefix="guard_test_"))
    try:
        # ---------------------------------------------------------------------
        # TEST 1: MAX CONCURRENT OPEN TRADES GUARD (3 Max)
        # ---------------------------------------------------------------------
        print("\n[TEST 1] Verifying Max Concurrent Open Trades Guard...")
        p_file = temp_dir / "paper_positions.json"
        p_hist = temp_dir / "paper_positions_history.json"
        broker = PaperBroker(filepath=p_file, history_filepath=p_hist, max_concurrent_trades=3)

        exit_plan = generate_exit_plan(entry=50000.0, stop_loss=49000.0, direction="LONG", target=52000.0)

        # Open 3 trades
        for i in range(1, 4):
            tid = f"trade_{i}"
            registered = broker.register_pending_setup(
                trade_id=tid,
                symbol="BTCUSDT",
                direction="LONG",
                timeframe="15m",
                entry_price=50000.0,
                stop_loss=49000.0,
                target=52000.0,
                quantity=0.02,
                max_loss_usd=20.0,
                exit_plan=exit_plan,
            )
            assert registered is True, f"Failed to register trade {i}"
            approved = broker.approve_trade(tid)
            assert approved is not None and approved.get("status") == "OPEN", f"Trade {i} did not open"

        open_trades = broker.get_open_positions()
        print(f"         Active open trades: {len(open_trades)}/3")
        assert len(open_trades) == 3, f"Expected 3 open positions, got {len(open_trades)}"

        # Attempt 4th trade
        can_open, reason = broker.can_open_new_trade()
        print(f"         4th trade can_open check: {can_open} -> Reason: '{reason}'")
        assert can_open is False, "4th trade was unexpectedly allowed!"
        assert "BLOCKED: MAX_CONCURRENT_TRADES_REACHED" in reason, f"Unexpected block reason: {reason}"

        # Attempt 4th trade registration (should be blocked)
        reg_4 = broker.register_pending_setup(
            trade_id="trade_4",
            symbol="ETHUSDT",
            direction="LONG",
            timeframe="15m",
            entry_price=3000.0,
            stop_loss=2900.0,
            target=3200.0,
            quantity=0.2,
            max_loss_usd=20.0,
            exit_plan=exit_plan,
        )
        assert reg_4 is False, "Pending setup registration was not blocked when 3 trades active!"

        # Test RiskBudgetManager can_open_trade and calculate_position directly
        rm = RiskBudgetManager(max_concurrent_trades=3)
        rm_ok, rm_reason = rm.can_open_trade(current_open_count=3)
        assert rm_ok is False and "BLOCKED: MAX_CONCURRENT_TRADES_REACHED" in rm_reason
        calc_res = rm.calculate_position(50000, 49000, 52000, current_open_count=3)
        assert calc_res["status"] == "BLOCK" and "MAX_CONCURRENT_TRADES_REACHED" in calc_res["reason"]

        print("         >> [OK] 4th Trade Concurrency Block Trigger: VERIFIED")

        # ---------------------------------------------------------------------
        # TEST 2: DAILY MAX DRAWDOWN HARD-STOP GUARD ($50 Max Loss)
        # ---------------------------------------------------------------------
        print("\n[TEST 2] Verifying Daily Max Drawdown Hard-Stop Guard ($50 limit)...")
        drawdown_file = temp_dir / "drawdown_positions.json"
        dd_hist = temp_dir / "drawdown_history.json"
        dd_broker = PaperBroker(filepath=drawdown_file, history_filepath=dd_hist, max_concurrent_trades=3, max_daily_loss_usd=50.0)

        # Before any losses
        assert dd_broker.get_today_realized_loss_usd() == 0.0
        can_open_before, _ = dd_broker.can_open_new_trade()
        assert can_open_before is True

        # Simulate 2 losing trades today totaling $50 loss (-$30 and -$20)
        today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        dd_broker.state["closed"] = [
            {"id": "loss_1", "symbol": "SOLUSDT", "total_pnl_usd": -30.0, "closed_ts": today_utc},
            {"id": "loss_2", "symbol": "BTCUSDT", "total_pnl_usd": -20.0, "closed_ts": today_utc},
        ]
        dd_broker._save()

        today_loss = dd_broker.get_today_realized_loss_usd()
        print(f"         Today's Net Realized Loss: ${today_loss:,.2f}")
        assert today_loss == 50.0, f"Expected loss 50.0, got {today_loss}"

        can_open_after, dd_reason = dd_broker.can_open_new_trade()
        print(f"         can_open check on $50 loss: {can_open_after} -> Status: '{dd_reason}'")
        assert can_open_after is False, "Trade was allowed despite hitting $50 daily loss hard-stop!"
        assert "STATUS: DAILY_LOSS_LIMIT_REACHED" in dd_reason, f"Unexpected reason: {dd_reason}"

        # Attempt to register setup
        reg_dd = dd_broker.register_pending_setup(
            trade_id="dd_trade_test",
            symbol="XRPUSDT",
            direction="LONG",
            timeframe="15m",
            entry_price=1.5,
            stop_loss=1.4,
            target=1.7,
            quantity=100.0,
            max_loss_usd=20.0,
            exit_plan=exit_plan,
        )
        assert reg_dd is False, "Setup registration was not blocked on daily loss hard-stop!"

        # Test RiskBudgetManager with daily loss hit
        rm_dd = RiskBudgetManager(max_daily_loss_usd=50.0)
        rm_dd_ok, rm_dd_reason = rm_dd.can_open_trade(0, today_realized_loss=50.0)
        assert rm_dd_ok is False and "STATUS: DAILY_LOSS_LIMIT_REACHED" in rm_dd_reason
        calc_dd = rm_dd.calculate_position(50000, 49000, 52000, today_realized_loss=50.0)
        assert calc_dd["status"] == "BLOCK" and "DAILY_LOSS_LIMIT_REACHED" in calc_dd["reason"]

        print("         >> [OK] Daily Drawdown Lock Activation on $50 Loss: VERIFIED")

        # ---------------------------------------------------------------------
        # TEST 3: STALE DATA & TIMESTAMP SANITY GUARD (3 Minutes = 180s)
        # ---------------------------------------------------------------------
        print("\n[TEST 3] Verifying Stale Data & Timestamp Sanity Guard (3-min lag limit)...")
        now = time.time()

        # 3a. Direct timestamp test: Fresh vs Stale
        fresh_ts = now - 30.0    # 30 seconds old -> Fresh
        stale_ts = now - 240.0   # 4 minutes old -> Stale (> 180s)

        ok_fresh, msg_fresh = verify_market_data_freshness("BTCUSDT", data_timestamp=fresh_ts)
        print(f"         Fresh data check (age 30s): {ok_fresh} -> '{msg_fresh}'")
        assert ok_fresh is True, "Fresh data was incorrectly flagged as stale"

        ok_stale, msg_stale = verify_market_data_freshness("BTCUSDT", data_timestamp=stale_ts)
        print(f"         Stale data check (age 240s): {ok_stale} -> '{msg_stale}'")
        assert ok_stale is False, "Stale data was not detected!"
        assert "SKIPPED: STALE_MARKET_DATA" in msg_stale, f"Unexpected message: {msg_stale}"

        # 3b. DataFrame candle metadata test
        df_fresh = pd.DataFrame([{"close_time": int((now - 10) * 1000)}])
        df_fresh.attrs["fetched_at"] = now - 10.0
        df_stale = pd.DataFrame([{"close_time": int((now - 300) * 1000)}])
        df_stale.attrs["fetched_at"] = now - 300.0

        assert verify_market_data_freshness("ETHUSDT", df=df_fresh)[0] is True
        stale_df_res, stale_df_msg = verify_market_data_freshness("ETHUSDT", df=df_stale)
        assert stale_df_res is False and "SKIPPED: STALE_MARKET_DATA" in stale_df_msg
        print(f"         Stale DataFrame check: {stale_df_res} -> '{stale_df_msg}'")

        # 3c. Client fetch cache test
        mock_client = MagicMock()
        mock_client.last_fetch_times = {"SOLUSDT": now - 210.0}
        stale_c_res, stale_c_msg = verify_market_data_freshness("SOLUSDT", client=mock_client)
        assert stale_c_res is False and "SKIPPED: STALE_MARKET_DATA" in stale_c_msg
        print(f"         Stale Client Cache check: {stale_c_res} -> '{stale_c_msg}'")

        # 3d. Order Trigger Execution Halt Test in handle_setup
        stale_setup = Setup(
            symbol="BTCUSDT",
            timeframe="15m",
            direction="LONG",
            sweep_time=pd.Timestamp.now(timezone.utc),
            disp_time=pd.Timestamp.now(timezone.utc),
            sweep_level=60000.0,
            sweep_extreme=59800.0,
            ob_low=60100.0,
            ob_high=60300.0,
            entry=60200.0,
            stop=59700.0,
            target=61500.0,
            retest="live",
            price=60200.0,
            atr=200.0,
        )
        test_state = {"alerted": {}, "stats": {"alerts": []}}
        # Calling handle_setup with stale dataframe halts execution (returns None)
        halt_res = handle_setup(stale_setup, datetime.now(timezone.utc), test_state, df=df_stale, client=mock_client)
        print(f"         handle_setup execution result on stale data: {halt_res} (Execution Halted)")
        assert halt_res is None, "handle_setup did not halt execution on stale market data!"

        print("         >> [OK] Stale Data Execution Halt & Filter: VERIFIED")

        print("\n======================================================================")
        print("ALL 3 LOGIC HOOKS & SAFETY GUARDS VERIFIED WITH 100% SUCCESS!")
        print("======================================================================")
        return True

    finally:
        # Clean up temp directory
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    success = test_guards()
    sys.exit(0 if success else 1)
