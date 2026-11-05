"""
test_autonomous_and_dual_mode.py - Unit & Integration Verification Suite:
1. Dual-Mode Toggle: HUMAN (Semi-Auto) vs AUTONOMOUS (Full AI Autonomy)
2. Autonomous Trade Lifecycle: Zero-Touch Entry, Dynamic Exit & Adaptive Self-Tuning
3. Web Server API Endpoints & 6 Micro-Agents Status
4. 4-Tab Google Sheets Headers & State Consistency
"""
import os
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure root directory in sys.path
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))
os.chdir(str(BASE_DIR))

from engine_adaptive_tuner import (
    load_adaptive_config,
    save_adaptive_config,
    apply_adaptive_config_to_strategy,
    run_adaptive_self_tuning,
)
from engine_paper_broker import PAPER_BROKER
from trader_bot import (
    CFG,
    SHEET_HEADERS,
    get_execution_mode,
    set_execution_mode,
    load_state,
    save_state,
    evaluate_autonomous_dynamic_exit,
)
import server


def test_dual_mode_toggle_state():
    """Verify state persistence and toggling between HUMAN and AUTONOMOUS."""
    # Test setting to HUMAN
    mode = set_execution_mode("HUMAN")
    assert mode == "HUMAN"
    assert get_execution_mode() == "HUMAN"

    # Test setting to AUTONOMOUS
    mode = set_execution_mode("AUTONOMOUS")
    assert mode == "AUTONOMOUS"
    assert get_execution_mode() == "AUTONOMOUS"

    # Reset back to HUMAN for clean testing baseline
    set_execution_mode("HUMAN")
    assert get_execution_mode() == "HUMAN"


def test_adaptive_config_lifecycle():
    """Verify adaptive_config.json loading, modifying, and applying onto StrategyConfig."""
    cfg = load_adaptive_config()
    assert "min_displacement_atr" in cfg
    assert "target_rr" in cfg
    assert "min_sweep_atr" in cfg

    # Test applying onto CFG
    cfg["min_displacement_atr"] = 1.35
    cfg["target_rr"] = 2.5
    save_adaptive_config(cfg)

    applied = apply_adaptive_config_to_strategy(CFG)
    assert CFG.disp_atr_mult == 1.35
    assert CFG.rr == 2.5

    # Run simulated self-tuning with mock trades
    mock_closed = [
        {"trade_id": "T1", "total_pnl_usd": 40.0, "realized_r": 2.0},
        {"trade_id": "T2", "total_pnl_usd": 30.0, "realized_r": 1.5},
        {"trade_id": "T3", "total_pnl_usd": -20.0, "realized_r": -1.0},
    ]
    new_cfg, row = run_adaptive_self_tuning(mock_closed)
    assert new_cfg["total_tunings"] >= 1
    assert row is not None
    assert len(row) == len(SHEET_HEADERS["AI_Self_Tuning_Log"])


@patch("server.send_telegram", return_value=True)
def test_server_control_endpoints(mock_tg):
    """Verify server mode toggle, agents, and adaptive-config REST endpoints."""
    # Test get_mode_endpoint
    res_mode = server.get_mode_endpoint()
    assert "execution_mode" in res_mode

    # Test toggle_mode_endpoint
    initial_mode = res_mode["execution_mode"]
    res_toggle = server.toggle_mode_endpoint()
    assert res_toggle["status"] == "SUCCESS"
    assert res_toggle["execution_mode"] != initial_mode

    # Toggle back
    server.toggle_mode_endpoint()

    # Test get_agents_endpoint
    agents = server.get_agents_endpoint()
    for key in ("scanner", "filter", "sizer", "executor", "logger", "equity"):
        assert key in agents
        assert "name" in agents[key]
        assert "status" in agents[key]

    # Test get_adaptive_config_endpoint
    res_tuner = server.get_adaptive_config_endpoint()
    assert "min_displacement_atr" in res_tuner


@patch("server.send_telegram", return_value=True)
@patch.object(server._client, "price", return_value=65200.0)
@patch("server.DELTA_BROKER.close_position_on_exchange", return_value={"status": "CLOSED_ON_EXCHANGE"})
@patch("server.DELTA_BROKER.cancel_all_orders_for_symbol", return_value={"status": "CLEANED"})
@patch.object(PAPER_BROKER, "can_open_new_trade", return_value=(True, "OK"))
def test_manual_close_early_endpoint(mock_can_open, mock_delta_cancel, mock_delta_close, mock_price, mock_tg):
    """Verify 1-click manual early position closure endpoint."""
    tid = f"TEST_MANUAL_{int(time.time())}"
    PAPER_BROKER.register_pending_setup(
        trade_id=tid,
        symbol="BTCUSDT",
        direction="LONG",
        timeframe="15m",
        entry_price=65000.0,
        stop_loss=64000.0,
        target=67000.0,
        quantity=0.01,
        max_loss_usd=10.0,
        exit_plan={"tp1": {"price": 66000.0}, "tp2": {"price": 67000.0}, "tp3": {"price": 68000.0}},
    )
    # Approve so it becomes an open position
    PAPER_BROKER.approve_trade(tid)
    assert any((p.get("trade_id") or p.get("id")) == tid for p in PAPER_BROKER.get_open_positions())

    # Call manual close endpoint
    close_resp = server.manual_close_trade(tid)
    assert close_resp["status"] == "CLOSED"
    assert not any((p.get("trade_id") or p.get("id")) == tid for p in PAPER_BROKER.get_open_positions())


@patch("trader_bot.send_telegram", return_value=True)
@patch("trader_bot.DELTA_BROKER.close_position_on_exchange", return_value={"status": "CLOSED_ON_EXCHANGE"})
@patch("trader_bot.DELTA_BROKER.cancel_all_orders_for_symbol", return_value={"status": "CLEANED"})
@patch.object(PAPER_BROKER, "can_open_new_trade", return_value=(True, "OK"))
def test_autonomous_dynamic_exit_mss(mock_can_open, mock_delta_cancel, mock_delta_close, mock_tg):
    """Verify AI dynamic exit triggers early exit on opposing MSS."""
    tid = f"TEST_MSS_{int(time.time())}"
    PAPER_BROKER.register_pending_setup(
        trade_id=tid,
        symbol="ETHUSDT",
        direction="LONG",
        timeframe="15m",
        entry_price=3000.0,
        stop_loss=2900.0,
        target=3200.0,
        quantity=0.1,
        max_loss_usd=10.0,
        exit_plan={"tp1": {"price": 3100.0}, "tp2": {"price": 3200.0}, "tp3": {"price": 3300.0}},
    )
    PAPER_BROKER.approve_trade(tid)
    open_pos = next(p for p in PAPER_BROKER.get_open_positions() if (p.get("trade_id") or p.get("id")) == tid)

    # Mock BinanceClient returning candles that broke recent structure low
    mock_client = MagicMock()
    import pandas as pd
    df_data = {
        "timestamp": [i for i in range(15)],
        "open": [2990.0] * 15,
        "high": [3010.0] * 15,
        "low": [2980.0] * 14 + [2950.0],  # Latest candle drops below swing low
        "close": [2995.0] * 14 + [2960.0], # Closes below swing low (2980.0) and below entry (3000.0)
        "volume": [100.0] * 15,
    }
    mock_df = pd.DataFrame(df_data)
    mock_client.klines.return_value = mock_df

    mock_state = {"execution_mode": "AUTONOMOUS", "sheet_queue": []}
    evaluate_autonomous_dynamic_exit(open_pos, mock_client, mock_state)

    # Position should have closed early due to opposing MSS
    assert not any((p.get("trade_id") or p.get("id")) == tid for p in PAPER_BROKER.get_open_positions())
    closed = next(p for p in PAPER_BROKER.get_closed_positions() if (p.get("trade_id") or p.get("id")) == tid)
    assert "AI MSS Panic Exit" in closed["exit_reason"]


def test_sheet_headers_specification():
    """Verify all 4 dedicated Google Sheets tabs have complete headers defined."""
    assert "Live_Positions" in SHEET_HEADERS
    assert "Trade_Journal" in SHEET_HEADERS
    assert "AI_Self_Tuning_Log" in SHEET_HEADERS
    assert "Daily_Summary" in SHEET_HEADERS


def test_cross_process_state_synchronization():
    """Verify that mode toggled externally is NOT overwritten when bot loop calls save_state."""
    # Step 1: Set mode to AUTONOMOUS
    set_execution_mode("AUTONOMOUS")
    assert get_execution_mode() == "AUTONOMOUS"

    # Step 2: Simulate trader_bot.py having an older in-memory state dict with "HUMAN"
    stale_state = load_state()
    stale_state["execution_mode"] = "HUMAN"
    # Calling save_state should refresh execution_mode from the single source of truth
    save_state(stale_state)

    # Step 3: Verify execution_mode remained AUTONOMOUS
    assert get_execution_mode() == "AUTONOMOUS"

    # Reset
    set_execution_mode("HUMAN")
    assert get_execution_mode() == "HUMAN"


def test_delta_close_position_on_exchange():
    """Verify Delta Exchange flatten mechanism submits reduce_only market orders."""
    from engine_delta_broker import DELTA_BROKER
    with patch.object(DELTA_BROKER, "enabled", True), \
         patch.object(DELTA_BROKER, "api_key", "test_key"), \
         patch.object(DELTA_BROKER, "api_secret", "test_secret"), \
         patch.object(DELTA_BROKER, "cancel_all_orders_for_symbol", return_value={"status": "CLEANED"}), \
         patch.object(DELTA_BROKER, "get_product_id", return_value=27), \
         patch.object(DELTA_BROKER, "get_open_positions_from_exchange", return_value=[{"product_id": 27, "size": 5}]), \
         patch.object(DELTA_BROKER, "_request", return_value={"success": True}) as mock_req:

        res = DELTA_BROKER.close_position_on_exchange("BTCUSDT")
        assert res["status"] == "CLOSED_ON_EXCHANGE"
        assert res["side"] == "sell"
        assert res["size"] == 5
        # Verify reduce_only was set in payload
        call_args = mock_req.call_args
        assert call_args[1]["payload"]["reduce_only"] is True


def test_closed_positions_normalization():
    """Verify get_closed_positions returns dicts with both id and trade_id."""
    closed = PAPER_BROKER.get_closed_positions()
    for p in closed:
        assert "id" in p
        assert "trade_id" in p
        assert p["id"] == p["trade_id"]


if __name__ == "__main__":
    print("Running test_dual_mode_toggle_state()...")
    test_dual_mode_toggle_state()
    print("PASS: test_dual_mode_toggle_state")

    print("Running test_adaptive_config_lifecycle()...")
    test_adaptive_config_lifecycle()
    print("PASS: test_adaptive_config_lifecycle")

    print("Running test_server_control_endpoints()...")
    test_server_control_endpoints()
    print("PASS: test_server_control_endpoints")

    print("Running test_manual_close_early_endpoint()...")
    test_manual_close_early_endpoint()
    print("PASS: test_manual_close_early_endpoint")

    print("Running test_autonomous_dynamic_exit_mss()...")
    test_autonomous_dynamic_exit_mss()
    print("PASS: test_autonomous_dynamic_exit_mss")

    print("Running test_sheet_headers_specification()...")
    test_sheet_headers_specification()
    print("PASS: test_sheet_headers_specification")

    print("Running test_cross_process_state_synchronization()...")
    test_cross_process_state_synchronization()
    print("PASS: test_cross_process_state_synchronization")

    print("Running test_delta_close_position_on_exchange()...")
    test_delta_close_position_on_exchange()
    print("PASS: test_delta_close_position_on_exchange")

    print("Running test_closed_positions_normalization()...")
    test_closed_positions_normalization()
    print("PASS: test_closed_positions_normalization")

    print("\nALL DUAL-MODE & AUTONOMOUS TESTS PASSED SUCCESSFULLY!")
