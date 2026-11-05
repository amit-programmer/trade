"""
test_sanity_guards.py - Standalone Verification for:
1. WebSocket Connection Lifecycle & Dead Client Cleanup (ConnectionManager)
2. Adaptive Tuner Parameter Clamping & Hard Bounds Guard (engine_adaptive_tuner)
"""
import asyncio
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
os.chdir(str(BASE_DIR))

from unittest.mock import AsyncMock, MagicMock
from engine_adaptive_tuner import (
    PARAM_BOUNDS,
    clamp_parameter,
    load_adaptive_config,
    save_adaptive_config,
    run_adaptive_self_tuning,
)
from server import ConnectionManager


def test_adaptive_tuner_hard_bounds_clamping():
    """Verify that parameters are strictly clamped within safe institutional ranges."""
    # 1. min_displacement_atr bounds: [1.0, 2.5]
    assert clamp_parameter("min_displacement_atr", 0.5, 1.2) == 1.0
    assert clamp_parameter("min_displacement_atr", 3.2, 1.2) == 2.5
    assert clamp_parameter("min_displacement_atr", 1.8, 1.2) == 1.8

    # 2. min_risk_reward bounds: [1.5, 4.0]
    assert clamp_parameter("min_risk_reward", 0.8, 2.0) == 1.5
    assert clamp_parameter("min_risk_reward", 5.5, 2.0) == 4.0
    assert clamp_parameter("min_risk_reward", 2.5, 2.0) == 2.5

    # 3. min_confidence_score bounds: [60, 90]
    assert clamp_parameter("min_confidence_score", 45, 75) == 60.0
    assert clamp_parameter("min_confidence_score", 98, 75) == 90.0
    assert clamp_parameter("min_confidence_score", 82, 75) == 82.0

    # 4. Test run_adaptive_self_tuning with extreme losses (should not drop below min bounds)
    extreme_losses = [
        {"trade_id": f"L{i}", "realized_pnl_usd": -20.0, "realized_r": -1.0, "exit_reason": "STOP_LOSS"}
        for i in range(10)
    ]
    cfg, _ = run_adaptive_self_tuning(extreme_losses)
    assert 1.0 <= cfg["min_displacement_atr"] <= 2.5
    assert 1.5 <= cfg["target_rr"] <= 4.0
    assert 1.5 <= cfg["min_risk_reward"] <= 4.0
    assert 60 <= cfg["min_confidence_score"] <= 90

    # 5. Test load_adaptive_config clamps out-of-bounds disk values
    save_adaptive_config({
        "min_displacement_atr": 99.9,
        "target_rr": -5.0,
        "min_confidence_score": 1000,
    })
    loaded = load_adaptive_config()
    assert loaded["min_displacement_atr"] == 2.5
    assert loaded["target_rr"] == 1.5
    assert loaded["min_confidence_score"] == 90
    print("[OK] Adaptive Tuner Hard Bounds Clamping Verified!")


def run_coro(coro):
    """Drives a lightweight coroutine to completion without OS event loop overhead."""
    try:
        while True:
            coro.send(None)
    except StopIteration as e:
        return e.value


def test_websocket_dead_client_cleanup():
    """Verify dead/dropped WebSocket connections are silently pruned without leaks."""
    print("  [Step 1] Initializing ConnectionManager...", flush=True)
    mgr = ConnectionManager()

    class MockWS:
        def __init__(self, name="ws"):
            self.name = name
            self.accepted = False
            self.sent = []
            self.fail = False
            self.client_state = "CONNECTED"

        async def accept(self):
            self.accepted = True

        async def send_json(self, msg):
            if self.fail:
                raise ConnectionResetError("Connection lost")
            self.sent.append(msg)

    ws1 = MockWS("ws1")
    ws2 = MockWS("ws2")
    ws3 = MockWS("ws3")

    print("  [Step 2] Connecting 3 WebSockets...", flush=True)
    run_coro(mgr.connect(ws1))
    run_coro(mgr.connect(ws2))
    run_coro(mgr.connect(ws3))
    assert len(mgr.active_connections) == 3
    print(f"  [Step 2 OK] Active connections: {len(mgr.active_connections)}", flush=True)

    print("  [Step 3] Manually disconnecting ws1...", flush=True)
    mgr.disconnect(ws1)
    assert len(mgr.active_connections) == 2
    assert ws1 not in mgr.active_connections
    print("  [Step 3 OK] ws1 successfully disconnected.", flush=True)

    print("  [Step 4] Simulating dropped client (ws2) during broadcast...", flush=True)
    ws2.fail = True

    run_coro(mgr.broadcast({"type": "PING"}))
    assert len(mgr.active_connections) == 1
    assert mgr.active_connections[0] is ws3
    print("  [Step 4 OK] Dead connection silently pruned on broadcast error!", flush=True)

    print("  [Step 5] Testing prune_dead_connections()...", flush=True)
    class StaleState:
        name = "DISCONNECTED"
    ws_stale = MockWS("ws_stale")
    ws_stale.client_state = StaleState()
    mgr.active_connections.append(ws_stale)
    assert len(mgr.active_connections) == 2

    pruned = mgr.prune_dead_connections()
    assert pruned == 1
    assert len(mgr.active_connections) == 1
    assert mgr.active_connections[0] is ws3
    print("  [Step 5 OK] prune_dead_connections cleaned stale client!", flush=True)
    print("[OK] WebSocket Dead Client Cleanup Verified!", flush=True)


if __name__ == "__main__":
    print("Running test_adaptive_tuner_hard_bounds_clamping()...", flush=True)
    test_adaptive_tuner_hard_bounds_clamping()
    print("Running test_websocket_dead_client_cleanup()...", flush=True)
    test_websocket_dead_client_cleanup()
    print("\nALL SANITY GUARDS VERIFIED SUCCESSFULLY (100% OK)!", flush=True)
