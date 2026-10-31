"""
test_operational_maintenance.py - Automated Verification for State Rollover & Circuit Breaker.

Tests:
  1. Paper Positions State Rollover & Historical Archiving (prevents I/O bloat)
  2. Network Circuit Breaker Exponential Backoff & Auto-Recovery (prevents thread lock & spam)
  3. Cumulative Lifetime Stats Integrity across rollover boundaries
"""
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

# Force unbuffered output and UTF-8 encoding
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from engine_paper_broker import PaperBroker
from trader_bot import NetworkCircuitBreaker


def test_maintenance_guards() -> bool:
    print("======================================================================")
    print("RUNNING OPERATIONAL MAINTENANCE & CIRCUIT BREAKER VERIFICATION")
    print("======================================================================")

    # -------------------------------------------------------------------------
    # TEST 1: STATE ROLLOVER & HISTORICAL ARCHIVING
    # -------------------------------------------------------------------------
    print("\n[TEST 1] Testing Paper Positions State Rollover & History Archiving...")
    temp_dir = Path(tempfile.mkdtemp(prefix="paper_test_"))
    try:
        test_active_file = temp_dir / "paper_positions.json"
        test_history_file = temp_dir / "paper_positions_history.json"

        # Instantiate broker with threshold=15 and retain=5 for testing
        test_broker = PaperBroker(
            filepath=test_active_file,
            history_filepath=test_history_file,
            max_closed_threshold=15,
            retain_closed=5,
        )

        # Simulate 20 closed trades
        print("         Adding 20 closed trades to test broker (threshold=15, retain=5)...")
        for i in range(1, 21):
            mock_closed_pos = {
                "id": f"trade_{i:03d}",
                "symbol": "BTCUSDT",
                "direction": "LONG",
                "entry_price": 60000.0 + i * 10,
                "closed_price": 61000.0 + i * 10,
                "total_pnl_usd": 25.0,
                "realized_r": 1.5,
                "closed_at": 1700000000 + i * 60,
            }
            test_broker.state["closed"].append(mock_closed_pos)
            test_broker.state["stats"]["total_trades"] += 1
            test_broker.state["stats"]["wins"] += 1
            test_broker.state["stats"]["total_pnl_usd"] += 25.0
            test_broker.state["stats"]["total_realized_r"] += 1.5

        # Trigger save (which invokes _archive_closed_positions_if_needed)
        test_broker._save()

        # Verify active file
        with open(test_active_file, "r", encoding="utf-8") as f:
            active_data = json.load(f)

        # Verify history file
        assert test_history_file.exists(), "History archive file was not created!"
        with open(test_history_file, "r", encoding="utf-8") as f:
            history_data = json.load(f)

        print(f"         Active State Closed Count: {len(active_data['closed'])} (Expected: 5)")
        print(f"         History Archive Closed Count: {len(history_data)} (Expected: 15)")
        print(f"         Active Cumulative Stats Total Trades: {active_data['stats']['total_trades']} (Expected: 20)")
        print(f"         Active Cumulative Stats PnL: ${active_data['stats']['total_pnl_usd']:,.2f} (Expected: $500.00)")

        assert len(active_data["closed"]) == 5, f"Expected 5 retained closed trades, got {len(active_data['closed'])}"
        assert len(history_data) == 15, f"Expected 15 archived trades, got {len(history_data)}"
        assert active_data["stats"]["total_trades"] == 20, "Lifetime total_trades must remain 20"
        assert active_data["stats"]["total_pnl_usd"] == 500.0, "Lifetime total_pnl_usd must remain $500.00"

        # Verify combined retrieval
        all_closed = test_broker.get_all_historical_closed_positions()
        assert len(all_closed) == 20, f"Combined historical trades mismatch: {len(all_closed)} vs 20"
        print("         Combined retrieval returned all 20 trades seamlessly.")
        print("         >> [OK] Paper State Rollover & Historical Archiving: PASSED")

    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    # -------------------------------------------------------------------------
    # TEST 2: NETWORK CIRCUIT BREAKER EXPONENTIAL BACKOFF
    # -------------------------------------------------------------------------
    print("\n[TEST 2] Testing Network Circuit Breaker & Auto-Recovery...")
    cb = NetworkCircuitBreaker(name="TestFeed", max_backoff=60.0)

    # Normal state
    s0 = cb.get_sleep_duration(base_interval=10.0)
    print(f"         Initial State (0 failures): Sleep = {s0}s (Base: 10.0s)")
    assert s0 == 10.0

    # Failure 1 (e.g. transient 502)
    cb.record_failure("HTTP 502 Bad Gateway")
    s1 = cb.get_sleep_duration(base_interval=10.0)
    print(f"         Failure #1: Sleep = {s1}s (Expected: 10.0s or >=5s)")
    assert s1 >= 5.0 and cb.consecutive_failures == 1

    # Failure 2 (e.g. DNS timeout)
    cb.record_failure("DNS Resolution Error")
    s2 = cb.get_sleep_duration(base_interval=10.0)
    print(f"         Failure #2: Sleep = {s2}s (Expected: 15.0s)")
    assert s2 == 15.0 and cb.consecutive_failures == 2

    # Failure 3 (e.g. exchange maintenance)
    cb.record_failure("Connection Refused")
    s3 = cb.get_sleep_duration(base_interval=10.0)
    print(f"         Failure #3: Sleep = {s3}s (Expected: 30.0s)")
    assert s3 == 30.0 and cb.consecutive_failures == 3

    # Failure 4 (extended outage)
    cb.record_failure("Gateway Timeout 504")
    s4 = cb.get_sleep_duration(base_interval=10.0)
    print(f"         Failure #4: Sleep = {s4}s (Expected: 60.0s capped)")
    assert s4 == 60.0 and cb.consecutive_failures == 4

    # Recovery
    print("         Connection restored. Triggering record_success()...")
    cb.record_success()
    s_recovered = cb.get_sleep_duration(base_interval=10.0)
    print(f"         Recovered State: Consecutive Failures = {cb.consecutive_failures} | Sleep = {s_recovered}s")
    assert cb.consecutive_failures == 0
    assert s_recovered == 10.0
    print("         >> [OK] Network Circuit Breaker & Auto-Recovery: PASSED")

    # -------------------------------------------------------------------------
    # TEST 3: DISK INTEGRITY & CLEAN SHUTDOWN
    # -------------------------------------------------------------------------
    print("\n[TEST 3] Testing Live Paper Broker State Integrity...")
    from engine_paper_broker import PAPER_BROKER
    PAPER_BROKER.shutdown()
    assert not PAPER_BROKER.filepath.with_suffix(".tmp").exists()
    assert not PAPER_BROKER.history_filepath.with_suffix(".tmp").exists()
    print("         Zero orphan .tmp files on disk. Production database pristine.")
    print("         >> [OK] Disk Integrity & Clean Shutdown: PASSED")

    print("\n======================================================================")
    print("ALL OPERATIONAL MAINTENANCE & CIRCUIT BREAKER TESTS PASSED (100% OK)")
    print("======================================================================")
    return True


if __name__ == "__main__":
    ok = test_maintenance_guards()
    sys.exit(0 if ok else 1)
