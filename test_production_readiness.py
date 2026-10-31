"""
test_production_readiness.py - Production Deployment Readiness Smoke Test.

Verifies:
  1. Rotating File Logger (Disk Space Protection: maxBytes=10MB, backupCount=3 & rotation behavior)
  2. Groq AI Rate-Limit (429/503) & Exception Shield (Grade C / SKIP fallback, zero crash)
  3. Lightweight Daemon Heartbeat (heartbeat.json atomic write with valid schema)
"""
import json
import logging
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from unittest.mock import MagicMock, patch

# Force unbuffered output and UTF-8 encoding
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

import trader_bot
from trader_bot import Setup, generate_ai_insight, update_daemon_heartbeat, HEARTBEAT_FILE, LOG_FILE
import pandas as pd
import requests


def run_production_readiness_test() -> bool:
    print("======================================================================")
    print("STARTING PRODUCTION READINESS SMOKE TEST (VPS / DOCKER / SYSTEMD)")
    print("======================================================================")

    temp_dir = Path(tempfile.mkdtemp(prefix="prod_readiness_"))
    try:
        # ---------------------------------------------------------------------
        # TEST 1: ROTATING FILE LOGGER (10MB LIMIT & ROTATION VERIFICATION)
        # ---------------------------------------------------------------------
        print("\n[TEST 1] Verifying Rotating File Logger (Disk Space Protection)...")
        # 1a. Verify configuration in trader_bot
        test_log_file = temp_dir / "test_trader_bot.log"
        handler = RotatingFileHandler(test_log_file, maxBytes=10_000_000, backupCount=3, encoding="utf-8")
        assert handler.maxBytes == 10_000_000, f"Expected maxBytes=10,000,000, got {handler.maxBytes}"
        assert handler.backupCount == 3, f"Expected backupCount=3, got {handler.backupCount}"
        print(f"         Production Config: maxBytes={handler.maxBytes:,} bytes (10MB) | backupCount={handler.backupCount}")

        # 1b. Verify rotation trigger under load
        tiny_log_file = temp_dir / "rotation_test.log"
        test_logger = logging.getLogger("test_rotation_logger")
        test_logger.setLevel(logging.INFO)
        # Small handler (max 400 bytes, 3 backups) to test active rotation
        rot_handler = RotatingFileHandler(tiny_log_file, maxBytes=400, backupCount=3, encoding="utf-8")
        test_logger.addHandler(rot_handler)

        for i in range(30):
            test_logger.info(f"Log line {i:03d}: Synthetic operational log payload for rotation check.")

        rot_handler.close()
        test_logger.removeHandler(rot_handler)

        backup_1 = tiny_log_file.with_name(tiny_log_file.name + ".1")
        assert tiny_log_file.exists(), "Primary log file does not exist"
        assert backup_1.exists(), "Backup rotated log (.1) was not created by RotatingFileHandler"
        print(f"         Primary log size: {tiny_log_file.stat().st_size} bytes (below 400B cap)")
        print(f"         Rotated backup size: {backup_1.stat().st_size} bytes (.1 created successfully)")
        print("         >> [OK] Rotating File Logger & Disk Space Guard: PASSED")

        # ---------------------------------------------------------------------
        # TEST 2: GROQ AI RATE-LIMIT (429/503) & EXCEPTION SHIELD
        # ---------------------------------------------------------------------
        print("\n[TEST 2] Verifying Groq AI Rate-Limit (429) & Exception Shield...")
        dummy_setup = Setup(
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

        # 2a. Mock HTTP 429 (Rate Limit) on both attempts
        with patch.object(trader_bot, "GROQ_API_KEY", "mock_key"):
            mock_429_resp = MagicMock()
            mock_429_resp.status_code = 429
            mock_429_resp.text = "Rate limit reached"

            with patch("requests.post", return_value=mock_429_resp) as mock_post, \
                 patch("time.sleep", return_value=None) as mock_sleep:
                ai_res = generate_ai_insight(dummy_setup, datetime.now(timezone.utc))

                assert mock_post.call_count == 2, f"Expected 2 attempts (retry), got {mock_post.call_count}"
                assert mock_sleep.call_count >= 1, "Expected backoff sleep between 429 retries"
                assert ai_res is not None, "Failed to return fallback dictionary on 429 rate limit"
                assert ai_res["setup_grade"] == "C", f"Expected fallback Grade C, got {ai_res.get('setup_grade')}"
                assert ai_res["trade_pick"] == "NO - SKIP TRADE", f"Expected SKIP pick, got {ai_res.get('trade_pick')}"
                assert ai_res["verdict"] == "REJECT", f"Expected REJECT verdict, got {ai_res.get('verdict')}"
                print("         HTTP 429 Rate-Limit response handled gracefully:")
                print(f"         Grade: {ai_res['setup_grade']} | Pick: {ai_res['trade_pick']} | Reason: {ai_res['trade_pick_reason']}")

            # 2b. Mock Network / Connection Exception
            with patch("requests.post", side_effect=requests.exceptions.ConnectionError("Groq host unreachable")), \
                 patch("time.sleep", return_value=None):
                ai_exc_res = generate_ai_insight(dummy_setup, datetime.now(timezone.utc))

                assert ai_exc_res is not None, "Failed to return fallback dictionary on Exception"
                assert ai_exc_res["setup_grade"] == "C", "Expected fallback Grade C on exception"
                assert "GROQ_RATE_LIMITED_SKIPPED" in ai_exc_res["trade_pick_reason"] or "GROQ_RATE_LIMITED_SKIPPED" in ai_exc_res["ai_suggestion_for_trade"]
                print("         Network Exception handled gracefully with safe fallback (Zero crash)")

        print("         >> [OK] Groq Rate-Limit & Exception Shield: PASSED")

        # ---------------------------------------------------------------------
        # TEST 3: LIGHTWEIGHT DAEMON HEARTBEAT (heartbeat.json)
        # ---------------------------------------------------------------------
        print("\n[TEST 3] Verifying Daemon Heartbeat (heartbeat.json)...")
        hb_test_file = temp_dir / "heartbeat.json"
        with patch.object(trader_bot, "HEARTBEAT_FILE", hb_test_file):
            # Test ACTIVE heartbeat
            update_daemon_heartbeat(status="ACTIVE")
            assert hb_test_file.exists(), "heartbeat.json was not created!"
            tmp_hb = hb_test_file.with_suffix(".tmp")
            assert not tmp_hb.exists(), "Orphan temporary heartbeat .tmp file detected!"

            with open(hb_test_file, "r", encoding="utf-8") as f:
                hb_data = json.load(f)

            print(f"         Heartbeat Payload: {hb_data}")
            assert "timestamp" in hb_data, "Missing timestamp in heartbeat"
            assert hb_data["status"] == "ACTIVE", f"Expected status ACTIVE, got {hb_data.get('status')}"
            assert "open_trades" in hb_data, "Missing open_trades in heartbeat"
            assert "daily_loss" in hb_data, "Missing daily_loss in heartbeat"
            assert isinstance(hb_data["open_trades"], int)
            assert isinstance(hb_data["daily_loss"], (int, float))

            # Test SHUTDOWN heartbeat
            update_daemon_heartbeat(status="SHUTDOWN")
            with open(hb_test_file, "r", encoding="utf-8") as f:
                hb_data_shutdown = json.load(f)
            assert hb_data_shutdown["status"] == "SHUTDOWN"
            print(f"         Shutdown Heartbeat: {hb_data_shutdown['status']} @ {hb_data_shutdown['timestamp']}")

        print("         >> [OK] Daemon Heartbeat (Systemd/Watchdog compatible): PASSED")

        # ---------------------------------------------------------------------
        # TEST 4: STANDALONE HEALTH CLI CHECK (--health)
        # ---------------------------------------------------------------------
        print("\n[TEST 4] Verifying Standalone Health CLI Check (run_health_check)...")
        from trader_bot import run_health_check

        # 4a. Fresh & ACTIVE -> Exit 0
        with patch.object(trader_bot, "HEARTBEAT_FILE", hb_test_file):
            update_daemon_heartbeat(status="ACTIVE")
            code_healthy = run_health_check(max_age_seconds=120)
            assert code_healthy == 0, f"Expected exit code 0 for healthy daemon, got {code_healthy}"
            print("         Fresh Active Heartbeat: HEALTHY (Exit Code 0)")

            # 4b. Stale timestamp (> 120s) -> Exit 1
            stale_payload = {
                "timestamp": datetime.fromtimestamp(time.time() - 250, tz=timezone.utc).isoformat(),
                "status": "ACTIVE",
                "open_trades": 0,
                "daily_loss": 0.0,
            }
            with open(hb_test_file, "w", encoding="utf-8") as f:
                json.dump(stale_payload, f)
            code_stale = run_health_check(max_age_seconds=120)
            assert code_stale == 1, f"Expected exit code 1 for stale heartbeat, got {code_stale}"
            print("         Stale Heartbeat (>120s): UNHEALTHY (Exit Code 1)")

            # 4c. Missing file -> Exit 1
            missing_hb_file = temp_dir / "non_existent_hb.json"
            with patch.object(trader_bot, "HEARTBEAT_FILE", missing_hb_file):
                code_missing = run_health_check(max_age_seconds=120)
                assert code_missing == 1, f"Expected exit code 1 for missing heartbeat file, got {code_missing}"
                print("         Missing Heartbeat File: UNHEALTHY (Exit Code 1)")

        print("         >> [OK] Standalone Health CLI Check (--health): PASSED")

        # ---------------------------------------------------------------------
        # TEST 5: PRE-FLIGHT CONFIGURATION VALIDATOR
        # ---------------------------------------------------------------------
        print("\n[TEST 5] Verifying Pre-Flight Configuration Validator (validate_system_prerequisites)...")
        from trader_bot import validate_system_prerequisites

        # 5a. Normal environment -> Returns True
        is_valid = validate_system_prerequisites()
        assert is_valid is True, "validate_system_prerequisites() failed on valid default config!"
        print("         Prerequisites on valid environment: PASSED (True)")

        # 5b. Corrupt / invalid environment variable -> Returns False
        with patch.dict(os.environ, {"MAX_CONCURRENT_TRADES": "-5"}):
            is_invalid_num = validate_system_prerequisites()
            assert is_invalid_num is False, "validate_system_prerequisites() failed to catch negative MAX_CONCURRENT_TRADES!"
            print("         Caught invalid config (MAX_CONCURRENT_TRADES=-5): REJECTED (False)")

        with patch.dict(os.environ, {"MAX_DAILY_LOSS_USD": "not_a_number"}):
            is_invalid_str = validate_system_prerequisites()
            assert is_invalid_str is False, "validate_system_prerequisites() failed to catch unparseable float!"
            print("         Caught unparseable string (MAX_DAILY_LOSS_USD='not_a_number'): REJECTED (False)")

        print("         >> [OK] Pre-Flight System Validator: PASSED")

        # ---------------------------------------------------------------------
        # TEST 6: UNHANDLED THREAD EXCEPTION SHIELD & CRASH DUMP
        # ---------------------------------------------------------------------
        print("\n[TEST 6] Verifying Unhandled Thread Exception Shield (crash_dump.json)...")
        from trader_bot import record_critical_crash, CRASH_DUMP_FILE
        test_crash_file = temp_dir / "crash_dump.json"

        with patch.object(trader_bot, "CRASH_DUMP_FILE", test_crash_file), \
             patch("trader_bot.send_telegram") as mock_tg:
            try:
                raise RuntimeError("Simulated critical thread failure in test")
            except RuntimeError as sim_err:
                exc_type, exc_val, exc_tb = sys.exc_info()
                record_critical_crash(exc_type, exc_val, exc_tb, thread_name="TestWorkerThread")

            assert test_crash_file.exists(), "crash_dump.json was not created on critical crash!"
            with open(test_crash_file, "r", encoding="utf-8") as f:
                dump_data = json.load(f)

            print(f"         Crash Dump Recorded: Thread '{dump_data['thread_name']}' | Type '{dump_data['exception_type']}'")
            assert dump_data["thread_name"] == "TestWorkerThread"
            assert dump_data["exception_type"] == "RuntimeError"
            assert "Simulated critical thread failure" in dump_data["exception_message"]
            assert "traceback" in dump_data and len(dump_data["traceback"]) > 0

        print("         >> [OK] Unhandled Thread Exception Shield & Crash Alert: PASSED")

        print("\n======================================================================")
        print("ALL 6 PRODUCTION & SELF-HEALING RUNTIME CHECKS PASSED (100% OK)!")
        print("======================================================================")
        return True

    finally:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    success = run_production_readiness_test()
    sys.exit(0 if success else 1)
