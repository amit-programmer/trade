#!/usr/bin/env python3
"""
stop_all.py - Emergency Process Cleaner & Telegram Connection Reset.

Terminates all running instances of trader_bot.py, server.py, and run_all.py.
Cleans up lock files (.trader_bot.pid) and releases the Telegram Bot API polling lock
to instantly fix HTTP 409 Conflict errors.
"""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PID_FILE = BASE_DIR / ".trader_bot.pid"
ENV_FILE = BASE_DIR / ".env"

print("\n" + "=" * 65)
print("🛑 [CLEANER] Scanning and terminating stale trading processes...")
print("=" * 65)

current_pid = os.getpid()
killed_count = 0

# 1. Check PID lock file
if PID_FILE.exists():
    try:
        pid_str = PID_FILE.read_text(encoding="utf-8").strip()
        if pid_str.isdigit():
            target_pid = int(pid_str)
            if target_pid != current_pid:
                print(f"👉 Terminating registered trader_bot PID: {target_pid}...")
                if sys.platform == "win32":
                    subprocess.run(["taskkill", "/F", "/PID", str(target_pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                else:
                    try:
                        os.kill(target_pid, signal.SIGKILL)
                    except Exception:
                        pass
                killed_count += 1
    except Exception as exc:
        print(f"Warning reading PID file: {exc}")
    finally:
        PID_FILE.unlink(missing_ok=True)

# 2. On Windows, find and terminate any python processes running trader_bot.py or run_all.py
if sys.platform == "win32":
    try:
        ps_cmd = (
            f"Get-CimInstance Win32_Process | "
            f"Where-Object {{ ($_.CommandLine -like '*trader_bot.py*' -or $_.CommandLine -like '*run_all.py*') -and $_.ProcessId -ne {current_pid} }} | "
            f"ForEach-Object {{ Write-Output $_.ProcessId; Stop-Process -Id $_.ProcessId -Force }}"
        )
        res = subprocess.run(["powershell", "-NoProfile", "-Command", ps_cmd], capture_output=True, text=True)
        pids = [p.strip() for p in res.stdout.splitlines() if p.strip().isdigit()]
        for p in pids:
            print(f"👉 Terminated background process PID {p}")
            killed_count += 1
    except Exception as exc:
        print(f"Process scan note: {exc}")
else:
    # Linux / Mac
    try:
        subprocess.run(["pkill", "-9", "-f", "trader_bot.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["pkill", "-9", "-f", "run_all.py"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass

# 3. Read TELEGRAM_TOKEN from .env and reset webhook on Telegram
token = ""
if ENV_FILE.exists():
    try:
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("TELEGRAM_TOKEN="):
                token = line.split("=", 1)[1].strip().strip('"').strip("'")
                break
    except Exception:
        pass

if not token:
    token = os.getenv("TELEGRAM_TOKEN", "")

if token:
    try:
        import requests
        print("🌐 Resetting Telegram Bot API polling state (deleteWebhook)...")
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/deleteWebhook",
            json={"drop_pending_updates": False},
            timeout=8,
        )
        if resp.status_code == 200:
            print("✅ Telegram webhook cleared and connection lock reset successfully!")
        else:
            print(f"⚠️ Telegram API response: HTTP {resp.status_code}")
    except Exception as exc:
        print(f"⚠️ Telegram reset request failed: {exc}")

print("\n" + "=" * 65)
print("🎉 SYSTEM CLEAN: All background bot instances stopped.")
print("You can now start freshly with: python run_all.py  or  python server.py --all")
print("=" * 65 + "\n")
