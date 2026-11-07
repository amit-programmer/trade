#!/usr/bin/env python3
"""
run_all.py - Unified Master Orchestrator for 24/7 SMC Crypto Trading System.

Spawns and manages both core services simultaneously:
1. FastAPI Web Dashboard & REST Gateway (server.py -> Port 8000)
2. 24/7 SMC Scanner & Quantitative Execution Engine (trader_bot.py)

Features:
- Single command startup: python run_all.py
- Real-time multiplexed stdout/stderr streams with [SERVER] and [BOT] prefixes
- Graceful shutdown of both child processes on Ctrl+C (SIGINT / SIGTERM)
- Automatic port cleanup and zombie process prevention
"""
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SERVER_SCRIPT = BASE_DIR / "server.py"
BOT_SCRIPT = BASE_DIR / "trader_bot.py"

CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"

# Windows cmd/powershell ANSI color support
if sys.platform == "win32":
    os.system("")

stop_requested = threading.Event()


def stream_output(pipe, prefix: str, color: str):
    """Reads lines from a subprocess pipe and prints them with prefix tag."""
    try:
        for line in iter(pipe.readline, ""):
            if not line:
                break
            clean_line = line.rstrip("\r\n")
            if clean_line:
                print(f"{color}{prefix}{RESET} {clean_line}", flush=True)
    except Exception:
        pass
    finally:
        try:
            pipe.close()
        except Exception:
            pass


def print_banner():
    banner = f"""
{BOLD}{CYAN}======================================================================{RESET}
{BOLD}{GREEN}       🤖 24/7 SMC TRADING SYSTEM - UNIFIED MASTER RUNNER 🚀{RESET}
{BOLD}{CYAN}======================================================================{RESET}
  {BOLD}• Web Dashboard:{RESET}     {CYAN}http://localhost:8000{RESET}
  {BOLD}• REST API & Docs:{RESET}   {CYAN}http://localhost:8000/docs{RESET}
  {BOLD}• Scanner Engine:{RESET}    {GREEN}trader_bot.py (24/7 SMC + Groq AI){RESET}
  {BOLD}• Mode Support:{RESET}      {YELLOW}Human-in-the-Loop & Autonomous AI{RESET}
  {BOLD}• Exit / Stop:{RESET}       {RED}Press Ctrl + C to stop all services{RESET}
{BOLD}{CYAN}======================================================================{RESET}
"""
    print(banner, flush=True)


def main():
    print_banner()

    py_exe = sys.executable
    processes = []

    # Start 1: FastAPI Dashboard Server
    server_cmd = [py_exe, str(SERVER_SCRIPT)]
    p_server = subprocess.Popen(
        server_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    processes.append(("SERVER", p_server))

    t_server = threading.Thread(
        target=stream_output,
        args=(p_server.stdout, "[SERVER]", CYAN),
        daemon=True,
    )
    t_server.start()

    # Give server a moment to bind port 8000
    time.sleep(1.0)

    # Start 2: Trader Bot Daemon
    bot_cmd = [py_exe, str(BOT_SCRIPT)]
    p_bot = subprocess.Popen(
        bot_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    processes.append(("BOT", p_bot))

    t_bot = threading.Thread(
        target=stream_output,
        args=(p_bot.stdout, "[BOT]   ", GREEN),
        daemon=True,
    )
    t_bot.start()

    def shutdown(signum=None, frame=None):
        if stop_requested.is_set():
            return
        stop_requested.set()
        print(f"\n{YELLOW}🛑 Stopping all services (server + trader_bot)...{RESET}", flush=True)

        for name, proc in processes:
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception:
                    pass

        # Give 3 seconds for graceful shutdown, then kill if stubborn
        start_wait = time.time()
        for name, proc in processes:
            while proc.poll() is None and (time.time() - start_wait) < 3.0:
                time.sleep(0.1)
            if proc.poll() is None:
                try:
                    proc.kill()
                except Exception:
                    pass
                print(f"{RED}⚠️  Force-killed {name} process.{RESET}", flush=True)
            else:
                print(f"{GREEN}✅ {name} stopped cleanly.{RESET}", flush=True)

        print(f"{BOLD}All services shut down. Goodbye! 👋{RESET}\n", flush=True)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, shutdown)

    # Supervise child processes
    try:
        while not stop_requested.is_set():
            time.sleep(0.5)
            # Check if either process died unexpectedly
            if p_server.poll() is not None:
                print(f"\n{RED}❌ Server process stopped unexpectedly (exit code {p_server.returncode}){RESET}")
                shutdown()
                break
            if p_bot.poll() is not None:
                print(f"\n{RED}❌ Trader Bot process stopped unexpectedly (exit code {p_bot.returncode}){RESET}")
                shutdown()
                break
    except KeyboardInterrupt:
        shutdown()


if __name__ == "__main__":
    main()

