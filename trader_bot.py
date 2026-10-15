#!/usr/bin/env python3
"""
trader_bot.py - 24/7 Liquidity Sweep + Order Block (MSS) scanner for Binance spot pairs.

What it does
------------
1. Pulls closed OHLCV candles from Binance's PUBLIC REST API (no API key needed).
2. Looks for the setup:  Liquidity Sweep  ->  Displacement / MSS  ->  Order Block retest.
3. Sends an instant Telegram alert when a (new) setup is live.
4. Logs to Google Sheets (via a service account):
      - "Alerts"        : one row per triggered setup (immediately)
      - "Daily Summary" : one row per day (written at day rollover)
      - "Scan Log"      : one row per scan cycle (optional, LOG_EVERY_CYCLE=true)
5. Runs forever. Every network/API call is wrapped so the loop never dies.

Configuration is read from environment variables (or a .env file next to this
script if python-dotenv is installed). See .env.example.

DISCLAIMER: This is a signal/alert tool, not financial advice. Nothing here
guarantees profit. Paper-trade / backtest before risking money.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import requests

# --- optional dependencies ---------------------------------------------------
try:  # .env support (optional)
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

try:
    import gspread
except ImportError:  # Sheets logging will simply be disabled
    gspread = None

try:
    from zoneinfo import ZoneInfo
except ImportError:  # very old Python
    ZoneInfo = None


# =============================================================================
# CONFIGURATION
# =============================================================================
BASE_DIR = Path(__file__).resolve().parent


def env_str(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except ValueError:
        return default


def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except ValueError:
        return default


def env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "y", "on")


SYMBOLS: List[str] = [
    s.strip().upper()
    for s in env_str("SYMBOLS", "ETHUSDT,SOLUSDT,XRPUSDT,PAXGUSDT").split(",")
    if s.strip()
]
TIMEFRAME = env_str("TIMEFRAME", "15m")            # e.g. 5m, 15m, 1h, 4h
SCAN_INTERVAL = env_int("SCAN_INTERVAL_SECONDS", 300)  # seconds between scans
KLINE_LIMIT = env_int("KLINE_LIMIT", 300)
REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT", 15)

TELEGRAM_TOKEN = env_str("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID")
SEND_STARTUP_MESSAGE = env_bool("SEND_STARTUP_MESSAGE", True)

GOOGLE_CREDENTIALS_FILE = env_str("GOOGLE_CREDENTIALS_FILE", str(BASE_DIR / "credentials.json"))
GOOGLE_SHEET_ID = env_str("GOOGLE_SHEET_ID")       # preferred (from the sheet URL)
GOOGLE_SHEET_NAME = env_str("GOOGLE_SHEET_NAME")   # alternative: open by title
LOG_EVERY_CYCLE = env_bool("LOG_EVERY_CYCLE", False)
SHEET_QUEUE_MAX = 2000

STATE_FILE = Path(env_str("STATE_FILE", str(BASE_DIR / "bot_state.json")))
LOG_FILE = Path(env_str("LOG_FILE", str(BASE_DIR / "trader_bot.log")))

# Day boundaries / timestamps use this timezone (IANA name), default UTC.
TZ_NAME = env_str("TIMEZONE", "UTC")
try:
    LOCAL_TZ = ZoneInfo(TZ_NAME) if ZoneInfo else timezone.utc
except Exception:
    LOCAL_TZ = timezone.utc

# Binance endpoints. data-api.binance.vision is Binance's public market-data-only
# mirror and is used as a fallback (helps when api.binance.com returns HTTP 451).
_custom_base = env_str("BINANCE_BASE_URL")
BINANCE_BASES = ([_custom_base] if _custom_base else []) + [
    "https://api.binance.com",
    "https://data-api.binance.vision",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]


@dataclass
class StrategyConfig:
    swing_k: int = 3               # candles each side that define a swing high/low (fractal)
    lookback: int = 50             # N candles back to look for liquidity pools (swings)
    reclaim_candles: int = 2       # sweep candle (or next) must close back inside the level
    min_sweep_atr: float = 0.10    # min wick depth beyond the level, in ATRs (filters noise)
    disp_window: int = 6           # displacement must appear within N candles of the sweep
    disp_atr_mult: float = 1.2     # displacement body >= mult * ATR(14)
    require_mss: bool = True       # displacement must close beyond last opposing swing (MSS)
    ob_back: int = 3               # OB may sit up to N candles before the sweep candle
    ob_use_body: bool = False      # False: OB = full candle range, True: candle body only
    max_setup_age: int = 12        # displacement must be within the last N closed candles
    tap_tolerance_atr: float = 1.0 # if only the last candle tapped the OB, price must still be near
    stop_buffer_atr: float = 0.25  # reference stop = sweep extreme -/+ buffer * ATR
    rr: float = 2.0                # reference take-profit in R multiples


CFG = StrategyConfig(
    swing_k=env_int("SWING_K", 3),
    lookback=env_int("SWING_LOOKBACK", 50),
    reclaim_candles=env_int("RECLAIM_CANDLES", 2),
    min_sweep_atr=env_float("MIN_SWEEP_ATR", 0.10),
    disp_window=env_int("DISP_WINDOW", 6),
    disp_atr_mult=env_float("DISP_ATR_MULT", 1.2),
    require_mss=env_bool("REQUIRE_MSS", True),
    ob_back=env_int("OB_BACK", 3),
    ob_use_body=env_bool("OB_USE_BODY", False),
    max_setup_age=env_int("MAX_SETUP_AGE", 12),
    tap_tolerance_atr=env_float("TAP_TOLERANCE_ATR", 1.0),
    stop_buffer_atr=env_float("STOP_BUFFER_ATR", 0.25),
    rr=env_float("RR", 2.0),
)

# =============================================================================
# LOGGING / SHUTDOWN
# =============================================================================
log = logging.getLogger("trader_bot")
STOP = threading.Event()


def setup_logging() -> None:
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    log.addHandler(console)
    try:
        fileh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3, encoding="utf-8")
        fileh.setFormatter(fmt)
        log.addHandler(fileh)
    except OSError as exc:  # read-only FS etc. - console logging still works
        log.warning("Could not open log file %s: %s", LOG_FILE, exc)


def _handle_signal(signum, _frame) -> None:
    log.info("Signal %s received - shutting down after current step...", signum)
    STOP.set()


def scrub(text: Any) -> str:
    """Remove secrets (Telegram token) from any text before logging it."""
    s = str(text)
    if TELEGRAM_TOKEN:
        s = s.replace(TELEGRAM_TOKEN, "<TOKEN>")
    return s


def fmt_price(p: float) -> str:
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 10:
        return f"{p:.3f}"
    if p >= 1:
        return f"{p:.4f}"
    return f"{p:.6f}"


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


# =============================================================================
# BINANCE PUBLIC API CLIENT
# =============================================================================
class BinanceError(Exception):
    pass


class BinanceClient:
    """Tiny public-endpoint client with retries, rate-limit and base-URL failover."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "trader-bot/1.0"})
        self.idx = 0

    def _get(self, path: str, params: Optional[dict] = None) -> Any:
        last_err: Optional[Exception] = None
        attempts = len(BINANCE_BASES) * 2
        for attempt in range(attempts):
            base = BINANCE_BASES[self.idx % len(BINANCE_BASES)]
            try:
                resp = self.session.get(base + path, params=params, timeout=REQUEST_TIMEOUT)
                if resp.status_code in (429, 418):  # rate limited / temporarily banned
                    try:
                        wait = min(int(resp.headers.get("Retry-After", 30)), 120)
                    except ValueError:
                        wait = 30
                    log.warning("Binance rate limit (%s). Sleeping %ss", resp.status_code, wait)
                    last_err = BinanceError(f"rate limited ({resp.status_code})")
                    if STOP.wait(wait):
                        break
                    continue
                if resp.status_code in (403, 451):  # geo / WAF block -> try another host
                    log.warning("%s returned %s (blocked). Switching endpoint.", base, resp.status_code)
                    last_err = BinanceError(f"blocked ({resp.status_code}) on {base}")
                    self.idx += 1
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as exc:
                last_err = exc
                log.warning("Binance request failed on %s: %s", base, exc)
                self.idx += 1
                if STOP.wait(min(2 ** (attempt // 2), 10)):
                    break
        raise BinanceError(f"All Binance endpoints failed for {path}: {last_err}")

    def klines(self, symbol: str, interval: str, limit: int) -> pd.DataFrame:
        raw = self._get("/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        if not isinstance(raw, list) or len(raw) < 20:
            raise BinanceError(f"Unexpected klines payload for {symbol}")
        df = pd.DataFrame(
            raw,
            columns=["open_time", "open", "high", "low", "close", "volume", "close_time",
                     "qav", "trades", "tbbav", "tbqav", "ignore"],
        )
        for col in ("open", "high", "low", "close", "volume"):
            df[col] = df[col].astype(float)
        now_ms = int(time.time() * 1000)
        df = df[df["close_time"].astype("int64") < now_ms].copy()  # keep CLOSED candles only
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
        return df.reset_index(drop=True)

    def price(self, symbol: str) -> float:
        data = self._get("/api/v3/ticker/price", {"symbol": symbol})
        return float(data["price"])


# =============================================================================
# STRATEGY: LIQUIDITY SWEEP + DISPLACEMENT/MSS + ORDER BLOCK RETEST
# =============================================================================
@dataclass
class Setup:
    symbol: str
    timeframe: str
    direction: str            # "LONG" or "SHORT"
    sweep_time: pd.Timestamp
    disp_time: pd.Timestamp
    sweep_level: float        # the swing high/low (liquidity pool) that was swept
    sweep_extreme: float      # the wick extreme of the sweep
    ob_low: float
    ob_high: float
    entry: float              # reference only (OB midpoint)
    stop: float               # reference only
    target: float             # reference only
    retest: str               # "live" or "wick"
    price: float

    @property
    def key(self) -> str:
        return f"{self.symbol}|{self.timeframe}|{self.direction}|{int(self.sweep_time.timestamp())}"


def _swing_idx(arr: np.ndarray, k: int, kind: str) -> List[int]:
    """Indices of fractal swing lows/highs (arr[i] is the min/max of arr[i-k : i+k+1])."""
    fn = np.min if kind == "low" else np.max
    return [i for i in range(k, len(arr) - k) if arr[i] == fn(arr[i - k: i + k + 1])]


def _detect_long(o, h, l, c, atr, price, cfg: StrategyConfig) -> Optional[dict]:
    """
    Bullish version:
      1. A swing LOW (sell-side liquidity) exists in the last `lookback` candles.
      2. A candle wicks BELOW it and price closes back ABOVE it (sweep + reclaim).
      3. A strong bullish displacement candle follows (and breaks the last lower-high = MSS).
      4. The last bearish candle before the displacement is the Order Block.
      5. Current price (or the last closed candle) retraces into the OB zone.
    The bearish setup is detected by running this on sign-flipped price arrays.
    """
    n, k = len(c), cfg.swing_k
    sw_lows = _swing_idx(l, k, "low")
    sw_highs = _swing_idx(h, k, "high")
    first_d = max(n - cfg.max_setup_age, k + 2)

    for d in range(n - 1, first_d - 1, -1):          # newest displacement first
        if c[d] <= o[d]:
            continue
        if (c[d] - o[d]) < cfg.disp_atr_mult * atr[d - 1]:
            continue

        for s in range(d, max(d - cfg.disp_window, k + 1) - 1, -1):   # candidate sweep candle
            # --- Step 1+2: swept liquidity pool ---------------------------------
            level = None
            for i in reversed(sw_lows):
                if i > s - k - 1:        # swing must be confirmed before the sweep candle
                    continue
                if i < s - cfg.lookback:
                    break
                lvl = l[i]
                if l[s] >= lvl:          # no wick beyond this level
                    continue
                if l[i + 1: s].min() < lvl:   # level already taken out earlier
                    continue
                level = lvl
                break
            if level is None:
                continue

            r = None                     # reclaim candle: closes back above the level
            for x in range(s, min(s + cfg.reclaim_candles - 1, d) + 1):
                if c[x] > level:
                    r = x
                    break
            if r is None:
                continue

            extreme = l[s: d + 1].min()
            if (level - l[s: r + 1].min()) < cfg.min_sweep_atr * atr[s]:
                continue                 # sweep too shallow = noise

            # --- Step 3: market structure shift -----------------------------------
            mss = None
            for j in reversed(sw_highs):
                if j < s:
                    mss = h[j]
                    break
            if mss is None:
                mss = h[max(0, s - 5): s].max() if s > 0 else h[s]
            if cfg.require_mss and c[d] <= mss:
                continue

            # --- Step 3b: order block = last bearish candle before displacement ----
            ob = None
            for j in range(d - 1, max(s - cfg.ob_back, 0) - 1, -1):
                if c[j] < o[j]:
                    ob = j
                    break
            if ob is None:
                continue
            ob_lo, ob_hi = (c[ob], o[ob]) if cfg.ob_use_body else (l[ob], h[ob])

            # --- invalidation ------------------------------------------------------
            if d + 1 < n and c[d + 1:].min() < ob_lo:   # a close below the OB kills it
                continue
            if price < ob_lo or price <= extreme:
                continue

            # --- Step 4: retrace into the OB --------------------------------------
            live_in = ob_lo <= price <= ob_hi
            tapped = (
                d < n - 1
                and l[n - 1] <= ob_hi and h[n - 1] >= ob_lo and c[n - 1] >= ob_lo
                and price <= ob_hi + cfg.tap_tolerance_atr * atr[n - 1]
            )
            if not (live_in or tapped):
                continue

            return dict(s=s, d=d, level=level, extreme=extreme, ob_lo=ob_lo, ob_hi=ob_hi,
                        retest="live" if live_in else "wick")
    return None


def detect_setup(df: pd.DataFrame, symbol: str, price: float, cfg: StrategyConfig) -> List[Setup]:
    if len(df) < cfg.lookback + 2 * cfg.swing_k + 20:
        raise BinanceError(f"{symbol}: not enough candles ({len(df)})")

    o, h, l, c = (df[x].to_numpy(dtype=float) for x in ("open", "high", "low", "close"))
    prev_c = np.concatenate(([c[0]], c[:-1]))
    tr = np.maximum.reduce([h - l, np.abs(h - prev_c), np.abs(l - prev_c)])
    atr = pd.Series(tr).rolling(14).mean().bfill().to_numpy()

    times = df["open_time"]
    out: List[Setup] = []

    # LONG
    hit = _detect_long(o, h, l, c, atr, price, cfg)
    if hit:
        entry = (hit["ob_lo"] + hit["ob_hi"]) / 2
        stop = hit["extreme"] - cfg.stop_buffer_atr * atr[hit["d"]]
        target = entry + cfg.rr * (entry - stop)
        out.append(Setup(symbol, TIMEFRAME, "LONG", times[hit["s"]], times[hit["d"]],
                         hit["level"], hit["extreme"], hit["ob_lo"], hit["ob_hi"],
                         entry, stop, target, hit["retest"], price))

    # SHORT = mirror image (flip signs; highs become lows and vice-versa)
    hit = _detect_long(-o, -l, -h, -c, atr, -price, cfg)
    if hit:
        ob_lo, ob_hi = -hit["ob_hi"], -hit["ob_lo"]
        level, extreme = -hit["level"], -hit["extreme"]
        entry = (ob_lo + ob_hi) / 2
        stop = extreme + cfg.stop_buffer_atr * atr[hit["d"]]
        target = entry - cfg.rr * (stop - entry)
        out.append(Setup(symbol, TIMEFRAME, "SHORT", times[hit["s"]], times[hit["d"]],
                         level, extreme, ob_lo, ob_hi, entry, stop, target, hit["retest"], price))
    return out


# =============================================================================
# TELEGRAM
# =============================================================================
def telegram_configured() -> bool:
    return bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram(text: str) -> bool:
    if not telegram_configured():
        log.warning("Telegram not configured (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID).")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "disable_web_page_preview": True}
    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                try:
                    wait = int(resp.json().get("parameters", {}).get("retry_after", 5))
                except Exception:
                    wait = 5
                STOP.wait(min(wait, 60))
                continue
            resp.raise_for_status()
            return True
        except requests.RequestException as exc:
            log.warning("Telegram send failed (attempt %d/3): %s", attempt + 1, scrub(exc))
            STOP.wait(2 ** attempt)
    return False


def format_alert(s: Setup, now: datetime) -> str:
    icon = "🟢" if s.direction == "LONG" else "🔴"
    retest = "price is inside the OB" if s.retest == "live" else "last closed candle wicked into the OB"
    return (
        f"🚨 LIQUIDITY ENTRY ALERT: {s.symbol} | Setup: Liquidity Sweep + OB Retest | "
        f"Current Price: {fmt_price(s.price)} | Time: {fmt_ts(now)}\n\n"
        f"{icon} Bias: {s.direction}   |   Timeframe: {s.timeframe}\n"
        f"Swept liquidity: {fmt_price(s.sweep_level)} (wick to {fmt_price(s.sweep_extreme)})\n"
        f"Order Block zone: {fmt_price(s.ob_low)} - {fmt_price(s.ob_high)}\n"
        f"Retest: {retest}\n"
        f"Reference levels ({CFG.rr:g}R): entry {fmt_price(s.entry)} | "
        f"SL {fmt_price(s.stop)} | TP {fmt_price(s.target)}\n\n"
        f"⚠️ Alert only - not financial advice. Confirm on your chart."
    )


# =============================================================================
# GOOGLE SHEETS
# =============================================================================
SHEET_HEADERS = {
    "Alerts": ["Timestamp", "Symbol", "Timeframe", "Direction", "Setup", "Price", "Swept Level",
               "OB Low", "OB High", "Ref Entry", "Ref Stop", "Ref Target"],
    "Daily Summary": ["Date", "Scan Cycles", "Errors", "Assets Scanned", "Market Status (day change)",
                      "Bias", "Setups Triggered", "Setup Details"],
    "Scan Log": ["Timestamp", "Assets Scanned", "Prices", "Triggered Setups", "Status"],
}


def sheets_enabled() -> bool:
    mode = env_str("ENABLE_SHEETS", "auto").lower()
    if mode in ("0", "false", "no", "off"):
        return False
    return bool(gspread and Path(GOOGLE_CREDENTIALS_FILE).exists() and (GOOGLE_SHEET_ID or GOOGLE_SHEET_NAME))


class SheetsLogger:
    def __init__(self) -> None:
        self.sh = None
        self.ws_cache: Dict[str, Any] = {}

    def _connect(self) -> None:
        gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
        self.sh = gc.open_by_key(GOOGLE_SHEET_ID) if GOOGLE_SHEET_ID else gc.open(GOOGLE_SHEET_NAME)
        self.ws_cache = {}
        log.info("Connected to Google Sheet: %s", self.sh.title)

    def _worksheet(self, tab: str):
        if tab in self.ws_cache:
            return self.ws_cache[tab]
        header = SHEET_HEADERS[tab]
        try:
            ws = self.sh.worksheet(tab)
        except gspread.exceptions.WorksheetNotFound:
            ws = self.sh.add_worksheet(title=tab, rows=1000, cols=max(len(header), 10))
            ws.append_row(header, value_input_option="RAW")
        self.ws_cache[tab] = ws
        return ws

    def append(self, tab: str, row: list) -> None:
        try:
            if self.sh is None:
                self._connect()
            self._worksheet(tab).append_row(row, value_input_option="RAW")
        except Exception:
            self.sh = None            # force a clean reconnect next time
            self.ws_cache = {}
            raise


# =============================================================================
# STATE (persisted so restarts don't re-send alerts or lose queued sheet rows)
# =============================================================================
def new_stats() -> dict:
    return {"scans": 0, "errors": 0, "symbols": {}, "alerts": []}


def load_state() -> dict:
    default = {"alerted": {}, "day": "", "stats": new_stats(), "sheet_queue": []}
    try:
        if STATE_FILE.exists():
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            for k, v in default.items():
                data.setdefault(k, v)
            return data
    except Exception as exc:
        log.warning("State file unreadable (%s) - starting fresh.", exc)
    return default


def save_state(state: dict) -> None:
    try:
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state), encoding="utf-8")
        os.replace(tmp, STATE_FILE)
    except OSError as exc:
        log.warning("Could not save state: %s", exc)


def queue_row(state: dict, tab: str, row: list) -> None:
    if not SHEETS_ON:
        return
    q = state["sheet_queue"]
    q.append({"tab": tab, "row": row})
    if len(q) > SHEET_QUEUE_MAX:
        del q[: len(q) - SHEET_QUEUE_MAX]


def flush_sheet_queue(state: dict, sheets: Optional[SheetsLogger]) -> None:
    """Send queued rows in order; stop at first failure and retry next cycle."""
    if not sheets:
        return
    q = state["sheet_queue"]
    while q and not STOP.is_set():
        item = q[0]
        try:
            sheets.append(item["tab"], item["row"])
        except Exception as exc:
            log.warning("Google Sheets write failed (%d row(s) queued, will retry): %s", len(q), exc)
            return
        q.pop(0)


# =============================================================================
# DAILY STATS / SUMMARY
# =============================================================================
def update_symbol_stats(stats: dict, sym: str, price: float) -> None:
    v = stats["symbols"].setdefault(sym, {"open": price, "high": price, "low": price, "last": price})
    v["high"], v["low"], v["last"] = max(v["high"], price), min(v["low"], price), price


def build_daily_row(day: str, stats: dict) -> list:
    parts, ups, downs = [], 0, 0
    for sym, v in stats["symbols"].items():
        chg = (v["last"] / v["open"] - 1) * 100 if v["open"] else 0.0
        ups += chg > 0.3
        downs += chg < -0.3
        parts.append(f"{sym} {chg:+.2f}% (H {fmt_price(v['high'])} / L {fmt_price(v['low'])})")
    bias = "Bullish" if ups > downs else "Bearish" if downs > ups else "Mixed / Neutral"
    alerts = stats["alerts"]
    return [day, stats["scans"], stats["errors"], ", ".join(stats["symbols"].keys()),
            " | ".join(parts) or "no data", bias, len(alerts), "; ".join(alerts) or "None"]


def roll_day_if_needed(state: dict, now: datetime) -> None:
    today = now.strftime("%Y-%m-%d")
    if state["day"] and state["day"] != today and state["stats"]["scans"] > 0:
        row = build_daily_row(state["day"], state["stats"])
        queue_row(state, "Daily Summary", row)
        log.info("Day rollover: queued daily summary for %s", state["day"])
        state["stats"] = new_stats()
    state["day"] = today
    cutoff = time.time() - 3 * 86400   # forget alert keys older than 3 days
    state["alerted"] = {k: t for k, t in state["alerted"].items() if t > cutoff}


# =============================================================================
# MAIN SCAN CYCLE
# =============================================================================
def handle_setup(setup: Setup, now: datetime, state: dict) -> Optional[str]:
    """Send Telegram + queue a sheet row. Returns a short description if handled."""
    log.info("SETUP %s %s | price %s | OB %s-%s", setup.symbol, setup.direction,
             fmt_price(setup.price), fmt_price(setup.ob_low), fmt_price(setup.ob_high))
    sent = send_telegram(format_alert(setup, now))
    if not sent and telegram_configured():
        log.warning("Alert for %s not delivered - will retry next cycle.", setup.symbol)
        return None                      # not marked as alerted -> retried next scan
    state["alerted"][setup.key] = time.time()
    desc = f"{setup.symbol} {setup.direction} @ {fmt_price(setup.price)}"
    state["stats"]["alerts"].append(desc)
    queue_row(state, "Alerts", [
        fmt_ts(now), setup.symbol, setup.timeframe, setup.direction, "Liquidity Sweep + OB Retest",
        round(setup.price, 6), round(setup.sweep_level, 6), round(setup.ob_low, 6),
        round(setup.ob_high, 6), round(setup.entry, 6), round(setup.stop, 6), round(setup.target, 6),
    ])
    return desc


def run_cycle(client: BinanceClient, sheets: Optional[SheetsLogger], state: dict, health: dict) -> None:
    now = datetime.now(LOCAL_TZ)
    roll_day_if_needed(state, now)
    stats = state["stats"]
    stats["scans"] += 1

    prices: Dict[str, float] = {}
    triggered: List[str] = []
    errors = 0

    for sym in SYMBOLS:
        if STOP.is_set():
            break
        try:
            df = client.klines(sym, TIMEFRAME, KLINE_LIMIT)
            price = client.price(sym)
            prices[sym] = price
            update_symbol_stats(stats, sym, price)
            for setup in detect_setup(df, sym, price, CFG):
                if setup.key in state["alerted"]:
                    continue
                desc = handle_setup(setup, now, state)
                if desc:
                    triggered.append(desc)
        except BinanceError as exc:
            errors += 1
            stats["errors"] += 1
            log.warning("%s: %s", sym, exc)
        except Exception:                # never let one symbol kill the cycle
            errors += 1
            stats["errors"] += 1
            log.exception("%s: unexpected error", sym)
        STOP.wait(0.3)                   # gentle pacing between symbols

    # --- health monitoring: warn via Telegram if everything keeps failing ----------
    if SYMBOLS and errors == len(SYMBOLS):
        health["fails"] += 1
        if health["fails"] == 6 and not health["notified"]:
            send_telegram("⚠️ trader_bot: all Binance requests have failed for 6 cycles in a row. Still retrying.")
            health["notified"] = True
    else:
        if health["notified"]:
            send_telegram("✅ trader_bot: data feed recovered.")
        health["fails"], health["notified"] = 0, False

    status = "OK" if errors == 0 else f"PARTIAL ({errors} error(s))"
    log.info("Cycle done | %s | %s | triggered: %s", status,
             ", ".join(f"{s} {fmt_price(p)}" for s, p in prices.items()) or "no prices",
             ", ".join(triggered) or "none")

    if LOG_EVERY_CYCLE:
        queue_row(state, "Scan Log", [
            fmt_ts(now), ", ".join(SYMBOLS),
            " | ".join(f"{s}:{fmt_price(p)}" for s, p in prices.items()),
            ", ".join(triggered) or "None", status,
        ])

    flush_sheet_queue(state, sheets)
    save_state(state)


# =============================================================================
# ENTRY POINTS
# =============================================================================
SHEETS_ON = False


def run_test() -> int:
    """`python trader_bot.py --test` : verifies Binance, Telegram and Google Sheets."""
    print("1) Binance public API ...")
    try:
        client = BinanceClient()
        print(f"   OK - {SYMBOLS[0]} price = {fmt_price(client.price(SYMBOLS[0]))}")
    except Exception as exc:
        print(f"   FAILED: {exc}")

    print("2) Telegram ...")
    if send_telegram("✅ trader_bot test message - Telegram is configured correctly."):
        print("   OK - check your Telegram chat")
    else:
        print("   FAILED (check TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID, and press START on your bot)")

    print("3) Google Sheets ...")
    if not sheets_enabled():
        print("   SKIPPED/DISABLED (need: pip install gspread, credentials.json, GOOGLE_SHEET_ID or NAME)")
    else:
        try:
            SheetsLogger().append("Scan Log", [fmt_ts(datetime.now(LOCAL_TZ)), "TEST", "-", "None", "TEST ROW"])
            print("   OK - a TEST row was added to the 'Scan Log' tab")
        except Exception as exc:
            print(f"   FAILED: {exc}")
    return 0


def main() -> int:
    global SHEETS_ON
    parser = argparse.ArgumentParser(description="Liquidity sweep + OB retest alert bot")
    parser.add_argument("--test", action="store_true", help="test Binance/Telegram/Sheets connections and exit")
    parser.add_argument("--once", action="store_true", help="run a single scan cycle and exit")
    args = parser.parse_args()

    setup_logging()
    SHEETS_ON = sheets_enabled()

    if args.test:
        return run_test()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    state = load_state()
    client = BinanceClient()
    sheets = SheetsLogger() if SHEETS_ON else None
    health = {"fails": 0, "notified": False}

    log.info("Starting | symbols=%s | tf=%s | interval=%ss | telegram=%s | sheets=%s | tz=%s",
             ",".join(SYMBOLS), TIMEFRAME, SCAN_INTERVAL, telegram_configured(), SHEETS_ON, TZ_NAME)
    if not SHEETS_ON:
        log.info("Google Sheets logging is OFF (missing gspread, credentials file, or sheet id/name).")
    if SEND_STARTUP_MESSAGE and telegram_configured() and not args.once:
        send_telegram(f"🤖 trader_bot started. Watching {', '.join(SYMBOLS)} on {TIMEFRAME}.")

    while not STOP.is_set():                       # 24/7 loop
        started = time.time()
        try:
            run_cycle(client, sheets, state, health)
        except Exception:                          # absolute safety net: never crash
            log.exception("Unhandled error in scan cycle - continuing")
        if args.once:
            break
        # Interruptible equivalent of time.sleep(SCAN_INTERVAL)
        STOP.wait(max(1.0, SCAN_INTERVAL - (time.time() - started)))

    save_state(state)
    log.info("Stopped cleanly.")
    return 0


if __name__ == "__main__":
    sys.exit(main())