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
import io
import json
import logging
import os
import re
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

try:
    import matplotlib
    matplotlib.use("Agg")  # Non-interactive backend safe for daemon / background threads
    import matplotlib.pyplot as plt
    import mplfinance as mpf
except ImportError:
    mpf = None
    plt = None

try:
    import cloudinary
    import cloudinary.uploader
    import cloudinary.api
except ImportError:
    cloudinary = None


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
    for s in env_str("SYMBOLS", "ETHUSDT,SOLUSDT,XRPUSDT,BTCUSDT,DOGEUSDT,PAXGUSDT,XAUTUSDT").split(",")
    if s.strip()
]
VALID_INTERVALS = {
    "1s", "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M"
}
_raw_env_tf = env_str("TIMEFRAMES") or env_str("TIMEFRAME", "15m, 5m, 1h, 4h, 1m")
_clean_env_tf = _raw_env_tf.split("#")[0].strip()
_raw_tfs = [t.strip().lower() for t in _clean_env_tf.split(",") if t.strip()]
TIMEFRAMES: List[str] = [t for t in _raw_tfs if t in VALID_INTERVALS] or ["15m", "5m", "1h", "4h", "1m"]
TIMEFRAME = ", ".join(TIMEFRAMES)            # display string of active timeframes
SCAN_INTERVAL = env_int("SCAN_INTERVAL_SECONDS", 60)  # seconds between scans
KLINE_LIMIT = env_int("KLINE_LIMIT", 300)
REQUEST_TIMEOUT = env_int("REQUEST_TIMEOUT", 15)

# --- Telegram Bot ---
TELEGRAM_TOKEN = env_str("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = env_str("TELEGRAM_CHAT_ID")
SEND_STARTUP_MESSAGE = env_bool("SEND_STARTUP_MESSAGE", True)

# --- Cloudinary (Chart Image Hosting) ---
CLOUDINARY_CLOUD_NAME = env_str("CLOUDINARY_CLOUD_NAME")
CLOUDINARY_API_KEY = env_str("CLOUDINARY_API_KEY")
CLOUDINARY_API_SECRET = env_str("CLOUDINARY_API_SECRET")


def cloudinary_configured() -> bool:
    return bool(cloudinary and CLOUDINARY_CLOUD_NAME and CLOUDINARY_API_KEY and CLOUDINARY_API_SECRET)


if cloudinary_configured():
    try:
        cloudinary.config(
            cloud_name=CLOUDINARY_CLOUD_NAME,
            api_key=CLOUDINARY_API_KEY,
            api_secret=CLOUDINARY_API_SECRET,
            secure=True,
        )
    except Exception as _c_exc:
        logging.getLogger("trader_bot").warning("Cloudinary initialization failed: %s", _c_exc)

# --- Groq AI (qwen/qwen3.8-27b) ---
GROQ_API_KEY = env_str("GROQ_API_KEY")
GROQ_MODEL = env_str("GROQ_MODEL", "qwen/qwen3.8-27b")

# --- Google Sheets (Cloud JSON string or local credentials.json fallback) ---
GOOGLE_CREDENTIALS_JSON = env_str("GOOGLE_CREDENTIALS_JSON")
GOOGLE_CREDENTIALS_FILE = env_str("GOOGLE_CREDENTIALS_FILE", str(BASE_DIR / "credentials.json"))
GOOGLE_SHEET_ID = env_str("GOOGLE_SHEET_ID")       # preferred (from the sheet URL)
GOOGLE_SHEET_NAME = env_str("GOOGLE_SHEET_NAME")   # alternative: open by title
LOG_EVERY_CYCLE = env_bool("LOG_EVERY_CYCLE", True) # true = append a row every scan cycle
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
SCANNING_ACTIVE = threading.Event()
SCANNING_ACTIVE.set()


def setup_logging() -> None:
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
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
    """Remove secrets (Telegram token, Groq key, Google credentials, Cloudinary keys) from any text before logging it."""
    s = str(text)
    if TELEGRAM_TOKEN:
        s = s.replace(TELEGRAM_TOKEN, "<TELEGRAM_TOKEN>")
    if GROQ_API_KEY:
        s = s.replace(GROQ_API_KEY, "<GROQ_API_KEY>")
    if CLOUDINARY_API_KEY:
        s = s.replace(CLOUDINARY_API_KEY, "<CLOUDINARY_API_KEY>")
    if CLOUDINARY_API_SECRET:
        s = s.replace(CLOUDINARY_API_SECRET, "<CLOUDINARY_API_SECRET>")
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
                if resp.status_code == 400:
                    err_msg = resp.text
                    try:
                        err_msg = resp.json().get("msg", resp.text)
                    except Exception:
                        pass
                    raise BinanceError(f"Bad Request (400) on {path}: {err_msg}")
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
        if not isinstance(raw, list) or not raw:
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
    atr: float = 0.0          # 14-period ATR for dynamic risk calculations
    htf_bias: Optional[str] = None
    confluence: Optional[str] = None
    ai_insight: Optional[str] = None
    ai_data: Optional[Dict[str, Any]] = None  # Full structured JSON from Groq
    chart_url: Optional[str] = None           # Cloudinary uploaded chart URL

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


def compute_trend(df: pd.DataFrame) -> str:
    """Calculates trend using price vs EMA20 & EMA50 on closed candles."""
    try:
        c = df["close"].to_numpy(dtype=float)
        if len(c) < 30:
            return "NEUTRAL"
        s = pd.Series(c)
        ema20 = s.ewm(span=20, adjust=False).mean().iloc[-1]
        ema50 = s.ewm(span=50, adjust=False).mean().iloc[-1]
        last = c[-1]
        if last > ema50 and ema20 >= ema50:
            return "BULLISH"
        elif last < ema50 and ema20 <= ema50:
            return "BEARISH"
        elif last > ema50:
            return "MILD BULLISH"
        elif last < ema50:
            return "MILD BEARISH"
        return "NEUTRAL"
    except Exception:
        return "NEUTRAL"


def detect_setup(df: pd.DataFrame, symbol: str, price: float, cfg: StrategyConfig, tf: str = "15m") -> List[Setup]:
    if len(df) < cfg.lookback + 2 * cfg.swing_k + 20:
        raise BinanceError(f"{symbol} ({tf}): not enough candles ({len(df)})")

    o, h, l, c = (df[x].to_numpy(dtype=float) for x in ("open", "high", "low", "close"))
    prev_c = np.concatenate(([c[0]], c[:-1]))
    tr = np.maximum.reduce([h - l, np.abs(h - prev_c), np.abs(l - prev_c)])
    atr = pd.Series(tr).rolling(14).mean().bfill().to_numpy()

    times = df["open_time"]
    out: List[Setup] = []

    # LONG
    hit = _detect_long(o, h, l, c, atr, price, cfg)
    if hit:
        cur_atr = float(atr[hit["d"]]) if hit["d"] < len(atr) else float(atr[-1])
        entry = (hit["ob_lo"] + hit["ob_hi"]) / 2
        stop = hit["extreme"] - cfg.stop_buffer_atr * cur_atr
        target = entry + cfg.rr * (entry - stop)
        out.append(Setup(symbol, tf, "LONG", times[hit["s"]], times[hit["d"]],
                         hit["level"], hit["extreme"], hit["ob_lo"], hit["ob_hi"],
                         entry, stop, target, hit["retest"], price, atr=cur_atr))

    # SHORT = mirror image (flip signs; highs become lows and vice-versa)
    hit = _detect_long(-o, -l, -h, -c, atr, -price, cfg)
    if hit:
        cur_atr = float(atr[hit["d"]]) if hit["d"] < len(atr) else float(atr[-1])
        ob_lo, ob_hi = -hit["ob_hi"], -hit["ob_lo"]
        level, extreme = -hit["level"], -hit["extreme"]
        entry = (ob_lo + ob_hi) / 2
        stop = extreme + cfg.stop_buffer_atr * cur_atr
        target = entry - cfg.rr * (stop - entry)
        out.append(Setup(symbol, tf, "SHORT", times[hit["s"]], times[hit["d"]],
                         level, extreme, ob_lo, ob_hi, entry, stop, target, hit["retest"], price, atr=cur_atr))
    return out


# =============================================================================
# TELEGRAM
# =============================================================================
def telegram_configured() -> bool:
    return bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram_msg(
    text: str,
    chat_id: Optional[str] = None,
    parse_mode: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
) -> Optional[dict]:
    target_chat = chat_id or TELEGRAM_CHAT_ID
    if not (TELEGRAM_TOKEN and target_chat):
        log.warning("Telegram not configured (missing TELEGRAM_BOT_TOKEN or chat_id).")
        return None
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload: Dict[str, Any] = {"chat_id": target_chat, "text": text, "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
        payload["allow_sending_without_reply"] = True

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
            res = resp.json()
            if res.get("ok"):
                return res.get("result")
            return None
        except requests.RequestException as exc:
            log.warning("Telegram send failed (attempt %d/3): %s", attempt + 1, scrub(exc))
            STOP.wait(2 ** attempt)
    return None


def send_telegram(
    text: str,
    chat_id: Optional[str] = None,
    parse_mode: Optional[str] = None,
    reply_to_message_id: Optional[int] = None,
) -> bool:
    return bool(send_telegram_msg(text, chat_id=chat_id, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id))


def send_telegram_chart_alert(
    caption: str,
    photo_url: Optional[str] = None,
    chat_id: Optional[str] = None,
    parse_mode: Optional[str] = "Markdown",
    reply_to_message_id: Optional[int] = None,
) -> bool:
    """
    Sends a photo alert via Telegram sendPhoto using the Cloudinary image URL.
    Includes robust fallback:
      - If photo_url is missing or sendPhoto fails, falls back to text-only send_telegram().
      - Safely handles Telegram's 1024-character caption limit by sending main summary on photo
        and full breakdown in a follow-up message if needed.
      - Supports reply_to_message_id to quote the requesting user's message in groups.
    """
    target_chat = chat_id or TELEGRAM_CHAT_ID
    if not (TELEGRAM_TOKEN and target_chat):
        log.warning("Telegram not configured (missing TELEGRAM_BOT_TOKEN or chat_id).")
        return False

    if not photo_url:
        return send_telegram(caption, chat_id=target_chat, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id)

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto"

    # Telegram sendPhoto caption maximum length is 1024 characters
    if len(caption) > 1020:
        cutoff = caption.rfind("\n", 0, 950)
        if cutoff == -1:
            cutoff = 950
        photo_caption = caption[:cutoff] + "\n\n_...Full AI institutional details below 👇_"
        followup_text = caption[cutoff:].strip()
    else:
        photo_caption = caption
        followup_text = None

    payload: Dict[str, Any] = {
        "chat_id": target_chat,
        "photo": photo_url,
        "caption": photo_caption,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id
        payload["allow_sending_without_reply"] = True

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
            if resp.status_code == 200:
                if followup_text:
                    sent_f = send_telegram(followup_text, chat_id=target_chat, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id)
                    if not sent_f and parse_mode:
                        send_telegram(followup_text, chat_id=target_chat, reply_to_message_id=reply_to_message_id)
                return True
            elif resp.status_code == 400 and parse_mode:
                log.info("sendPhoto 400 with parse_mode=%s; retrying photo with plain text caption...", parse_mode)
                plain_payload = dict(payload)
                plain_payload.pop("parse_mode", None)
                try:
                    retry_resp = requests.post(url, json=plain_payload, timeout=REQUEST_TIMEOUT)
                    if retry_resp.status_code == 200:
                        if followup_text:
                            sent_f = send_telegram(followup_text, chat_id=target_chat, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id)
                            if not sent_f and parse_mode:
                                send_telegram(followup_text, chat_id=target_chat, reply_to_message_id=reply_to_message_id)
                        return True
                except Exception as exc:
                    log.warning("Retry sendPhoto without parse_mode failed: %s", scrub(exc))
            else:
                log.warning("Telegram sendPhoto returned HTTP %d: %s (attempt %d/3)", resp.status_code, scrub(resp.text), attempt + 1)
        except requests.RequestException as exc:
            log.warning("Telegram sendPhoto failed (attempt %d/3): %s", attempt + 1, scrub(exc))
            STOP.wait(2 ** attempt)

    # Fallback mechanism: send standard text alert so alerts are NEVER missed
    log.info("sendPhoto failed; falling back to standard text alert.")
    sent = send_telegram(caption, chat_id=target_chat, parse_mode=parse_mode, reply_to_message_id=reply_to_message_id)
    if not sent and parse_mode:
        sent = send_telegram(caption, chat_id=target_chat, reply_to_message_id=reply_to_message_id)
    return sent


# =============================================================================
# TELEGRAM GROUP MANAGEMENT, ANTI-SPAM & MODERATION
# =============================================================================
BOT_INFO_CACHE: Dict[str, Any] = {}


def get_bot_info() -> Dict[str, Any]:
    """Fetches and caches the bot's user info (ID, username, first_name)."""
    global BOT_INFO_CACHE
    if BOT_INFO_CACHE:
        return BOT_INFO_CACHE
    if not TELEGRAM_TOKEN:
        return {}
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe", timeout=10)
        if r.status_code == 200 and r.json().get("ok"):
            BOT_INFO_CACHE = r.json()["result"]
            return BOT_INFO_CACHE
    except Exception as exc:
        log.warning("getMe failed: %s", scrub(exc))
    return {}


def telegram_delete_message(chat_id: str, message_id: int) -> bool:
    """Deletes a message from a chat (requires manage messages / delete permission)."""
    if not (TELEGRAM_TOKEN and chat_id and message_id):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteMessage"
        resp = requests.post(url, json={"chat_id": chat_id, "message_id": message_id}, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("deleteMessage error: %s", scrub(exc))
        return False


def telegram_ban_member(chat_id: str, user_id: int) -> bool:
    """Bans a user from the group (requires ban users permission)."""
    if not (TELEGRAM_TOKEN and chat_id and user_id):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/banChatMember"
        resp = requests.post(url, json={"chat_id": chat_id, "user_id": user_id}, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("banChatMember error: %s", scrub(exc))
        return False


def telegram_unban_member(chat_id: str, user_id: int) -> bool:
    """Unbans a user from the group."""
    if not (TELEGRAM_TOKEN and chat_id and user_id):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/unbanChatMember"
        resp = requests.post(url, json={"chat_id": chat_id, "user_id": user_id, "only_if_banned": True}, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("unbanChatMember error: %s", scrub(exc))
        return False


def telegram_mute_member(chat_id: str, user_id: int, duration_sec: int = 3600) -> bool:
    """Restricts a user from sending messages for duration_sec seconds."""
    if not (TELEGRAM_TOKEN and chat_id and user_id):
        return False
    try:
        until_date = int(time.time()) + max(60, duration_sec)
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/restrictChatMember"
        payload = {
            "chat_id": chat_id,
            "user_id": user_id,
            "permissions": {
                "can_send_messages": False,
                "can_send_media_messages": False,
                "can_send_other_messages": False,
                "can_add_web_page_previews": False,
            },
            "until_date": until_date,
        }
        resp = requests.post(url, json=payload, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("restrictChatMember error: %s", scrub(exc))
        return False


def telegram_pin_message(chat_id: str, message_id: int, notify: bool = False) -> bool:
    """Pins a message in the chat."""
    if not (TELEGRAM_TOKEN and chat_id and message_id):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/pinChatMessage"
        resp = requests.post(url, json={"chat_id": chat_id, "message_id": message_id, "disable_notification": not notify}, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("pinChatMessage error: %s", scrub(exc))
        return False


def telegram_unpin_message(chat_id: str, message_id: Optional[int] = None) -> bool:
    """Unpins a message in the chat."""
    if not (TELEGRAM_TOKEN and chat_id):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/unpinChatMessage"
        payload: Dict[str, Any] = {"chat_id": chat_id}
        if message_id:
            payload["message_id"] = message_id
        resp = requests.post(url, json=payload, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("unpinChatMessage error: %s", scrub(exc))
        return False


def telegram_set_title(chat_id: str, title: str) -> bool:
    """Sets group/channel title (requires change channel info permission)."""
    if not (TELEGRAM_TOKEN and chat_id and title):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setChatTitle"
        resp = requests.post(url, json={"chat_id": chat_id, "title": title[:128]}, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("setChatTitle error: %s", scrub(exc))
        return False


def telegram_set_description(chat_id: str, description: str) -> bool:
    """Sets group/channel description (requires change channel info permission)."""
    if not (TELEGRAM_TOKEN and chat_id):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setChatDescription"
        resp = requests.post(url, json={"chat_id": chat_id, "description": description[:255]}, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("setChatDescription error: %s", scrub(exc))
        return False


def telegram_edit_message(chat_id: str, message_id: int, text: str, parse_mode: Optional[str] = "Markdown") -> bool:
    """Edits a previously posted message (requires edit message permission)."""
    if not (TELEGRAM_TOKEN and chat_id and message_id and text):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        resp = requests.post(url, json=payload, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("editMessage error: %s", scrub(exc))
        return False


def telegram_delete_messages(chat_id: str, message_ids: List[int]) -> bool:
    """Deletes multiple messages in bulk (requires delete message permission)."""
    if not (TELEGRAM_TOKEN and chat_id and message_ids):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteMessages"
        resp = requests.post(url, json={"chat_id": chat_id, "message_ids": message_ids}, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("deleteMessages error: %s", scrub(exc))
        return False


def telegram_unmute_member(chat_id: str, user_id: int) -> bool:
    """Unmutes a restricted user in the group."""
    if not (TELEGRAM_TOKEN and chat_id and user_id):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/restrictChatMember"
        payload = {
            "chat_id": chat_id,
            "user_id": user_id,
            "permissions": {
                "can_send_messages": True,
                "can_send_media_messages": True,
                "can_send_other_messages": True,
                "can_add_web_page_previews": True,
            },
        }
        resp = requests.post(url, json=payload, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("unmute member error: %s", scrub(exc))
        return False


def telegram_send_dm(user_id: int, text: str, parse_mode: Optional[str] = "Markdown") -> bool:
    """Sends a direct private message to a specific user (requires direct messages capability)."""
    return send_telegram(text, chat_id=str(user_id), parse_mode=parse_mode)


def telegram_delete_chat_photo(chat_id: str) -> bool:
    """Deletes the group/channel avatar (requires change channel info permission)."""
    if not (TELEGRAM_TOKEN and chat_id):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteChatPhoto"
        resp = requests.post(url, json={"chat_id": chat_id}, timeout=10)
        return bool(resp.status_code == 200 and resp.json().get("ok"))
    except Exception as exc:
        log.warning("deleteChatPhoto error: %s", scrub(exc))
        return False


def telegram_kick_member(chat_id: str, user_id: int) -> bool:
    """Kicks a user from the group (bans and immediately unbans)."""
    if not (TELEGRAM_TOKEN and chat_id and user_id):
        return False
    ok_ban = telegram_ban_member(chat_id, user_id)
    if ok_ban:
        telegram_unban_member(chat_id, user_id)
        return True
    return False


def telegram_get_chat(chat_id: str) -> Optional[dict]:
    """Fetches chat information from Telegram API."""
    if not (TELEGRAM_TOKEN and chat_id):
        return None
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getChat"
        resp = requests.get(url, params={"chat_id": chat_id}, timeout=10)
        if resp.status_code == 200 and resp.json().get("ok"):
            return resp.json().get("result")
    except Exception as exc:
        log.warning("getChat error: %s", scrub(exc))
    return None


def telegram_get_member_count(chat_id: str) -> Optional[int]:
    """Gets total member count of the chat."""
    if not (TELEGRAM_TOKEN and chat_id):
        return None
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getChatMemberCount"
        resp = requests.get(url, params={"chat_id": chat_id}, timeout=10)
        if resp.status_code == 200 and resp.json().get("ok"):
            return resp.json().get("result")
    except Exception as exc:
        log.warning("getChatMemberCount error: %s", scrub(exc))
    return None


def parse_duration_sec(arg: str, default_sec: int = 3600) -> int:
    """Parses human-friendly durations like 30m, 2h, 1d into seconds."""
    arg = (arg or "").strip().lower()
    if not arg:
        return default_sec
    try:
        if arg.endswith("m"):
            return max(60, int(float(arg[:-1]) * 60))
        elif arg.endswith("h"):
            return max(60, int(float(arg[:-1]) * 3600))
        elif arg.endswith("d"):
            return max(60, int(float(arg[:-1]) * 86400))
        elif arg.isdigit():
            return max(60, int(arg))
    except Exception:
        pass
    return default_sec


def is_telegram_admin(chat_id: str, user_id: Optional[int]) -> bool:
    """Verifies whether a user is a group administrator or creator, or the configured bot owner."""
    if not user_id:
        return False
    if TELEGRAM_CHAT_ID and str(user_id) == str(TELEGRAM_CHAT_ID):
        return True
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getChatMember"
        resp = requests.get(url, params={"chat_id": chat_id, "user_id": user_id}, timeout=10)
        if resp.status_code == 200 and resp.json().get("ok"):
            status = resp.json()["result"].get("status")
            return status in ("creator", "administrator")
    except Exception as exc:
        log.warning("is_telegram_admin check failed: %s", scrub(exc))
    return False


def is_spam_message(text: str) -> bool:
    """Detects unsolicited promotion, spam invite links, and scam phrases."""
    t_lower = text.lower()
    spam_patterns = [
        "t.me/+",
        "t.me/joinchat",
        "whatsapp.com",
        "wa.me/",
        "free crypto signals",
        "guaranteed profit",
        "100x pump",
        "inbox me for investment",
        "dm me to earn",
        "earn 1000$ daily",
        "investment plan",
        "doubled in 24 hours",
        "giveaway crypto",
        "whatsapp me",
    ]
    return any(p in t_lower for p in spam_patterns)


DAILY_LEVELS_CACHE: Dict[str, Tuple[float, float, float, float, float]] = {}  # sym -> (timestamp, pdo, pdh, pdl, pdc)


def get_daily_levels(client: Optional[BinanceClient], symbol: str) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Fetches and caches Previous Day Candle OHLC (PDO, PDH, PDL, PDC) for 1 hour."""
    if not client or not symbol:
        return None, None, None, None
    now_ts = time.time()
    cached = DAILY_LEVELS_CACHE.get(symbol)
    if cached and (now_ts - cached[0] < 3600):
        return cached[1], cached[2], cached[3], cached[4]
    try:
        df_1d = client.klines(symbol, "1d", 3)
        if df_1d is not None and len(df_1d) >= 1:
            prev_day = df_1d.iloc[-1]
            pdo = float(prev_day["open"])
            pdh = float(prev_day["high"])
            pdl = float(prev_day["low"])
            pdc = float(prev_day["close"])
            DAILY_LEVELS_CACHE[symbol] = (now_ts, pdo, pdh, pdl, pdc)
            return pdo, pdh, pdl, pdc
    except Exception as exc:
        log.warning("Could not fetch daily levels (PDO/PDH/PDL/PDC) for %s: %s", symbol, exc)
    return None, None, None, None


def generate_and_upload_chart(
    df: pd.DataFrame,
    setup: Optional[Setup] = None,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    support: Optional[float] = None,
    resistance: Optional[float] = None,
    pdo: Optional[float] = None,
    pdh: Optional[float] = None,
    pdl: Optional[float] = None,
    pdc: Optional[float] = None,
) -> Optional[str]:
    """
    Renders a high-precision, institutional dark-themed candlestick chart using mplfinance & matplotlib.
    Includes:
      - 65-candle framing with clear wicks and volume bars
      - Professional TradingView dark institutional theme (#089981 teal green / #f23645 crisp red)
      - Dynamic EMA 20 & EMA 50 ribbons
      - Full Previous 1-Day Candle OHLC levels:
          * PDH (Previous Day High - Orange)
          * PDL (Previous Day Low - Blue)
          * PDO (Previous Day Open - Purple)
          * PDC (Previous Day Close - Gold)
      - Shaded ICT Order Block rectangular zone (cyan translucent span with border markers)
      - Precision price callout tags on right axis (Sweep level, Entry, Dynamic SL, Dynamic TP)
      - Live Current Price dotted line and badge
      - Top HUD Header (Asset, Timeframe, Live Price, Strategy, Confluence, PD-OHLC summary)
      - Saves to high-res 180 DPI PNG buffer and uploads to Cloudinary inside 'crypto_trading_charts'.
    Returns secure_url or None.
    """
    if not mpf:
        log.warning("mplfinance not installed; skipping chart generation.")
        return None
    if not cloudinary_configured():
        log.info("Cloudinary not configured (CLOUDINARY_CLOUD_NAME/API_KEY/API_SECRET); skipping chart upload.")
        return None

    try:
        # Take the last 65 candles for clear structural context
        candles = df.tail(65).copy()
        if len(candles) < 10:
            log.warning("Not enough candles to generate chart (%d)", len(candles))
            return None

        # Build clean DataFrame for mplfinance
        chart_df = pd.DataFrame({
            "Open": candles["open"].astype(float),
            "High": candles["high"].astype(float),
            "Low": candles["low"].astype(float),
            "Close": candles["close"].astype(float),
            "Volume": candles["volume"].astype(float),
        })
        if "open_time" in candles.columns:
            chart_df.index = pd.to_datetime(candles["open_time"])
        else:
            chart_df.index = pd.to_datetime(candles.index)

        # Build custom institutional dark theme (TradingView dark palette)
        mc = mpf.make_marketcolors(
            up="#089981",         # Clean TradingView teal green
            down="#f23645",       # Crisp TradingView red
            edge="inherit",
            wick="inherit",
            volume={"up": "#089981", "down": "#f23645"},
            ohlc="inherit",
        )
        style = mpf.make_mpf_style(
            base_mpf_style="nightclouds",
            marketcolors=mc,
            facecolor="#131722",
            edgecolor="#1e222d",
            figcolor="#131722",
            gridcolor="#1e222d",
            gridstyle="--",
            y_on_right=True,
            rc={
                "font.size": 8.5,
                "text.color": "#d1d4dc",
                "axes.labelcolor": "#d1d4dc",
                "xtick.color": "#787b86",
                "ytick.color": "#787b86",
            },
        )

        # Target symbol & timeframe
        chart_sym = setup.symbol if setup else (symbol or (SYMBOLS[0] if SYMBOLS else "ETHUSDT"))
        chart_tf = setup.timeframe if setup else (timeframe or "15m")
        last_price = chart_df["Close"].iloc[-1]

        # Add EMA 20 & 50 overlays
        addplots = []
        try:
            if len(chart_df) >= 20:
                ema20 = chart_df["Close"].ewm(span=20, adjust=False).mean()
                addplots.append(mpf.make_addplot(ema20, color="#2962ff", width=1.2))
            if len(chart_df) >= 50:
                ema50 = chart_df["Close"].ewm(span=50, adjust=False).mean()
                addplots.append(mpf.make_addplot(ema50, color="#ff9800", width=1.2))
        except Exception:
            pass

        # Render figure with mplfinance returnfig=True for precision visual customization
        fig, axlist = mpf.plot(
            chart_df,
            type="candle",
            volume=True,
            style=style,
            addplot=addplots if addplots else None,
            figsize=(12.8, 7.2),
            returnfig=True,
        )

        ax = axlist[0]
        xlim = ax.get_xlim()
        width = xlim[1] - xlim[0]
        x_badge1 = xlim[1] - width * 0.22
        x_badge2 = xlim[1] - width * 0.40

        # Mark Previous 1-Day Candle OHLC (PDH, PDL, PDO, PDC)
        pd_parts = []
        if pdh:
            ax.axhline(pdh, color="#ff7043", linestyle="--", linewidth=1.25, alpha=0.9)
            ax.text(
                xlim[0] + width * 0.015, pdh,
                "PDH (1D High)",
                color="#ff7043", fontsize=7.2, weight="bold", verticalalignment="bottom"
            )
            ax.text(
                x_badge1, pdh,
                f" [PDH - High] {fmt_price(pdh)} ",
                color="#ffffff", fontsize=7.5, weight="bold", verticalalignment="center",
                bbox=dict(boxstyle="square,pad=0.2", facecolor="#d84315", edgecolor="none")
            )
            pd_parts.append(f"High: {fmt_price(pdh)}")
        if pdl:
            ax.axhline(pdl, color="#42a5f5", linestyle="--", linewidth=1.25, alpha=0.9)
            ax.text(
                xlim[0] + width * 0.015, pdl,
                "PDL (1D Low)",
                color="#42a5f5", fontsize=7.2, weight="bold", verticalalignment="top"
            )
            ax.text(
                x_badge1, pdl,
                f" [PDL - Low] {fmt_price(pdl)} ",
                color="#ffffff", fontsize=7.5, weight="bold", verticalalignment="center",
                bbox=dict(boxstyle="square,pad=0.2", facecolor="#1565c0", edgecolor="none")
            )
            pd_parts.append(f"Low: {fmt_price(pdl)}")
        if pdo:
            ax.axhline(pdo, color="#ba68c8", linestyle="--", linewidth=1.1, alpha=0.85)
            ax.text(
                xlim[0] + width * 0.015, pdo,
                "PDO (1D Open)",
                color="#ba68c8", fontsize=7.2, weight="bold", verticalalignment="bottom"
            )
            ax.text(
                x_badge2, pdo,
                f" [PDO - Open] {fmt_price(pdo)} ",
                color="#ffffff", fontsize=7.5, weight="bold", verticalalignment="center",
                bbox=dict(boxstyle="square,pad=0.2", facecolor="#6a1b9a", edgecolor="none")
            )
            pd_parts.append(f"Open: {fmt_price(pdo)}")
        if pdc:
            ax.axhline(pdc, color="#ffd54f", linestyle="--", linewidth=1.1, alpha=0.85)
            ax.text(
                xlim[0] + width * 0.015, pdc,
                "PDC (1D Close)",
                color="#ffd54f", fontsize=7.2, weight="bold", verticalalignment="bottom"
            )
            ax.text(
                x_badge2, pdc,
                f" [PDC - Close] {fmt_price(pdc)} ",
                color="#000000", fontsize=7.5, weight="bold", verticalalignment="center",
                bbox=dict(boxstyle="square,pad=0.2", facecolor="#ffd54f", edgecolor="none")
            )
            pd_parts.append(f"Close: {fmt_price(pdc)}")
        pd_ohlc_str = f" • PD-OHLC: {' | '.join(pd_parts)}" if pd_parts else ""

        if setup:
            # 1. Shaded Order Block Zone
            ax.axhspan(setup.ob_low, setup.ob_high, color="#00e5ff", alpha=0.18, zorder=1)
            ax.axhline(setup.ob_high, color="#00e5ff", linestyle=":", linewidth=0.8, alpha=0.6)
            ax.axhline(setup.ob_low, color="#00e5ff", linestyle=":", linewidth=0.8, alpha=0.6)
            ob_mid = (setup.ob_low + setup.ob_high) / 2
            ax.text(
                x_badge1, ob_mid,
                f" [OB ZONE] {fmt_price(setup.ob_low)} - {fmt_price(setup.ob_high)} ",
                color="#00e5ff", fontsize=7.5, weight="bold", verticalalignment="center",
                bbox=dict(boxstyle="square,pad=0.2", facecolor="#131722", edgecolor="#00e5ff", linewidth=0.8)
            )

            # 2. Swept Liquidity Pool Line & Tag
            if setup.sweep_level:
                ax.axhline(setup.sweep_level, color="#ffd700", linestyle="--", linewidth=1.4)
                ax.text(
                    x_badge1, setup.sweep_level,
                    f" [SWEPT LIQUIDITY] {fmt_price(setup.sweep_level)} ",
                    color="#000000", fontsize=7.5, weight="bold", verticalalignment="center",
                    bbox=dict(boxstyle="square,pad=0.2", facecolor="#ffd700", edgecolor="none")
                )

            # 3. Model Entry Line & Tag
            if setup.entry:
                ax.axhline(setup.entry, color="#00e5ff", linestyle="-", linewidth=1.2)
                ax.text(
                    x_badge1, setup.entry,
                    f" [ENTRY] {fmt_price(setup.entry)} ",
                    color="#ffffff", fontsize=7.5, weight="bold", verticalalignment="center",
                    bbox=dict(boxstyle="square,pad=0.2", facecolor="#00838f", edgecolor="none")
                )

            # 4. Stop Loss Line & Tag
            if setup.stop:
                ax.axhline(setup.stop, color="#f23645", linestyle="--", linewidth=1.3)
                ax.text(
                    x_badge1, setup.stop,
                    f" [STOP LOSS] {fmt_price(setup.stop)} ",
                    color="#ffffff", fontsize=7.5, weight="bold", verticalalignment="center",
                    bbox=dict(boxstyle="square,pad=0.2", facecolor="#b71c1c", edgecolor="none")
                )

            # 5. Take Profit Line & Tag
            if setup.target:
                ax.axhline(setup.target, color="#089981", linestyle="--", linewidth=1.3)
                ax.text(
                    x_badge1, setup.target,
                    f" [TAKE PROFIT] {fmt_price(setup.target)} ({CFG.rr:g}R) ",
                    color="#ffffff", fontsize=7.5, weight="bold", verticalalignment="center",
                    bbox=dict(boxstyle="square,pad=0.2", facecolor="#089981", edgecolor="none")
                )

            # 6. Live Current Price Marker
            ax.axhline(last_price, color="#ffffff", linestyle=":", linewidth=1.1, alpha=0.9)
            ax.text(
                x_badge1, last_price,
                f" [LIVE PRICE] {fmt_price(last_price)} ",
                color="#ffffff", fontsize=7.5, weight="bold", verticalalignment="center",
                bbox=dict(boxstyle="square,pad=0.2", facecolor="#37474f", edgecolor="#90a4ae", linewidth=0.8)
            )

            # Header Titles
            fig.text(0.06, 0.958, f"{chart_sym} [{chart_tf}] • {setup.direction} Setup • Live: {fmt_price(last_price)}", fontsize=12, weight="bold", color="#ffffff")
            fig.text(0.06, 0.922, f"Strategy: Liquidity Sweep + Order Block Retest{pd_ohlc_str} • EMA 20/50", fontsize=8.5, color="#00e5ff")
        else:
            # Market Overview (No Active Setup Triggered)
            if support:
                ax.axhline(support, color="#089981", linestyle="--", linewidth=1.3)
                ax.text(
                    x_badge1, support,
                    f" [24H SUPPORT] {fmt_price(support)} ",
                    color="#089981", fontsize=7.5, weight="bold", verticalalignment="center",
                    bbox=dict(boxstyle="square,pad=0.2", facecolor="#1e222d", edgecolor="#089981", linewidth=0.8)
                )
            if resistance:
                ax.axhline(resistance, color="#f23645", linestyle="--", linewidth=1.3)
                ax.text(
                    x_badge1, resistance,
                    f" [24H RESISTANCE] {fmt_price(resistance)} ",
                    color="#f23645", fontsize=7.5, weight="bold", verticalalignment="center",
                    bbox=dict(boxstyle="square,pad=0.2", facecolor="#1e222d", edgecolor="#f23645", linewidth=0.8)
                )

            # Live Price Marker
            ax.axhline(last_price, color="#2962ff", linestyle=":", linewidth=1.3, alpha=0.9)
            ax.text(
                x_badge1, last_price,
                f" [LIVE PRICE] {fmt_price(last_price)} ",
                color="#ffffff", fontsize=7.5, weight="bold", verticalalignment="center",
                bbox=dict(boxstyle="square,pad=0.2", facecolor="#2962ff", edgecolor="none")
            )

            fig.text(0.06, 0.958, f"{chart_sym} [{chart_tf}] • Binance Real-Time Structure • Live: {fmt_price(last_price)}", fontsize=12, weight="bold", color="#ffffff")
            fig.text(0.06, 0.922, f"Multi-Timeframe Structure{pd_ohlc_str} • EMA 20 (Blue) | EMA 50 (Orange)", fontsize=8.5, color="#90a4ae")

        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight", dpi=180, facecolor="#131722")
        plt.close(fig)
        buf.seek(0)

        # Upload directly to Cloudinary
        public_id = f"{chart_sym}_{chart_tf}_{int(time.time())}"
        upload_result = cloudinary.uploader.upload(
            buf,
            folder="crypto_trading_charts",
            public_id=public_id,
            resource_type="image",
            overwrite=True,
        )
        secure_url = upload_result.get("secure_url")
        log.info("High-precision chart generated and uploaded to Cloudinary: %s", secure_url)
        return secure_url
    except Exception as exc:
        log.warning("Chart generation or Cloudinary upload failed: %s", scrub(exc))
        return None


def generate_ai_insight(setup: Setup, now: datetime) -> Optional[Dict[str, Any]]:
    """
    Sends detected setup to Groq API using 'qwen/qwen3.8-27b' in JSON mode.
    Strictly enforces:
      1. AI Trade Decision (trade_pick: YES/NO, trade_pick_reason, ai_suggestion_for_trade).
      2. Market Regime Check (TRENDING_BULLISH, TRENDING_BEARISH, RANGING, CHOPPY).
      3. Dynamic Risk Parameters (suggested Entry, Stop Loss based on ATR/swing buffer, Take Profit, R:R).
      4. Setup Grading ('A+', 'A', 'B+', 'B', 'C', 'D') and Confidence Score (0-100).
    Never raises an exception (falls back gracefully so the bot loop continues).
    """
    if not GROQ_API_KEY:
        log.warning("Groq API key not configured (GROQ_API_KEY). Skipping AI commentary.")
        return None

    system_prompt = (
        "You are an elite quantitative crypto trader and Smart Money Concepts (ICT/SMC) risk manager.\n"
        "Analyze the provided Liquidity Sweep + Order Block trading setup.\n"
        "You MUST explicitly decide whether a trader should PICK THIS TRADE or NOT:\n"
        '- trade_pick: "YES - TAKE TRADE" or "NO - SKIP TRADE"\n'
        "- trade_pick_reason: 1 clear institutional sentence explaining why to pick or skip this trade.\n"
        "- ai_suggestion_for_trade: detailed institutional advice on exact entry trigger, trade management (e.g. breakeven rule, partial TP), or reasons to avoid/protect capital.\n\n"
        "You MUST evaluate the market regime before validating the setup:\n"
        "- Market Regime must be one of: TRENDING_BULLISH, TRENDING_BEARISH, RANGING, CHOPPY.\n"
        "- In a RANGING regime: Sweeps of range highs/lows into opposing order blocks are prime setups.\n"
        "- In a TRENDING regime: Favor trend-aligned continuation sweeps. Downgrade counter-trend sweeps to 'B' or 'C' with verdict REJECT and trade_pick 'NO - SKIP TRADE' unless strong multi-timeframe reversal divergence exists.\n"
        "- In a CHOPPY regime: Downgrade to 'C' or 'D' with verdict REJECT and trade_pick 'NO - SKIP TRADE' due to false breakout and whipsaw risk.\n\n"
        "You MUST calculate dynamic risk parameters:\n"
        "- suggested_entry: exact optimal entry level inside or at the Order Block.\n"
        "- suggested_sl: dynamic Stop Loss placed beyond the sweep wick extreme using 1.0x to 1.5x ATR buffer.\n"
        "- suggested_tp: dynamic Take Profit targeting the next major opposing liquidity pool, maintaining at least 2.0+ R:R.\n"
        "- risk_reward_ratio: calculated R:R based on suggested entry, SL, and TP.\n\n"
        "Respond ONLY with a valid JSON object matching this schema:\n"
        "{\n"
        '  "trade_pick": "YES - TAKE TRADE" | "NO - SKIP TRADE",\n'
        '  "trade_pick_reason": "clear institutional reason why to pick or skip",\n'
        '  "setup_grade": "A+" | "A" | "B+" | "B" | "C" | "D",\n'
        '  "confidence_score": integer (0-100),\n'
        '  "verdict": "APPROVE" | "REJECT",\n'
        '  "market_regime": "TRENDING_BULLISH" | "TRENDING_BEARISH" | "RANGING" | "CHOPPY",\n'
        '  "regime_analysis": "concise technical evaluation of trending vs ranging price action",\n'
        '  "dynamic_risk": {\n'
        '    "suggested_entry": float,\n'
        '    "suggested_sl": float,\n'
        '    "suggested_tp": float,\n'
        '    "risk_reward_ratio": float,\n'
        '    "sl_tp_rationale": "brief ATR and swing structure risk rationale"\n'
        "  },\n"
        '  "liquidity_target": "brief description of next opposing liquidity pool",\n'
        '  "actionable_bias": "concise execution trigger and invalidation rule",\n'
        '  "ai_suggestion_for_trade": "exact institutional advice on how to trade this setup or why to avoid it",\n'
        '  "summary_rationale": "1-2 sentence institutional breakdown"\n'
        "}"
    )

    atr_pct = (setup.atr / setup.price * 100) if setup.price else 0.0
    user_prompt = (
        f"Asset: {setup.symbol}\n"
        f"Direction: {setup.direction}\n"
        f"Timeframe: {setup.timeframe}\n"
        f"Current Price: {fmt_price(setup.price)}\n"
        f"Swept Level: {fmt_price(setup.sweep_level)} (Wick Extreme: {fmt_price(setup.sweep_extreme)})\n"
        f"Order Block Zone: {fmt_price(setup.ob_low)} - {fmt_price(setup.ob_high)}\n"
        f"ATR(14): {fmt_price(setup.atr)} ({atr_pct:.2f}%)\n"
        f"HTF Structure (4h/1h): {setup.htf_bias or 'Not available'}\n"
        f"Confluence: {setup.confluence or 'Standard Setup'}\n"
        f"Retest Status: {'Price currently inside OB zone' if setup.retest == 'live' else 'Last closed candle wicked into OB'}\n"
        f"Proposed Ref Entry: {fmt_price(setup.entry)} | SL: {fmt_price(setup.stop)} | TP: {fmt_price(setup.target)} (R:R {CFG.rr:g})\n"
        f"Timestamp: {fmt_ts(now)}"
    )

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.2,
        "max_tokens": 700,
    }

    for attempt in range(2):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                raw_content = resp.json()["choices"][0]["message"]["content"].strip()
                parsed = json.loads(raw_content)
                log.info("Groq AI response for %s %s (%s) | Pick: %s | Grade: %s | Score: %s | Regime: %s",
                         setup.symbol, setup.direction, setup.timeframe,
                         parsed.get("trade_pick"), parsed.get("setup_grade"), parsed.get("confidence_score"), parsed.get("market_regime"))
                return parsed
            elif resp.status_code == 429:
                log.warning("Groq rate limit (429). Pausing before retry...")
                time.sleep(2)
                continue
            else:
                log.warning("Groq API error HTTP %s: %s", resp.status_code, scrub(resp.text))
                break
        except Exception as exc:
            log.warning("Groq API request failed (attempt %d/2): %s", attempt + 1, scrub(exc))
            time.sleep(1)

    return None


def is_high_confidence_setup(ai_data: Optional[Dict[str, Any]]) -> bool:
    """
    Evaluates whether setup meets strict institutional quality criteria.
    Requires Grade in ('A+', 'A', 'B+'), verdict == 'APPROVE', trade_pick starting with 'YES', and confidence >= 65.
    If AI is not configured or failed, returns True to allow fallback alerts.
    """
    if not ai_data:
        return True
    grade = str(ai_data.get("setup_grade", "")).strip().upper()
    confidence = int(ai_data.get("confidence_score", 0))
    verdict = str(ai_data.get("verdict", "")).strip().upper()
    pick = str(ai_data.get("trade_pick", "")).strip().upper()

    if grade in ("A+", "A", "B+") and verdict == "APPROVE" and "YES" in pick and confidence >= 65:
        return True
    return False


def format_ai_insight_text(data: Dict[str, Any]) -> str:
    """Formats structured Groq JSON into a clean, professional institutional summary for Telegram & logging."""
    pick = data.get("trade_pick", "YES - TAKE TRADE" if data.get("verdict") == "APPROVE" else "NO - SKIP TRADE")
    pick_icon = "✅" if "YES" in str(pick).upper() else "❌"
    pick_reason = data.get("trade_pick_reason", "")
    grade = data.get("setup_grade", "N/A")
    score = data.get("confidence_score", "N/A")
    verdict = data.get("verdict", "N/A")
    regime = data.get("market_regime", "N/A")
    regime_desc = data.get("regime_analysis", "")
    risk = data.get("dynamic_risk", {}) or {}
    s_entry = risk.get("suggested_entry")
    s_sl = risk.get("suggested_sl")
    s_tp = risk.get("suggested_tp")
    rr = risk.get("risk_reward_ratio")
    rationale = risk.get("sl_tp_rationale", "")
    target = data.get("liquidity_target", "")
    action = data.get("actionable_bias", "")
    suggestion = data.get("ai_suggestion_for_trade", "")

    lines = [
        f"🤖 *AI Trade Decision:* {pick_icon} *{pick}* (Grade `{grade}` | {score}/100)",
    ]
    if pick_reason:
        lines.append(f"💬 *AI Decision Reason:* _{pick_reason}_")

    lines.append(f"🌐 *Market Regime:* `{regime}`")
    if regime_desc:
        lines.append(f"   _{regime_desc}_")

    if s_entry is not None and s_sl is not None and s_tp is not None:
        rr_str = f"{rr:g}R" if isinstance(rr, (int, float)) else f"{rr}"
        lines.append(f"🛡️ *Dynamic Risk (ATR/Structure):* Entry `{fmt_price(float(s_entry))}` | SL `{fmt_price(float(s_sl))}` | TP `{fmt_price(float(s_tp))}` ({rr_str})")
        if rationale:
            lines.append(f"   _{rationale}_")

    if target:
        lines.append(f"🎯 *Liquidity Target:* {target}")
    if suggestion:
        lines.append(f"💡 *AI Suggestion for Trade:* {suggestion}")
    elif action:
        lines.append(f"💡 *AI Suggestion for Trade:* {action}")

    return "\n".join(lines)


def generate_market_ai_suggestions_batch(assets_data: List[Dict[str, Any]]) -> Dict[str, Dict[str, str]]:
    """
    Sends all monitored assets to Groq API using 'qwen/qwen3.8-27b' in a single fast batch request
    to generate real institutional AI Trade Picks & Trade Suggestions for each crypto asset.
    Returns: Dict[symbol, {"trade_pick": str, "ai_suggestion": str}]
    """
    if not GROQ_API_KEY or not assets_data:
        return {}

    system_prompt = (
        "You are an elite quantitative crypto trader and portfolio risk manager.\n"
        "Analyze the live market data for each crypto asset.\n"
        "For EACH asset, you MUST provide:\n"
        '1. trade_pick: A short, decisive recommendation, e.g. "BUY ON DIP (LONG)", "SELL ON RALLY (SHORT)", "WAIT / NEUTRAL", "BREAKOUT WATCH", or "AVOID / CHOPPY".\n'
        "2. ai_suggestion: 1-2 punchy, institutional sentences with exact actionable price advice (key support/resistance levels, entry zones, risk warnings).\n\n"
        "Respond ONLY with a valid JSON object mapping each symbol to its analysis:\n"
        "{\n"
        '  "ETHUSDT": {\n'
        '    "trade_pick": "...",\n'
        '    "ai_suggestion": "..."\n'
        "  },\n"
        "  ...\n"
        "}"
    )

    lines = ["Live crypto assets data:"]
    for item in assets_data:
        sym = item.get("symbol")
        price = item.get("price", 0.0)
        chg = item.get("chg", 0.0)
        h = item.get("high", 0.0)
        l = item.get("low", 0.0)
        trend = item.get("trend") or "RANGING"
        pd_info = ""
        if "pdh" in item and item["pdh"]:
            pd_info = f" | PrevDay: H {fmt_price(item['pdh'])}, L {fmt_price(item['pdl'])}, O {fmt_price(item['pdo'])}, C {fmt_price(item['pdc'])}"
        lines.append(f"- {sym}: Price ${price:,.2f} | 24h: {chg:+.2f}% (Range: ${l:,.2f} - ${h:,.2f}) | Structure: {trend}{pd_info}")
    user_prompt = "\n".join(lines)

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.2,
        "max_tokens": 800
    }

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            raw = resp.json()["choices"][0]["message"]["content"].strip()
            parsed = json.loads(raw)
            log.info("Batch Groq AI trade suggestions generated for %d assets.", len(parsed))
            return parsed
        else:
            log.warning("Batch Groq AI suggestion error HTTP %s: %s", resp.status_code, scrub(resp.text))
    except Exception as exc:
        log.warning("Batch Groq AI suggestion failed: %s", scrub(exc))
    return {}


def format_alert(s: Setup, now: datetime, is_high_conf: bool = True) -> str:
    icon = "🟢" if s.direction == "LONG" else "🔴"
    retest = "Price is inside the OB zone" if s.retest == "live" else "Last closed candle wicked into the OB"
    header_tag = "🚨 *HIGH-PROBABILITY ENTRY ALERT*" if is_high_conf else "⚠️ *TRADE SETUP DETECTED (AI CAUTION)*"

    lines = [
        f"{header_tag}: *{s.symbol} ({s.timeframe})*",
        f"Setup: *Liquidity Sweep + OB Retest* | Price: `{fmt_price(s.price)}`",
        f"Time: `{fmt_ts(now)}`",
        "",
        f"{icon} *Direction:* {s.direction}   |   *Timeframe:* {s.timeframe}",
        f"💧 *Swept Liquidity:* `{fmt_price(s.sweep_level)}` (Wick to `{fmt_price(s.sweep_extreme)}`)",
        f"🧱 *Order Block Zone:* `{fmt_price(s.ob_low)} - {fmt_price(s.ob_high)}`",
        f"🔄 *Retest Status:* {retest}",
        f"🎯 *Reference Model Levels ({CFG.rr:g}R):* Entry `{fmt_price(s.entry)}` | SL `{fmt_price(s.stop)}` | TP `{fmt_price(s.target)}`",
    ]

    if s.htf_bias:
        lines.append(f"🌐 *HTF Structure (4h/1h):* {s.htf_bias}")
    if s.confluence:
        lines.append(f"📊 *Confluence:* {s.confluence}")

    if s.ai_data:
        lines.append("")
        lines.append("🤖 *Groq AI Institutional Analysis (Qwen 27B):*")
        lines.append(format_ai_insight_text(s.ai_data))
    elif s.ai_insight:
        lines.append("")
        lines.append("🤖 *Groq AI Market Intelligence:*")
        lines.append(s.ai_insight)

    if s.chart_url:
        lines.append("")
        lines.append(f"🖼️ *Institutional Chart:* [View High-Res Chart]({s.chart_url})")

    lines.append(f"📈 *Live Interactive Chart:* [Open {s.symbol} on TradingView](https://www.tradingview.com/chart/?symbol=BINANCE:{s.symbol})")

    sheet_url = get_sheet_link()
    if sheet_url:
        lines.append("")
        lines.append(f"📊 *Google Sheet:* [Open Live Sheet & Audit Log]({sheet_url})")

    lines.append("")
    lines.append("⚠️ _Quantitative alert only - not financial advice. Confirm with price action on your chart._")

    return "\n".join(lines)


# =============================================================================
# GOOGLE SHEETS
# =============================================================================
SHEET_HEADERS = {
    "Alerts": [
        "Timestamp", "Symbol", "Timeframe", "Direction", "Setup", "Price",
        "Swept Level", "OB Low", "OB High", "Ref Entry", "Ref Stop", "Ref Target",
        "Confluence", "AI Trade Pick", "AI Pick Reason", "AI Suggestion for Trade",
        "Setup Grade", "Confidence Score", "Market Regime", "Dynamic Entry",
        "Dynamic Stop Loss", "Dynamic Take Profit", "Risk Reward", "Chart URL"
    ],
    "AI Trade Suggestions": [
        "Timestamp", "Symbol", "Timeframe", "Direction", "Current Price",
        "AI Trade Pick", "AI Pick Reason", "AI Suggestion for Trade",
        "Setup Grade", "Confidence Score", "Market Regime",
        "Dynamic Entry", "Dynamic Stop Loss", "Dynamic Take Profit", "Risk Reward",
        "Liquidity Target", "Actionable Plan & Invalidation", "Chart URL"
    ],
    "Daily Summary": [
        "Date", "Scan Cycles", "Errors", "Assets Scanned", "Market Status (day change)",
        "Bias", "Setups Triggered", "Setup Details"
    ],
    "Scan Log": [
        "Timestamp", "Assets Scanned", "Prices", "Triggered Setups", "Status"
    ],
}


def get_sheet_link() -> Optional[str]:
    """Returns the direct clickable URL to the Google Sheet if configured."""
    custom_url = env_str("GOOGLE_SHEET_URL")
    if custom_url:
        return custom_url
    if GOOGLE_SHEET_ID:
        return f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/edit"
    return None


def sheets_enabled() -> bool:
    mode = env_str("ENABLE_SHEETS", "auto").lower()
    if mode in ("0", "false", "no", "off"):
        return False
    if not gspread:
        return False
    has_creds = bool(GOOGLE_CREDENTIALS_JSON or Path(GOOGLE_CREDENTIALS_FILE).exists())
    has_target = bool(GOOGLE_SHEET_ID or GOOGLE_SHEET_NAME)
    return has_creds and has_target


class SheetsLogger:
    def __init__(self) -> None:
        self.sh = None
        self.ws_cache: Dict[str, Any] = {}

    def _connect(self) -> None:
        if GOOGLE_CREDENTIALS_JSON:
            try:
                raw_json = GOOGLE_CREDENTIALS_JSON.strip()
                if (raw_json.startswith("'") and raw_json.endswith("'")) or (raw_json.startswith('"') and raw_json.endswith('"')):
                    raw_json = raw_json[1:-1].strip()
                creds_dict = json.loads(raw_json)
                if "private_key" in creds_dict and "\\n" in creds_dict["private_key"]:
                    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
                gc = gspread.service_account_from_dict(creds_dict)
                log.info("Connected to Google Sheets via .env secret (GOOGLE_CREDENTIALS_JSON)")
            except Exception as exc:
                log.warning("Failed to authenticate via GOOGLE_CREDENTIALS_JSON (.env): %s. Falling back to %s", exc, GOOGLE_CREDENTIALS_FILE)
                if Path(GOOGLE_CREDENTIALS_FILE).exists():
                    gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
                    log.info("Connected to Google Sheets via %s", GOOGLE_CREDENTIALS_FILE)
                else:
                    raise
        else:
            gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
            log.info("Connected to Google Sheets via %s", GOOGLE_CREDENTIALS_FILE)

        self.sh = gc.open_by_key(GOOGLE_SHEET_ID) if GOOGLE_SHEET_ID else gc.open(GOOGLE_SHEET_NAME)
        self.ws_cache = {}
        log.info("Connected to Google Sheet: %s", self.sh.title)

    def _worksheet(self, tab: str):
        if tab in self.ws_cache:
            return self.ws_cache[tab]
        header = SHEET_HEADERS.get(tab, [])
        try:
            ws = self.sh.worksheet(tab)
            # Ensure headers match if existing row 1 is outdated or shorter
            existing = ws.row_values(1)
            if not existing and header:
                ws.append_row(header, value_input_option="RAW")
            elif existing and len(existing) < len(header):
                try:
                    ws.insert_row(header, 1, value_input_option="RAW")
                except Exception:
                    pass
        except gspread.exceptions.WorksheetNotFound:
            ws = self.sh.add_worksheet(title=tab, rows=1000, cols=max(len(header), 12))
            if header:
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

    def update_sheet1(self, rows: list) -> None:
        """Append rows to Sheet1 with explicit AI Trade Pick & Suggestion columns."""
        headers = [
            "Timestamp", "Asset / Symbol", "Current Price", "24h High", "24h Low",
            "24h Change %", "Bias", "Scanner Status", "AI Trade Pick", "AI Suggestion for Trade"
        ]
        for attempt in range(2):
            try:
                if self.sh is None:
                    self._connect()
                try:
                    ws = self.sh.worksheet("Sheet1")
                except Exception:
                    ws = self.sh.add_worksheet(title="Sheet1", rows=1000, cols=12)

                # Check if headers exist; if empty or outdated, ensure full headers
                existing = ws.row_values(1)
                if not existing or len(existing) < len(headers):
                    try:
                        ws.insert_row(headers, 1, value_input_option="RAW")
                    except Exception:
                        ws.append_row(headers, value_input_option="RAW")

                # Append new data below old data (never clear)
                ws.append_rows(rows, value_input_option="RAW")
                return
            except Exception as exc:
                self.sh = None
                self.ws_cache = {}
                if attempt == 1:
                    log.warning("Could not append to Sheet1: %s", exc)
                time.sleep(1)


# =============================================================================
# STATE (persisted so restarts don't re-send alerts or lose queued sheet rows)
# =============================================================================
def new_stats() -> dict:
    return {"scans": 0, "errors": 0, "symbols": {}, "alerts": []}


def load_state() -> dict:
    default = {
        "alerted": {},
        "day": "",
        "stats": new_stats(),
        "sheet_queue": [],
        "custom_welcome": "",
        "stories": {},
        "active_stream": {},
    }
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
def handle_setup(setup: Setup, now: datetime, state: dict, df: Optional[pd.DataFrame] = None, client: Optional[BinanceClient] = None) -> Optional[str]:
    """Generate structured Groq AI insight, render chart to Cloudinary, apply confidence filter, send Telegram photo alert, and queue Google Sheets row."""
    log.info("SETUP %s %s (%s) | price %s | OB %s-%s", setup.symbol, setup.direction, setup.timeframe,
             fmt_price(setup.price), fmt_price(setup.ob_low), fmt_price(setup.ob_high))

    # 1. Fetch structured Groq AI insight (JSON mode)
    ai_data = generate_ai_insight(setup, now)
    setup.ai_data = ai_data
    setup.ai_insight = format_ai_insight_text(ai_data) if ai_data else None

    # Render chart to Cloudinary immediately (with PDO, PDH, PDL, PDC levels)
    chart_url = None
    if df is not None:
        pdo, pdh, pdl, pdc = get_daily_levels(client, setup.symbol) if client else (None, None, None, None)
        chart_url = generate_and_upload_chart(df, setup=setup, pdo=pdo, pdh=pdh, pdl=pdl, pdc=pdc)
    setup.chart_url = chart_url

    # Parse AI fields
    pick = ai_data.get("trade_pick", "YES - TAKE TRADE" if ai_data and ai_data.get("verdict") == "APPROVE" else "NO - SKIP TRADE") if ai_data else "N/A"
    pick_reason = ai_data.get("trade_pick_reason", ai_data.get("summary_rationale", "N/A")) if ai_data else "N/A"
    suggestion = ai_data.get("ai_suggestion_for_trade", ai_data.get("actionable_bias", "N/A")) if ai_data else "N/A"
    grade = ai_data.get("setup_grade", "N/A") if ai_data else "N/A"
    score = ai_data.get("confidence_score", "N/A") if ai_data else "N/A"
    regime = ai_data.get("market_regime", "N/A") if ai_data else "N/A"
    verdict = ai_data.get("verdict", "N/A") if ai_data else "N/A"
    risk = ai_data.get("dynamic_risk", {}) if ai_data else {}
    dyn_entry = risk.get("suggested_entry", setup.entry) or setup.entry
    dyn_sl = risk.get("suggested_sl", setup.stop) or setup.stop
    dyn_tp = risk.get("suggested_tp", setup.target) or setup.target
    dyn_rr = risk.get("risk_reward_ratio", CFG.rr) or CFG.rr
    liq_target = ai_data.get("liquidity_target", "N/A") if ai_data else "N/A"
    actionable = ai_data.get("actionable_bias", "N/A") if ai_data else "N/A"

    # 2. Check setup confidence level (High vs Caution)
    is_high_conf = is_high_confidence_setup(ai_data)
    setup_status = f"Grade {grade} ({score}/100)" if ai_data else "Raw Strategy Setup"

    # 3. Always dispatch Telegram Photo Alert with full AI suggestions and chart!
    msg = format_alert(setup, now, is_high_conf=is_high_conf)
    sent = send_telegram_chart_alert(msg, photo_url=chart_url, parse_mode="Markdown")
    if not sent and telegram_configured():
        sent = send_telegram(msg)        # retry plain text without parse_mode if Markdown parsing failed
        if not sent:
            log.warning("Alert for %s (%s) not delivered - will retry next cycle.", setup.symbol, setup.timeframe)
            return None

    state["alerted"][setup.key] = time.time()
    desc = f"{setup.symbol} {setup.direction} ({setup.timeframe}) @ {fmt_price(setup.price)}"
    if ai_data and "setup_grade" in ai_data:
        desc += f" [Pick: {pick} | Grade {grade}]"
    state["stats"]["alerts"].append(desc)

    # 4. Queue row in Google Sheets "Alerts" tab
    queue_row(state, "Alerts", [
        fmt_ts(now), setup.symbol, setup.timeframe, setup.direction,
        f"Liquidity Sweep + OB Retest [{setup_status}]",
        round(setup.price, 6), round(setup.sweep_level, 6), round(setup.ob_low, 6),
        round(setup.ob_high, 6), round(float(dyn_entry), 6), round(float(dyn_sl), 6), round(float(dyn_tp), 6),
        setup.confluence or "Standard",
        pick, pick_reason, suggestion,
        grade, score, regime,
        round(float(dyn_entry), 6), round(float(dyn_sl), 6), round(float(dyn_tp), 6), dyn_rr,
        chart_url or "N/A",
    ])

    # 5. Queue in dedicated "AI Trade Suggestions" tab
    queue_row(state, "AI Trade Suggestions", [
        fmt_ts(now), setup.symbol, setup.timeframe, setup.direction, round(setup.price, 6),
        pick, pick_reason, suggestion,
        grade, score, regime,
        round(float(dyn_entry), 6), round(float(dyn_sl), 6), round(float(dyn_tp), 6), dyn_rr,
        liq_target, actionable,
        chart_url or "N/A",
    ])
    return desc


def run_cycle(client: BinanceClient, sheets: Optional[SheetsLogger], state: dict, health: dict) -> None:
    if not SCANNING_ACTIVE.is_set():
        return
    now = datetime.now(LOCAL_TZ)
    roll_day_if_needed(state, now)
    stats = state["stats"]
    stats["scans"] += 1

    prices: Dict[str, float] = {}
    triggered: List[str] = []
    errors = 0

    for sym in SYMBOLS:
        if STOP.is_set() or not SCANNING_ACTIVE.is_set():
            break
        try:
            price = client.price(sym)
            prices[sym] = price
            update_symbol_stats(stats, sym, price)

            # 1. Fetch candles across all configured timeframes
            tf_dfs: Dict[str, pd.DataFrame] = {}
            for tf in TIMEFRAMES:
                try:
                    df = client.klines(sym, tf, KLINE_LIMIT)
                    tf_dfs[tf] = df
                except BinanceError as exc:
                    log.warning("%s (%s): %s", sym, tf, exc)
                STOP.wait(0.05)

            # 2. Compute Higher-Timeframe (HTF) trend & structure (4h, 1h)
            htf_parts = []
            for htf in ("4h", "1h"):
                if htf in tf_dfs:
                    tr = compute_trend(tf_dfs[htf])
                    ico = "🟢" if "BULL" in tr else "🔴" if "BEAR" in tr else "🟡"
                    htf_parts.append(f"{htf} {ico} {tr}")
            htf_summary = " | ".join(htf_parts) if htf_parts else None

            # 3. Detect Liquidity Sweep + OB Retest setups across all timeframes
            for tf, df in tf_dfs.items():
                for setup in detect_setup(df, sym, price, CFG, tf):
                    setup.htf_bias = htf_summary
                    if htf_summary:
                        if setup.direction == "LONG":
                            if "BULL" in htf_summary and "BEAR" not in htf_summary:
                                setup.confluence = "⚡ High Probability (Aligned with HTF Bull Trend)"
                            elif "BEAR" in htf_summary and "BULL" not in htf_summary:
                                setup.confluence = "⚠️ Counter-Trend Retracement (HTF is Bearish)"
                            else:
                                setup.confluence = "⚖️ Mixed HTF Alignment"
                        else:  # SHORT
                            if "BEAR" in htf_summary and "BULL" not in htf_summary:
                                setup.confluence = "⚡ High Probability (Aligned with HTF Bear Trend)"
                            elif "BULL" in htf_summary and "BEAR" not in htf_summary:
                                setup.confluence = "⚠️ Counter-Trend Retracement (HTF is Bullish)"
                            else:
                                setup.confluence = "⚖️ Mixed HTF Alignment"
                    elif tf in ("4h", "1h"):
                        setup.confluence = "💎 HTF Setup"

                    if setup.key in state["alerted"]:
                        continue
                    desc = handle_setup(setup, now, state, df=df, client=client)
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
        STOP.wait(0.2)                   # gentle pacing between symbols

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
# TELEGRAM ON-DEMAND INTERACTIVE LISTENER (/start, /scan, /status)
# =============================================================================
def handle_telegram_start(client: BinanceClient, sheets: Optional[SheetsLogger], state: dict, chat_id: str, is_activity_report: bool = False) -> None:
    now = datetime.now(LOCAL_TZ)
    if is_activity_report:
        send_telegram("🤖 *Main abhi analysis aur live activity report taiyar kar raha hoon... Ek pal rukhein!*", chat_id=chat_id, parse_mode="Markdown")
    else:
        send_telegram("⏳ *Fetching live market data & generating Groq AI trade suggestions...*", chat_id=chat_id, parse_mode="Markdown")

    market_raw = []
    prices_summary = []
    triggered = []
    mtf_trends = []

    # 1. Fetch 24h ticker data and multi-timeframe candles
    for sym in SYMBOLS:
        if STOP.is_set():
            break
        try:
            data_24h = client._get("/api/v3/ticker/24hr", {"symbol": sym})
            price = float(data_24h.get("lastPrice", 0))
            high = float(data_24h.get("highPrice", 0))
            low = float(data_24h.get("lowPrice", 0))
            chg = float(data_24h.get("priceChangePercent", 0))

            price_str = f"${price:,.4f}" if price < 10 else f"${price:,.2f}"
            high_str = f"${high:,.4f}" if high < 10 else f"${high:,.2f}"
            low_str = f"${low:,.4f}" if low < 10 else f"${low:,.2f}"

            prices_summary.append(f"{sym}:{fmt_price(price)}")

            # Multi-timeframe trend & setup check
            sym_tfs = []
            tf_dfs = {}
            for tf in TIMEFRAMES:
                try:
                    df = client.klines(sym, tf, KLINE_LIMIT)
                    tf_dfs[tf] = df
                    tr = compute_trend(df)
                    ico = "🟢" if "BULL" in tr else "🔴" if "BEAR" in tr else "🟡"
                    sym_tfs.append(f"{tf} {ico}")
                except Exception as exc:
                    log.warning("%s (%s): on-demand kline fetch failed: %s", sym, tf, exc)
                STOP.wait(0.05)

            if sym_tfs:
                mtf_trends.append(f"• *{sym}*: " + " | ".join(sym_tfs))

            # Confluence check for setups
            htf_parts = []
            for htf in ("4h", "1h"):
                if htf in tf_dfs:
                    tr = compute_trend(tf_dfs[htf])
                    ico = "🟢" if "BULL" in tr else "🔴" if "BEAR" in tr else "🟡"
                    htf_parts.append(f"{htf} {ico} {tr}")
            htf_summary = " | ".join(htf_parts) if htf_parts else None

            # Detect any active setups
            sym_setups = []
            for tf, df in tf_dfs.items():
                try:
                    for setup in detect_setup(df, sym, price, CFG, tf):
                        setup.htf_bias = htf_summary
                        desc = f"{setup.symbol} {setup.direction} ({tf}) @ {fmt_price(setup.price)}"
                        triggered.append(desc)
                        sym_setups.append(setup)
                except Exception as exc:
                    log.warning("%s (%s): setup check failed: %s", sym, tf, exc)

            primary_df = tf_dfs.get("15m")
            if primary_df is None and tf_dfs:
                primary_df = next(iter(tf_dfs.values()), None)

            market_raw.append({
                "sym": sym,
                "price": price,
                "high": high,
                "low": low,
                "chg": chg,
                "price_str": price_str,
                "high_str": high_str,
                "low_str": low_str,
                "htf_summary": htf_summary,
                "setups": sym_setups,
                "df_primary": primary_df,
                "tf_dfs": tf_dfs,
            })
        except Exception as exc:
            log.warning("Error fetching on-demand data for %s: %s", sym, exc)
        STOP.wait(0.1)

    # 2. Generate Real AI Trade Picks & Suggestions via Groq batch analysis
    ai_batch = generate_market_ai_suggestions_batch([
        {
            "symbol": item["sym"],
            "price": item["price"],
            "high": item["high"],
            "low": item["low"],
            "chg": item["chg"],
            "trend": item["htf_summary"] or ("BULLISH" if item["chg"] >= 0 else "BEARISH")
        }
        for item in market_raw
    ])

    # 3. Generate Live Chart Images via Cloudinary for ALL Monitored Symbols
    sym_chart_urls: Dict[str, str] = {}
    report_chart_url = None

    for item in market_raw:
        sym = item["sym"]
        sym_chart = None

        # If a Liquidity Sweep setup was detected for this symbol, render the setup chart
        for s in item["setups"]:
            s_tf = s.timeframe
            s_df = item.get("tf_dfs", {}).get(s_tf)
            if s_df is not None:
                s_pdo, s_pdh, s_pdl, s_pdc = get_daily_levels(client, s.symbol)
                s.chart_url = generate_and_upload_chart(s_df, setup=s, pdo=s_pdo, pdh=s_pdh, pdl=s_pdl, pdc=s_pdc)
                if s.chart_url:
                    sym_chart = s.chart_url
                    if not report_chart_url:
                        report_chart_url = s.chart_url
                    break

        # If no setup triggered for this symbol, render its real-time multi-timeframe structure chart with PD-OHLC!
        if not sym_chart:
            sym_df = item.get("df_primary")
            if sym_df is None:
                try:
                    sym_df = client.klines(sym, "15m", 65)
                except Exception:
                    pass
            if sym_df is not None:
                pdo, pdh, pdl, pdc = get_daily_levels(client, sym)
                sym_chart = generate_and_upload_chart(
                    sym_df,
                    symbol=sym,
                    timeframe="15m",
                    support=item.get("low"),
                    resistance=item.get("high"),
                    pdo=pdo,
                    pdh=pdh,
                    pdl=pdl,
                    pdc=pdc,
                )

        if sym_chart:
            sym_chart_urls[sym] = sym_chart
            if not report_chart_url:
                report_chart_url = sym_chart

    # Select the most volatile/top moving symbol for the featured Telegram report photo if no active setup triggered
    if not report_chart_url and market_raw:
        top_item = max(market_raw, key=lambda it: abs(it.get("chg", 0.0)))
        report_chart_url = sym_chart_urls.get(top_item["sym"])

    if is_activity_report:
        status_str = "🟢 ACTIVE & SCANNING 24/7" if SCANNING_ACTIVE.is_set() else "🛑 PAUSED (Band)"
        stats = state.get("stats", {})
        total_scans = stats.get("scans", 0)
        total_alerts = len(stats.get("alerts", []))
        lines = [
            "🤖 *BOT LIVE ACTIVITY & ANALYSIS REPORT*",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "📌 *Sawaal:* _What are you doing? / Kya kar rahe ho?_",
            "",
            f"⚡ *Bot Status:* {status_str}",
            f"🕒 *Current Time:* {fmt_ts(now)}",
            f"⏱️ *Scan Interval:* Har {SCAN_INTERVAL} seconds me automated scan",
            f"🎯 *Strategy:* Liquidity Sweep + Displacement (MSS) + Order Block Retest",
            f"🧠 *AI Engine:* Groq `{GROQ_MODEL}` (Market Regime + Dynamic ATR SL/TP)",
            "",
            "📋 *Abhi Kya Kaam Ho Raha Hai:*",
            f"1. *Multi-Asset Monitor:* 7 assets ({', '.join(SYMBOLS)}) par 24/7 tracking chal rahi hai.",
            f"2. *Multi-Timeframe Scan:* {', '.join(TIMEFRAMES)} candles par liquidity grab aur OB zones detect ho rahe hain.",
            "3. *Groq AI Filtering:* Weak setups (< Grade B+) ko suppress karke sirf high-probability trades filter kiye ja rahe hain.",
            "4. *Google Sheets Sync:* Har alert aur AI suggestion live Google Sheet me cloud-logged ho raha hai.",
            "",
            "📈 *Aaj Ki Activity Stats:*",
            f"• Total Scan Cycles: `{total_scans}`",
            f"• High-Probability Alerts: `{total_alerts}`",
            f"• Google Sheets Connected: `{'YES ✅' if SHEETS_ON else 'NO ❌'}`",
            "",
            "📊 *Live Market Snapshot & Current AI Picks:*"
        ]
    else:
        lines = [
            "📊 *MULTI-TIMEFRAME SCANNER & AI TRADE SUGGESTIONS*",
            "",
            f"🕒 *Time:* {fmt_ts(now)}",
            f"⏱️ *Active Timeframes:* {', '.join(TIMEFRAMES)}",
            "",
            "🤖 *AI Trade Picks & Suggestions for All Monitored Assets:*"
        ]

    market_rows = []
    for item in market_raw:
        sym = item["sym"]
        price = item["price"]
        high = item["high"]
        low = item["low"]
        chg = item["chg"]
        htf_summary = item["htf_summary"]
        sym_setups = item["setups"]

        ai_item = ai_batch.get(sym, {})
        ai_pick = ai_item.get("trade_pick", "BUY ON DIP (LONG)" if chg > 1 else "WAIT / NEUTRAL")
        ai_sugg = ai_item.get("ai_suggestion", f"24h change {chg:+.2f}%. Await key support retest.")

        # If a live Liquidity Sweep setup was detected, prioritize that setup
        if sym_setups:
            s = sym_setups[0]
            ai_pick = f"YES - {s.direction} ({s.timeframe})"
            ai_sugg = f"Liquidity sweep at {fmt_price(s.sweep_level)}. Retest in OB zone ({fmt_price(s.ob_low)} - {fmt_price(s.ob_high)}). Entry {fmt_price(s.entry)}, SL {fmt_price(s.stop)}, TP {fmt_price(s.target)}."

        chart_link_md = f" • [📊 Chart]({sym_chart_urls[sym]})" if sym in sym_chart_urls else ""
        lines.append(f"• *{sym}*: `{item['price_str']}` ({chg:+.2f}%) | 🤖 *Pick:* `{ai_pick}`{chart_link_md}")
        lines.append(f"  💡 *AI Suggestion:* _{ai_sugg}_")

        market_rows.append([
            now.strftime("%Y-%m-%d %H:%M:%S"),
            sym,
            item["price_str"],
            item["high_str"],
            item["low_str"],
            f"{chg:+.2f}%",
            "BULLISH" if chg >= 0 else "BEARISH",
            "ACTIVE SCANNING",
            ai_pick,
            ai_sugg
        ])

        sym_chart = sym_chart_urls.get(sym, report_chart_url or "N/A")

        # Also queue directly into dedicated "AI Trade Suggestions" Google Sheet tab!
        queue_row(state, "AI Trade Suggestions", [
            fmt_ts(now), sym, "LIVE SCAN (15m/1h/4h)", "OVERVIEW", round(price, 6),
            ai_pick, f"24h change {chg:+.2f}%, Trend: {htf_summary or 'Neutral'}",
            ai_sugg, "A" if "YES" in ai_pick else "B", 75, htf_summary or "RANGING",
            round(price, 6), round(low, 6), round(high, 6), "2.0R",
            f"24h High: ${high:,.2f}", f"Key Support: ${low:,.2f} | Key Resistance: ${high:,.2f}",
            sym_chart
        ])

        # If any setups triggered, also log to Alerts tab
        for s in sym_setups:
            queue_row(state, "Alerts", [
                fmt_ts(now), s.symbol, s.timeframe, s.direction, "Liquidity Sweep + OB Retest",
                round(s.price, 6), round(s.sweep_level, 6), round(s.ob_low, 6),
                round(s.ob_high, 6), round(s.entry, 6), round(s.stop, 6), round(s.target, 6),
                s.confluence or "Standard",
                ai_pick, "On-demand sweep detected", ai_sugg,
                "A", 80, htf_summary or "RANGING",
                round(s.entry, 6), round(s.stop, 6), round(s.target, 6), CFG.rr,
                s.chart_url or sym_chart or "N/A"
            ])

    if mtf_trends:
        lines.append("")
        lines.append("🌐 *Multi-Timeframe Trend Matrix (EMA Structure):*")
        lines.extend(mtf_trends)

    lines.append("")
    lines.append("🎯 *Active Setups / Signals:*")
    if triggered:
        for t in triggered:
            lines.append(f"🚨 *{t}*")
    else:
        if is_activity_report:
            lines.append(f"• Abhi koi live sweep trigger nahi hua hai across {', '.join(TIMEFRAMES)}. Market actively monitor ho rahi hai aur next optimal order block retest ka wait chal raha hai.")
        else:
            lines.append(f"• No active sweep trigger right now across {', '.join(TIMEFRAMES)}. Follow AI suggestions above.")

    sheets_status = "Google Sheets Logging OFF"
    if sheets:
        try:
            sheets.update_sheet1(market_rows)
            queue_row(state, "Scan Log", [
                fmt_ts(now), ", ".join(SYMBOLS),
                " | ".join(prices_summary),
                ", ".join(triggered) or "None",
                "ACTIVITY REPORT" if is_activity_report else "TELEGRAM ON-DEMAND",
            ])
            flush_sheet_queue(state, sheets)
            save_state(state)
            sheets_status = "Google Sheet (Sheet1 & AI Trade Suggestions) Updated ✅"
        except Exception as exc:
            log.warning("On-demand sheet update failed: %s", exc)
            sheets_status = "Google Sheet update failed"

    if report_chart_url:
        lines.append("")
        lines.append(f"🖼️ *Live Candlestick Chart:* [View High-Res Chart Image]({report_chart_url})")

    target_sym = "BTCUSDT" if "BTCUSDT" in SYMBOLS else SYMBOLS[0]
    lines.append(f"📈 *Live TradingView Chart:* [Open {target_sym} on TradingView](https://www.tradingview.com/chart/?symbol=BINANCE:{target_sym})")

    lines.append("")
    sheet_url = get_sheet_link()
    if sheet_url:
        lines.append(f"📊 *Google Sheet:* [Open Live Sheet & Audit Log]({sheet_url})")
    lines.append(f"📋 *Sheet Status:* {sheets_status}")
    status_indicator = "🟢 Analysis Active 24/7 (Send 'end' to pause)" if SCANNING_ACTIVE.is_set() else "🛑 Analysis Paused (Send 'start' to resume)"
    lines.append(f"🤖 *Status:* {status_indicator}")

    if is_activity_report:
        lines.append("")
        lines.append("💡 *Quick Actions:*")
        lines.append("• Send `end` - Analysis pause karne ke liye")
        lines.append("• Send `start` - Analysis resume / live sync karne ke liye")
        lines.append("• Send `/scan` - Instant sweep scan ke liye")

    msg_text = "\n".join(lines)
    send_telegram_chart_alert(msg_text, photo_url=report_chart_url, chat_id=chat_id, parse_mode="Markdown")


def handle_telegram_charts(client: BinanceClient, chat_id: str, requested_sym: Optional[str] = None) -> None:
    """
    Generates and sends live institutional charts for all monitored symbols (or a requested symbol)
    directly to Telegram, along with each symbol's:
      - Exact Previous 1-Day Candle OHLC levels (PDH, PDL, PDO, PDC)
      - Groq AI Trade Decision / Pick (YES/NO/BUY/WAIT)
      - Actionable Groq AI Suggestion for Trade
    """
    targets = [requested_sym] if (requested_sym and requested_sym in SYMBOLS) else SYMBOLS
    sym_count = len(targets)
    if sym_count > 1:
        send_telegram(
            f"🖼️ *Sabhi {sym_count} symbols ke live charts, PD-OHLC levels aur Groq AI Trade Suggestions taiyar ho rahe hain... Ek pal rukhein!*",
            chat_id=chat_id,
            parse_mode="Markdown",
        )
    else:
        send_telegram(
            f"🖼️ *{targets[0]} ka live chart, PD-OHLC levels aur AI suggestion taiyar ho raha hai...*",
            chat_id=chat_id,
            parse_mode="Markdown",
        )

    # 1. Pre-fetch market data & daily levels to generate batch Groq AI suggestions
    market_items = []
    symbol_data = {}
    for sym in targets:
        if STOP.is_set():
            break
        try:
            try:
                data_24h = client._get("/api/v3/ticker/24hr", {"symbol": sym})
                price = float(data_24h.get("lastPrice", 0))
                high = float(data_24h.get("highPrice", 0))
                low = float(data_24h.get("lowPrice", 0))
                chg = float(data_24h.get("priceChangePercent", 0))
            except Exception:
                price = client.price(sym)
                high, low, chg = price, price, 0.0

            pdo, pdh, pdl, pdc = get_daily_levels(client, sym)
            item = {
                "symbol": sym,
                "price": price,
                "high": high,
                "low": low,
                "chg": chg,
                "pdo": pdo,
                "pdh": pdh,
                "pdl": pdl,
                "pdc": pdc,
                "trend": "BULLISH" if chg >= 0 else "BEARISH",
            }
            market_items.append(item)
            symbol_data[sym] = item
        except Exception as exc:
            log.warning("Could not pre-fetch data for %s: %s", sym, exc)

    # 2. Generate Real AI Trade Picks & Suggestions via Groq batch analysis
    ai_batch = generate_market_ai_suggestions_batch(market_items) if market_items else {}

    # 3. Render and dispatch individual chart alerts
    sent_count = 0
    for sym in targets:
        if STOP.is_set():
            break
        try:
            item = symbol_data.get(sym)
            if not item:
                continue

            price = item["price"]
            high = item["high"]
            low = item["low"]
            chg = item["chg"]
            pdo = item["pdo"]
            pdh = item["pdh"]
            pdl = item["pdl"]
            pdc = item["pdc"]

            price_str = f"${price:,.4f}" if price < 10 else f"${price:,.2f}"
            high_str = f"${high:,.4f}" if high < 10 else f"${high:,.2f}"
            low_str = f"${low:,.4f}" if low < 10 else f"${low:,.2f}"

            # Fetch 15m candles
            df = client.klines(sym, "15m", 65)
            if df is None or len(df) < 10:
                log.warning("Not enough candles to generate chart for %s", sym)
                continue

            # Generate high-precision institutional chart
            chart_url = generate_and_upload_chart(
                df,
                symbol=sym,
                timeframe="15m",
                support=low,
                resistance=high,
                pdo=pdo,
                pdh=pdh,
                pdl=pdl,
                pdc=pdc,
            )

            # AI trade decision and suggestion
            ai_item = ai_batch.get(sym, {})
            ai_pick = ai_item.get(
                "trade_pick",
                "BUY ON DIP (LONG)" if chg > 0.5 else "SELL ON RALLY (SHORT)" if chg < -0.5 else "WAIT / NEUTRAL"
            )
            ai_sugg = ai_item.get(
                "ai_suggestion",
                f"24h change {chg:+.2f}%. Monitor PDH ({fmt_price(pdh)}) and PDL ({fmt_price(pdl)}) for institutional liquidity grab."
            )

            chg_sign = "+" if chg >= 0 else ""
            pdh_val = fmt_price(pdh) if pdh else "N/A"
            pdl_val = fmt_price(pdl) if pdl else "N/A"
            pdo_val = fmt_price(pdo) if pdo else "N/A"
            pdc_val = fmt_price(pdc) if pdc else "N/A"

            caption = (
                f"📊 *{sym} Institutional Structure Chart (15m)*\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"💰 *Live Price:* `{price_str}` ({chg_sign}{chg:.2f}%)\n"
                f"📈 *24h Range:* Low `{low_str}` ➔ High `{high_str}`\n\n"
                f"📍 *Previous 1-Day Candle Levels (PD-OHLC):*\n"
                f"• 🟠 *PDH (Prev Day High):* `{pdh_val}`\n"
                f"• 🔵 *PDL (Prev Day Low):* `{pdl_val}`\n"
                f"• 🟣 *PDO (Prev Day Open):* `{pdo_val}`\n"
                f"• 🟡 *PDC (Prev Day Close):* `{pdc_val}`\n\n"
                f"🤖 *AI Trade Pick:* `{ai_pick}`\n"
                f"💡 *AI Suggestion for Trade:* _{ai_sugg}_\n\n"
                f"🌐 *Overlays:* EMA 20 (Blue) | EMA 50 (Orange)\n"
                f"🔗 [Open Live Chart on TradingView](https://www.tradingview.com/chart/?symbol=BINANCE:{sym})"
            )

            send_telegram_chart_alert(caption, photo_url=chart_url, chat_id=chat_id, parse_mode="Markdown")
            sent_count += 1
            STOP.wait(0.7)  # Gentle pacing to respect Telegram rate limits
        except Exception as exc:
            log.warning("Failed to generate/send chart for %s: %s", sym, exc)

    if sym_count > 1 and sent_count > 0:
        send_telegram(
            f"✅ *Sabhi {sent_count} symbols ke charts, PD-OHLC levels aur AI Suggestions deliver ho gaye hain!*",
            chat_id=chat_id,
            parse_mode="Markdown",
        )


def ask_groq_trading_question(question: str, live_context: str = "") -> str:
    """
    Sends a user trading question to Groq AI (qwen/qwen3.8-27b) with an institutional
    crypto quantitative analyst prompt. Formats and returns an institutional, actionable answer.
    """
    if not GROQ_API_KEY:
        return "⚠️ Groq API key is not configured in `.env` (`GROQ_API_KEY`). Please configure it to get AI trading answers."

    system_prompt = (
        "You are an elite institutional quantitative cryptocurrency analyst and Smart Money Concepts (ICT/SMC) trading mentor.\n"
        "Your task is to answer user trading questions with high technical precision, practical clarity, and institutional risk management.\n\n"
        "Core Trading Framework:\n"
        "1. Smart Money Concepts: Liquidity Sweeps (BSL/SSL), Displacement (ATR expansion), Market Structure Shifts (MSS), Order Blocks (OB), and Fair Value Gaps (FVG).\n"
        "2. Daily Key Levels: Previous Day High (PDH), Low (PDL), Open (PDO), Close (PDC).\n"
        "3. Risk Management: Position sizing (1-2% max risk per trade), RR >= 2.0, precise invalidation (Stop Loss), and dynamic targets.\n"
        "4. Tone: Professional, realistic, direct. If the user asks in Hindi or Hinglish, reply in clear, professional Hinglish. If in English, reply in English.\n"
        "5. Keep the answer structured using bold headings, bullet points, and concise explanations so it reads beautifully on Telegram.\n"
        "6. Always include a short risk disclaimer at the end."
    )

    user_content = f"User Question: {question}"
    if live_context:
        user_content += f"\n\nLive Market & Quantitative Data:\n{live_context}"

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": GROQ_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.3,
        "max_tokens": 800,
    }

    for attempt in range(2):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                raw_content = resp.json()["choices"][0]["message"]["content"].strip()
                return raw_content
            elif resp.status_code == 429:
                time.sleep(2)
                continue
            else:
                log.warning("Groq AI question error HTTP %s: %s", resp.status_code, scrub(resp.text))
                return f"⚠️ Groq AI API error (HTTP {resp.status_code}). Please try again."
        except Exception as exc:
            log.warning("Groq AI question request failed: %s", scrub(exc))
            time.sleep(1)

    return "⚠️ Groq AI se connect karne me problem aayi. Please ek baar phir try karein."


def handle_telegram_trading_question(
    client: BinanceClient,
    chat_id: str,
    question: str,
    reply_to_message_id: Optional[int] = None,
    replied_text: Optional[str] = None,
) -> None:
    """Handles an interactive trading question from Telegram and replies directly to user using Groq AI."""
    clean_q = question.strip()
    bot_info = get_bot_info()
    bot_user = (bot_info.get("username") or "").lower()

    # Strip prefixes like @ai, #ai, /ai, ai, /ask, ask, @tado, tado, /tado, @botusername
    patterns_to_strip = [
        "/ask", "ask", "/ai", "ai", "@ai", "#ai", "/q", "q",
        "@tado", "tado", "/tado", "#tado", "@todo", "todo",
    ]
    if bot_user:
        patterns_to_strip.extend([f"@{bot_user}", f"/ask@{bot_user}", f"/ai@{bot_user}", f"/tado@{bot_user}"])

    for prefix in patterns_to_strip:
        p_len = len(prefix)
        if clean_q.lower().startswith(prefix + " ") or clean_q.lower().startswith(prefix + ":") or clean_q.lower().startswith(prefix + ","):
            clean_q = clean_q[p_len:].strip(": ,")
            break

    clean_q = re.sub(r'^(?:@ai|#ai|ai|@tado|#tado|tado|@todo|todo)\b[:,\s]*', '', clean_q, flags=re.IGNORECASE).strip()
    if bot_user:
        clean_q = re.sub(rf'^@{re.escape(bot_user)}\b[:,\s]*', '', clean_q, flags=re.IGNORECASE).strip()

    if not clean_q or len(clean_q) < 3:
        send_telegram(
            "❓ *Aapka sawal bahut chota hai.* Kripya apna trading sawal detail me puchein.\n\n"
            "*Examples:*\n"
            "• `ai BTC me long entry kaha le sakte hain?`\n"
            "• `@ai Order Block aur FVG me kya difference hai?`\n"
            "• `@ai Risk-to-reward ratio kaise calculate karein?`\n"
            "• `/ask ETH ka current trend aur key levels kya hain?`",
            chat_id=chat_id,
            parse_mode="Markdown",
            reply_to_message_id=reply_to_message_id,
        )
        return

    send_telegram(
        f"🧠 *Groq AI Analyst ({GROQ_MODEL}) aapke trading question ka analysis kar raha hai...*",
        chat_id=chat_id,
        parse_mode="Markdown",
        reply_to_message_id=reply_to_message_id,
    )

    # Detect if any monitored symbol is mentioned to provide live quantitative context
    live_context = ""
    words = [w.strip(",.!?#/@\"'").upper() for w in clean_q.split()]
    detected_sym = None
    for s in SYMBOLS:
        base = s.replace("USDT", "")
        if base in words or s in words or base.lower() in clean_q.lower():
            detected_sym = s
            break

    if detected_sym:
        try:
            cur_price = client.price(detected_sym)
            pdo, pdh, pdl, pdc = get_daily_levels(client, detected_sym)
            live_context = (
                f"Asset: {detected_sym}\n"
                f"Current Live Price: {fmt_price(cur_price)}\n"
            )
            if pdh and pdl:
                live_context += (
                    f"Previous Day High (PDH): {fmt_price(pdh)}\n"
                    f"Previous Day Low (PDL): {fmt_price(pdl)}\n"
                    f"Previous Day Open (PDO): {fmt_price(pdo)}\n"
                    f"Previous Day Close (PDC): {fmt_price(pdc)}\n"
                )
        except Exception as exc:
            log.warning("Could not fetch live context for %s: %s", detected_sym, exc)

    if replied_text:
        live_context += f"\nContext from user's quoted/replied message:\n\"{replied_text[:400]}\"\n"

    ai_reply = ask_groq_trading_question(clean_q, live_context=live_context)
    full_msg = f"💡 *Groq AI Institutional Trading Answer:*\n━━━━━━━━━━━━━━━━━━━━\n{ai_reply}"

    # Split if Telegram message limit (4096) is exceeded
    if len(full_msg) > 4000:
        chunks = [full_msg[i:i+3800] for i in range(0, len(full_msg), 3800)]
        for chunk in chunks:
            send_telegram(chunk, chat_id=chat_id, parse_mode="Markdown", reply_to_message_id=reply_to_message_id)
    else:
        send_telegram(full_msg, chat_id=chat_id, parse_mode="Markdown", reply_to_message_id=reply_to_message_id)


def telegram_listener_loop(client: BinanceClient, sheets: Optional[SheetsLogger], state: dict) -> None:
    if not TELEGRAM_TOKEN:
        return
    offset = 0
    try:
        r = requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates", params={"limit": 1}, timeout=10)
        data = r.json()
        if data.get("ok") and data.get("result"):
            offset = data["result"][-1]["update_id"] + 1
    except Exception:
        pass

    log.info("Telegram command listener active (listening for /start, start, /scan, /status)...")

    while not STOP.is_set():
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
            resp = requests.get(url, params={"offset": offset, "timeout": 10}, timeout=20)
            if resp.status_code != 200:
                STOP.wait(3)
                continue
            res = resp.json()
            if not res.get("ok"):
                STOP.wait(3)
                continue

            for update in res.get("result", []):
                offset = max(offset, update["update_id"] + 1)
                msg = update.get("message") or update.get("edited_message")
                if not msg:
                    continue
                chat = msg.get("chat", {})
                chat_id = str(chat.get("id"))
                chat_type = chat.get("type", "private")  # "private", "group", "supergroup", "channel"
                chat_title = chat.get("title") or "Trading Community"
                user = msg.get("from", {})
                user_id = user.get("id")
                user_name = user.get("first_name") or user.get("username") or "Trader"
                message_id = msg.get("message_id")

                # --- 1. WELCOME NEW MEMBERS ---
                new_members = msg.get("new_chat_members")
                if new_members:
                    bot_info = get_bot_info()
                    bot_id = bot_info.get("id")
                    for m in new_members:
                        if m.get("id") == bot_id:
                            # The bot itself was added to the group
                            send_telegram(
                                f"👋 *Namaste {chat_title}!*\n\n"
                                f"🤖 *Crypto Liquidity Sweep & SMC AI Trading Bot* active ho gaya hai.\n\n"
                                f"• 📊 *Real-time Signals:* Institutional 24/7 Liquidity Sweep + Order Block alerts.\n"
                                f"• 🧠 *Groq AI Analysis:* Market Structure & Trade Grading (Qwen 27B).\n"
                                f"• 📈 *Visual Charts:* Candlestick charts with PD-OHLC (Daily High/Low) levels.\n"
                                f"• 🛡️ *Group Security:* Active Auto-Moderation & Anti-Spam protection.\n\n"
                                f"👉 Type `/help` ya `/charts` to begin!",
                                chat_id=chat_id,
                                parse_mode="Markdown",
                            )
                        else:
                            # A new member joined the group
                            m_name = m.get("first_name") or m.get("username") or "Trader"
                            m_username = f"@{m.get('username')}" if m.get("username") else m_name
                            custom_w = state.get("custom_welcome")
                            if custom_w:
                                welcome_msg = (
                                    custom_w
                                    .replace("{name}", m_name)
                                    .replace("{user}", m_username)
                                    .replace("{chat_title}", chat_title)
                                )
                            else:
                                welcome_msg = (
                                    f"👋 *Welcome to {chat_title}, {m_name}!*\n\n"
                                    f"📈 *Yahan aapko live institutional crypto market alerts aur AI analysis milega.*\n\n"
                                    f"💡 *Quick Start Commands:*\n"
                                    f"• `/charts` - Sabhi coins ke live charts + PDH/PDL levels\n"
                                    f"• `/scan` - Instant sweep aur Order Block scan\n"
                                    f"• `/ask <question>` - Groq AI se koi bhi trading question puchein\n"
                                    f"• `/rules` - Group guidelines & risk management rules\n\n"
                                    f"⚠️ *Note:* Spamming, unauthorized links, ya direct promotions strictly prohibited hain."
                                )
                            send_telegram(welcome_msg, chat_id=chat_id, parse_mode="Markdown")
                    continue

                text = (msg.get("text") or "").strip()
                if not text:
                    continue

                log.info("Telegram incoming from chat %s ('%s', user '%s'): '%s'", chat_id, chat_title, user_name, text)

                # --- 2. ANTI-SPAM & AUTO-MODERATION (GROUPS) ---
                if chat_type in ("group", "supergroup"):
                    if is_spam_message(text):
                        if not is_telegram_admin(chat_id, user_id):
                            telegram_delete_message(chat_id, message_id)
                            user_tag = f"@{user.get('username')}" if user.get("username") else user_name
                            warn_msg = (
                                f"⚠️ *Warning:* {user_tag}, promotional links aur spamming group me strictly prohibited hai!\n"
                                f"_Aapka message Auto-Moderator ne delete kar diya hai._"
                            )
                            send_telegram(warn_msg, chat_id=chat_id, parse_mode="Markdown")
                            continue

                # Normalize text & command
                bot_info = get_bot_info()
                bot_username = (bot_info.get("username") or "").lower()
                clean_lower = text.lower().strip()
                first_word = clean_lower.split()[0] if clean_lower else ""
                # Strip @botusername from command e.g. /scan@my_bot -> /scan
                cmd = first_word.split("@")[0] if "@" in first_word else first_word

                # Check if bot was explicitly tagged or message is reply to bot
                is_bot_tagged = bool(bot_username and f"@{bot_username}" in clean_lower)
                reply_to_msg = msg.get("reply_to_message")
                is_reply_to_bot = bool(reply_to_msg and reply_to_msg.get("from", {}).get("id") == bot_info.get("id"))

                # --- 3. GROUP MODERATION & ADMIN COMMANDS (7 PERMISSIONS) ---

                # Permission 1: Change Channel Info
                if cmd in ("/settitle", "settitle"):
                    if is_telegram_admin(chat_id, user_id):
                        new_t = text[len(first_word):].strip()
                        if new_t:
                            if telegram_set_title(chat_id, new_t):
                                send_telegram(f"✅ *Group title updated to:* `{new_t}`", chat_id=chat_id, parse_mode="Markdown")
                            else:
                                send_telegram("❌ Title change karne me error aayi. Please bot permissions check karein.", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage: `/settitle <Naya Title>`", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf group administrators hi title change kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/setdesc", "setdesc", "/setdescription", "setdescription"):
                    if is_telegram_admin(chat_id, user_id):
                        new_d = text[len(first_word):].strip()
                        if new_d:
                            if telegram_set_description(chat_id, new_d):
                                send_telegram("✅ *Group description successfully updated!*", chat_id=chat_id, parse_mode="Markdown")
                            else:
                                send_telegram("❌ Description change karne me error aayi. Please bot permissions check karein.", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage: `/setdesc <Naya Description>`", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf group administrators hi description change kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/delphoto", "delphoto", "/deletephoto", "deletephoto"):
                    if is_telegram_admin(chat_id, user_id):
                        if telegram_delete_chat_photo(chat_id):
                            send_telegram("✅ *Group/Channel photo successfully removed.*", chat_id=chat_id, parse_mode="Markdown")
                        else:
                            send_telegram("❌ Photo delete karne me error aayi. Please bot permissions verify karein.", chat_id=chat_id)
                    else:
                        send_telegram("❌ Sirf group administrators hi chat photo delete kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/channelinfo", "channelinfo", "/chatinfo", "chatinfo"):
                    chat_info = telegram_get_chat(chat_id)
                    member_cnt = telegram_get_member_count(chat_id)
                    c_title = chat_info.get("title", chat_title) if chat_info else chat_title
                    c_desc = chat_info.get("description", "None") if chat_info else "None"
                    c_type = chat_info.get("type", chat_type) if chat_info else chat_type
                    cnt_str = f"{member_cnt:,}" if member_cnt else "N/A"
                    info_msg = (
                        f"📋 *Channel / Group Information:*\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"• *Title:* {c_title}\n"
                        f"• *Type:* `{c_type}`\n"
                        f"• *Chat ID:* `{chat_id}`\n"
                        f"• *Total Members:* {cnt_str}\n"
                        f"• *Description:* {c_desc}\n\n"
                        f"🛡️ *Configured Bot Permissions Status:*\n"
                        f"  ✅ *Change Channel Info* (`/settitle`, `/setdesc`, `/delphoto`)\n"
                        f"  ✅ *Manage Messages* (Post `/announce`, Edit `/edit`, Delete `/del`, `/purge`)\n"
                        f"  ✅ *Manage Stories* (Post `/story`, Edit `/editstory`, Delete `/delstory`)\n"
                        f"  ✅ *Manage Direct Messages* (`/dm`, `/dmme`, 1-on-1 AI Assistant)\n"
                        f"  ✅ *Send Welcome Messages* (Auto-greeting + `/setwelcome`)\n"
                        f"  ✅ *Manage Live Streams* (`/livestream`, `/endstream`, `/streamschedule`)\n"
                        f"  ✅ *Ban Users* (`/ban`, `/unban`, `/kick`, `/mute`, `/unmute`)"
                    )
                    send_telegram(info_msg, chat_id=chat_id, parse_mode="Markdown")
                    continue

                # Permission 2: Manage Messages (Post, Edit, Delete)
                if cmd in ("/announce", "announce", "/broadcast", "broadcast"):
                    if is_telegram_admin(chat_id, user_id):
                        ann_text = text[len(first_word):].strip()
                        if ann_text:
                            ann_msg = (
                                f"📢 *OFFICIAL ANNOUNCEMENT*\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"{ann_text}\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"👤 _Posted by Administrator_"
                            )
                            send_telegram(ann_msg, chat_id=chat_id, parse_mode="Markdown")
                        else:
                            send_telegram("ℹ️ Usage: `/announce <Aapka message>`", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators announcement bhej sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/post", "post", "/say", "say"):
                    if is_telegram_admin(chat_id, user_id):
                        raw_msg = text[len(first_word):].strip()
                        if raw_msg:
                            telegram_delete_message(chat_id, message_id)
                            send_telegram(raw_msg, chat_id=chat_id, parse_mode="Markdown")
                        else:
                            send_telegram("ℹ️ Usage: `/post <Aapka message>`", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators hi `/post` command use kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/edit", "edit"):
                    if is_telegram_admin(chat_id, user_id):
                        if reply_to_msg:
                            new_text = text[len(first_word):].strip()
                            if new_text:
                                target_mid = reply_to_msg.get("message_id")
                                if telegram_edit_message(chat_id, target_mid, new_text):
                                    telegram_delete_message(chat_id, message_id)
                                else:
                                    send_telegram("❌ Message edit nahi ho saka. (Note: Bot sirf apne messages edit kar sakta hai).", chat_id=chat_id)
                            else:
                                send_telegram("ℹ️ Usage: Kisi message ko reply karke `/edit <Naya Text>` likhein.", chat_id=chat_id, parse_mode="Markdown")
                        else:
                            send_telegram("ℹ️ Jis message ko edit karna hai, usko reply karke `/edit <Naya Text>` likhein.", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators hi messages edit kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/del", "del", "/delete", "delete"):
                    if is_telegram_admin(chat_id, user_id):
                        if reply_to_msg:
                            telegram_delete_message(chat_id, reply_to_msg.get("message_id"))
                            telegram_delete_message(chat_id, message_id)
                        else:
                            send_telegram("ℹ️ Kisi message ko delete karne ke liye us message ko reply karke `/del` likhein.", chat_id=chat_id)
                    else:
                        send_telegram("❌ Sirf group administrators hi `/del` command use kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/purge", "purge", "/clean", "clean"):
                    if is_telegram_admin(chat_id, user_id):
                        count_arg = text[len(first_word):].strip()
                        try:
                            count = int(count_arg.split()[0]) if count_arg else 5
                            count = min(max(1, count), 100)
                        except Exception:
                            count = 5
                        mids_to_del = [message_id - i for i in range(count + 1)]
                        ok = telegram_delete_messages(chat_id, mids_to_del)
                        if not ok:
                            for m in mids_to_del:
                                telegram_delete_message(chat_id, m)
                        send_telegram(f"🧹 *{count} messages successfully clean kar diye gaye hain.*", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators hi `/purge` command use kar sakte hain.", chat_id=chat_id)
                    continue

                # Permission 3: Manage Stories (Post, Edit, Delete)
                if cmd in ("/story", "story", "/poststory", "poststory"):
                    if is_telegram_admin(chat_id, user_id):
                        story_content = text[len(first_word):].strip()
                        if story_content:
                            story_id = f"story_{int(time.time()) % 100000}"
                            parts = story_content.split("|", 1)
                            headline = parts[0].strip() if len(parts) > 1 else "Market Flash Story"
                            body = parts[1].strip() if len(parts) > 1 else parts[0].strip()
                            story_card = (
                                f"🌟 *══════ 24H MARKET STORY ══════* 🌟\n\n"
                                f"📌 *{headline}*\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"{body}\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"🕒 *Active 24 Hours* • Story ID: `#{story_id}`\n"
                                f"⚡ _SMC Institutional Intelligence_"
                            )
                            sent = send_telegram_msg(story_card, chat_id=chat_id, parse_mode="Markdown")
                            if sent and sent.get("message_id"):
                                smid = sent.get("message_id")
                                telegram_pin_message(chat_id, smid, notify=False)
                                state.setdefault("stories", {})[story_id] = {
                                    "message_id": smid,
                                    "chat_id": chat_id,
                                    "headline": headline,
                                    "body": body,
                                    "timestamp": time.time(),
                                }
                                save_state(state)
                                telegram_delete_message(chat_id, message_id)
                            else:
                                send_telegram("❌ Story post karne me error aayi.", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage: `/story <Headline> | <Details>`", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators hi Market Stories post kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/editstory", "editstory"):
                    if is_telegram_admin(chat_id, user_id):
                        new_story_text = text[len(first_word):].strip()
                        target_sid = None
                        target_mid = None
                        if reply_to_msg:
                            r_mid = reply_to_msg.get("message_id")
                            for sid, sdata in state.get("stories", {}).items():
                                if sdata.get("message_id") == r_mid:
                                    target_sid = sid
                                    target_mid = r_mid
                                    break
                        if not target_sid and new_story_text:
                            first_token = new_story_text.split()[0].replace("#", "")
                            if first_token in state.get("stories", {}):
                                target_sid = first_token
                                target_mid = state["stories"][target_sid].get("message_id")
                                new_story_text = new_story_text[len(first_token):].strip()

                        if target_mid and new_story_text:
                            parts = new_story_text.split("|", 1)
                            headline = parts[0].strip() if len(parts) > 1 else "Market Flash Story (Updated)"
                            body = parts[1].strip() if len(parts) > 1 else parts[0].strip()
                            updated_card = (
                                f"🌟 *══════ 24H MARKET STORY ══════* 🌟\n\n"
                                f"📌 *{headline}*\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"{body}\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"🕒 *Active 24 Hours* • Story ID: `#{target_sid}` *(Updated)*\n"
                                f"⚡ _SMC Institutional Intelligence_"
                            )
                            if telegram_edit_message(chat_id, target_mid, updated_card):
                                state["stories"][target_sid]["headline"] = headline
                                state["stories"][target_sid]["body"] = body
                                save_state(state)
                                send_telegram(f"✅ *Story #{target_sid} successfully updated!*", chat_id=chat_id, parse_mode="Markdown")
                            else:
                                send_telegram("❌ Story edit karne me error aayi.", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage: Reply to story with `/editstory <Headline> | <New text>` ya `/editstory <story_id> <New text>`", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators hi Stories edit kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/delstory", "delstory", "/deletestory", "deletestory"):
                    if is_telegram_admin(chat_id, user_id):
                        target_sid = None
                        target_mid = None
                        if reply_to_msg:
                            r_mid = reply_to_msg.get("message_id")
                            for sid, sdata in list(state.get("stories", {}).items()):
                                if sdata.get("message_id") == r_mid:
                                    target_sid = sid
                                    target_mid = r_mid
                                    break
                        if not target_sid:
                            arg = text[len(first_word):].strip().replace("#", "")
                            if arg in state.get("stories", {}):
                                target_sid = arg
                                target_mid = state["stories"][target_sid].get("message_id")

                        if target_mid:
                            telegram_delete_message(chat_id, target_mid)
                            telegram_unpin_message(chat_id, target_mid)
                            if target_sid and target_sid in state.get("stories", {}):
                                del state["stories"][target_sid]
                                save_state(state)
                            send_telegram("✅ *Market story successfully deleted and unpinned.*", chat_id=chat_id, parse_mode="Markdown")
                        else:
                            send_telegram("ℹ️ Usage: Reply to story with `/delstory` ya `/delstory <story_id>`", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators hi Stories delete kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/stories", "stories"):
                    stories = state.get("stories", {})
                    if not stories:
                        send_telegram("ℹ️ Is group me koi active Market Story nahi hai. Admin `/story` command se 24h story post kar sakte hain.", chat_id=chat_id)
                    else:
                        lines = ["🌟 *Active Market Stories:*", "━━━━━━━━━━━━━━━━━━━━"]
                        for sid, sdata in stories.items():
                            hl = sdata.get("headline", "Market Story")
                            elapsed_m = int((time.time() - sdata.get("timestamp", time.time())) / 60)
                            lines.append(f"• `#{sid}`: *{hl}* ({elapsed_m}m ago)")
                        lines.append("━━━━━━━━━━━━━━━━━━━━")
                        lines.append("💡 Edit ke liye `/editstory <id>`, delete ke liye `/delstory <id>`")
                        send_telegram("\n".join(lines), chat_id=chat_id, parse_mode="Markdown")
                    continue

                # Permission 4: Manage Direct Messages (DM)
                if cmd in ("/dm", "dm", "/pm", "pm"):
                    if is_telegram_admin(chat_id, user_id):
                        dm_content = text[len(first_word):].strip()
                        target_uid = None
                        dm_text = ""
                        if reply_to_msg:
                            target_uid = reply_to_msg.get("from", {}).get("id")
                            dm_text = dm_content
                        elif dm_content:
                            parts = dm_content.split(None, 1)
                            if parts[0].isdigit() and len(parts) > 1:
                                target_uid = int(parts[0])
                                dm_text = parts[1].strip()

                        if target_uid and dm_text:
                            pm_msg = (
                                f"📩 *Direct Message from {chat_title} Admin:*\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"{dm_text}\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"👤 _Sent via {bot_username or 'Institutional Bot'}_"
                            )
                            if telegram_send_dm(target_uid, pm_msg):
                                send_telegram(f"✅ *Direct Message (DM) successfully sent to user `{target_uid}`!*", chat_id=chat_id, parse_mode="Markdown")
                            else:
                                send_telegram("❌ DM bhejne me error aayi. (User ko bot ke saath pehle private chat me `/start` dabana padta hai).", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage:\n• User ke message ko reply karke `/dm <message>` likhein\n• Ya `/dm <user_id> <message>` likhein.", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators hi `/dm` command se direct messages bhej sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/dmme", "dmme", "/pmme", "pmme"):
                    if user_id:
                        sheet_url = get_sheet_link()
                        sheet_line = f"• 📊 [Live Google Sheet Analysis]({sheet_url})\n" if sheet_url else ""
                        private_briefing = (
                            f"👋 *Namaste {user_name}! Aapka Private Market Briefing:*\n\n"
                            f"📈 *Monitored Pairs:* {', '.join(SYMBOLS)}\n"
                            f"⏱️ *Active Timeframes:* {', '.join(TIMEFRAMES)}\n"
                            f"🧠 *AI Model:* Groq `{GROQ_MODEL}`\n"
                            f"{sheet_line}\n"
                            f"💡 *Private Commands:*\n"
                            f"• `/charts` - Instant live candlestick charts with PD-OHLC levels\n"
                            f"• `/scan` - Real-time liquidity sweep & order block detection\n"
                            f"• `/ask <question>` - Ask any trading or market structure query\n"
                            f"• `/status` - Check 24/7 background scanner status"
                        )
                        if telegram_send_dm(user_id, private_briefing):
                            send_telegram(f"✅ {user_name}, aapko direct message (DM) bhej diya gaya hai! Please check your private chat.", chat_id=chat_id, reply_to_message_id=message_id)
                        else:
                            send_telegram(f"⚠️ {user_name}, bot aapko DM nahi bhej saka. Kripya pehle bot ke private chat me `/start` dabayein.", chat_id=chat_id, reply_to_message_id=message_id)
                    continue

                # Permission 5: Send Welcome Messages
                if cmd in ("/setwelcome", "setwelcome"):
                    if is_telegram_admin(chat_id, user_id):
                        new_w = text[len(first_word):].strip()
                        if new_w:
                            state["custom_welcome"] = new_w
                            save_state(state)
                            send_telegram("✅ *Custom welcome message successfully saved!*", chat_id=chat_id, parse_mode="Markdown")
                        else:
                            send_telegram(
                                "ℹ️ Usage: `/setwelcome <Aapka Welcome Message>`\n\n"
                                "*Placeholders Available:*\n"
                                "• `{name}` - New member ka naam\n"
                                "• `{user}` - @username ya naam\n"
                                "• `{chat_title}` - Group title",
                                chat_id=chat_id,
                                parse_mode="Markdown",
                            )
                    else:
                        send_telegram("❌ Sirf administrators hi welcome message set kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/viewwelcome", "viewwelcome", "/welcome", "welcome"):
                    custom_w = state.get("custom_welcome")
                    if custom_w:
                        sample = custom_w.replace("{name}", user_name).replace("{user}", f"@{user.get('username') or user_name}").replace("{chat_title}", chat_title)
                        send_telegram(f"👋 *Current Welcome Message Template:*\n━━━━━━━━━━━━━━━━━━━━\n{sample}\n━━━━━━━━━━━━━━━━━━━━\n💡 Reset karne ke liye `/resetwelcome` use karein.", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("👋 *Default Institutional Welcome Active:*\nNew members ke judne par automated welcome message with SMC trading commands bheja jata hai.", chat_id=chat_id, parse_mode="Markdown")
                    continue

                if cmd in ("/resetwelcome", "resetwelcome"):
                    if is_telegram_admin(chat_id, user_id):
                        state["custom_welcome"] = ""
                        save_state(state)
                        send_telegram("✅ *Welcome message default institutional template par reset kar diya gaya hai.*", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators hi welcome message reset kar sakte hain.", chat_id=chat_id)
                    continue

                # Permission 6: Manage Live Streams
                if cmd in ("/livestream", "livestream", "/stream", "stream"):
                    if is_telegram_admin(chat_id, user_id):
                        stream_topic = text[len(first_word):].strip() or "Live Institutional Crypto SMC Trading & Analysis"
                        stream_card = (
                            f"🔴 *══════ LIVE TRADING STREAM ══════* 🔴\n\n"
                            f"🎙️ *Topic:* {stream_topic}\n"
                            f"📅 *Status:* 🟢 *LIVE SESSION ACTIVE NOW*\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"📈 *Agenda:* Real-time Market Liquidity Sweeps, Order Blocks, and Q&A!\n"
                            f"👉 Voice/Video Chat abhi start karein aur join karein!\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"⚡ _Hosted by {user_name} (Admin)_"
                        )
                        sent = send_telegram_msg(stream_card, chat_id=chat_id, parse_mode="Markdown")
                        if sent and sent.get("message_id"):
                            smid = sent.get("message_id")
                            telegram_pin_message(chat_id, smid, notify=True)
                            state["active_stream"] = {
                                "message_id": smid,
                                "chat_id": chat_id,
                                "topic": stream_topic,
                                "host": user_name,
                                "started_at": time.time(),
                            }
                            save_state(state)
                        else:
                            send_telegram("❌ Live stream announce karne me error aayi.", chat_id=chat_id)
                    else:
                        send_telegram("❌ Sirf administrators hi live streams announce/pin kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/endstream", "endstream"):
                    if is_telegram_admin(chat_id, user_id):
                        active_stream = state.get("active_stream", {})
                        stream_mid = active_stream.get("message_id")
                        if stream_mid:
                            telegram_unpin_message(chat_id, stream_mid)
                        state["active_stream"] = {}
                        save_state(state)
                        recap_arg = text[len(first_word):].strip()
                        recap_extra = f"\n\n📝 *Key Takeaways:*\n{recap_arg}" if recap_arg else ""
                        conclude_msg = (
                            f"🏁 *LIVE TRADING STREAM CONCLUDED!*\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"Aaj ka live session successfully complete ho gaya hai.{recap_extra}\n\n"
                            f"🙏 Thank you sabhi traders for joining! 24/7 scanning channel me active rahegi."
                        )
                        send_telegram(conclude_msg, chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators hi live stream end kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/streamschedule", "streamschedule"):
                    if is_telegram_admin(chat_id, user_id):
                        sched_text = text[len(first_word):].strip()
                        if sched_text:
                            sched_msg = (
                                f"📅 *UPCOMING LIVE STREAM SCHEDULE:*\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"{sched_text}\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"🔔 Set your reminders! Live trading execution & SMC breakdowns."
                            )
                            sent = send_telegram_msg(sched_msg, chat_id=chat_id, parse_mode="Markdown")
                            if sent and sent.get("message_id"):
                                telegram_pin_message(chat_id, sent.get("message_id"), notify=False)
                        else:
                            send_telegram("ℹ️ Usage: `/streamschedule <Date & Time> - <Topic>`", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf administrators schedule announce kar sakte hain.", chat_id=chat_id)
                    continue

                # Permission 7: Ban Users & Moderation
                if cmd in ("/ban", "ban"):
                    if is_telegram_admin(chat_id, user_id):
                        target_id = None
                        target_name = None
                        if reply_to_msg:
                            target_user = reply_to_msg.get("from", {})
                            target_id = target_user.get("id")
                            target_name = target_user.get("first_name") or target_user.get("username") or str(target_id)
                        else:
                            raw_arg = text[len(first_word):].strip()
                            if raw_arg and raw_arg.split()[0].isdigit():
                                target_id = int(raw_arg.split()[0])
                                target_name = f"User `{target_id}`"

                        if target_id:
                            if telegram_ban_member(chat_id, target_id):
                                send_telegram(f"🚫 *User {target_name} ko group se permanently ban kar diya gaya hai.*", chat_id=chat_id, parse_mode="Markdown")
                                if reply_to_msg:
                                    telegram_delete_message(chat_id, reply_to_msg.get("message_id"))
                            else:
                                send_telegram("❌ User ko ban karne me error aayi. Please bot permissions verify karein.", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage:\n• User ke message ko reply karke `/ban` send karein\n• Ya `/ban <user_id>` send karein.", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf group administrators hi `/ban` command use kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/unban", "unban"):
                    if is_telegram_admin(chat_id, user_id):
                        target_id = None
                        target_name = None
                        if reply_to_msg:
                            target_user = reply_to_msg.get("from", {})
                            target_id = target_user.get("id")
                            target_name = target_user.get("first_name") or target_user.get("username") or str(target_id)
                        else:
                            raw_arg = text[len(first_word):].strip()
                            if raw_arg and raw_arg.split()[0].isdigit():
                                target_id = int(raw_arg.split()[0])
                                target_name = f"User `{target_id}`"

                        if target_id:
                            if telegram_unban_member(chat_id, target_id):
                                send_telegram(f"✅ *User {target_name} unban ho gaya hai.*", chat_id=chat_id, parse_mode="Markdown")
                            else:
                                send_telegram("❌ User ko unban karne me error aayi.", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage: Reply karke `/unban` ya `/unban <user_id>` send karein.", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf group administrators hi `/unban` command use kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/kick", "kick"):
                    if is_telegram_admin(chat_id, user_id):
                        target_id = None
                        target_name = None
                        if reply_to_msg:
                            target_user = reply_to_msg.get("from", {})
                            target_id = target_user.get("id")
                            target_name = target_user.get("first_name") or target_user.get("username") or str(target_id)
                        else:
                            raw_arg = text[len(first_word):].strip()
                            if raw_arg and raw_arg.split()[0].isdigit():
                                target_id = int(raw_arg.split()[0])
                                target_name = f"User `{target_id}`"

                        if target_id:
                            if telegram_kick_member(chat_id, target_id):
                                send_telegram(f"👢 *User {target_name} ko group se kick kar diya gaya hai.*", chat_id=chat_id, parse_mode="Markdown")
                                if reply_to_msg:
                                    telegram_delete_message(chat_id, reply_to_msg.get("message_id"))
                            else:
                                send_telegram("❌ User ko kick karne me error aayi.", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage: Reply karke `/kick` ya `/kick <user_id>` send karein.", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf group administrators hi `/kick` command use kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/mute", "mute", "/restrict", "restrict"):
                    if is_telegram_admin(chat_id, user_id):
                        target_id = None
                        target_name = None
                        dur_arg = ""
                        raw_arg = text[len(first_word):].strip()
                        tokens = raw_arg.split()

                        if reply_to_msg:
                            target_user = reply_to_msg.get("from", {})
                            target_id = target_user.get("id")
                            target_name = target_user.get("first_name") or target_user.get("username") or str(target_id)
                            dur_arg = tokens[0] if tokens else "1h"
                        elif tokens and tokens[0].isdigit():
                            target_id = int(tokens[0])
                            target_name = f"User `{target_id}`"
                            dur_arg = tokens[1] if len(tokens) > 1 else "1h"

                        if target_id:
                            sec = parse_duration_sec(dur_arg, default_sec=3600)
                            mins = sec // 60
                            dur_desc = f"{mins // 60}h" if mins >= 60 else f"{mins}m"
                            if telegram_mute_member(chat_id, target_id, duration_sec=sec):
                                send_telegram(f"🔇 *User {target_name} ko {dur_desc} ke liye mute kar diya gaya hai.*", chat_id=chat_id, parse_mode="Markdown")
                            else:
                                send_telegram("❌ User ko mute karne me error aayi.", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage: Reply karke `/mute [30m|2h|1d]` ya `/mute <user_id> [duration]` send karein.", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf group administrators hi `/mute` command use kar sakte hain.", chat_id=chat_id)
                    continue

                if cmd in ("/unmute", "unmute"):
                    if is_telegram_admin(chat_id, user_id):
                        target_id = None
                        target_name = None
                        if reply_to_msg:
                            target_user = reply_to_msg.get("from", {})
                            target_id = target_user.get("id")
                            target_name = target_user.get("first_name") or target_user.get("username") or str(target_id)
                        else:
                            raw_arg = text[len(first_word):].strip()
                            if raw_arg and raw_arg.split()[0].isdigit():
                                target_id = int(raw_arg.split()[0])
                                target_name = f"User `{target_id}`"

                        if target_id:
                            if telegram_unmute_member(chat_id, target_id):
                                send_telegram(f"🔊 *User {target_name} ko successfully unmute kar diya gaya hai.*", chat_id=chat_id, parse_mode="Markdown")
                            else:
                                send_telegram("❌ User ko unmute karne me error aayi.", chat_id=chat_id)
                        else:
                            send_telegram("ℹ️ Usage: Reply karke `/unmute` ya `/unmute <user_id>` send karein.", chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram("❌ Sirf group administrators hi `/unmute` command use kar sakte hain.", chat_id=chat_id)
                    continue

                # Group Rules & Community Information
                if cmd in ("/rules", "rules"):
                    rules_msg = (
                        f"📜 *{chat_title} - Group Rules & Guidelines:*\n\n"
                        f"1. 🚫 *No Spam & Promotion:* Unsolicited telegram/external invite links ya promotional spam strictly prohibited hai.\n"
                        f"2. 📈 *Trading Discipline:* Har trade par proper Stop Loss (SL) aur risk management (1-2% capital risk) follow karein.\n"
                        f"3. 🤝 *Respect Community:* Respectful aur informative discussion rakhein.\n"
                        f"4. 🤖 *AI Trading Mentor:* Groq AI se koi bhi question puchne ke liye `@ai <question>` use karein.\n"
                        f"5. ⚠️ *Disclaimer:* Yahan provide kiye gaye signals educational purpose ke liye hain. Always DYOR!"
                    )
                    send_telegram(rules_msg, chat_id=chat_id, parse_mode="Markdown")
                    continue

                log.info("Telegram command received from %s (%s): '%s'", chat_id, chat_type, text)

                # --- 4. TRADING BOT SCANNER & CHART COMMANDS ---
                is_chart_cmd = (
                    cmd in ("/chart", "chart", "/charts", "charts", "/image", "image", "/images", "images", "/photo", "photo")
                    or "chart image" in clean_lower
                    or "chart images" in clean_lower
                    or "send chart" in clean_lower
                    or "send image" in clean_lower
                    or "chart bhejo" in clean_lower
                    or "image bhejo" in clean_lower
                    or "bhejo chart" in clean_lower
                    or "bhejo image" in clean_lower
                    or "all charts" in clean_lower
                    or "har chart" in clean_lower
                    or "chart pic" in clean_lower
                )

                requested_chart_sym = None
                words = [w.strip(",.!?#/@") for w in clean_lower.split()]
                for s in SYMBOLS:
                    base = s.replace("USDT", "").lower()
                    if base in words or s.lower() in words:
                        requested_chart_sym = s
                        break

                is_activity_cmd = (
                    cmd in ("/activity", "activity", "/report", "report", "/what", "what")
                    or "what are doing" in clean_lower
                    or "what are you doing" in clean_lower
                    or "what doing" in clean_lower
                    or "what u doing" in clean_lower
                    or "what r u doing" in clean_lower
                    or "kya kar rahe" in clean_lower
                    or "kya kar rhe" in clean_lower
                    or "kya kr rahe" in clean_lower
                    or "kya kr rhe" in clean_lower
                    or "kya kar rha" in clean_lower
                    or "kya kr rha" in clean_lower
                    or "kya chal raha" in clean_lower
                    or "kya chal rha" in clean_lower
                    or "kya ho raha" in clean_lower
                    or "whats going on" in clean_lower
                    or "what's going on" in clean_lower
                    or "analysis report" in clean_lower
                )

                is_stop_cmd = (
                    cmd in ("/end", "end", "/stop", "stop", "/pause", "pause", "/band", "band", "/close", "close")
                    or "analysis band" in clean_lower
                    or "band kar" in clean_lower
                    or "stop analysis" in clean_lower
                    or "end analysis" in clean_lower
                )

                is_start_cmd = (
                    cmd in ("/start", "start", "/resume", "resume", "/scan", "scan", "/data", "data", "/price", "price")
                    or "analysis shuru" in clean_lower
                    or "start analysis" in clean_lower
                    or "resume analysis" in clean_lower
                    or "shuru kar" in clean_lower
                )

                is_status_cmd = (
                    cmd in ("/status", "status")
                    or "bot status" in clean_lower
                )

                is_exit_cmd = (
                    cmd in ("/exit", "exit", "/shutdown", "shutdown", "/kill", "kill")
                )

                is_help_cmd = (
                    cmd in ("/help", "help")
                )

                words_clean = [w.strip(",.!?#/@\"'").lower() for w in clean_lower.split()]
                is_ai_tagged = (
                    "@ai" in clean_lower
                    or "#ai" in clean_lower
                    or clean_lower.startswith("ai ")
                    or clean_lower.startswith("ai,")
                    or clean_lower.startswith("ai:")
                    or clean_lower.startswith("/ai")
                    or clean_lower.startswith("@ai")
                    or ("ai" in words_clean[:3])
                    or "@tado" in clean_lower
                    or "#tado" in clean_lower
                    or clean_lower.startswith("tado ")
                    or clean_lower.startswith("tado,")
                    or clean_lower.startswith("tado:")
                    or clean_lower.startswith("/tado")
                    or clean_lower.startswith("@tado")
                    or ("tado" in words_clean[:3])
                    or "@todo" in clean_lower
                    or clean_lower.startswith("todo ")
                    or ("todo" in words_clean[:3])
                    or is_bot_tagged
                    or is_reply_to_bot
                    or cmd in ("/ask", "ask", "/ai", "ai", "/q", "q", "/tado", "tado")
                    or clean_lower.startswith("/ask ")
                    or clean_lower.startswith("ask ")
                )

                if is_chart_cmd:
                    log.info("Chart images requested by %s ('%s'), requested_sym=%s", chat_id, text, requested_chart_sym)
                    handle_telegram_charts(client, chat_id, requested_sym=requested_chart_sym)
                elif is_activity_cmd:
                    log.info("Activity report requested by %s ('%s')", chat_id, text)
                    handle_telegram_start(client, sheets, state, chat_id, is_activity_report=True)
                elif is_stop_cmd:
                    # In groups, only admins can pause analysis
                    if chat_type in ("group", "supergroup") and not is_telegram_admin(chat_id, user_id):
                        send_telegram("❌ *Sirf group administrators ya bot owner hi analysis pause kar sakte hain.*", chat_id=chat_id, parse_mode="Markdown")
                        continue
                    if SCANNING_ACTIVE.is_set():
                        SCANNING_ACTIVE.clear()
                        log.info("Analysis PAUSED via Telegram command by %s ('%s')", chat_id, text)
                        sheet_url = get_sheet_link()
                        sheet_line = f"• 📊 [Open Live Google Sheet]({sheet_url})\n" if sheet_url else ""
                        msg_end = (
                            "🛑 *Market Analysis Band Kar Diya Gaya Hai (Paused)*\n\n"
                            "• 24/7 background scanning aur Groq AI analysis stop kar di gayi hai.\n"
                            "• Naye trades/setups ke alerts aana band ho gaye hain.\n"
                            f"{sheet_line}\n"
                            "👉 Analysis wapas shuru karne ke liye *start* ya */start* send karein."
                        )
                        send_telegram(msg_end, chat_id=chat_id, parse_mode="Markdown")
                    else:
                        send_telegram(
                            "ℹ️ *Analysis pehle se hi band hai.*\n\n👉 Shuru karne ke liye *start* ya */start* send karein.",
                            chat_id=chat_id,
                            parse_mode="Markdown",
                        )
                elif is_start_cmd:
                    # In groups, only admins can resume analysis
                    if chat_type in ("group", "supergroup") and not is_telegram_admin(chat_id, user_id):
                        send_telegram("❌ *Sirf group administrators ya bot owner hi analysis start/resume kar sakte hain.*", chat_id=chat_id, parse_mode="Markdown")
                        continue
                    was_paused = not SCANNING_ACTIVE.is_set()
                    if was_paused:
                        SCANNING_ACTIVE.set()
                        log.info("Analysis RESUMED via Telegram command by %s ('%s')", chat_id, text)
                        send_telegram(
                            "▶️ *Market Analysis Shuru / Resumed!*\n\n"
                            "• 24/7 scanning aur AI analysis active ho gayi hai.\n"
                            "• Naye setups monitor honge aur alerts aayenge.\n\n"
                            "⏳ *Live market data fetch ho raha hai...*",
                            chat_id=chat_id,
                            parse_mode="Markdown",
                        )
                    handle_telegram_start(client, sheets, state, chat_id)
                elif is_status_cmd:
                    status_str = "🟢 ACTIVE (Scanning 24/7)" if SCANNING_ACTIVE.is_set() else "🛑 PAUSED (Band)"
                    sheet_url = get_sheet_link()
                    sheet_line = f"• 📊 [Open Live Google Sheet]({sheet_url})\n" if sheet_url else ""
                    status_msg = (
                        f"🤖 *Bot Status:* {status_str}\n\n"
                        f"• *Pairs:* {', '.join(SYMBOLS)}\n"
                        f"• *Timeframes:* {', '.join(TIMEFRAMES)}\n"
                        f"• *Scan Cycle:* {SCAN_INTERVAL}s\n"
                        f"• *Groq AI Model:* `{GROQ_MODEL}`\n"
                        f"• *Chat Type:* `{chat_type}` ({chat_title})\n"
                        f"{sheet_line}\n"
                        "💡 *Commands:*\n"
                        "• `chart image` ya `/charts` - Sabhi charts + PD-OHLC levels\n"
                        "• `chart <coin>` (e.g. `chart btc`) - Single chart image\n"
                        "• `what are doing` ya `kya kar rahe ho` - Analysis report\n"
                        "• `/ask <question>` - Groq AI se direct institutional jawab payein\n"
                        "• `/rules` - Group guidelines\n"
                        "• `end` ya `/end` - Analysis band karein (Admin)\n"
                        "• `start` ya `/start` - Analysis shuru karein (Admin)\n"
                        "• `/status` - Bot status dekhein"
                    )
                    send_telegram(status_msg, chat_id=chat_id, parse_mode="Markdown")
                elif is_exit_cmd:
                    if chat_type in ("group", "supergroup") and not is_telegram_admin(chat_id, user_id):
                        send_telegram("❌ *Sirf group administrators ya bot owner hi bot shutdown kar sakte hain.*", chat_id=chat_id, parse_mode="Markdown")
                        continue
                    send_telegram("🛑 *Bot process ko completely stop (shutdown) kiya ja raha hai...*", chat_id=chat_id, parse_mode="Markdown")
                    log.info("Complete shutdown triggered via Telegram command by %s", chat_id)
                    SCANNING_ACTIVE.clear()
                    STOP.set()
                elif is_help_cmd:
                    status_str = "🟢 ACTIVE" if SCANNING_ACTIVE.is_set() else "🛑 PAUSED"
                    sheet_url = get_sheet_link()
                    sheet_line = f"• 📊 [Open Live Google Sheet]({sheet_url})\n" if sheet_url else ""
                    help_msg = (
                        f"🤖 *Crypto Trading & Group Bot Hub ({chat_title}):*\n\n"
                        "📈 *Market & Trading Signals:*\n"
                        "• `chart image` ya `/charts` - *Har symbol ka visual chart + PDH/PDL*\n"
                        "• `chart <coin>` (e.g. `chart btc`) - *Single coin chart image*\n"
                        "• `what are doing` ya `kya kar rahe ho` - *Bot Activity Report*\n"
                        "• `@ai <question>` (e.g. `@ai BTC trend?`) - *Groq AI Institutional Answer*\n"
                        "• `/scan` - *Instant liquidity sweep & OB scan*\n"
                        "• `/status` - *Current bot & scanner state*\n"
                        "• `/rules` - *Group rules & trading risk policy*\n\n"
                        "🛡️ *Group Administration & Moderation (7 Permissions):*\n"
                        "1️⃣ *Change Channel Info:*\n"
                        "  • `/settitle <Title>` - Update title\n"
                        "  • `/setdesc <Desc>` - Update description\n"
                        "  • `/delphoto` - Remove group photo\n"
                        "  • `/channelinfo` - Full channel & permission overview\n\n"
                        "2️⃣ *Manage Messages:*\n"
                        "  • `/announce <text>` - Official framed announcement\n"
                        "  • `/post <text>` - Post plain message as bot\n"
                        "  • `/edit <new text>` (Reply) - Edit bot message\n"
                        "  • `/del` (Reply) - Delete target message\n"
                        "  • `/purge <count>` - Bulk delete recent messages\n\n"
                        "3️⃣ *Manage Stories:*\n"
                        "  • `/story <Headline> | <Text>` - Post pinned 24H story\n"
                        "  • `/editstory <id> <new text>` - Edit active story\n"
                        "  • `/delstory <id>` - Delete & unpin story\n"
                        "  • `/stories` - List active market stories\n\n"
                        "4️⃣ *Manage Direct Messages (DM):*\n"
                        "  • `/dm <message>` (Reply) - Send direct PM to member\n"
                        "  • `/dm <user_id> <message>` - Send DM by User ID\n"
                        "  • `/dmme` - Get private market report in your DM\n\n"
                        "5️⃣ *Send Welcome Messages:*\n"
                        "  • `/setwelcome <template>` - Customize greeting message\n"
                        "  • `/viewwelcome` - Preview current greeting\n"
                        "  • `/resetwelcome` - Reset to institutional default\n\n"
                        "6️⃣ *Manage Live Streams:*\n"
                        "  • `/livestream <topic>` - Announce & pin live trading stream\n"
                        "  • `/endstream [recap]` - Conclude session & unpin\n"
                        "  • `/streamschedule <time> - <topic>` - Announce stream schedule\n\n"
                        "7️⃣ *Ban Users & Moderation:*\n"
                        "  • `/ban` (Reply or `<id>`) - Permanent ban\n"
                        "  • `/unban` (Reply or `<id>`) - Unban user\n"
                        "  • `/kick` (Reply or `<id>`) - Remove member (kick)\n"
                        "  • `/mute [30m|2h|1d]` - Restrict messaging\n"
                        "  • `/unmute` (Reply or `<id>`) - Restore messaging\n\n"
                        "⚙️ *System Controls:*\n"
                        "  • `/end` - Pause 24/7 scanning\n"
                        "  • `/start` - Resume 24/7 scanning\n"
                        f"{sheet_line}\n"
                        f"Current State: {status_str}"
                    )
                    send_telegram(help_msg, chat_id=chat_id, parse_mode="Markdown")
                elif is_ai_tagged or chat_type == "private":
                    # User asked a trading question or tagged AI / bot!
                    # Process with Groq AI Institutional Analyst and reply directly to user's message!
                    replied_txt = reply_to_msg.get("text") if reply_to_msg else None
                    log.info("AI tagged / trading query from %s (%s): '%s'", chat_id, chat_type, text)
                    handle_telegram_trading_question(
                        client,
                        chat_id,
                        text,
                        reply_to_message_id=message_id,
                        replied_text=replied_txt,
                    )
                else:
                    # In groups, ignore casual messages between members so bot does not spam
                    pass
        except Exception as exc:
            log.warning("Telegram listener error: %s", scrub(exc))
            STOP.wait(3)


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
        print("   SKIPPED/DISABLED (need: pip install gspread, credentials.json or GOOGLE_CREDENTIALS_JSON, GOOGLE_SHEET_ID or NAME)")
    else:
        try:
            auth_source = ".env secret (GOOGLE_CREDENTIALS_JSON)" if GOOGLE_CREDENTIALS_JSON else f"file ({GOOGLE_CREDENTIALS_FILE})"
            SheetsLogger().append("Scan Log", [fmt_ts(datetime.now(LOCAL_TZ)), "TEST", "-", "None", "TEST ROW"])
            print(f"   OK - connected and appended to 'Scan Log' via {auth_source}")
        except Exception as exc:
            print(f"   FAILED: {exc}")

    print("4) Groq AI API ...")
    if not GROQ_API_KEY:
        print("   SKIPPED/DISABLED (need: GROQ_API_KEY in .env)")
    else:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            h = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
            p = {
                "model": GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": "You are a crypto quantitative analyst. Reply in JSON: {\"status\": \"READY\", \"model\": \"qwen\"}"},
                    {"role": "user", "content": "Ping test."}
                ],
                "response_format": {"type": "json_object"},
                "max_tokens": 40
            }
            r = requests.post(url, headers=h, json=p, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                resp_json = r.json()
                content = resp_json["choices"][0]["message"]["content"].strip()
                print(f"   OK - Model '{GROQ_MODEL}' connected successfully (JSON mode: {content})")
            else:
                print(f"   FAILED (HTTP {r.status_code}: {r.text})")
        except Exception as exc:
            print(f"   FAILED: {exc}")

    print("5) Cloudinary & mplfinance ...")
    if not mpf:
        print("   SKIPPED/DISABLED: mplfinance not installed")
    else:
        print("   OK - mplfinance loaded successfully")

    if not cloudinary_configured():
        print("   SKIPPED/DISABLED (need: CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY, CLOUDINARY_API_SECRET in .env)")
    else:
        try:
            ping_res = cloudinary.api.ping()
            print(f"   OK - Cloudinary connected successfully (status: {ping_res.get('status', 'OK')})")
        except Exception as exc:
            print(f"   WARNING (Cloudinary ping failed: {exc})")
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

    log.info("Starting | symbols=%s | timeframes=%s | interval=%ss | telegram=%s | sheets=%s | tz=%s",
             ",".join(SYMBOLS), TIMEFRAME, SCAN_INTERVAL, telegram_configured(), SHEETS_ON, TZ_NAME)
    if not SHEETS_ON:
        log.info("Google Sheets logging is OFF (missing gspread, credentials file, or sheet id/name).")
    if SEND_STARTUP_MESSAGE and telegram_configured() and not args.once:
        sheet_url = get_sheet_link()
        sheet_text = f"\n📊 *Google Sheet:* [Open Live Sheet]({sheet_url})" if sheet_url else ""
        send_telegram(f"🤖 *trader_bot started.* Watching {', '.join(SYMBOLS)} on {TIMEFRAME} ({SCAN_INTERVAL}s scan cycle).{sheet_text}", parse_mode="Markdown")

    if TELEGRAM_TOKEN and not args.once:
        t_listener = threading.Thread(
            target=telegram_listener_loop,
            args=(client, sheets, state),
            name="TelegramListener",
            daemon=True,
        )
        t_listener.start()

    while not STOP.is_set():                       # 24/7 loop
        if not SCANNING_ACTIVE.is_set():
            # Analysis is paused by user ('end' command)
            # Sleep lightly so we stay responsive to STOP or start
            STOP.wait(2.0)
            continue

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