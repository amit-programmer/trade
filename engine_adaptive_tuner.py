"""
engine_adaptive_tuner.py - AI Adaptive Logic Self-Tuning & Dynamic Quant Param Optimizer.

Provides:
- Persistence and loading of adaptive_config.json
- Integration with StrategyConfig (CFG) to update min_displacement_atr, target_rr, min_sweep_atr
- Groq AI quantitative post-mortem analysis of recent closed trades
- Automatic parameter adjustment based on market regime and win/loss feedback
- Audit logging for Google Sheets tab 'AI_Self_Tuning_Log' and Telegram notifications
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests

log = logging.getLogger("adaptive_tuner")

BASE_DIR = Path(__file__).resolve().parent
ADAPTIVE_CONFIG_FILE = BASE_DIR / "adaptive_config.json"

DEFAULT_ADAPTIVE_CONFIG: Dict[str, Any] = {
    "min_displacement_atr": 1.2,
    "target_rr": 2.0,
    "min_sweep_atr": 0.10,
    "max_setup_age": 12,
    "last_tuned_at": None,
    "tuning_rationale": "Initial institutional baseline configuration",
    "total_tunings": 0,
    "history": [],
}


def load_adaptive_config() -> Dict[str, Any]:
    """Loads adaptive_config.json with safe fallback to baseline defaults."""
    if not ADAPTIVE_CONFIG_FILE.exists():
        save_adaptive_config(DEFAULT_ADAPTIVE_CONFIG)
        return dict(DEFAULT_ADAPTIVE_CONFIG)
    try:
        data = json.loads(ADAPTIVE_CONFIG_FILE.read_text(encoding="utf-8"))
        for k, v in DEFAULT_ADAPTIVE_CONFIG.items():
            data.setdefault(k, v)
        return data
    except Exception as exc:
        log.warning("Failed to parse %s (%s). Reverting to defaults.", ADAPTIVE_CONFIG_FILE.name, exc)
        return dict(DEFAULT_ADAPTIVE_CONFIG)


def save_adaptive_config(config: Dict[str, Any]) -> None:
    """Atomically persists adaptive_config.json to disk."""
    try:
        tmp_file = ADAPTIVE_CONFIG_FILE.with_suffix(".tmp")
        tmp_file.write_text(json.dumps(config, indent=2), encoding="utf-8")
        tmp_file.replace(ADAPTIVE_CONFIG_FILE)
    except Exception as exc:
        log.warning("Failed to save %s: %s", ADAPTIVE_CONFIG_FILE.name, exc)


def apply_adaptive_config_to_strategy(cfg_obj: Any) -> Dict[str, Any]:
    """
    Applies current parameters from adaptive_config.json onto StrategyConfig instance.
    Updates disp_atr_mult, rr, min_sweep_atr, max_setup_age in memory.
    """
    config = load_adaptive_config()
    try:
        if hasattr(cfg_obj, "disp_atr_mult"):
            cfg_obj.disp_atr_mult = float(config.get("min_displacement_atr", 1.2))
        if hasattr(cfg_obj, "rr"):
            cfg_obj.rr = float(config.get("target_rr", 2.0))
        if hasattr(cfg_obj, "min_sweep_atr"):
            cfg_obj.min_sweep_atr = float(config.get("min_sweep_atr", 0.10))
        if hasattr(cfg_obj, "max_setup_age"):
            cfg_obj.max_setup_age = int(config.get("max_setup_age", 12))
        log.info(
            "Adaptive Config Applied -> Disp ATR: %.2fx | Target RR: %.1fR | Sweep ATR: %.2f | Max Age: %d",
            cfg_obj.disp_atr_mult, cfg_obj.rr, cfg_obj.min_sweep_atr, cfg_obj.max_setup_age
        )
    except Exception as exc:
        log.warning("Error applying adaptive config: %s", exc)
    return config


def run_adaptive_self_tuning(
    closed_positions: List[Dict[str, Any]],
    groq_api_key: Optional[str] = None,
    groq_model: str = "qwen/qwen3.8-27b",
) -> Tuple[Dict[str, Any], Optional[List[Any]]]:
    """
    Analyzes recent closed trade performance, calls Groq AI quantitative post-mortem,
    and updates adaptive_config.json with optimized parameters.
    Returns (new_config, sheet_log_row).
    """
    current_config = load_adaptive_config()
    ts_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # If insufficient trade history, apply sensible regime adjustments or maintain baseline
    if not closed_positions:
        rationale = "Insufficient closed trade samples for post-mortem analysis; baseline maintained."
        current_config["last_tuned_at"] = ts_now
        current_config["tuning_rationale"] = rationale
        save_adaptive_config(current_config)
        log_row = [ts_now, "Periodic Health Check", "ALL", "Unchanged", "Unchanged", rationale, "N/A (0 trades)"]
        return current_config, log_row

    total_trades = len(closed_positions)
    wins = [p for p in closed_positions if float(p.get("realized_pnl_usd", 0)) > 0]
    losses = [p for p in closed_positions if float(p.get("realized_pnl_usd", 0)) < 0]
    win_rate = (len(wins) / total_trades * 100) if total_trades > 0 else 0.0

    # Classify failure patterns
    stop_hits = [p for p in losses if "STOP" in str(p.get("exit_reason", "")).upper()]
    be_hits = [p for p in closed_positions if "BE" in str(p.get("exit_reason", "")).upper()]

    system_prompt = (
        "You are an institutional quantitative risk director and SMC trading algorithmic optimizer.\n"
        "Your task is to analyze recent live closed trade performance and determine whether to tighten or expand parameters.\n"
        "Rules:\n"
        "1. If frequent Stop-Loss hits occur due to false sweeps or choppy fakeouts, increase min_displacement_atr (e.g. from 1.2 to 1.4-1.6) and min_sweep_atr.\n"
        "2. If win rate is high (>60%) and momentum is strong, expand target_rr (e.g. from 2.0 to 2.5-3.0) to maximize trending runners.\n"
        "3. Keep parameters within realistic professional ranges:\n"
        "   - min_displacement_atr: between 1.0 and 2.0\n"
        "   - target_rr: between 1.5 and 3.5\n"
        "   - min_sweep_atr: between 0.08 and 0.25\n"
        "   - max_setup_age: between 8 and 18\n\n"
        "Respond ONLY with a JSON object:\n"
        "{\n"
        '  "min_displacement_atr": float,\n'
        '  "target_rr": float,\n'
        '  "min_sweep_atr": float,\n'
        '  "max_setup_age": int,\n'
        '  "tuning_rationale": "Clear institutional rationale for parameter update"\n'
        "}"
    )

    summary_text = (
        f"Total Trades: {total_trades} | Wins: {len(wins)} | Losses: {len(losses)} | "
        f"Win Rate: {win_rate:.1f}% | Stop Hits: {len(stop_hits)} | Breakeven Exits: {len(be_hits)}\n"
        f"Current Config -> Disp ATR: {current_config.get('min_displacement_atr')} | "
        f"Target RR: {current_config.get('target_rr')} | Sweep ATR: {current_config.get('min_sweep_atr')}"
    )

    new_disp = current_config.get("min_displacement_atr", 1.2)
    new_rr = current_config.get("target_rr", 2.0)
    new_sweep = current_config.get("min_sweep_atr", 0.10)
    new_age = current_config.get("max_setup_age", 12)
    rationale = "Algorithmic feedback loop verified."

    # Call Groq AI if key is present
    if groq_api_key:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {"Authorization": f"Bearer {groq_api_key}", "Content-Type": "application/json"}
            payload = {
                "model": groq_model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": summary_text},
                ],
                "response_format": {"type": "json_object"},
                "temperature": 0.2,
                "max_tokens": 400,
            }
            resp = requests.post(url, headers=headers, json=payload, timeout=12)
            if resp.status_code == 200:
                parsed = json.loads(resp.json()["choices"][0]["message"]["content"])
                new_disp = round(float(parsed.get("min_displacement_atr", new_disp)), 2)
                new_rr = round(float(parsed.get("target_rr", new_rr)), 2)
                new_sweep = round(float(parsed.get("min_sweep_atr", new_sweep)), 2)
                new_age = int(parsed.get("max_setup_age", new_age))
                rationale = str(parsed.get("tuning_rationale", rationale))
                log.info("Groq AI Post-Mortem Tuning response: %s", rationale)
        except Exception as groq_err:
            log.warning("Groq AI tuning call failed (%s). Applying statistical rule-based fallback.", groq_err)

    # Statistical rule-based fallback if Groq call failed or returned unchanged
    if not groq_api_key or rationale == "Algorithmic feedback loop verified.":
        if win_rate < 40.0 and len(losses) >= 2:
            new_disp = min(1.6, round(new_disp + 0.15, 2))
            new_sweep = min(0.20, round(new_sweep + 0.03, 2))
            rationale = f"Tightened displacement filter to {new_disp}x ATR to eliminate choppy sweep false-breakouts (Win Rate: {win_rate:.1f}%)."
        elif win_rate >= 65.0 and len(wins) >= 3:
            new_rr = min(3.0, round(new_rr + 0.25, 2))
            rationale = f"High win rate ({win_rate:.1f}%); expanded target RR to {new_rr}R to capture deeper institutional liquidity runs."

    old_disp = current_config.get("min_displacement_atr")
    old_rr = current_config.get("target_rr")

    current_config["min_displacement_atr"] = max(1.0, min(2.0, new_disp))
    current_config["target_rr"] = max(1.5, min(3.5, new_rr))
    current_config["min_sweep_atr"] = max(0.08, min(0.25, new_sweep))
    current_config["max_setup_age"] = max(8, min(18, new_age))
    current_config["last_tuned_at"] = ts_now
    current_config["tuning_rationale"] = rationale
    current_config["total_tunings"] = int(current_config.get("total_tunings", 0)) + 1

    history_item = {
        "timestamp": ts_now,
        "old_disp": old_disp,
        "new_disp": current_config["min_displacement_atr"],
        "old_rr": old_rr,
        "new_rr": current_config["target_rr"],
        "win_rate": round(win_rate, 1),
        "total_trades": total_trades,
        "rationale": rationale,
    }
    history = current_config.get("history", [])
    history.append(history_item)
    current_config["history"] = history[-50:]

    save_adaptive_config(current_config)

    avg_r = (sum(float(t.get("realized_r", 0)) for t in closed_positions) / total_trades) if total_trades else 0.0
    log_row = [
        ts_now,
        current_config["min_displacement_atr"],
        current_config["target_rr"],
        current_config.get("min_sweep_atr", 0.10),
        round(win_rate, 1),
        round(avg_r, 2),
        total_trades,
        rationale,
    ]

    return current_config, log_row
