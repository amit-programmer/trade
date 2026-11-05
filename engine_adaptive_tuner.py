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

# =============================================================================
# STRICT HARD BOUNDS CLAMPING GUARD
# =============================================================================
# Prevents AI or statistical tuning loops from ever setting reckless or invalid parameters.
PARAM_BOUNDS: Dict[str, Tuple[float, float]] = {
    "min_displacement_atr": (1.0, 2.5),
    "min_risk_reward": (1.5, 4.0),
    "target_rr": (1.5, 4.0),
    "min_confidence_score": (60.0, 90.0),
    "min_sweep_atr": (0.08, 0.25),
    "max_setup_age": (8.0, 18.0),
}


def clamp_parameter(name: str, value: Any, default: float) -> float:
    """Clamps a numeric tuning parameter to its safe, strict institutional bounds."""
    try:
        val = float(value)
    except (ValueError, TypeError):
        val = default
    if name in PARAM_BOUNDS:
        low, high = PARAM_BOUNDS[name]
        return max(low, min(high, val))
    return val


DEFAULT_ADAPTIVE_CONFIG: Dict[str, Any] = {
    "min_displacement_atr": 1.2,
    "target_rr": 2.0,
    "min_risk_reward": 2.0,
    "min_confidence_score": 75,
    "min_sweep_atr": 0.10,
    "max_setup_age": 12,
    "last_tuned_at": None,
    "tuning_rationale": "Initial institutional baseline configuration",
    "total_tunings": 0,
    "history": [],
}


def load_adaptive_config() -> Dict[str, Any]:
    """Loads adaptive_config.json with safe fallback and strict parameter bounds clamping."""
    if not ADAPTIVE_CONFIG_FILE.exists():
        save_adaptive_config(DEFAULT_ADAPTIVE_CONFIG)
        return dict(DEFAULT_ADAPTIVE_CONFIG)
    try:
        data = json.loads(ADAPTIVE_CONFIG_FILE.read_text(encoding="utf-8"))
        raw_rr = data.get("min_risk_reward")
        if raw_rr is None:
            raw_rr = data.get("target_rr", 2.0)
        for k, v in DEFAULT_ADAPTIVE_CONFIG.items():
            data.setdefault(k, v)
        # Apply strict clamping to loaded values to prevent manual corruption or out-of-range state
        data["min_displacement_atr"] = round(clamp_parameter("min_displacement_atr", data.get("min_displacement_atr"), 1.2), 2)
        rr_val = round(clamp_parameter("min_risk_reward", raw_rr, 2.0), 2)
        data["target_rr"] = rr_val
        data["min_risk_reward"] = rr_val
        data["min_confidence_score"] = int(round(clamp_parameter("min_confidence_score", data.get("min_confidence_score", 75), 75)))
        data["min_sweep_atr"] = round(clamp_parameter("min_sweep_atr", data.get("min_sweep_atr"), 0.10), 2)
        data["max_setup_age"] = int(round(clamp_parameter("max_setup_age", data.get("max_setup_age"), 12)))
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
        "1. If frequent Stop-Loss hits occur due to false sweeps or choppy fakeouts, increase min_displacement_atr (1.0 to 2.5) and min_confidence_score (60 to 90).\n"
        "2. If win rate is high (>60%) and momentum is strong, expand min_risk_reward / target_rr (1.5 to 4.0) to maximize trending runners.\n"
        "3. Strictly enforce institutional bounds:\n"
        "   - min_displacement_atr: between 1.0 and 2.5\n"
        "   - min_risk_reward: between 1.5 and 4.0\n"
        "   - min_confidence_score: between 60 and 90\n"
        "   - min_sweep_atr: between 0.08 and 0.25\n"
        "   - max_setup_age: between 8 and 18\n\n"
        "Respond ONLY with a JSON object:\n"
        "{\n"
        '  "min_displacement_atr": float,\n'
        '  "min_risk_reward": float,\n'
        '  "target_rr": float,\n'
        '  "min_confidence_score": int,\n'
        '  "min_sweep_atr": float,\n'
        '  "max_setup_age": int,\n'
        '  "tuning_rationale": "Clear institutional rationale for parameter update"\n'
        "}"
    )

    summary_text = (
        f"Total Trades: {total_trades} | Wins: {len(wins)} | Losses: {len(losses)} | "
        f"Win Rate: {win_rate:.1f}% | Stop Hits: {len(stop_hits)} | Breakeven Exits: {len(be_hits)}\n"
        f"Current Config -> Disp ATR: {current_config.get('min_displacement_atr')} | "
        f"Target RR: {current_config.get('target_rr')} | Min Confidence: {current_config.get('min_confidence_score', 75)} | Sweep ATR: {current_config.get('min_sweep_atr')}"
    )

    new_disp = current_config.get("min_displacement_atr", 1.2)
    new_rr = current_config.get("min_risk_reward", current_config.get("target_rr", 2.0))
    new_conf = current_config.get("min_confidence_score", 75)
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
                new_disp = float(parsed.get("min_displacement_atr", new_disp))
                new_rr = float(parsed.get("min_risk_reward", parsed.get("target_rr", new_rr)))
                new_conf = int(parsed.get("min_confidence_score", new_conf))
                new_sweep = float(parsed.get("min_sweep_atr", new_sweep))
                new_age = int(parsed.get("max_setup_age", new_age))
                rationale = str(parsed.get("tuning_rationale", rationale))
                log.info("Groq AI Post-Mortem Tuning response: %s", rationale)
        except Exception as groq_err:
            log.warning("Groq AI tuning call failed (%s). Applying statistical rule-based fallback.", groq_err)

    # Statistical rule-based fallback if Groq call failed or returned unchanged
    if not groq_api_key or rationale == "Algorithmic feedback loop verified.":
        if win_rate < 40.0 and len(losses) >= 2:
            new_disp = round(new_disp + 0.15, 2)
            new_conf = min(85, new_conf + 5)
            new_sweep = round(new_sweep + 0.03, 2)
            rationale = f"Tightened displacement filter ({new_disp}x ATR) and raised confidence filter ({new_conf}) to eliminate choppy sweep false-breakouts (Win Rate: {win_rate:.1f}%)."
        elif win_rate >= 65.0 and len(wins) >= 3:
            new_rr = round(new_rr + 0.25, 2)
            rationale = f"High win rate ({win_rate:.1f}%); expanded target RR to {new_rr}R to capture deeper institutional liquidity runs."

    old_disp = current_config.get("min_displacement_atr")
    old_rr = current_config.get("target_rr")
    old_conf = current_config.get("min_confidence_score", 75)

    # Apply strict hard clamping bounds
    current_config["min_displacement_atr"] = round(clamp_parameter("min_displacement_atr", new_disp, 1.2), 2)
    rr_clamped = round(clamp_parameter("min_risk_reward", new_rr, 2.0), 2)
    current_config["target_rr"] = rr_clamped
    current_config["min_risk_reward"] = rr_clamped
    current_config["min_confidence_score"] = int(round(clamp_parameter("min_confidence_score", new_conf, 75)))
    current_config["min_sweep_atr"] = round(clamp_parameter("min_sweep_atr", new_sweep, 0.10), 2)
    current_config["max_setup_age"] = int(round(clamp_parameter("max_setup_age", new_age, 12)))
    current_config["last_tuned_at"] = ts_now
    current_config["tuning_rationale"] = rationale
    current_config["total_tunings"] = int(current_config.get("total_tunings", 0)) + 1

    history_item = {
        "timestamp": ts_now,
        "old_disp": old_disp,
        "new_disp": current_config["min_displacement_atr"],
        "old_rr": old_rr,
        "new_rr": current_config["target_rr"],
        "old_conf": old_conf,
        "new_conf": current_config["min_confidence_score"],
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
