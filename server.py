"""
server.py - FastAPI Web Dashboard & REST API for 24/7 SMC Crypto Trading Bot.

Provides:
- Web Dashboard UI on http://127.0.0.1:8000/
- Real-time bot status, paper trading portfolio, Fear & Greed sentiment, and Whale derivatives flow
- Interactive REST endpoints to control bot scanning and 1-click paper trade approvals
- Full multi-process state synchronization with trader_bot.py
"""
from __future__ import annotations

import json
import logging
import threading
import collections
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from engine_paper_broker import PAPER_BROKER
from engine_macro_filter import get_crypto_sentiment, check_macro_event_block
from engine_whale_tracker import get_whale_derivatives_data
from engine_delta_broker import DELTA_BROKER
from engine_adaptive_tuner import (
    load_adaptive_config,
    save_adaptive_config,
    run_adaptive_self_tuning,
    apply_adaptive_config_to_strategy,
)

# Import core configurations and state helpers safely from trader_bot
from trader_bot import (
    CFG,
    SYMBOLS,
    TIMEFRAMES,
    SCAN_INTERVAL,
    GROQ_MODEL,
    SCANNING_ACTIVE,
    BinanceClient,
    generate_daily_bias_screener,
    get_sheet_link,
    load_state,
    save_state,
    get_execution_mode,
    set_execution_mode,
    fmt_price,
    send_telegram,
    telegram_configured,
)

log = logging.getLogger("trade_server")

# --- WebSocket Streaming & Terminal Log Ring Buffer ---
TERMINAL_LOGS: collections.deque[Dict[str, str]] = collections.deque(maxlen=300)


class TerminalLogHandler(logging.Handler):
    def emit(self, record):
        try:
            msg = self.format(record)
            TERMINAL_LOGS.append({
                "time": datetime.now(timezone.utc).strftime("%H:%M:%S"),
                "level": record.levelname,
                "msg": msg,
            })
        except Exception:
            pass


_term_handler = TerminalLogHandler()
_term_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s] %(message)s", datefmt="%H:%M:%S"))
logging.getLogger().addHandler(_term_handler)


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._lock = threading.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        with self._lock:
            if websocket not in self.active_connections:
                self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        with self._lock:
            try:
                if websocket in self.active_connections:
                    self.active_connections.remove(websocket)
            except Exception:
                pass

    def prune_dead_connections(self) -> int:
        """Removes any stale, disconnected, or dead WebSocket connections from the active pool."""
        with self._lock:
            alive: List[WebSocket] = []
            for ws in self.active_connections:
                try:
                    state = getattr(ws, "client_state", None)
                    if state is not None and getattr(state, "name", "") == "DISCONNECTED":
                        continue
                    alive.append(ws)
                except Exception:
                    continue
            pruned = len(self.active_connections) - len(alive)
            self.active_connections = alive
            return pruned

    async def broadcast(self, message: dict):
        with self._lock:
            conns = list(self.active_connections)
        dead: List[WebSocket] = []
        for connection in conns:
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)
        if dead:
            with self._lock:
                for d in dead:
                    try:
                        if d in self.active_connections:
                            self.active_connections.remove(d)
                    except Exception:
                        pass


manager = ConnectionManager()

app = FastAPI(
    title="SMC Crypto Trading Bot API & Dashboard",
    description="24/7 Quantitative Liquidity Sweep Scanner, Paper Trading Engine & Smart Money Intelligence",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_client = BinanceClient()


@app.get("/health")
def health_check() -> Dict[str, Any]:
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "scanning_active": SCANNING_ACTIVE.is_set(),
    }


@app.get("/api/status")
def get_status() -> Dict[str, Any]:
    state = load_state()
    stats = state.get("stats", {})
    return {
        "scanning_active": SCANNING_ACTIVE.is_set(),
        "symbols": SYMBOLS,
        "timeframes": TIMEFRAMES,
        "scan_interval_sec": SCAN_INTERVAL,
        "groq_model": GROQ_MODEL,
        "total_scans": stats.get("scans", 0),
        "total_alerts": len(stats.get("alerts", [])),
        "errors": stats.get("errors", 0),
        "sheet_link": get_sheet_link(),
        "recent_alerts": stats.get("alerts", [])[-10:],
    }


@app.get("/api/paper")
def get_paper_trades() -> Dict[str, Any]:
    positions = PAPER_BROKER.get_open_positions()
    summary = PAPER_BROKER.get_summary_text()
    history = PAPER_BROKER.get_closed_positions()
    pending = PAPER_BROKER.pending_approvals
    return {
        "open_positions": positions,
        "closed_positions_count": len(history),
        "closed_positions": history[-20:],
        "summary_text": summary,
        "pending_count": len(pending),
        "pending_setups": pending,
    }


@app.post("/api/paper/approve/{trade_id}")
def approve_paper_trade(trade_id: str) -> Dict[str, Any]:
    trade = PAPER_BROKER.approve_trade(trade_id)
    if not trade:
        raise HTTPException(status_code=404, detail="Pending trade not found or already processed")
    if trade.get("status") == "BLOCKED":
        return {
            "status": "BLOCKED",
            "message": trade.get("blocked_reason") or trade.get("detail", "Blocked by risk guard"),
            "trade": trade,
        }

    delta_res = DELTA_BROKER.execute_approved_trade(trade)
    delta_lines = []
    if delta_res.get("status") in ("EXECUTED", "SUBMITTED"):
        oid = delta_res.get("order_id") or "Submitted"
        pid = delta_res.get("product_id")
        c_size = delta_res.get("size", trade.get("quantity"))
        fill_str = f" @ `${delta_res.get('actual_fill_price'):,.4f}`" if delta_res.get("actual_fill_price") else ""
        delta_lines.append(f"• ⚡ *Delta Testnet Order:* Placed (ID: `{oid}` | {c_size} Lots{fill_str} | Prod: `{pid}`)")
        if delta_res.get("slippage_usd") and abs(delta_res.get("slippage_usd", 0)) > 1e-4:
            delta_lines.append(f"• 🔍 *Slippage Guard:* Real Fill synced `${trade['entry_price']:,.4f}` (Shift: `{delta_res.get('slippage_usd'):+.2f}` USD)")
        if delta_res.get("stop_loss_response"):
            delta_lines.append(f"• 🛡️ *Bracket Stop-Loss:* Active on Delta @ `${trade['stop_loss']:,.4f}`")
        if delta_res.get("take_profit_response"):
            delta_lines.append(f"• 🎯 *Bracket Take-Profit:* Active on Delta @ `${trade['tp1']:,.4f}`")
    elif delta_res.get("status") == "BLOCKED":
        delta_lines.append(f"• 🛑 *Delta Testnet:* Blocked ({delta_res.get('reason')} - {delta_res.get('detail')})")
    elif DELTA_BROKER.enabled:
        delta_lines.append(f"• ⚠️ *Delta Testnet:* {delta_res.get('detail') or 'Routing check required'}")
    else:
        delta_lines.append("• 📑 *Execution Mode:* Virtual Paper Trading Portfolio (Delta Testnet disabled)")
    delta_text = "\n" + "\n".join(delta_lines)

    if telegram_configured():
        send_telegram(
            f"🟢 *Paper Trade Approved via Web Dashboard!*\n\n"
            f"• Pair: *{trade['symbol']} ({trade['direction']})*\n"
            f"• Sizing: `{trade['quantity']} {trade['symbol'].replace('USDT', '')}` (~${trade['max_loss_usd']:g} Risk)\n"
            f"• Entry: `${trade['entry_price']:,.4f}`\n"
            f"• Target 1 (1.5R): `${trade['tp1']:,.4f}` (Auto-BE trigger)\n"
            f"• Target 2: `${trade['tp2']:,.4f}`\n"
            f"• Stop Loss: `${trade['stop_loss']:,.4f}`"
            f"{delta_text}",
            parse_mode="Markdown"
        )
    return {"status": "APPROVED", "trade_id": trade_id, "trade": trade, "delta_execution": delta_res}


@app.post("/api/paper/reject/{trade_id}")
def reject_paper_trade(trade_id: str) -> Dict[str, Any]:
    success = PAPER_BROKER.reject_trade(trade_id)
    if not success:
        raise HTTPException(status_code=404, detail="Pending trade not found or already processed")
    return {"status": "REJECTED", "trade_id": trade_id}


@app.get("/api/sentiment")
def get_sentiment() -> Dict[str, Any]:
    return get_crypto_sentiment()


@app.get("/api/macro-events")
def get_macro_events() -> Dict[str, Any]:
    return check_macro_event_block(buffer_pre_min=30, buffer_post_min=15)


@app.get("/api/whales")
def get_whales() -> Dict[str, Any]:
    flows = {}
    for s in SYMBOLS:
        try:
            flows[s] = get_whale_derivatives_data(s)
        except Exception as exc:
            flows[s] = {"error": str(exc)}
    return {"whale_flows": flows}


@app.get("/api/screener")
def get_screener() -> Dict[str, Any]:
    screener_text = generate_daily_bias_screener(_client)
    return {"screener_text": screener_text}


@app.get("/api/delta")
def get_delta_status() -> Dict[str, Any]:
    return {
        "enabled": DELTA_BROKER.enabled,
        "base_url": DELTA_BROKER.base_url,
        "has_api_key": bool(DELTA_BROKER.api_key),
        "has_api_secret": bool(DELTA_BROKER.api_secret),
        "product_cache": DELTA_BROKER._product_cache,
        "balance": DELTA_BROKER.get_balance() if (DELTA_BROKER.enabled and DELTA_BROKER.api_key) else {"status": "DISABLED_OR_NO_KEYS"},
    }


@app.post("/api/control/pause")
def pause_scanning() -> Dict[str, Any]:
    SCANNING_ACTIVE.clear()
    log.info("Scanning paused via API")
    return {"status": "PAUSED", "scanning_active": False}


@app.post("/api/control/resume")
def resume_scanning() -> Dict[str, Any]:
    SCANNING_ACTIVE.set()
    log.info("Scanning resumed via API")
    return {"status": "RESUMED", "scanning_active": True}


def get_micro_agents_status() -> Dict[str, Any]:
    state = load_state()
    stats = state.get("stats", {})
    sentiment = get_crypto_sentiment()
    macro = check_macro_event_block(buffer_pre_min=30, buffer_post_min=15)
    open_pos = PAPER_BROKER.get_open_positions()
    closed_pos = PAPER_BROKER.get_closed_positions()
    wins = sum(1 for p in closed_pos if float(p.get("total_pnl_usd", 0)) > 0)
    win_rate = (wins / len(closed_pos) * 100) if closed_pos else 0.0
    tot_pnl = sum(float(p.get("total_pnl_usd", 0)) for p in closed_pos)
    today_loss = PAPER_BROKER.get_today_realized_loss_usd()
    exec_m = get_execution_mode()

    return {
        "scanner": {
            "name": "Scanner Agent",
            "status": "RUNNING" if SCANNING_ACTIVE.is_set() else "PAUSED",
            "detail": f"{len(SYMBOLS)} Pairs | {len(TIMEFRAMES)} TFs | {stats.get('scans', 0)} cycles",
            "active": SCANNING_ACTIVE.is_set(),
        },
        "filter": {
            "name": "Filter Agent",
            "status": "BLOCKED" if macro.get("is_blocked") else "CLEAR",
            "detail": f"F&G: {sentiment.get('score', 50)} ({sentiment.get('classification', 'Neutral')}) | Macro: {macro.get('status', 'OK')}",
            "active": not macro.get("is_blocked"),
        },
        "sizer": {
            "name": "Sizer Agent",
            "status": "ARMED",
            "detail": f"$20 Max 1R Risk | ATR Sizing | ${today_loss:.2f}/$50 Daily Loss",
            "active": True,
        },
        "executor": {
            "name": "Executor Agent",
            "status": exec_m,
            "detail": f"Broker: {'Delta Testnet' if DELTA_BROKER.enabled else 'Paper Portfolio'} | Mode: {exec_m}",
            "active": True,
        },
        "logger": {
            "name": "Logger Agent",
            "status": "SYNCED",
            "detail": f"Telegram Alerting | 4-Tab Google Sheets | Log Rotation",
            "active": True,
        },
        "equity": {
            "name": "Equity Agent",
            "status": "HEALTHY",
            "detail": f"Net PnL: ${tot_pnl:+.2f} | Win Rate: {win_rate:.1f}% ({len(closed_pos)} trades) | Open: {len(open_pos)}",
            "active": True,
        },
    }


@app.get("/api/agents")
def get_agents_endpoint() -> Dict[str, Any]:
    return get_micro_agents_status()


@app.get("/api/control/mode")
def get_mode_endpoint() -> Dict[str, Any]:
    return {"execution_mode": get_execution_mode()}


@app.post("/api/control/toggle-mode")
def toggle_mode_endpoint() -> Dict[str, Any]:
    curr = get_execution_mode()
    target = "AUTONOMOUS" if curr == "HUMAN" else "HUMAN"
    new_m = set_execution_mode(target)
    if telegram_configured():
        send_telegram(
            f"🔄 *Master Execution Mode Toggled via Web Dashboard!*\n\n"
            f"• *New Mode:* `{new_m}`\n"
            f"• *Behavior:* {'🤖 AI Full Autonomy (Zero-touch Grade A/A+ execution)' if new_m == 'AUTONOMOUS' else '👤 Human Approval Required'}",
            parse_mode="Markdown",
        )
    return {"status": "SUCCESS", "previous_mode": curr, "execution_mode": new_m}


@app.post("/api/control/mode")
async def set_mode_endpoint(req: Request) -> Dict[str, Any]:
    try:
        body = await req.json()
        target = body.get("mode", "HUMAN")
    except Exception:
        target = "HUMAN"
    new_m = set_execution_mode(target)
    return {"status": "SUCCESS", "execution_mode": new_m}


@app.post("/api/paper/close/{trade_id}")
def manual_close_trade(trade_id: str) -> Dict[str, Any]:
    open_positions = PAPER_BROKER.get_open_positions()
    pos = next((p for p in open_positions if p["trade_id"] == trade_id), None)
    if not pos:
        raise HTTPException(status_code=404, detail="Open trade not found")
    sym = pos["symbol"]
    try:
        cur_price = _client.price(sym)
    except Exception:
        cur_price = float(pos.get("entry_price", 0.0))
    closed_event = PAPER_BROKER.close_position_early(trade_id, cur_price, reason="MANUAL_UI_EARLY_CLOSE")
    if not closed_event:
        raise HTTPException(status_code=400, detail="Could not close position early")
    if hasattr(DELTA_BROKER, "close_position_on_exchange"):
        DELTA_BROKER.close_position_on_exchange(sym)
    else:
        DELTA_BROKER.cancel_all_orders_for_symbol(sym)
    if telegram_configured():
        send_telegram(
            f"🛑 *Position Closed Early via Web Dashboard*\n\n"
            f"• Symbol: *{sym} ({pos.get('direction')})*\n"
            f"• Exit Price: `${cur_price:,.4f}`\n"
            f"• Realized PnL: `${closed_event.get('total_pnl_usd', 0):+.2f}`",
            parse_mode="Markdown",
        )
    return {"status": "CLOSED", "trade_id": trade_id, "event": closed_event}


@app.get("/api/adaptive-config")
def get_adaptive_config_endpoint() -> Dict[str, Any]:
    return load_adaptive_config()


@app.post("/api/control/run-tuner")
def run_tuner_endpoint() -> Dict[str, Any]:
    closed = PAPER_BROKER.get_closed_positions()
    new_cfg, row = run_adaptive_self_tuning(closed)
    apply_adaptive_config_to_strategy(CFG)
    return {"status": "SUCCESS", "adaptive_config": new_cfg, "log_row": row}


@app.websocket("/ws/terminal")
async def websocket_terminal(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Initial snapshot on connection
        snapshot = {
            "type": "SNAPSHOT",
            "execution_mode": get_execution_mode(),
            "scanning_active": SCANNING_ACTIVE.is_set(),
            "open_positions": PAPER_BROKER.get_open_positions(),
            "pending_approvals": PAPER_BROKER.pending_approvals,
            "closed_count": len(PAPER_BROKER.get_closed_positions()),
            "logs": list(TERMINAL_LOGS)[-50:],
            "agents": get_micro_agents_status(),
        }
        await websocket.send_json(snapshot)

        # Heartbeat & client command listener
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("action") == "PING":
                    await websocket.send_json({"type": "PONG", "timestamp": time.time()})
                elif msg.get("action") == "TOGGLE_MODE":
                    curr = get_execution_mode()
                    new_m = set_execution_mode("AUTONOMOUS" if curr == "HUMAN" else "HUMAN")
                    await manager.broadcast({"type": "MODE_CHANGE", "execution_mode": new_m})
                elif msg.get("action") == "REFRESH":
                    await websocket.send_json({
                        "type": "REFRESH",
                        "agents": get_micro_agents_status(),
                        "open_positions": PAPER_BROKER.get_open_positions(),
                        "pending_approvals": PAPER_BROKER.pending_approvals,
                        "logs": list(TERMINAL_LOGS)[-20:],
                    })
            except Exception:
                pass
    except (WebSocketDisconnect, ConnectionResetError):
        pass
    except Exception as exc:
        log.debug("WebSocket client disconnected: %s", exc)
    finally:
        manager.disconnect(websocket)


# In-memory mtime-based template cache to prevent repeated disk I/O on dashboard requests
_INDEX_HTML_CACHE: Dict[str, Any] = {"mtime": 0.0, "content": ""}


def _get_cached_index_html(tmpl_path: Path) -> str:
    """Reads index.html with mtime caching for zero-disk-I/O overhead while supporting hot reload."""
    try:
        mtime = tmpl_path.stat().st_mtime
        if mtime != _INDEX_HTML_CACHE["mtime"] or not _INDEX_HTML_CACHE["content"]:
            _INDEX_HTML_CACHE["content"] = tmpl_path.read_text(encoding="utf-8")
            _INDEX_HTML_CACHE["mtime"] = mtime
        return _INDEX_HTML_CACHE["content"]
    except Exception:
        return tmpl_path.read_text(encoding="utf-8")


@app.get("/", response_class=HTMLResponse)
def index_dashboard() -> Any:
    """Institutional Dark Theme Dashboard for Web Browsers."""
    tmpl_path = Path(__file__).resolve().parent / "templates" / "index.html"
    if tmpl_path.exists():
        return HTMLResponse(content=_get_cached_index_html(tmpl_path))

    status_str = "🟢 ACTIVE" if SCANNING_ACTIVE.is_set() else "🛑 PAUSED"
    status_badge_class = "badge-active" if SCANNING_ACTIVE.is_set() else "badge-paused"
    sheet_link = get_sheet_link() or "#"
    sent = get_crypto_sentiment()
    fg_score = sent.get("score", 50)
    fg_class = sent.get("classification", "Neutral")
    advice = sent.get("advice", "")

    macro_block = check_macro_event_block(buffer_pre_min=30, buffer_post_min=15)
    macro_status_str = "BLOCKED ⚠️" if macro_block.get("is_blocked") else "CLEAR ✅"
    macro_badge_cls = "badge-paused" if macro_block.get("is_blocked") else "badge-active"
    macro_sub = macro_block.get("reason", "No high-impact announcements near")

    delta_enabled = DELTA_BROKER.enabled
    delta_mode_str = "DELTA TESTNET ⚡" if delta_enabled else "VIRTUAL PAPER 📑"
    delta_badge_cls = "badge-stage" if delta_enabled else "badge-active"
    delta_sub = f"Base: {DELTA_BROKER.base_url.replace('https://', '')}" if delta_enabled else "Internal atomic portfolio tracking"

    positions = PAPER_BROKER.get_open_positions()
    pending = PAPER_BROKER.pending_approvals

    # Pending approvals table
    pending_rows_html = ""
    if not pending:
        pending_rows_html = '<tr><td colspan="7" class="text-center text-muted">No pending trade approvals. Waiting for high-probability SMC liquidity sweep setup.</td></tr>'
    else:
        for tid, p in pending.items():
            pending_rows_html += f"""
            <tr>
                <td><strong>{p.get('symbol')}</strong></td>
                <td><span class="badge badge-{p.get('direction', 'LONG').lower()}">{p.get('direction')} ({p.get('timeframe')})</span></td>
                <td>${p.get('entry_price', 0):,.4f}</td>
                <td>${p.get('stop_loss', 0):,.4f}</td>
                <td>${p.get('tp1', 0):,.4f} (Auto-BE)</td>
                <td><span class="badge badge-stage">{p.get('ai_grade', 'A')} ({p.get('sim_win_prob', 'N/A')} Win)</span></td>
                <td>
                    <button onclick="approveTrade('{tid}')" class="btn btn-green">🟢 Approve</button>
                    <button onclick="rejectTrade('{tid}')" class="btn btn-red">🔴 Skip</button>
                </td>
            </tr>
            """

    # Open positions table
    pos_rows_html = ""
    if not positions:
        pos_rows_html = '<tr><td colspan="7" class="text-center text-muted">No open virtual trades active.</td></tr>'
    else:
        for p in positions:
            pnl = p.get("current_pnl_usd", 0.0)
            pnl_cls = "text-green" if pnl >= 0 else "text-red"
            pnl_sign = "+" if pnl >= 0 else ""
            pos_rows_html += f"""
            <tr>
                <td><strong>{p.get('symbol')}</strong></td>
                <td><span class="badge badge-{p.get('direction', 'LONG').lower()}">{p.get('direction')}</span></td>
                <td>${p.get('entry_price', 0):,.4f}</td>
                <td>${p.get('current_price', p.get('entry_price', 0)):,.4f}</td>
                <td>${p.get('stop_loss', 0):,.4f}</td>
                <td><span class="badge badge-stage">{p.get('stage', 'OPEN')}</span></td>
                <td class="{pnl_cls}"><strong>{pnl_sign}${pnl:.2f} ({pnl_sign}{p.get('current_r', 0):+.2f}R)</strong></td>
            </tr>
            """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SMC Quantitative Trading Terminal</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg: #090c10;
            --surface: #11151c;
            --surface-border: #1c2331;
            --primary: #3b82f6;
            --green: #10b981;
            --red: #ef4444;
            --yellow: #f59e0b;
            --text-main: #f3f4f6;
            --text-muted: #9ca3af;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            background: var(--bg);
            color: var(--text-main);
            font-family: 'Inter', -apple-system, sans-serif;
            padding: 24px;
            min-height: 100vh;
        }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--surface-border);
            padding-bottom: 18px;
            margin-bottom: 24px;
        }}
        .brand {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .brand h1 {{
            font-size: 20px;
            font-weight: 700;
            letter-spacing: -0.5px;
        }}
        .grid-cards {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}
        .card {{
            background: var(--surface);
            border: 1px solid var(--surface-border);
            border-radius: 12px;
            padding: 18px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.3);
        }}
        .card-title {{
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.8px;
            color: var(--text-muted);
            margin-bottom: 8px;
        }}
        .card-value {{
            font-size: 24px;
            font-weight: 800;
            font-family: 'JetBrains Mono', monospace;
        }}
        .card-sub {{
            font-size: 13px;
            color: var(--text-muted);
            margin-top: 6px;
        }}
        .badge {{
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 12px;
            font-weight: 700;
            text-transform: uppercase;
        }}
        .badge-active {{ background: rgba(16, 185, 129, 0.15); color: var(--green); border: 1px solid rgba(16, 185, 129, 0.3); }}
        .badge-paused {{ background: rgba(239, 68, 68, 0.15); color: var(--red); border: 1px solid rgba(239, 68, 68, 0.3); }}
        .badge-long {{ background: rgba(16, 185, 129, 0.2); color: var(--green); }}
        .badge-short {{ background: rgba(239, 68, 68, 0.2); color: var(--red); }}
        .badge-stage {{ background: rgba(59, 130, 246, 0.2); color: var(--primary); }}
        .table-wrap {{
            background: var(--surface);
            border: 1px solid var(--surface-border);
            border-radius: 12px;
            overflow-x: auto;
            margin-bottom: 24px;
        }}
        .table-header {{
            padding: 16px 18px;
            border-bottom: 1px solid var(--surface-border);
            font-size: 15px;
            font-weight: 700;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            font-size: 14px;
        }}
        th, td {{
            padding: 14px 18px;
            border-bottom: 1px solid var(--surface-border);
        }}
        th {{
            background: rgba(255,255,255,0.02);
            color: var(--text-muted);
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.8px;
        }}
        .text-green {{ color: var(--green); }}
        .text-red {{ color: var(--red); }}
        .text-muted {{ color: var(--text-muted); }}
        .text-center {{ text-align: center; }}
        .btn {{
            background: var(--surface);
            border: 1px solid var(--surface-border);
            color: var(--text-main);
            padding: 6px 14px;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 600;
            cursor: pointer;
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 6px;
            transition: all 0.2s;
        }}
        .btn:hover {{
            background: rgba(255,255,255,0.06);
            border-color: rgba(255,255,255,0.2);
        }}
        .btn-green {{ background: rgba(16, 185, 129, 0.2); color: var(--green); border: 1px solid rgba(16, 185, 129, 0.4); }}
        .btn-green:hover {{ background: rgba(16, 185, 129, 0.4); }}
        .btn-red {{ background: rgba(239, 68, 68, 0.2); color: var(--red); border: 1px solid rgba(239, 68, 68, 0.4); }}
        .btn-red:hover {{ background: rgba(239, 68, 68, 0.4); }}
        .btn-primary {{
            background: var(--primary);
            color: #fff;
            border: none;
        }}
        .btn-primary:hover {{
            background: #2563eb;
        }}
        .btn-group {{
            display: flex;
            gap: 10px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <div class="brand">
            <span style="font-size: 26px;">⚡</span>
            <div>
                <h1>SMC Quantitative Trading Terminal</h1>
                <p style="font-size: 12px; color: var(--text-muted);">24/7 Binance Liquidity Sweep + Groq AI Qwen 27B + Monte Carlo 10,000x</p>
            </div>
        </div>
        <div class="btn-group">
            <a href="{sheet_link}" target="_blank" class="btn">📊 Google Sheet</a>
            <a href="/api/screener" target="_blank" class="btn">📋 Bias Screener</a>
            <a href="/api/delta" target="_blank" class="btn">⚡ Delta Broker API</a>
            <a href="/docs" target="_blank" class="btn btn-primary">📖 Swagger Docs</a>
        </div>
    </div>

    <div class="grid-cards">
        <div class="card">
            <div class="card-title">Scanner Engine Status</div>
            <div class="card-value"><span class="badge {status_badge_class}">{status_str}</span></div>
            <div class="card-sub">{len(SYMBOLS)} Pairs • {', '.join(TIMEFRAMES)} • Cycle: {SCAN_INTERVAL}s</div>
        </div>
        <div class="card">
            <div class="card-title">Fear & Greed Index</div>
            <div class="card-value text-green">{fg_score}<span style="font-size: 16px; color: var(--text-muted);">/100</span></div>
            <div class="card-sub">{fg_class} • {advice}</div>
        </div>
        <div class="card">
            <div class="card-title">Macro Event Buffer (30m)</div>
            <div class="card-value"><span class="badge {macro_badge_cls}">{macro_status_str}</span></div>
            <div class="card-sub">{macro_sub}</div>
        </div>
        <div class="card">
            <div class="card-title">Virtual Broker Portfolio</div>
            <div class="card-value text-green">{len(positions)} Active</div>
            <div class="card-sub">{len(pending)} Awaiting Approval • TP1 Auto-BE</div>
        </div>
        <div class="card">
            <div class="card-title">Execution Mode</div>
            <div class="card-value"><span class="badge {delta_badge_cls}">{delta_mode_str}</span></div>
            <div class="card-sub">{delta_sub}</div>
        </div>
    </div>

    <!-- PENDING SETUPS SECTION -->
    <div class="table-wrap">
        <div class="table-header">
            <span>⏳ Setups Awaiting 1-Click Interactive Approval ({len(pending)})</span>
            <span style="font-size: 12px; color: var(--text-muted);">Approve here or via Telegram Buttons</span>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Asset</th>
                    <th>Direction</th>
                    <th>Entry Level</th>
                    <th>Stop Loss</th>
                    <th>Target 1</th>
                    <th>Grade & MC Odds</th>
                    <th>Actions</th>
                </tr>
            </thead>
            <tbody>
                {pending_rows_html}
            </tbody>
        </table>
    </div>

    <!-- OPEN POSITIONS SECTION -->
    <div class="table-wrap">
        <div class="table-header">
            <span>⚡ Active Open Virtual Positions ({len(positions)})</span>
            <span style="font-size: 12px; color: var(--text-muted);">Real-time tracking every 10s • Auto-BE at TP1</span>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Asset</th>
                    <th>Direction</th>
                    <th>Entry Price</th>
                    <th>Current Price</th>
                    <th>Stop Loss</th>
                    <th>Stage</th>
                    <th>Unrealized PnL</th>
                </tr>
            </thead>
            <tbody>
                {pos_rows_html}
            </tbody>
        </table>
    </div>

    <div style="display: flex; justify-content: space-between; align-items: center; font-size: 12px; color: var(--text-muted);">
        <div>Terminal Engine: <code>trader_bot.py</code> (FastAPI ASGI Gateway & Synchronized State)</div>
        <div>Auto-refreshes every 10 seconds</div>
    </div>

    <script>
        async function approveTrade(tradeId) {{
            try {{
                const res = await fetch(`/api/paper/approve/${{tradeId}}`, {{ method: 'POST' }});
                if (res.ok) {{
                    window.location.reload();
                }} else {{
                    alert('Error approving trade');
                }}
            }} catch (e) {{
                alert('Connection error: ' + e);
            }}
        }}

        async function rejectTrade(tradeId) {{
            try {{
                const res = await fetch(`/api/paper/reject/${{tradeId}}`, {{ method: 'POST' }});
                if (res.ok) {{
                    window.location.reload();
                }} else {{
                    alert('Error rejecting trade');
                }}
            }} catch (e) {{
                alert('Connection error: ' + e);
            }}
        }}

        setTimeout(() => {{ window.location.reload(); }}, 10000);
    </script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
