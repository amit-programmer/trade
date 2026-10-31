"""
engine_paper_broker.py - Virtual Paper Trading and Position Lifecycle Manager.

Handles interactive Telegram trade approval, manages open virtual positions,
executes dynamic 3-tier profit taking with automatic Breakeven stops, and logs performance.
Fully synchronized across multiple processes (trader_bot.py & server.py) via atomic file operations.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from engine_exit_manager import check_trade_stage, generate_exit_plan

log = logging.getLogger("trader_bot")
PAPER_FILE = Path(__file__).resolve().parent / "paper_positions.json"
HISTORY_FILE = Path(__file__).resolve().parent / "paper_positions_history.json"
_LOCK = threading.RLock()


class PaperBroker:
    def __init__(
        self,
        filepath: Path = PAPER_FILE,
        history_filepath: Optional[Path] = None,
        max_closed_threshold: int = 100,
        retain_closed: int = 10,
        max_concurrent_trades: int = 3,
        max_daily_loss_usd: float = 50.0,
    ):
        self.filepath = filepath
        self.history_filepath = history_filepath or HISTORY_FILE
        self.max_closed_threshold = int(max_closed_threshold)
        self.retain_closed = int(retain_closed)
        self.max_concurrent_trades = int(os.getenv("MAX_CONCURRENT_TRADES", str(max_concurrent_trades)))
        self.max_daily_loss_usd = float(os.getenv("MAX_DAILY_LOSS_USD", str(max_daily_loss_usd)))
        self.state: Dict[str, Any] = self._load()

    def _load(self) -> Dict[str, Any]:
        """Loads paper positions state from disk with safe defaults."""
        default_state: Dict[str, Any] = {
            "pending": {},      # trade_id -> setup data waiting for user button click
            "open": {},         # trade_id -> active position
            "closed": [],       # completed trade history
            "stats": {
                "total_trades": 0,
                "wins": 0,
                "losses": 0,
                "breakevens": 0,
                "total_pnl_usd": 0.0,
                "total_realized_r": 0.0,
            },
        }
        if self.filepath.exists():
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for k, v in default_state.items():
                        data.setdefault(k, v)
                    return data
            except Exception as exc:
                log.warning("Could not read paper positions file %s: %s", self.filepath, exc)
        return default_state

    def _reload(self) -> None:
        """Refreshes state from disk to synchronize with external processes (trader_bot / server)."""
        self.state = self._load()

    def _archive_closed_positions_if_needed(self) -> int:
        """
        State Rollover / Archiving Guard:
        When closed trades count exceeds max_closed_threshold (default 100),
        moves older records into paper_positions_history.json, retaining
        only the most recent retain_closed (default 10) records in the active state.
        Cumulative lifetime statistics in self.state['stats'] remain intact.
        Returns the number of records archived.
        """
        closed_trades = self.state.get("closed", [])
        if len(closed_trades) <= self.max_closed_threshold:
            return 0

        to_archive = closed_trades[:-self.retain_closed]
        to_retain = closed_trades[-self.retain_closed:]

        # Load existing history or initialize fresh list
        history: List[Dict[str, Any]] = []
        if self.history_filepath.exists():
            try:
                with open(self.history_filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        history = data
            except Exception as exc:
                log.warning("Could not read paper history file %s: %s", self.history_filepath, exc)

        history.extend(to_archive)

        temp_hist = self.history_filepath.with_suffix(".tmp")
        try:
            with open(temp_hist, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2, default=str)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            os.replace(temp_hist, self.history_filepath)
            self.state["closed"] = to_retain
            log.info(
                "Paper positions rollover: Archived %d closed trades to %s. Active state retained %d latest.",
                len(to_archive), self.history_filepath.name, len(to_retain)
            )
            return len(to_archive)
        except Exception as exc:
            log.warning("Could not archive paper positions: %s", exc)
            return 0
        finally:
            if temp_hist.exists():
                try:
                    temp_hist.unlink()
                except Exception:
                    pass

    def _save(self) -> None:
        """Atomically saves state to disk using a temporary file with fsync to avoid corruption or orphan files."""
        self._archive_closed_positions_if_needed()
        temp_file = self.filepath.with_suffix(".tmp")
        try:
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(self.state, f, indent=2, default=str)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            os.replace(temp_file, self.filepath)
        except Exception as exc:
            log.warning("Could not save paper positions: %s", exc)
        finally:
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass

    def shutdown(self) -> None:
        """Gracefully flushes state to disk and cleans any lingering .tmp files."""
        with _LOCK:
            self._save()
            temp_file = self.filepath.with_suffix(".tmp")
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass

    @property
    def pending_approvals(self) -> Dict[str, Any]:
        with _LOCK:
            self._reload()
            return dict(self.state.get("pending", {}))

    def get_open_positions(self) -> List[Dict[str, Any]]:
        with _LOCK:
            self._reload()
            return list(self.state.get("open", {}).values())

    def get_closed_positions(self) -> List[Dict[str, Any]]:
        with _LOCK:
            self._reload()
            return list(self.state.get("closed", []))

    def get_all_historical_closed_positions(self) -> List[Dict[str, Any]]:
        """Returns all historical closed positions combined with active state."""
        with _LOCK:
            self._reload()
            history: List[Dict[str, Any]] = []
            if self.history_filepath.exists():
                try:
                    with open(self.history_filepath, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            history = data
                except Exception:
                    pass
            return history + list(self.state.get("closed", []))

    def register_pending_setup(
        self,
        trade_id: str,
        symbol: str,
        direction: str,
        timeframe: str,
        entry_price: float,
        stop_loss: float,
        target: float,
        quantity: float,
        max_loss_usd: float,
        exit_plan: Dict[str, Any],
        ai_grade: str = "A",
        sim_win_prob: str = "50%",
    ) -> bool:
        """Stores setup while waiting for user interactive button click (Approve/Reject) if safety limits permit."""
        can_open, block_reason = self.can_open_new_trade()
        if not can_open:
            log.warning("Pending setup %s registration blocked by risk guard: %s", trade_id, block_reason)
            return False

        with _LOCK:
            self._reload()
            self.state["pending"][trade_id] = {
                "id": trade_id,
                "symbol": symbol,
                "direction": direction.upper(),
                "timeframe": timeframe,
                "entry_price": float(entry_price),
                "stop_loss": float(stop_loss),
                "initial_stop_loss": float(stop_loss),
                "target": float(target),
                "tp1": float(exit_plan["tp1"]["price"]),
                "tp2": float(exit_plan["tp2"]["price"]),
                "tp3": float(exit_plan["tp3"]["price"]),
                "quantity": float(quantity),
                "max_loss_usd": float(max_loss_usd),
                "ai_grade": ai_grade,
                "sim_win_prob": sim_win_prob,
                "created_at": time.time(),
                "created_ts": datetime.now(timezone.utc).isoformat(),
            }
            self._save()
        log.info("Registered pending paper setup %s (%s %s)", trade_id, symbol, direction)
        return True

    def get_today_realized_loss_usd(self) -> float:
        """Calculates today's net realized loss (in USD) since 00:00 UTC across active and archived closed trades."""
        today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        net_pnl = 0.0
        has_trades = False
        all_closed = self.get_all_historical_closed_positions()
        for t in all_closed:
            c_ts = t.get("closed_ts", "")
            if c_ts.startswith(today_utc):
                has_trades = True
                pnl = float(t.get("total_pnl_usd", 0.0))
                net_pnl += pnl
        if not has_trades or net_pnl >= 0:
            return 0.0
        return round(abs(net_pnl), 2)

    def can_open_new_trade(self) -> tuple[bool, str]:
        """
        Risk Governance Check:
        1. Max Concurrent Open Trades Guard (default: 3, env: MAX_CONCURRENT_TRADES)
        2. Daily Max Drawdown Hard-Stop Guard (default: $50, env: MAX_DAILY_LOSS_USD)
        """
        self.max_concurrent_trades = int(os.getenv("MAX_CONCURRENT_TRADES", str(self.max_concurrent_trades)))
        self.max_daily_loss_usd = float(os.getenv("MAX_DAILY_LOSS_USD", str(self.max_daily_loss_usd)))

        open_count = len(self.state.get("open", {}))
        if open_count >= self.max_concurrent_trades:
            return False, f"BLOCKED: MAX_CONCURRENT_TRADES_REACHED ({open_count}/{self.max_concurrent_trades} active)"

        today_loss = self.get_today_realized_loss_usd()
        if today_loss >= self.max_daily_loss_usd:
            return False, f"STATUS: DAILY_LOSS_LIMIT_REACHED (${today_loss:,.2f} >= max ${self.max_daily_loss_usd:,.2f})"

        return True, "OK"

    def approve_trade(self, trade_id: str) -> Optional[Dict[str, Any]]:
        """Moves a trade from pending to open (1-click approval) if capital safety limits permit."""
        with _LOCK:
            self._reload()
            # If already open, return it
            if trade_id in self.state.get("open", {}):
                return self.state["open"][trade_id]

            trade = self.state["pending"].get(trade_id)
            if not trade:
                return None

            # Risk Governance Check
            can_open, block_reason = self.can_open_new_trade()
            if not can_open:
                log.warning("Trade %s approval blocked by risk guard: %s", trade_id, block_reason)
                blocked_trade = dict(trade)
                blocked_trade["status"] = "BLOCKED"
                blocked_trade["blocked_reason"] = block_reason
                blocked_trade["detail"] = block_reason
                return blocked_trade

            trade = self.state["pending"].pop(trade_id)
            trade["status"] = "OPEN"
            trade["stage"] = "OPEN"
            trade["opened_at"] = time.time()
            trade["opened_ts"] = datetime.now(timezone.utc).isoformat()
            trade["booked_pnl_usd"] = 0.0
            trade["booked_r"] = 0.0
            trade["remaining_qty"] = trade["quantity"]
            trade["current_price"] = trade["entry_price"]
            trade["current_pnl_usd"] = 0.0
            trade["current_r"] = 0.0
            self.state["open"][trade_id] = trade
            self._save()
        log.info("Paper trade %s APPROVED: %s %s @ %s", trade_id, trade["symbol"], trade["direction"], trade["entry_price"])
        return trade

    def reject_trade(self, trade_id: str) -> bool:
        """Removes a setup from pending."""
        with _LOCK:
            self._reload()
            trade = self.state["pending"].pop(trade_id, None)
            if trade:
                self._save()
                log.info("Paper trade %s REJECTED: %s", trade_id, trade.get("symbol"))
                return True
        return False

    def update_entry_price_with_fill(self, trade_id: str, actual_fill_price: float) -> Optional[Dict[str, Any]]:
        """
        Slippage Guard: Updates the entry price of an open or pending trade
        with the actual average fill price from exchange execution,
        and recalculates TP1, TP2, TP3 and Breakeven levels.
        """
        actual_fill_price = float(actual_fill_price)
        with _LOCK:
            self._reload()
            pos = self.state.get("open", {}).get(trade_id)
            if not pos:
                pos = self.state.get("pending", {}).get(trade_id)
            if not pos:
                return None

            old_entry = float(pos.get("entry_price", actual_fill_price))
            pos["original_entry_price"] = old_entry
            pos["entry_price"] = actual_fill_price
            pos["slippage_usd"] = round(actual_fill_price - old_entry, 6)

            # Recalculate dynamic brackets from new fill price
            plan = generate_exit_plan(
                entry=actual_fill_price,
                stop_loss=float(pos.get("initial_stop_loss", pos.get("stop_loss", 0.0))),
                direction=pos.get("direction", "LONG"),
                target=float(pos.get("target", 0.0)),
            )
            pos["tp1"] = plan["tp1"]["price"]
            pos["tp2"] = plan["tp2"]["price"]
            pos["tp3"] = plan["tp3"]["price"]
            pos["breakeven_price"] = actual_fill_price
            if pos.get("stage") == "OPEN":
                pos["current_price"] = actual_fill_price

            self._save()
            log.info("Slippage Guard: Trade %s entry updated to %s (old: %s, new TP1: %s, TP2: %s)",
                     trade_id, actual_fill_price, old_entry, pos["tp1"], pos["tp2"])
            return pos

    def update_positions(self, current_prices: Dict[str, float]) -> List[Dict[str, Any]]:
        """
        Evaluates all open positions against latest market prices.
        Returns a list of milestone alert events (TP1, TP2, TP3, STOP, BE, STALE) to notify via Telegram.
        """
        events: List[Dict[str, Any]] = []
        to_close: List[Tuple[str, Dict[str, Any]]] = []

        with _LOCK:
            self._reload()
            open_positions = list(self.state.get("open", {}).items())

            for trade_id, pos in open_positions:
                sym = pos["symbol"]
                price = current_prices.get(sym)
                if price is None or price <= 0:
                    continue

                pos["current_price"] = price
                entry = float(pos["entry_price"])
                initial_sl = float(pos["initial_stop_loss"])
                risk_unit = abs(entry - initial_sl) or 1.0
                total_qty = float(pos["quantity"])
                is_long = pos["direction"] == "LONG"

                # Update current floating PnL
                rem_qty = float(pos.get("remaining_qty", total_qty))
                floating_pnl = rem_qty * (price - entry if is_long else entry - price)
                pos["current_pnl_usd"] = round(float(pos.get("booked_pnl_usd", 0.0)) + floating_pnl, 2)
                pos["current_r"] = round(pos["current_pnl_usd"] / float(pos.get("max_loss_usd", 20.0)), 2)

                elapsed = time.time() - float(pos.get("opened_at", time.time()))
                check = check_trade_stage(pos, price, elapsed_seconds=elapsed)
                event_type = check.get("event")

                if event_type == "HOLD":
                    continue

                if event_type == "TP1_HIT":
                    # Book 50% profit, move Stop to Breakeven
                    booked_qty = total_qty * 0.5
                    pnl_step = booked_qty * (price - entry if is_long else entry - price)
                    pos["booked_pnl_usd"] = round(pos.get("booked_pnl_usd", 0.0) + pnl_step, 2)
                    pos["booked_r"] = round(pos.get("booked_r", 0.0) + 0.75, 2)  # 50% * 1.5R
                    pos["remaining_qty"] = round(total_qty * 0.5, 4)
                    pos["stop_loss"] = entry  # Moved to Breakeven
                    pos["stage"] = "TP1_HIT"
                    events.append({
                        "type": "TP1_HIT",
                        "trade_id": trade_id,
                        "symbol": sym,
                        "direction": pos["direction"],
                        "exit_price": price,
                        "new_stop": entry,
                        "remaining_qty": pos["remaining_qty"],
                        "booked_pnl_usd": pos["booked_pnl_usd"],
                        "trade_data": dict(pos),
                        "message": (
                            f"🎯 *PAPER TRADE TP1 HIT: {sym} ({pos['direction']})*\n"
                            f"• Price: `${price:,.4f}`\n"
                            f"• 50% Locked: `+${pnl_step:,.2f}` (+1.5R)\n"
                            f"• 🛡️ *Stop-Loss moved to Breakeven:* `${entry:,.4f}` (Risk Free Trade!)"
                        ),
                    })

                elif event_type == "TP2_HIT":
                    # Book 30% profit
                    booked_qty = total_qty * 0.3
                    pnl_step = booked_qty * (price - entry if is_long else entry - price)
                    pos["booked_pnl_usd"] = round(pos.get("booked_pnl_usd", 0.0) + pnl_step, 2)
                    r_step = 0.3 * (abs(price - entry) / risk_unit)
                    pos["booked_r"] = round(pos.get("booked_r", 0.0) + r_step, 2)
                    pos["remaining_qty"] = round(total_qty * 0.2, 4)
                    pos["stage"] = "TP2_HIT"
                    events.append({
                        "type": "TP2_HIT",
                        "trade_id": trade_id,
                        "symbol": sym,
                        "direction": pos["direction"],
                        "exit_price": price,
                        "remaining_qty": pos["remaining_qty"],
                        "booked_pnl_usd": pos["booked_pnl_usd"],
                        "trade_data": dict(pos),
                        "message": (
                            f"🎯 *PAPER TRADE TP2 HIT: {sym} ({pos['direction']})*\n"
                            f"• Price: `${price:,.4f}`\n"
                            f"• 30% Locked: `+${pnl_step:,.2f}`\n"
                            f"• Total Secured: `+${pos['booked_pnl_usd']:,.2f}`\n"
                            f"• 🏃 20% Runner remaining with trailing stop"
                        ),
                    })

                elif event_type in ("TP3_HIT", "STOP_HIT", "BE_HIT", "STALE_ALERT"):
                    # Complete trade closure
                    rem_qty = float(pos.get("remaining_qty", total_qty))
                    pnl_final_step = rem_qty * (price - entry if is_long else entry - price)
                    total_pnl = round(float(pos.get("booked_pnl_usd", 0.0)) + pnl_final_step, 2)
                    total_r = round(total_pnl / float(pos.get("max_loss_usd", 20.0)), 2)

                    pos["closed_price"] = price
                    pos["closed_at"] = time.time()
                    pos["closed_ts"] = datetime.now(timezone.utc).isoformat()
                    pos["total_pnl_usd"] = total_pnl
                    pos["realized_r"] = total_r
                    pos["exit_reason"] = check.get("description", event_type)

                    to_close.append((trade_id, pos))

                    icon = "🏆" if total_pnl > 0 else ("⚖️" if total_pnl == 0 else "🛑")
                    events.append({
                        "type": event_type,
                        "trade_id": trade_id,
                        "symbol": sym,
                        "direction": pos["direction"],
                        "exit_price": price,
                        "total_pnl_usd": total_pnl,
                        "realized_r": total_r,
                        "exit_reason": pos["exit_reason"],
                        "trade_data": dict(pos),
                        "message": (
                            f"{icon} *PAPER TRADE CLOSED: {sym} ({pos['direction']})*\n"
                            f"• Outcome: *{pos['exit_reason']}*\n"
                            f"• Exit Price: `${price:,.4f}` (Entry: `${entry:,.4f}`)\n"
                            f"• Realized P&L: `{'+$' if total_pnl>=0 else '-$'}{abs(total_pnl):,.2f}` ({total_r:+.2f}R)\n"
                            f"• Duration: `{int(elapsed // 60)} mins`"
                        ),
                    })

            # Finalize closures
            for tid, closed_pos in to_close:
                self.state["open"].pop(tid, None)
                self.state["closed"].append(closed_pos)
                self.state["stats"]["total_trades"] += 1
                pnl = closed_pos.get("total_pnl_usd", 0.0)
                self.state["stats"]["total_pnl_usd"] = round(self.state["stats"]["total_pnl_usd"] + pnl, 2)
                self.state["stats"]["total_realized_r"] = round(self.state["stats"]["total_realized_r"] + closed_pos.get("realized_r", 0.0), 2)
                if pnl > 0.05:
                    self.state["stats"]["wins"] += 1
                elif pnl < -0.05:
                    self.state["stats"]["losses"] += 1
                else:
                    self.state["stats"]["breakevens"] += 1

            self._save()

        return events

    def get_summary_text(self) -> str:
        """Returns clean markdown summary of paper trading performance & open positions."""
        with _LOCK:
            self._reload()
            st = self.state["stats"]
            open_p = list(self.state["open"].values())
            pending_p = list(self.state["pending"].values())

        total = st.get("total_trades", 0)
        wins = st.get("wins", 0)
        losses = st.get("losses", 0)
        be = st.get("breakevens", 0)
        win_rate = round((wins / total) * 100, 1) if total > 0 else 0.0
        pnl = st.get("total_pnl_usd", 0.0)
        pnl_sign = "+$" if pnl >= 0 else "-$"
        pnl_r = st.get("total_realized_r", 0.0)

        lines = [
            "📑 *PAPER TRADING PORTFOLIO REPORT*",
            "━━━━━━━━━━━━━━━━━━━━",
            f"• *Closed Trades:* {total} (Wins: {wins} | Losses: {losses} | BE: {be})",
            f"• *Win Rate:* `{win_rate}%`",
            f"• *Realized P&L:* `{pnl_sign}{abs(pnl):,.2f}` (`{pnl_r:+.2f}R`)",
            "",
            f"⚡ *Active Open Positions:* {len(open_p)}",
        ]

        if not open_p:
            lines.append("  _No active virtual positions at the moment._")
        else:
            for p in open_p:
                cur_p = p.get("current_price", p["entry_price"])
                pnl_u = p.get("current_pnl_usd", 0.0)
                pnl_s = "+" if pnl_u >= 0 else ""
                lines.append(
                    f"  • *{p['symbol']} ({p['direction']})* | Entry: `${p['entry_price']:,.4f}` | "
                    f"Price: `${cur_p:,.4f}` | PnL: `{pnl_s}${pnl_u:.2f}` ({p.get('current_r', 0):+.2f}R) | "
                    f"Stop: `${p['stop_loss']:,.4f}` | Stage: `{p.get('stage', 'OPEN')}`"
                )

        if pending_p:
            lines.append(f"\n⏳ *Awaiting Approval:* {len(pending_p)} setup(s)")

        return "\n".join(lines)


# Singleton instance used by trader_bot and server
PAPER_BROKER = PaperBroker()
