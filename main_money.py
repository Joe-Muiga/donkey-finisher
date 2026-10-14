"""
main_money.py - profit-securing money rules for the MAIN (collector) account.

Same rules as the follower accounts (see money_rules.py), wrapped in a small stateful object the
engines call:

    start(client)   once at boot: load saved P/floor, read the live balance, apply the midnight reset
    on_balance(b)   every live balance update from Deriv (sync, runs inside the event loop)
    next_stake()    stake for the next buy, or None = stopped (floor reached) / not ready
    reserve(stake)  hard rule: only True if balance - pending - stake >= floor; holds the stake
    note_buy(buy)   feed the buy response so the cached balance drops immediately
    release(stake)  drop the hold taken by reserve()
    snapshot()      dict for the dashboard (keep_alive "money")

Reset = midnight Kenya time (21:00 UTC). Floor is sticky until the next reset.
State survives redeploys via the Render env var MAIN_MONEY_STATE (needs RENDER_API_KEY +
RENDER_SERVICE_ID, same as predictor_stats) and a local file as a same-container fallback.
Env: MAIN_LOCK_THRESHOLD (default 5.0) - balance at/above which the floor is armed.
"""
import asyncio
import json
import logging
import os
import time
from dataclasses import replace
from datetime import datetime, timezone
from typing import Optional

import money_rules as mr

logger = logging.getLogger(__name__)

ENV_KEY = "MAIN_MONEY_STATE"
STATE_FILE = os.environ.get("MAIN_MONEY_STATE_FILE", "/tmp/main_money_state.json")
PERSIST_MIN_INTERVAL = 300.0     # seconds between Render env writes (each write can trigger a deploy)


def _lock_threshold() -> float:
    try:
        return float((os.environ.get("MAIN_LOCK_THRESHOLD") or "").strip() or 5.0)
    except ValueError:
        return 5.0


def _fmt(x: Optional[float]) -> str:
    return "-" if x is None else f"{x:.2f}"


class MainMoney:
    def __init__(self):
        self.state = mr.FState()
        self.lock_threshold = _lock_threshold()
        self.balance: Optional[float] = None
        self.ready = False
        self._pending = 0.0           # stakes reserved but not yet reflected in the balance
        self._credit = 0.0            # part of the holds already reflected via note_buy()
        self._last_persist = 0.0
        self._persisted_key = None
        self._persist_task: Optional[asyncio.Task] = None

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def start(self, client) -> None:
        self._load()
        try:
            bal = await client.fetch_balance_live()
        except Exception as exc:
            logger.warning("MAIN MONEY: live balance fetch failed (%s) - using cached value", exc)
            bal = float(getattr(client, "balance", 0.0) or 0.0)
        if bal <= 0:
            raise RuntimeError(f"no usable balance ({bal})")
        self.balance = bal
        self._reset_or_update(bal, boot=True)
        self.ready = True
        logger.info("MAIN MONEY ready: balance=%.2f P=%s tier=%s floor=%s lock>=%.2f min stake=%.2f",
                    bal, _fmt(self.state.p), self.state.tier, _fmt(self.state.floor),
                    self.lock_threshold, mr.MIN_STAKE)

    def _reset_or_update(self, bal: float, boot: bool = False) -> None:
        today = mr.kenya_day_ts(time.time())
        if mr.needs_reset(self.state, today):
            self.state = mr.apply_reset(self.state, bal, self.lock_threshold, today)
            logger.info("MAIN MONEY reset (Kenya midnight%s): P=%.2f tier=%s floor=%s",
                        ", boot" if boot else "", bal, self.state.tier, _fmt(self.state.floor))
            self._persist(force=True)
            return
        new = mr.update_on_balance(self.state, bal, self.lock_threshold)
        if new != self.state:
            if new.floor != self.state.floor:
                logger.info("MAIN MONEY floor %s -> %s (peak %s)", _fmt(self.state.floor),
                            _fmt(new.floor), _fmt(new.peak))
            self.state = new
            self._persist()

    # ── balance feed ─────────────────────────────────────────────────────────
    def on_balance(self, bal: float) -> None:
        try:
            bal = float(bal)
        except (TypeError, ValueError):
            return
        self.balance = bal
        if not self.ready:
            return
        self._reset_or_update(bal)
        ms = mr.milestone(self.state)
        if ms and not self.state.milestone_hit and bal >= ms:
            self.state = replace(self.state, milestone_hit=True)
            logger.info("MAIN MONEY milestone %.2f reached (balance %.2f) - trading continues", ms, bal)
            self._persist()

    # ── stake / buy rule ─────────────────────────────────────────────────────
    def _eff(self) -> Optional[float]:
        if not self.ready or self.balance is None:
            return None
        return mr.effective_balance(self.balance, self._pending)

    def next_stake(self) -> Optional[float]:
        eff = self._eff()
        if eff is None:
            return None
        return mr.stake_for(self.state, eff)

    def reserve(self, stake: float) -> bool:
        eff = self._eff()
        if eff is None or not mr.can_buy(self.state, eff, stake):
            return False
        self._pending += stake
        return True

    def note_buy(self, buy: dict) -> None:
        """Deriv deducts the buy price at once; use balance_after so the next reserve() is exact."""
        try:
            price = float((buy or {}).get("buy_price") or 0.0)
            after = (buy or {}).get("balance_after")
            if after is not None:
                self.on_balance(float(after))
            elif self.balance is not None and price:
                self.on_balance(self.balance - price)
            moved = min(self._pending, price)     # that money is now out of the balance itself
            self._pending -= moved
            self._credit += moved
        except Exception as exc:
            logger.debug("MAIN MONEY note_buy ignored: %s", exc)

    def release(self, stake: float) -> None:
        if self._credit >= stake - mr.EPS:
            self._credit -= stake
        else:
            self._pending -= stake
        self._pending = max(self._pending, 0.0)
        self._credit = max(self._credit, 0.0)

    # ── dashboard ────────────────────────────────────────────────────────────
    def snapshot(self) -> dict:
        s, bal = self.state, self.balance
        stopped = bool(self.ready and bal is not None and mr.is_stopped(s, mr.effective_balance(bal, self._pending)))
        return {"ready": self.ready, "tier": s.tier, "p": s.p, "peak": s.peak, "floor": s.floor,
                "balance": bal, "tradable": None if bal is None else round(mr.tradable(s, bal), 2),
                "next_stake": self.next_stake(), "stopped": stopped, "milestone": mr.milestone(s),
                "milestone_hit": s.milestone_hit, "last_reset_date": s.last_reset_date,
                "lock_threshold": self.lock_threshold}

    # ── persistence ──────────────────────────────────────────────────────────
    def _payload(self) -> dict:
        s = self.state
        return {"p": s.p, "peak": s.peak, "floor": s.floor,
                "last_reset_date": s.last_reset_date, "milestone_hit": s.milestone_hit}

    def _load(self) -> None:
        raw = (os.environ.get(ENV_KEY) or "").strip()
        if not raw and os.path.exists(STATE_FILE):
            try:
                raw = open(STATE_FILE).read()
            except OSError:
                raw = ""
        if not raw:
            return
        try:
            d = json.loads(raw)
            self.state = mr.FState(p=d.get("p"), peak=d.get("peak"), floor=d.get("floor"),
                                   last_reset_date=d.get("last_reset_date"),
                                   milestone_hit=bool(d.get("milestone_hit")))
            logger.info("MAIN MONEY loaded saved state: P=%s floor=%s day=%s",
                        _fmt(self.state.p), _fmt(self.state.floor), self.state.last_reset_date)
        except Exception as exc:
            logger.warning("MAIN MONEY saved state unreadable (%s) - starting fresh", exc)

    def _persist(self, force: bool = False) -> None:
        """Local file every time; Render env var only on floor/day changes, throttled
        (an env-var write can trigger a Render deploy)."""
        payload = self._payload()
        try:
            with open(STATE_FILE, "w") as f:
                json.dump(payload, f)
        except OSError:
            pass
        key = (payload["floor"], payload["last_reset_date"])
        now = time.monotonic()
        if key == self._persisted_key:
            return
        if not force and now - self._last_persist < PERSIST_MIN_INTERVAL:
            return
        self._persisted_key, self._last_persist = key, now
        try:
            import env_persist
            if not env_persist.configured():
                os.environ[ENV_KEY] = json.dumps(payload)
                return
            if self._persist_task is None or self._persist_task.done():
                self._persist_task = asyncio.ensure_future(
                    env_persist.put_env_vars_async({ENV_KEY: json.dumps(payload)}))
        except Exception as exc:
            logger.warning("MAIN MONEY persist error: %s", str(exc)[:80])
