"""
mh_engine.py - MH MODE engine (subclass of SetEngine; money rules, client, watchdog, settle loop reused).

  * Boot: per symbol PROBE FIRST (timed -> rtt), then history (MH_HISTORY_TICKS), walk-forward in memory,
    symbols initialised concurrently, each worker starts as soon as ITS init is done.
  * Signal: every tick, horizons 1..MH_HMAX_TRADE, all 18 Over/Under covers from the calibrated blended
    distribution; admitted when p_lb * payout/stake - 1 > MH_MIN_EDGE (lag-mixed P_eff, deadline gate).
  * Execution: rank by edge_lb/h, stakes computed sequentially (money.reserve), direct buys in parallel,
    no trade-count cap (money reserve + API semaphore only).
  * Everything new is in memory and rebuilt at boot; the 10-minute redeploy refits the models.
"""
import asyncio
import collections
import logging
import statistics
import time
from typing import Dict, List, Optional, Tuple

import account_history
import config
import mh_predictor as mp
import predictor as pred
import target_guard
from bot_engine import BotEngine, _cfg
from set_engine import SetEngine, account_allowed

logger = logging.getLogger(__name__)
RATIO_TTL = 90.0


class MHSignal:
    __slots__ = ("sym", "idx", "recv", "epoch", "cands", "done")

    def __init__(self, sym, idx, recv, epoch, cands):
        self.sym, self.idx, self.recv, self.epoch, self.cands, self.done = sym, idx, recv, epoch, cands, False


class Cand:
    __slots__ = ("sig", "h", "ci", "p", "lb", "r", "edge", "score")

    def __init__(self, sig, h, ci, p, lb, r, edge):
        self.sig, self.h, self.ci, self.p, self.lb, self.r, self.edge = sig, h, ci, p, lb, r, edge
        self.score = edge / h


class MHEngine(SetEngine):
    def __init__(self):
        super().__init__()
        self.hmax = int(_cfg("MH_HMAX_TRADE", 5))
        self.min_edge = float(_cfg("MH_MIN_EDGE", 0.0))
        self.max_per_target = int(_cfg("MH_MAX_PER_TARGET", 2))
        self.deadline_frac = float(_cfg("MH_DEADLINE_FRAC", 0.85))
        self.price_tol = float(_cfg("MH_PRICE_TOL", 0.02))
        self.api_n = max(2, int(_cfg("MH_API_CONCURRENCY", 6)))
        self.mh: Dict[str, mp.MHSymbol] = {}
        self._sig: Dict[str, MHSignal] = {}
        self._signals = self._sig                        # alias used by inherited code
        self._pre: Dict[str, collections.deque] = {}      # raw (epoch, quote) ticks while a symbol is not ready
        self._hold: set = set()                           # symbols being resynced
        self._idx: Dict[str, int] = {}                    # tick counter per symbol
        self._ep2idx: Dict[str, Dict[int, int]] = {}
        self._epq: Dict[str, collections.deque] = {}
        self._recv: Dict[str, float] = {}
        self._T: Dict[str, float] = {}
        self._q: Dict[str, float] = {}                    # EWMA of "late" settlements
        self._rtt: Optional[float] = None
        self._rtt_ready = False
        self._ratio_cache: Dict[tuple, Tuple[float, float]] = {}
        self._tcount: Dict[tuple, int] = {}
        self._brk: Dict[str, collections.deque] = {}
        self._muted: set = set()
        self._hs_n = collections.Counter()
        self._hs_w = collections.Counter()
        self._hs_p = collections.defaultdict(float)
        self._tasks: set = set()
        self._sem: Optional[asyncio.Semaphore] = None
        self._throttled_until = 0.0
        self._boot_t0 = time.time()

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def run(self):
        ok, why = account_allowed()
        if not ok:
            logger.critical("MH MODE REFUSED TO START: %s", why)
            return
        self._sem = asyncio.Semaphore(self.api_n)
        logger.info("MH MODE: %s | horizons 1..%d, 18 covers, min stake %.2f, api concurrency %d, min edge %.3f",
                    why, self.hmax, self.stake, self.api_n, self.min_edge)
        await BotEngine.run(self)

    def _extra_tasks(self) -> list:
        return [self._scanner_loop(), self._report_loop(), account_history.run(self.client)]

    def _on_balance(self, bal: float):
        super()._on_balance(bal)
        try:
            account_history.note_balance(bal)
        except Exception:
            pass

    # ── boot: concurrent init, probe first ───────────────────────────────────
    async def _init_symbols(self) -> list:
        self._boot_t0 = time.time()
        sem = asyncio.Semaphore(max(1, int(_cfg("MH_BOOT_CONCURRENCY", 5))))

        async def one(sym: str) -> Optional[str]:
            for attempt in (1, 2):                          # one retry: a hiccup must not drop a good symbol
                try:
                    await self._init_symbol(sym, sem)       # probe runs at once for every symbol; history is gated by sem
                    self._worker_started.add(sym)           # worker starts now, not after the slowest symbol
                    self._early_tasks.append(asyncio.create_task(self._worker(sym)))
                    return sym
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("SYMBOL %s start-up attempt %d failed: %s", sym, attempt, exc)
                    await asyncio.sleep(3)
            logger.error("SYMBOL SKIPPED %s: no tick history / no digit contract could be priced "
                         "(not offered on this account?)", sym)
            for d in (self.predictors, self._events, self._live, self._ratios, self._decimals,
                      self._last_tick_wall, self._recent_epochs, self.mh, self._pre):
                d.pop(sym, None)
            return None

        res = await asyncio.gather(*(one(s) for s in list(self.symbols)))
        self._rtt_ready = True
        logger.info("MH BOOT done in %.1fs, rtt estimate %.3fs", time.time() - self._boot_t0, self._rtt or 0.0)
        return [s for s in res if s]

    async def _init_symbol(self, sym: str, sem: Optional[asyncio.Semaphore] = None):
        self._events[sym] = asyncio.Event()
        self._live[sym] = collections.deque(maxlen=10)
        self._pre[sym] = collections.deque(maxlen=5000)
        self._last_tick_wall[sym] = time.time()
        self.mh.pop(sym, None)
        try:
            await self._probe_timed(sym)                    # 1) probe (no history download if it fails)
            await self.client.subscribe_ticks(sym, lambda tick, s=sym: self._on_tick(s, tick))   # buffer from now on
            if sem is None:
                await self._build_symbol(sym)               # 2) history + walk-forward
            else:
                async with sem:                             # only MH_BOOT_CONCURRENCY histories download at once
                    await self._build_symbol(sym)
        except BaseException:
            try:
                await self.client.unsubscribe_ticks(sym)
            except Exception:
                pass
            raise
        mhs = self.mh[sym]
        logger.info("READY %s: decimals=%d ticks=%d val_logloss(h)=%s tradable_h=%s", sym, self._decimals[sym], mhs.n,
                    {h: (round(hs.val_ll, 4) if hs.val_ll is not None else None) for h, hs in mhs.hs.items()},
                    [h for h, hs in mhs.hs.items() if hs.tradable])

    async def _probe_timed(self, sym: str) -> None:
        cand = ("OVER", 4)
        t0 = time.perf_counter()
        prop = await self._propose(sym, cand, self.stake, 1)
        dt = time.perf_counter() - t0
        if not prop or not prop.get("payout"):
            raise RuntimeError("no digit contract could be priced")
        ratio = float(prop["payout"]) / self.stake
        self._ratios.setdefault(sym, {})[cand] = ratio
        self._ratio_cache[(sym, mp.COVERS.index(cand), 1)] = (ratio, time.time())
        self._note_rtt(dt)

    def _horizon_deadline(self) -> float:
        budget = float(_cfg("MH_BOOT_BUDGET_SECONDS", 75))
        return (self._boot_t0 if not self._rtt_ready else time.time()) + budget

    async def _build_symbol(self, sym: str) -> None:
        """Download history, walk-forward, replay ticks that arrived meanwhile, then publish self.mh[sym]."""
        ticks = await self.client.get_tick_history(sym, int(_cfg("MH_HISTORY_TICKS", 15000)))
        if len(ticks) < 600:
            raise RuntimeError(f"only {len(ticks)} ticks returned")
        quotes = [t["quote"] for t in ticks]
        if sym not in self._decimals:
            dec = (_cfg("DIGIT_DECIMALS", {}) or {}).get(sym)
            self._decimals[sym] = int(pred.infer_decimals(quotes) if dec is None else dec)
        dec = self._decimals[sym]
        digits = pred.digits_from_quotes(quotes, dec)
        last_ep = ticks[-1]["epoch"]
        mhs = mp.MHSymbol(sym, self.hmax, 10, float(_cfg("MH_DECAY", 0.998)))
        rep = await mhs.fit(digits, last_ep, deadline=self._horizon_deadline())
        if not mhs.hs:
            raise RuntimeError("history too short for the models")
        eps = [int(t["epoch"]) for t in ticks[-500:]]
        n = len(ticks)
        self._idx[sym] = n - 1
        self._ep2idx[sym] = {ep: n - len(eps) + i for i, ep in enumerate(eps)}
        self._epq[sym] = collections.deque(eps, maxlen=500)
        self._recent_epochs[sym] = [float(e) for e in eps[-50:]]
        self._T[sym] = self._median_gap(eps)
        self._q.setdefault(sym, 0.1)
        for ep, qt in list(self._pre.get(sym, ())):         # ticks that arrived while we were downloading/fitting
            if ep > last_ep:
                self._ingest(sym, mhs, ep, pred.last_digit(qt, dec))
        self._pre[sym].clear()
        self.mh[sym] = mhs                                  # no await between replay and publish
        logger.info("FIT %s: %s", sym, rep["h"])

    @staticmethod
    def _median_gap(eps) -> float:
        gaps = [b - a for a, b in zip(eps, eps[1:]) if b > a]
        return float(statistics.median(gaps)) if gaps else 2.0

    # ── ticks ────────────────────────────────────────────────────────────────
    def _ingest(self, sym: str, mhs: mp.MHSymbol, ep: float, digit: int) -> bool:
        if not mhs.on_tick(digit, ep):
            return False
        i = self._idx[sym] + 1
        self._idx[sym] = i
        e = int(ep)
        q = self._epq[sym]
        if len(q) == q.maxlen:
            self._ep2idx[sym].pop(q[0], None)
        q.append(e)
        self._ep2idx[sym][e] = i
        eps = self._recent_epochs.setdefault(sym, [])
        eps.append(float(ep))
        del eps[:-50]
        self._last_epoch[sym] = ep
        if i % 50 == 0:
            self._T[sym] = self._median_gap(list(q))
        return True

    def _on_tick(self, sym: str, tick: dict):
        try:
            qt, ep = float(tick["quote"]), float(tick.get("epoch", time.time()))
        except Exception:
            return
        now = time.time()
        self._last_tick_wall[sym] = now
        mhs = self.mh.get(sym)
        if mhs is None or sym in self._hold:
            buf = self._pre.get(sym)
            if buf is not None:
                buf.append((ep, qt))
            return
        if self._ingest(sym, mhs, ep, pred.last_digit(qt, self._decimals[sym])):
            self._recv[sym] = now
            ev = self._events.get(sym)
            if ev is not None:
                ev.set()

    # ── per-tick signal ──────────────────────────────────────────────────────
    async def _worker(self, sym: str):
        ev = self._events[sym]
        while True:
            await ev.wait()
            ev.clear()
            try:
                mhs = self.mh.get(sym)
                if mhs is None or sym in self._hold or sym in self._muted or not mhs.ready():
                    self._sig.pop(sym, None)
                    continue
                q = self._q.get(sym, 0.1)
                cands = {}
                for h in range(1, self.hmax + 1):
                    sc = mhs.screen(h, q, self.min_edge)
                    if sc:
                        cands[h] = sc
                if cands:
                    self._sig[sym] = MHSignal(sym, self._idx[sym], self._recv.get(sym, time.time()),
                                              self._last_epoch.get(sym), cands)
                    self._scan_event.set()
                else:
                    self._sig.pop(sym, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("signal error %s: %s", sym, exc, exc_info=True)
                self._sig.pop(sym, None)

    # ── timing / rtt ─────────────────────────────────────────────────────────
    def _note_rtt(self, dt: float) -> None:
        if not self._rtt_ready:
            self._rtt = dt if self._rtt is None else min(self._rtt, dt)    # boot: best probe
        else:
            self._rtt = 0.8 * self._rtt + 0.2 * dt

    def _late(self, sig: MHSignal, extra: float = 0.0) -> bool:
        T = self._T.get(sig.sym, 2.0)
        return (time.time() - sig.recv) + (self._rtt or 0.3) + extra > self.deadline_frac * T

    # ── API helpers ──────────────────────────────────────────────────────────
    def _ratio_get(self, sym: str, ci: int, h: int) -> Optional[float]:
        v = self._ratio_cache.get((sym, ci, h))
        return v[0] if v and time.time() - v[1] < RATIO_TTL else None

    async def _ratio_fetch(self, sym: str, ci: int, h: int) -> None:
        async with self._sem:
            t0 = time.perf_counter()
            prop = await self._propose(sym, mp.COVERS[ci], self.stake, h)
            self._note_rtt(time.perf_counter() - t0)
        if prop and prop.get("payout"):
            self._ratio_cache[(sym, ci, h)] = (float(prop["payout"]) / self.stake, time.time())

    async def _on_api_error(self, what: str, exc: Exception):
        if "rate" in str(exc).lower():
            self._throttle()
        await super()._on_api_error(what, exc)

    def _throttle(self) -> None:
        now = time.time()
        if now < self._throttled_until:
            return
        self._throttled_until = now + 30.0
        asyncio.get_running_loop().create_task(self._ballast())

    async def _ballast(self) -> None:
        """Hold half of the API permits for 30 s after a rate-limit error."""
        got = 0
        try:
            for _ in range(self.api_n // 2):
                await asyncio.wait_for(self._sem.acquire(), timeout=5)
                got += 1
        except asyncio.TimeoutError:
            pass
        logger.warning("RATE LIMIT: api concurrency %d -> %d for 30 s", self.api_n, self.api_n - got)
        await asyncio.sleep(max(0.0, self._throttled_until - time.time()))
        for _ in range(got):
            self._sem.release()

    # ── scanner: admit, rank, stake sequentially, buy in parallel ────────────
    async def _scanner_loop(self):
        coalesce = float(_cfg("SET_COALESCE_SECONDS", 0.05))
        while True:
            await self._scan_event.wait()
            if coalesce > 0:
                await asyncio.sleep(coalesce)              # ticks of several symbols often land together
            self._scan_event.clear()
            if self._blocked():
                continue
            try:
                await self._scan_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("scan error: %s", exc, exc_info=True)

    async def _scan_once(self):
        self._scans += 1
        sigs: List[MHSignal] = []
        for sym, sig in list(self._sig.items()):
            if sig.done or sym in self._muted or sym in self._hold or self._idx.get(sym) != sig.idx:
                continue
            sig.done = True                                 # each signal is evaluated exactly once
            if self._late(sig):
                continue
            sigs.append(sig)
        if not sigs:
            return
        need = set()                                        # live payout ratios still missing (top 3 per sym/h)
        for sig in sigs:
            for h, sc in sig.cands.items():
                for ci, _p, _lb, _e in sc[:3]:
                    if self._ratio_get(sig.sym, ci, h) is None:
                        need.add((sig.sym, ci, h))
        if need:
            await asyncio.gather(*(self._ratio_fetch(*k) for k in need), return_exceptions=True)
        cands: List[Cand] = []
        for sig in sigs:
            for h, sc in sig.cands.items():
                best = None
                for ci, p, lb, _e in sc:
                    r = self._ratio_get(sig.sym, ci, h)
                    if r is None:
                        continue
                    edge = lb * r - 1.0
                    if edge > self.min_edge and (best is None or edge > best.edge):
                        best = Cand(sig, h, ci, p, lb, r, edge)
                if best is not None:
                    cands.append(best)
        for sym in {s.sym for s in sigs}:                   # forget old target counters
            cut = self._idx.get(sym, 0) - 30
            for k in [k for k in self._tcount if k[0] == sym and k[1] < cut]:
                self._tcount.pop(k, None)
        if not cands:
            self._last_decision = f"scan {self._scans}: {len(sigs)} signal(s), none admitted"
            return
        cands.sort(key=lambda c: -c.score)
        picks: List[Tuple[Cand, float]] = []
        for c in cands:
            if self._late(c.sig) or self._blocked():
                continue
            tk = (c.sig.sym, c.sig.idx + c.h)
            if self._tcount.get(tk, 0) >= self.max_per_target:
                continue
            stake = self._next_stake()                      # shrinks as pending grows: best candidate = biggest stake
            if stake is None:
                self._last_decision = "stopped by money rules (floor reached) - resumes at next reset"
                break
            if not self.money.reserve(stake):
                self._last_decision = "skip: buying would breach the profit floor"
                break
            self._tcount[tk] = self._tcount.get(tk, 0) + 1
            picks.append((c, stake))
        for c, stake in picks:
            self._inflight += 1
            t = asyncio.create_task(self._buy_one(c, stake))
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)

    async def _buy_one(self, c: Cand, stake: float) -> None:
        sig, h, cand = c.sig, c.h, mp.COVERS[c.ci]
        sym = sig.sym
        tk = (sym, sig.idx + h)
        released = False
        ok = False
        try:
            err: Optional[Exception] = None
            resp = None
            async with self._sem:
                if self._late(sig):                         # waited too long for a permit
                    return
                t0 = time.perf_counter()
                try:
                    price = round(stake * (1.0 + self.price_tol) + 1e-9, 2)
                    resp = await self._buy_direct(sym, cand, price, stake, h)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    err = exc
                self._note_rtt(time.perf_counter() - t0)
            if err is not None:
                await self._on_api_error("buy", err)
                return
            buy = (resp or {}).get("buy") or {}
            self.money.note_buy(buy)
            self.money.release(stake)
            released = True
            cid = str(buy.get("contract_id", ""))
            if not cid:
                logger.warning("BUY returned no contract_id: %s", resp)
                return
            price_paid = float(buy.get("buy_price") or stake)
            payout = float(buy.get("payout") or 0.0)
            ratio = payout / price_paid if payout > 0 and price_paid > 0 else c.r
            ok = True
            self._open[cid] = {"sym": sym, "cand": cand, "stake": price_paid, "ratio": ratio,
                               "opened_at": float(buy.get("purchase_time") or time.time()),
                               "decision_epoch": sig.epoch, "t0": time.time(), "h": h, "sig_idx": sig.idx,
                               "p_cal": c.p, "p_lb": c.lb, "score": c.score, "pred_digits": (), "pred_ranked": ()}
            tg = f"{cand[0]}{cand[1]}"
            self._last_decision = f"BUY {sym} {tg} h={h} p={c.p:.3f} lb={c.lb:.3f} r={ratio:.3f} edge_lb={c.edge:+.3f}"
            logger.info("MH TRADE %s %s %s h=%d p_cal=%.3f p_lb=%.3f paid=%.2f payout=%.2f ratio=%.3f edge_lb=%+.3f "
                        "score=%.4f q=%.2f shortcode=%s", cid, sym, tg, h, c.p, c.lb, price_paid, payout, ratio, c.edge,
                        c.score, self._q.get(sym, 0.0), buy.get("shortcode", "?"))
            self.client._contract_symbol_map[cid] = sym
            try:
                await self.client.subscribe_contract(
                    cid, lambda msg, _c=cid: asyncio.create_task(self._on_contract_result(_c, msg)), symbol=sym)
            except Exception as exc:
                logger.warning("subscribe_contract(%s) failed (settle loop will poll): %s", cid, exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("buy flow error %s: %s", sym, exc, exc_info=True)
        finally:
            if not released:
                self.money.release(stake)
            self._inflight -= 1
            if not ok:
                self._tcount[tk] = max(0, self._tcount.get(tk, 1) - 1)

    # ── settlement: lag EWMA, per-horizon stats, per-symbol breaker ──────────
    def _inspect_settlement(self, cid: str, info: dict, poc: dict) -> None:
        sym, h, cand = info["sym"], info.get("h"), info["cand"]
        if h is None:
            return
        want_type, want_bar = pred.contract_type(cand), cand[1]
        got_type, got_bar = poc.get("contract_type"), poc.get("barrier")
        if (got_type and got_type != want_type) or (want_bar is not None and got_bar is not None and str(got_bar) != str(want_bar)):
            logger.error("CONTRACT MISMATCH %s: bought %s barrier=%s but Deriv settled %s barrier=%s",
                         cid, want_type, want_bar, got_type, got_bar)
        profit = float(poc.get("profit") or 0.0)
        won = profit > 0
        p = float(info.get("p_cal") or 0.0)
        self._set_trades += 1
        self._hs_n[h] += 1
        self._hs_w[h] += int(won)
        self._hs_p[h] += p
        realized = None
        try:                                                # late = settled on a later tick than intended
            ex = poc.get("exit_tick_time")
            m = self._ep2idx.get(sym, {})
            if ex is not None and int(float(ex)) in m:
                realized = m[int(float(ex))] - int(info["sig_idx"])
                late = 1.0 if realized > h else 0.0
                self._q[sym] = 0.95 * self._q.get(sym, 0.1) + 0.05 * late
        except Exception:
            pass
        logger.info("MH RESULT %s %s h=%d %s realized_h=%s q=%.2f p_cal=%.3f profit=%+.2f", cid, sym, h,
                    "WIN" if won else "LOSS", realized, self._q.get(sym, 0.1), p, profit)
        dq = self._brk.setdefault(sym, collections.deque(maxlen=80))
        dq.append((1.0 if won else 0.0, p))
        if sym not in self._muted and len(dq) >= 40:
            var = sum(pp * (1 - pp) for _, pp in dq)
            z = sum(w - pp for w, pp in dq) / (var ** 0.5) if var > 0 else 0.0
            if z < -2.0:
                self._muted.add(sym)
                self._sig.pop(sym, None)
                logger.warning("MH BREAKER: %s muted for the rest of this life (z=%.2f over %d trades)", sym, z, len(dq))
        try:
            account_history.note_pnl(profit)
        except Exception:
            pass

    def _note_lag(self, info: dict, poc: dict):
        return                                              # replaced by the q EWMA in _inspect_settlement

    # ── resync after a reconnect (periodic refit is OFF in MH mode) ──────────
    async def _refit_loop(self):
        while True:
            await asyncio.sleep(5)
            if not self.client.is_connected or not self._needs_resync():
                continue
            conn_now = self._conn_count()
            ok_all = True
            for sym in list(self.symbols):
                try:
                    await self._resync_symbol(sym)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    ok_all = False
                    logger.error("resync error %s: %s", sym, exc)
            if ok_all and self._conn_count() == conn_now:
                self._seen_conn = conn_now

    async def _resync_symbol(self, sym: str) -> None:
        mhs = self.mh.get(sym)
        self._hold.add(sym)
        self._sig.pop(sym, None)
        try:
            ticks = await self.client.get_tick_history(sym, 1000)
            dec = self._decimals[sym]
            if ticks and mhs is not None and mhs.last_epoch is not None and ticks[0]["epoch"] <= mhs.last_epoch:
                for t in ticks:                             # gap fully covered: feed only the missing ticks
                    if t["epoch"] > mhs.last_epoch:
                        self._ingest(sym, mhs, t["epoch"], pred.last_digit(t["quote"], dec))
                for ep, qt in list(self._pre.get(sym, ())):
                    if ep > mhs.last_epoch:
                        self._ingest(sym, mhs, ep, pred.last_digit(qt, dec))
                self._pre[sym].clear()
                logger.info("RESYNC %s: gap filled, %d ticks", sym, len(ticks))
            else:
                await self._build_symbol(sym)               # gap too long: full refit (replays held ticks)
                logger.info("RESYNC %s: full refit", sym)
        finally:
            self._hold.discard(sym)

    # ── reporting ────────────────────────────────────────────────────────────
    def _n_tradable(self) -> int:
        return sum(1 for m in self.mh.values() for h, hs in m.hs.items() if hs.tradable and h <= self.hmax and not hs.muted())

    def report_text(self) -> str:
        T = self._n_trades
        trade = (f"trades {T}: win {100.0 * self._n_wins / T:.1f}% vs break-even {100.0 * self._be_sum / T:.1f}% "
                 f"| P&L {self._total_pnl:+.2f}") if T else "trades 0"
        per_h = " ".join(f"h{h}:{self._hs_n[h]}t/{self._hs_w[h]}w exc{self._hs_w[h] - self._hs_p[h]:+.1f}"
                         for h in range(1, self.hmax + 1) if self._hs_n[h]) or "no settled trades"
        qs = ",".join(f"{s}:{self._q.get(s, 0.1):.2f}" for s in self.symbols)
        total = sum(1 for m in self.mh.values() for h in m.hs if h <= self.hmax)
        return (f"SET REPORT | MH {trade} | per-h {per_h} | tradable (sym,h) {self._n_tradable()}/{total} | "
                f"q {qs} | muted {sorted(self._muted) or '-'} | open {len(self._open)} | signals {len(self._sig)} | "
                f"scans {self._scans} | rtt {self._rtt or 0:.3f}s")

    async def _dashboard_loop(self):
        while True:
            await asyncio.sleep(5)
            try:
                import keep_alive
                keep_alive.update_status(
                    balance=round(self.client.balance, 2), session="MH MODE (demo)",
                    current_symbol=",".join(self.symbols), last_signal=self._last_decision,
                    tradeable_count=self._n_tradable(), open_contracts_count=len(self._open),
                    target_balance=target_guard.target(), money=self.money.snapshot())
                keep_alive.set_active_trades(len(self._open))
                account_history.note_balance(self.client.balance)
            except Exception:
                pass
