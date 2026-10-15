"""
mh_engine.py - MH MODE engine (subclass of SetEngine; flat stake, client, watchdog, settle loop reused).

  * Boot: per symbol PROBE FIRST (timed -> rtt), then history (MH_HISTORY_TICKS), walk-forward in memory,
    symbols initialised concurrently, each worker starts as soon as ITS init is done.
  * Signal: every tick, horizons 1..MH_HMAX_TRADE, all 18 Over/Under covers from the calibrated blended
    distribution; admitted when p_lb * payout/stake - 1 > MH_MIN_EDGE (lag-mixed P_eff, deadline gate).
  * Execution (MH_MINUTE_RANK=1, default): every symbol is scanned in parallel (own task per symbol) and ranked
    TWICE inside each UTC minute: (1) the best contract (cover/horizon) of each symbol, (2) those per-symbol
    bests ranked against each other; only the overall top MH_MINUTE_TOP_N (default 1) is bought in that minute.
    The ranking lives for that minute only; the next minute starts from scratch. MH_MINUTE_RANK=0 = old
    behaviour (every admitted candidate is bought). Direct buys run in parallel (API semaphore only).
  * Everything new is in memory and rebuilt at boot; the 10-minute redeploy refits the models.
"""
import asyncio
import collections
import json
import logging
import os
import random
import statistics
import time
from typing import Dict, List, Optional, Tuple

import numpy as np

import account_history
import config
import exp_mode
import restart_scheduler
import stream_diag
from exp_stats import EXP
import mh_predictor as mp
import predictor as pred
import set_predictor as sp
import target_guard
from bot_engine import BotEngine, _cfg
from set_engine import SetEngine, account_allowed

logger = logging.getLogger(__name__)
RATIO_TTL = float(getattr(config, "MH_RATIO_TTL", 600.0))   # digit-contract payout ratios are ~constant: cache long


def parse_shortcode(sc) -> Optional[dict]:
    """DIGITOVER_R_100_1.90_1700000000_4T_4_0 -> {type, sym, ticks, barrier}; None if it does not parse."""
    try:
        parts = str(sc).split("_")
        k = next(i for i, x in enumerate(parts) if i >= 4 and x.endswith("T") and x[:-1].isdigit())
        return {"type": parts[0], "sym": "_".join(parts[1:k - 2]), "ticks": int(parts[k][:-1]),
                "barrier": parts[k + 1] if len(parts) > k + 1 else None}
    except Exception:
        return None


def match_in_doubt(rows, want: dict, known) -> Optional[dict]:
    """Pick the portfolio / profit_table row that is the in-doubt buy: same symbol, contract type, barrier, ticks,
    stake and purchase_time inside the window; contracts already tracked (`known`) are never matched.
    want = {sym, type, barrier, ticks, stake, max_price, t_lo, t_hi, t_sent}. Closest purchase_time wins."""
    best = None
    for r in rows or []:
        cid = str(r.get("contract_id", ""))
        if not cid or cid in known:
            continue
        sc = parse_shortcode(r.get("shortcode"))
        typ = r.get("contract_type") or (sc or {}).get("type")
        sym = r.get("symbol") or (sc or {}).get("sym")
        if typ != want["type"] or sym != want["sym"]:
            continue
        if sc is not None:
            if sc["ticks"] != want["ticks"]:
                continue
            if want["barrier"] is not None and sc["barrier"] is not None and str(sc["barrier"]) != str(want["barrier"]):
                continue
        try:
            bp, pt = float(r.get("buy_price") or 0.0), float(r.get("purchase_time") or 0.0)
        except (TypeError, ValueError):
            continue
        if not (want["stake"] - 0.011 <= bp <= want["max_price"] + 0.011) or not (want["t_lo"] <= pt <= want["t_hi"]):
            continue
        d = abs(pt - want["t_sent"])
        if best is None or d < best[0]:
            best = (d, r)
    return best[1] if best else None


class MHSignal:
    __slots__ = ("sym", "idx", "recv", "epoch", "cands", "done", "info", "test")

    def __init__(self, sym, idx, recv, epoch, cands, info=None):
        self.sym, self.idx, self.recv, self.epoch, self.cands, self.done = sym, idx, recv, epoch, cands, False
        self.info = info or {}                           # cluster mode: {h: (cluster digits, mass)}
        self.test = None                                 # test mode: {"zd": ZoneDecision, "p_by_h": {h: probs}}


class Cand:
    __slots__ = ("sig", "h", "ci", "p", "lb", "r", "edge", "score", "cl", "arm", "extra", "qts")

    def __init__(self, sig, h, ci, p, lb, r, edge):
        self.sig, self.h, self.ci, self.p, self.lb, self.r, self.edge = sig, h, ci, p, lb, r, edge
        self.score = edge / h
        self.cl = None                                   # cluster mode: (cluster, mass, width)
        self.arm = None                                  # test mode: "test" | "ctl"
        self.extra = None                                # extra fields stored with the open contract
        self.qts = None                                  # wall-clock time of the admission quote (payout-slip diagnostics)


def p_eff_mix(ph, pn, q: float):
    """Lag mix (1-q)*P_h + q*P_(h+1), floored at 1e-4 and renormalised; pn None -> P_h."""
    if pn is None:
        return ph
    p = np.maximum((1.0 - q) * np.asarray(ph) + q * np.asarray(pn), 1e-4)
    return p / p.sum()


def top_cluster(p, size: int) -> Tuple[int, ...]:
    """The `size` most probable digits (ties -> lower digit), sorted ascending."""
    return tuple(sorted(sorted(range(10), key=lambda d: (-float(p[d]), d))[:size]))


def _num(name: str, default: float) -> float:
    """Env var first (Render), then config.py, then the default."""
    try:
        raw = os.environ.get(name)
        if raw is not None and raw.strip() != "":
            return float(raw)
    except ValueError:
        pass
    try:
        return float(_cfg(name, default))
    except (TypeError, ValueError):
        return float(default)


def cover_width(cand) -> int:
    return 9 - cand[1] if cand[0] == "OVER" else cand[1]


def pick_fits(cluster, max_width: int, widths, pick: str):
    """Covering contracts of the cluster, filtered by allowed widths, ordered by the pick rule."""
    fits = [(c, w) for c, w in sp.covering_contracts(cluster, max_width) if w in widths]
    fits.sort(key=lambda x: x[1], reverse=(pick != "tightest"))
    return fits


class MHEngine(SetEngine):
    def __init__(self):
        super().__init__()
        self.hmax = int(_cfg("MH_HMAX_TRADE", 5))
        self.min_edge = float(_cfg("MH_MIN_EDGE", 0.0))
        self.min_ratio = float(_cfg("MH_MIN_PAYOUT_RATIO", 2.0))   # payout/stake must be >= this
        self.max_per_target = int(_cfg("MH_MAX_PER_TARGET", 2))
        self.cluster = bool(int(_cfg("MH_CLUSTER", 1)))
        self.set_size = int(_cfg("SET_SIZE", 4))
        self.max_width = int(_cfg("SET_MAX_COVER_WIDTH", 5))
        self.widths = tuple(int(w) for w in (_cfg("MH_CLUSTER_WIDTHS", (3, 4, 5)) or (3, 4, 5)))
        pk = str(_cfg("MH_CLUSTER_PICK", "widest")).strip().lower()
        self.pick = pk if pk in ("widest", "tightest") else "widest"
        self.min_set_prob = float(_cfg("SET_MIN_SET_PROB", 0.0))
        self.max_open = int(_cfg("MH_MAX_OPEN", 0))
        self.max_per_scan = int(_cfg("MH_MAX_PER_SCAN", 0))
        self._cl_formed = 0                               # cluster counters (memory only)
        self._cl_spread = 0
        self._cl_floor = 0
        self._cl_cover = collections.Counter()
        self._cl_width = collections.Counter()
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
        self.buy_timeout = float(_cfg("MH_BUY_TIMEOUT_SECONDS", 3.0))
        self.reconcile_wait = float(_cfg("MH_RECONCILE_WAIT_SECONDS", 2.0))
        self.reconcile_retry = float(_cfg("MH_RECONCILE_RETRY_SECONDS", 3.0))
        self.reconcile_safety = float(_cfg("MH_RECONCILE_SAFETY_SECONDS", 30.0))
        self.adopt_orphans = bool(int(_cfg("MH_ADOPT_ORPHANS_ON_BOOT", 1)))
        self._doubt_targets: set = set()                  # (sym, target idx) whose buy is in doubt / unresolved: never rebuy
        self._doubt_open: set = set()                     # reconcile tasks that have not reached a decision yet
        self._reconciles: set = set()
        self._known_cids: "collections.OrderedDict[str, None]" = collections.OrderedDict()   # every contract tracked (bounded)
        self.max_quote_age = float(_cfg("MH_MAX_QUOTE_AGE_SECONDS", 0.0))   # 0 = off; else older quotes are refetched
        self._slips: collections.deque = collections.deque(maxlen=500)      # paid_ratio - quote_ratio (measurement only)
        self._slip_low = 0                                                  # buys with paid_ratio < MH_MIN_PAYOUT_RATIO
        self._slip_n = 0
        self._rtt_warned: set = set()
        self.inproc_refit = float(_cfg("MH_INPROC_REFIT_SECONDS", 0.0))      # 0 = off; else background refit period per symbol
        self._bg_buf: Dict[str, collections.deque] = {}                      # raw ticks seen while a background refit builds
        rk = str(_cfg("MH_RANK_KEY", "mass")).strip().lower()
        self.rank_key = rk if rk in ("mass", "edge") else "mass"      # mass = raw top-cluster mass (old); edge = calibrated lb*r-1
        self.rank_min_edge = float(_cfg("MH_RANK_MIN_EDGE", self.min_edge))   # admission edge floor when MH_RANK_KEY=edge
        self.oos_check = bool(int(_cfg("MH_OOS_CHECK", 0)))
        self.oos_min_n = int(_cfg("MH_OOS_MIN_N", 30))
        self.oos_margin = float(_cfg("MH_OOS_MARGIN", 0.0))
        self._cl_oos = 0                                  # covers refused by the held-out check
        self._rtt: Optional[float] = None
        self._rtt_ready = False
        self._ratio_cache: Dict[tuple, Tuple[float, float]] = {}
        self._tcount: Dict[tuple, int] = {}
        # minute ranking: per-symbol best contract, then best overall, once per UTC minute
        self.minute_rank = bool(int(_num("MH_MINUTE_RANK", 1)))
        rw = int(_num("MH_RANK_WINDOW_SECONDS", 30))
        if rw not in (30, 60):
            logger.warning("MH_RANK_WINDOW_SECONDS=%s is not 30 or 60: using 30", rw)
            rw = 30
        self.rank_window = rw                                                # ranking window: 30 = :00/:30 each minute, 60 = old
        rm = str(_cfg("MH_RANK_MODE", "snapshot")).strip().lower()
        self.rank_mode = rm if rm in ("snapshot", "bar") else "snapshot"
        self.minute_top_n = max(1, int(_num("MH_WINDOW_TOP_N", _num("MH_MINUTE_TOP_N", 1))))   # contracts bought per WINDOW
        self.rank_delay = min(max(0.0, _num("MH_RANK_DELAY_SECONDS", 1.0)), 0.5 * rw)          # always < the window length
        self._bar_pool: Dict[str, "Cand"] = {}
        self._rnd_rej: Dict[str, str] = {}
        self._rnd_snap = 0
        self._rnd_seen = False
        self._rnd_c0 = (0, 0, 0, 0)
        self._rnd_minute = -1                                                # id of the current ranking window
        self._rnd_best: Dict[str, "Cand"] = {}                               # level 1: best contract per symbol (this minute)
        self._rnd_n = 0                                                      # contracts picked in this minute
        self._brk: Dict[str, collections.deque] = {}
        self._muted: set = set()
        self._hs_n = collections.Counter()
        self._hs_w = collections.Counter()
        self._hs_p = collections.defaultdict(float)
        self._tasks: set = set()
        self._sem: Optional[asyncio.Semaphore] = None
        self._throttled_until = 0.0
        self._boot_t0 = time.time()
        # proposal pacing (token-spacing): min seconds between two proposal requests
        self._rl_base = float(_cfg("MH_PROPOSAL_MIN_INTERVAL", 0.30))
        self._rl_gap = self._rl_base
        self._rl_next = 0.0
        self._rl_lock: Optional[asyncio.Lock] = None
        # rate-limit cooldown: while active NO proposal is sent (retrying while blocked only keeps us blocked)
        self._rl_cool_base = float(_cfg("MH_RATE_COOLDOWN", 5.0))
        self._rl_cool_until = 0.0
        self._rl_strikes = 0
        self._rl_last_end = 0.0
        self._max_ratio_fetch = max(1, int(_cfg("MH_MAX_RATIO_FETCH", 10)))   # proposals per scan
        self._cl_gate = 0                                 # cluster safeguards: horizons refused by gate/mute, covers refused by edge
        self._cl_edge = 0
        self._mute_ts: Dict[str, float] = {}
        self._rtt_hist: Dict[str, collections.deque] = {}
        # ── pre-registered test mode (exp_mode.py) ──
        self.test_mode = bool(_cfg("TEST_MODE", False))
        self.ledger = None
        self._ledger_on = self.test_mode or bool(int(os.environ.get("LEDGER_ENABLED", "0") or 0))
        self.diag = stream_diag.StreamDiag(int(_cfg("TEST_DIAG_BLOCK", 1000)))
        self._shadow: Dict[str, tuple] = {}               # sym -> (tick idx, top digit, zone, fired) awaiting its outcome
        self._dup: collections.Counter = collections.Counter()
        self._ntick: collections.Counter = collections.Counter()
        self._unk_h: collections.Counter = collections.Counter()
        self._set_n: collections.Counter = collections.Counter()
        self._inflight_test = 0
        self._test_pending = 0.0
        self._stop_t0 = 0.0
        self._rng = random.Random(int(_cfg("TEST_CONTROL_SEED", 12345)))
        if self.test_mode:
            self.stake = float(_cfg("TEST_STAKE", 1.0))   # one flat stake, also the pricing stake
            self.max_per_target = 1                       # max one contract per (symbol, target tick) per arm
            self.max_open = int(_cfg("TEST_MAX_OPEN", 20))
            self._feed_followers = False                  # follower mirroring OFF
            self.cluster = False

    # ── lifecycle ────────────────────────────────────────────────────────────
    async def run(self):
        ok, why = account_allowed()
        if not ok:
            logger.critical("MH MODE REFUSED TO START: %s", why)
            return
        self._sem = asyncio.Semaphore(self.api_n)
        self._rl_lock = asyncio.Lock()
        if self.test_mode:
            logger.warning("TEST MODE (pre-registered primary test) | config hash %s | stake %.2f flat | threshold %.3f | "
                           "h=1 OVER5/UNDER4 | max open %d | max trades %d | control=%s(%s) | demo only",
                           exp_mode.config_hash(), self.stake, float(_cfg("TEST_ZONE_THRESHOLD", 0.45)), self.max_open,
                           int(_cfg("TEST_MAX_TRADES", 4000)), bool(_cfg("TEST_CONTROL", True)), _cfg("TEST_CONTROL_MODE", "random"))
        logger.info("MH MODE: %s | horizons 1..%d, 18 covers, min stake %.2f, api concurrency %d, min edge %.3f, min payout ratio %.2f | cluster=%s size=%d widths=%s pick=%s max/target=%d",
                    why, self.hmax, self.stake, self.api_n, self.min_edge, self.min_ratio,
                    self.cluster, self.set_size, self.widths, self.pick, self.max_per_target)
        if mp.WILSON_Z != 1.64 and self.min_edge > 0:
            logger.warning("MH CONFIG: MH_WILSON_Z=%.2f AND MH_MIN_EDGE=%.3f are both raised - they overlap (z 1.64->1.96 is about "
                           "+0.02 of edge at payout 1.9); use ONE of them", mp.WILSON_Z, self.min_edge)
        logger.info("MH RANK: window=%ds mode=%s key=%s top_n=%d delay=%.1fs", self.rank_window, self.rank_mode, self.rank_key,
                    self.minute_top_n, self.rank_delay)
        logger.info("MH CONFIG effective: MH_DEADLINE_RTT=%s MH_HMAX_TRADE=%d MH_BUY_TIMEOUT_SECONDS=%.1f REDEPLOY_DRAIN=%s "
                    "MH_MIN_EDGE=%.3f WILSON_Z=%.2f MH_GATE_Z=%.2f", str(_cfg("MH_DEADLINE_RTT", "p50")).lower(), self.hmax,
                    self.buy_timeout, _cfg("REDEPLOY_DRAIN", 1), self.min_edge, mp.WILSON_Z, float(_cfg("MH_GATE_Z", 1.96)))
        await BotEngine.run(self)

    def _extra_tasks(self) -> list:
        t = [self._scanner_loop(), self._report_loop(), account_history.run(self.client), self._adopt_orphans()]
        if self._ledger_on:
            t.append(self._exp_loop())
        if self.inproc_refit > 0:
            t.append(self._inproc_refit_loop())
        return t

    def _on_balance(self, bal: float):
        super()._on_balance(bal)
        try:
            account_history.note_balance(bal)
        except Exception:
            pass

    def _blocked(self) -> bool:
        if super()._blocked() or restart_scheduler.buys_blocked():
            return True                                    # includes the last REDEPLOY_BUY_BLOCK_SECONDS before a redeploy
        try:
            import kill_switch
            if kill_switch.is_paused():
                return True
        except Exception:
            pass
        return bool(self.test_mode and EXP.stop_reason(self._open_test()))

    def _open_test(self) -> int:
        return sum(1 for i in self._open.values() if i.get("arm") == "test")

    # ── boot: concurrent init, probe first ───────────────────────────────────
    async def _init_symbols(self) -> list:
        self._boot_t0 = time.time()
        if self._ledger_on:
            await self._exp_boot()
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
        if self._ledger_on:
            await self._exp_ready()                         # also marks the engine ready for the redeploy guard
        else:
            import redeploy_guard
            redeploy_guard.mark_engine_ready()              # without this the 7-minute redeploy preflight refuses forever
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
        self._note_rtt(dt, sym)

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
        if self.test_mode and sym not in self.diag.sym:
            self.diag.add_many(sym, digits)
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
        if self.test_mode:
            self._exp_on_digit(sym, i, digit)
        return True

    def _on_tick(self, sym: str, tick: dict):
        try:
            qt, ep = float(tick["quote"]), float(tick.get("epoch", time.time()))
        except Exception:
            return
        now = time.time()
        self._last_tick_wall[sym] = now
        bg = self._bg_buf.get(sym)
        if bg is not None:
            bg.append((ep, qt))                           # background refit replays these before the swap
        mhs = self.mh.get(sym)
        if mhs is not None and mhs.last_epoch is not None and sym not in self._hold:
            if ep <= mhs.last_epoch:
                self._dup[sym] += 1                       # duplicate / out-of-order epoch: tick dropped (1 s resolution)
            else:
                self._ntick[sym] += 1
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
                if self.test_mode:
                    if mhs is None or sym in self._hold or not mhs.ready():
                        self._sig.pop(sym, None)
                    else:
                        self._test_step(sym, mhs)
                    continue
                if mhs is None or sym in self._hold or sym in self._muted or not mhs.ready():
                    self._sig.pop(sym, None)
                    continue
                q = self._q.get(sym, 0.1)
                cands = {}
                info = {}
                for h in range(1, self.hmax + 1):
                    if self.cluster:
                        r = self._cluster_cands(mhs, h, q)
                        if r:
                            cands[h], info[h] = r[0], r[1]
                        continue
                    sc = mhs.screen(h, q, self.min_edge)
                    if sc:
                        cands[h] = sc
                if cands:
                    self._sig[sym] = MHSignal(sym, self._idx[sym], self._recv.get(sym, time.time()),
                                              self._last_epoch.get(sym), cands, info)
                    self._scan_event.set()
                else:
                    self._sig.pop(sym, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("signal error %s: %s", sym, exc, exc_info=True)
                self._sig.pop(sym, None)

    def _cluster_cands(self, mhs: mp.MHSymbol, h: int, q: float):
        """Cluster mode: top-SET_SIZE digits of P_eff -> covering Over/Under contracts, ordered by the pick rule."""
        hs = mhs.hs.get(h)
        if hs is None or hs.cur is None:
            return None
        if not hs.tradable or hs.muted() or not hs.cal:    # significance gate (log-loss CI vs uniform) + per-horizon mute
            self._cl_gate += 1
            return None
        nxt = mhs.hs.get(h + 1)
        p = p_eff_mix(hs.cur[1], nxt.cur[1] if (nxt is not None and nxt.cur is not None) else None, q)
        cl = top_cluster(p, self.set_size)
        mass = float(sum(p[d] for d in cl))
        if self.rank_key != "edge" and self.min_set_prob > 0 and mass < self.min_set_prob:
            return None                                      # raw-mass floor; under MH_RANK_KEY=edge the floor is MH_RANK_MIN_EDGE
        self._cl_formed += 1
        fits = pick_fits(cl, self.max_width, self.widths, self.pick)[:3]
        if not fits:
            self._cl_spread += 1
            return None
        out = []
        for cand, _w in fits:
            ci = mp.COVERS.index(cand)
            pc = float(p @ mp.W[:, ci])
            pcal, plb = hs.cal[ci](pc)                       # calibrated p and its Wilson lower bound
            if self.oos_check and not mp.oos_admissible(hs.cal[ci], pc, 1.0 / self.min_ratio, self.oos_min_n, self.oos_margin):
                self._cl_oos += 1                            # bin does not beat break-even on the held-out slice
                continue
            out.append((ci, float(pcal), float(plb), mass))
        if not out:
            return None                                      # every cover refused by the held-out check
        return out, (cl, mass)

    # ── timing / rtt ─────────────────────────────────────────────────────────
    def _note_rtt(self, dt: float, sym: Optional[str] = None) -> None:
        if sym is not None:
            self._rtt_hist.setdefault(sym, collections.deque(maxlen=200)).append(dt)
        if not self._rtt_ready:
            self._rtt = dt if self._rtt is None else min(self._rtt, dt)    # boot: best probe
        else:
            self._rtt = 0.8 * self._rtt + 0.2 * dt

    def _rtt_q(self, sym: str, q: float) -> Optional[float]:
        h = self._rtt_hist.get(sym)
        if not h or len(h) < 20:
            return None
        v = sorted(h)
        return v[min(len(v) - 1, int(q * len(v)))]

    def _rtt_for(self, sym: str) -> float:
        mode = str(_cfg("MH_DEADLINE_RTT", "p50")).strip().lower()
        if mode in ("p50", "p95"):
            v = self._rtt_q(sym, 0.5 if mode == "p50" else 0.95)
            if v is not None:
                return v                                    # needs >= 20 samples for this symbol; until then the EWMA below
        elif mode != "ewma" and "mode" not in self._rtt_warned:
            self._rtt_warned.add("mode")
            logger.warning("MH_DEADLINE_RTT=%r is not p50|p95|ewma: using the EWMA rtt", mode)
        return self._rtt or 0.3

    def _late(self, sig: MHSignal, extra: float = 0.0) -> bool:
        """Hard deadline gate: time since tick arrival + measured RTT must stay under MH_DEADLINE_FRAC x tick gap."""
        T = self._T.get(sig.sym, 2.0)
        return (time.time() - sig.recv) + self._rtt_for(sig.sym) + extra > self.deadline_frac * T

    def _skip_late(self, sig: MHSignal, where: str) -> None:
        EXP.skipped_late[sig.sym] += 1
        n = sum(EXP.skipped_late.values())
        if n % 50 == 1:
            logger.info("skipped_late %s idx=%d (%s) - %d so far", sig.sym, sig.idx, where, n)
        if self.ledger is not None and self.test_mode:
            self.ledger.event("skipped_late", sym=sig.sym, idx=sig.idx, where=where,
                              age=round(time.time() - sig.recv, 4), rtt=round(self._rtt_for(sig.sym), 4))

    # ── API helpers ──────────────────────────────────────────────────────────
    def _ratio_get(self, sym: str, ci: int, h: int) -> Optional[float]:
        v = self._ratio_cache.get((sym, ci, h))
        ttl = min(RATIO_TTL, self.max_quote_age) if self.max_quote_age > 0 else RATIO_TTL
        return v[0] if v and time.time() - v[1] < ttl else None

    def _ratio_ts(self, sym: str, ci: int, h: int) -> Optional[float]:
        v = self._ratio_cache.get((sym, ci, h))
        return v[1] if v else None

    async def _pace(self) -> None:
        """Space proposal requests at least self._rl_gap seconds apart (called BEFORE taking an API permit)."""
        if self._rl_lock is None:
            self._rl_lock = asyncio.Lock()
        async with self._rl_lock:
            now = time.monotonic()
            if self._rl_next > now:
                await asyncio.sleep(self._rl_next - now)
                now = time.monotonic()
            self._rl_next = max(now, self._rl_next) + self._rl_gap

    async def _ratio_fetch(self, sym: str, ci: int, h: int) -> None:
        if time.monotonic() < self._rl_cool_until:
            return                                          # cooling down after a rate limit: send nothing
        await self._pace()
        if time.monotonic() < self._rl_cool_until:
            return
        async with self._sem:
            t0 = time.perf_counter()
            prop = await self._propose(sym, mp.COVERS[ci], self.stake, h)
            self._note_rtt(time.perf_counter() - t0, sym)
        if prop and prop.get("payout"):
            self._ratio_cache[(sym, ci, h)] = (float(prop["payout"]) / self.stake, time.time())

    async def _on_api_error(self, what: str, exc: Exception):
        if "rate" in str(exc).lower():
            self._throttle()
            if what == "buy":
                await asyncio.sleep(1.0)        # buys are not paced; brief backoff outside the permit
            return
        await super()._on_api_error(what, exc)

    def _throttle(self) -> None:
        """Rate limit hit: stop sending proposals for a cooldown (doubles if it keeps happening, max 60 s)."""
        now = time.monotonic()
        if now < self._rl_cool_until:
            return                                          # stragglers already in flight; cooldown running
        self._rl_strikes = self._rl_strikes + 1 if now - self._rl_last_end < 15.0 else 1
        cool = min(60.0, self._rl_cool_base * (2 ** (self._rl_strikes - 1)))
        self._rl_cool_until = now + cool
        self._rl_last_end = self._rl_cool_until
        logger.warning("RATE LIMIT: no proposals for %.0f s (strike %d)", cool, self._rl_strikes)

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
            for sym in [k for k, g in list(self._sig.items()) if not g.done]:
                t = asyncio.create_task(self._scan_sym(sym))     # every symbol is scanned in parallel
                self._tasks.add(t)
                t.add_done_callback(self._tasks.discard)

    async def _scan_sym(self, sym: str):
        try:
            await self._scan_once(only=(sym,))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("scan error %s: %s", sym, exc, exc_info=True)

    async def _scan_once(self, only=None):
        self._scans += 1
        sigs: List[MHSignal] = []
        for sym, sig in list(self._sig.items()):
            if only is not None and sym not in only:
                continue
            if sig.done or sym in self._muted or sym in self._hold or self._idx.get(sym) != sig.idx:
                continue
            sig.done = True                                 # each signal is evaluated exactly once
            if self._late(sig):
                self._skip_late(sig, "scan")
                continue
            sigs.append(sig)
        if not sigs:
            return
        if self.test_mode:
            await self._scan_test(sigs)
            return
        need: list = []                                     # payout ratios still missing (top 3 per sym/h)
        seen = set()
        for rank in range(3):                               # best cover of every (sym, h) first, then 2nd, then 3rd
            for sig in sigs:
                for h, sc in sig.cands.items():
                    if rank < len(sc):
                        k = (sig.sym, sc[rank][0], h)
                        if k not in seen and self._ratio_get(*k) is None:
                            seen.add(k)
                            need.append(k)
        need = need[: self._max_ratio_fetch]                # cap per scan; the cache fills over the next scans
        if need and time.monotonic() >= self._rl_cool_until:
            await asyncio.gather(*(self._ratio_fetch(*k) for k in need), return_exceptions=True)
        cands: List[Cand] = []
        for sig in sigs:
            if self.cluster:                                # first cover (pick order) whose real quote passes the floor
                for h, sc in sig.cands.items():
                    for ci, p, lb, mass in sc:
                        r = self._ratio_get(sig.sym, ci, h)
                        if r is None:
                            continue
                        if r < self.min_ratio:
                            self._cl_floor += 1
                            continue
                        if lb * r - 1.0 <= (self.rank_min_edge if self.rank_key == "edge" else self.min_edge):   # real Wilson lb x real quote must beat the edge floor
                            self._cl_edge += 1
                            continue
                        c = Cand(sig, h, ci, p, lb, r, lb * r - 1.0)
                        c.qts = self._ratio_ts(sig.sym, ci, h)
                        c.score = mass
                        c.cl = (sig.info.get(h, ((), mass))[0], mass, cover_width(mp.COVERS[ci]))
                        cands.append(c)
                        break
                continue
            for h, sc in sig.cands.items():
                best = None
                for ci, p, lb, _e in sc:
                    r = self._ratio_get(sig.sym, ci, h)
                    if r is None or r < self.min_ratio:
                        continue
                    edge = lb * r - 1.0
                    if edge > self.min_edge and (best is None or edge > best.edge):
                        best = Cand(sig, h, ci, p, lb, r, edge)
                        best.qts = self._ratio_ts(sig.sym, ci, h)
                if best is not None:
                    cands.append(best)
        for sym in {s.sym for s in sigs}:                   # forget old target counters
            cut = self._idx.get(sym, 0) - 30
            for k in [k for k in self._tcount if k[0] == sym and k[1] < cut]:
                self._tcount.pop(k, None)
            self._doubt_targets = {k for k in self._doubt_targets if not (k[0] == sym and k[1] < cut)}
        if self.minute_rank:
            self._rank_minute(sigs, cands)
            return
        if not cands:
            self._last_decision = f"scan {self._scans}: {len(sigs)} signal(s), none admitted"
            return
        cands.sort(key=self._cand_key if self.cluster else (lambda c: -c.score))
        picks: List[Tuple[Cand, float]] = []
        for c in cands:
            if self._late(c.sig) or self._blocked():
                continue
            if self.max_open and len(self._open) + self._inflight + len(picks) >= self.max_open:
                break
            if self.max_per_scan and len(picks) >= self.max_per_scan:
                break
            tk = (c.sig.sym, c.sig.idx + c.h)
            if tk in self._doubt_targets or self._tcount.get(tk, 0) >= self.max_per_target:
                continue                                    # an in-doubt buy keeps its target closed
            stake = self._next_stake()                      # shrinks as pending grows: best candidate = biggest stake
            if stake is None:
                self._last_decision = "skip: no stake available"
                break
            if not self.money.reserve(stake):
                self._last_decision = "skip: balance cannot pay the stake"
                break
            self._tcount[tk] = self._tcount.get(tk, 0) + 1
            picks.append((c, stake))
        if picks:
            self._rank_shadow(cands, picks, f"scan {self._scans}")
        for c, stake in picks:
            self._inflight += 1
            t = asyncio.create_task(self._buy_one(c, stake))
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)

    @staticmethod
    def _key_mass(c: "Cand"):
        return (-c.score, -c.p)                             # old order: raw top-cluster mass, then calibrated p

    @staticmethod
    def _key_edge(c: "Cand"):
        return (-c.edge, -c.lb, -c.p)                       # calibrated cover lb x real quote - 1

    def _cand_key(self, c: "Cand"):
        return self._key_edge(c) if (self.rank_key == "edge" and self.cluster) else self._key_mass(c)

    def _rank_shadow(self, pool: List["Cand"], picks: List[Tuple["Cand", float]], where: str) -> None:
        """Both orderings over the SAME pool, logged for offline comparison. No API calls; c.score is never touched."""
        if not pool:
            return
        tag = lambda x: f"{x.sig.sym}:{mp.COVERS[x.ci][0]}{mp.COVERS[x.ci][1]}:h{x.h}"
        bm, be = sorted(pool, key=self._key_mass), sorted(pool, key=self._key_edge)
        agree = int(bm[0] is be[0])
        bought = [{"c": tag(c), "mass": round(c.score, 4), "edge": round(c.edge, 4), "lb": round(c.lb, 4)} for c, _ in picks]
        rm = tag(bm[1]) if len(bm) > 1 else "-"
        re_ = tag(be[1]) if len(be) > 1 else "-"
        logger.info("RANK SHADOW %s pool=%d key=%s mass_top=%s edge_top=%s agree=%d bought=%s runner_mass=%s runner_edge=%s",
                    where, len(pool), self.rank_key, tag(bm[0]), tag(be[0]), agree,
                    [(b["c"], b["mass"], b["edge"]) for b in bought], rm, re_)
        if self.ledger is not None:
            self.ledger.event("rank_shadow", where=where, pool=len(pool), key=self.rank_key,
                              mass_order=[tag(x) for x in bm[:5]], edge_order=[tag(x) for x in be[:5]],
                              mass_top=tag(bm[0]), edge_top=tag(be[0]), agree=agree, bought=bought,
                              runner_mass=rm, runner_edge=re_)

    def _win_id(self, now: Optional[float] = None) -> int:
        return int((time.time() if now is None else now) // self.rank_window)

    def _win_label(self, win: int) -> str:
        t = win * self.rank_window
        return f"{int(t % 86400 // 3600):02d}:{int(t % 3600 // 60):02d}:{int(t % 60):02d}"

    def _roll_window(self, win: int) -> None:
        """A new ranking window starts: log how the old one went, then reset the bookkeeping.
        (_rnd_minute / _rnd_n / _rnd_best now hold the WINDOW id / picks / per-symbol bests; 60 s = the old minute.)"""
        prev = self._rnd_minute
        if prev >= 0 and self._rnd_seen:
            bar = len(self._rnd_best)
            rej = collections.Counter(self._rnd_rej.values())
            rej.update({"min_ratio": self._cl_floor - self._rnd_c0[0], "edge": self._cl_edge - self._rnd_c0[1],
                        "gate/mute": self._cl_gate - self._rnd_c0[2], "oos": self._cl_oos - self._rnd_c0[3]})
            rej = {k: v for k, v in rej.items() if v > 0}
            if self._rnd_n == 0:
                logger.info("WINDOW RANK %s: no admissible candidate | pool snapshot=%d bar=%d | rejected: %s",
                            self._win_label(prev), self._rnd_snap, bar, ", ".join(f"{k}={v}" for k, v in sorted(rej.items(), key=lambda x: -x[1])) or "-")
            else:
                logger.info("WINDOW RANK %s: bought %d | pool snapshot=%d bar=%d", self._win_label(prev), self._rnd_n, self._rnd_snap, bar)
        self._bar_pool = dict(self._rnd_best) if self.rank_mode == "bar" else {}   # bar mode: the window that just closed
        self._rnd_minute, self._rnd_best, self._rnd_n = win, {}, 0
        self._rnd_rej, self._rnd_snap, self._rnd_seen = {}, 0, False
        self._rnd_c0 = (self._cl_floor, self._cl_edge, self._cl_gate, self._cl_oos)

    def _bar_refresh(self, c: Cand) -> Optional[Cand]:
        """Bar mode: the ranking used the closed window's information; the BUY uses the symbol's current state.
        Re-derive the candidate at the current tick (fresh p / lb, current idx so h still means 'h-th tick after the buy'),
        then re-check quote floor, edge floor, deadline (_late), mute / hold. Nothing after the buy tick is ever used."""
        sym = c.sig.sym
        mhs, idx = self.mh.get(sym), self._idx.get(sym)
        if sym in self._muted or sym in self._hold:
            self._rnd_rej[sym] = "muted/held"
            return None
        if mhs is None or idx is None or not mhs.ready():
            self._rnd_rej[sym] = "no_model"
            return None
        r_ = self._cluster_cands(mhs, c.h, self._q.get(sym, 0.1))
        if not r_:
            self._rnd_rej[sym] = "gate/oos"
            return None
        sc, info = r_
        sig = MHSignal(sym, idx, self._recv.get(sym, time.time()), self._last_epoch.get(sym), {c.h: sc}, {c.h: info})
        if self._late(sig):
            self._rnd_rej[sym] = "late"
            return None
        thr = self.rank_min_edge if self.rank_key == "edge" else self.min_edge
        for ci, p, lb, mass in sc:
            r = self._ratio_get(sym, ci, c.h)
            if r is None:
                self._rnd_rej.setdefault(sym, "no_quote")
                continue
            if r < self.min_ratio or lb * r - 1.0 <= thr:
                self._rnd_rej[sym] = "min_ratio/edge"
                continue
            fc = Cand(sig, c.h, ci, p, lb, r, lb * r - 1.0)
            fc.score, fc.qts = mass, self._ratio_ts(sym, ci, c.h)
            fc.cl = (info[0], mass, cover_width(mp.COVERS[ci]))
            fc.extra = {"rank_mode": "bar", "stale_ticks": idx - c.sig.idx, "rank_sig_idx": c.sig.idx,
                        "rank_p": round(c.p, 4), "rank_lb": round(c.lb, 4)}
            self._rnd_rej.pop(sym, None)
            return fc
        return None

    def _rank_minute(self, sigs: List[MHSignal], cands: List[Cand]) -> None:
        """Two-level ranking, fresh every window (MH_RANK_WINDOW_SECONDS: 30 = :00/:30 of each UTC minute, 60 = old minute).
        Level 1: each symbol keeps its single best contract (over/under, any horizon) from its newest signal.
        Level 2: those per-symbol bests are ranked against each other; the top MH_WINDOW_TOP_N is bought, once per window.
        snapshot mode: only candidates whose symbol has not ticked since the signal.  bar mode: rank the closed window's
        bests, buy against the current state (see _bar_refresh)."""
        now = time.time()
        win = self._win_id(now)
        if win != self._rnd_minute:                         # new window: old ranking is discarded
            self._roll_window(win)
        self._rnd_seen = True
        by_sym: Dict[str, Cand] = {}
        for c in cands:                                     # level 1
            b = by_sym.get(c.sig.sym)
            if b is None or self._cand_key(c) < self._cand_key(b):
                by_sym[c.sig.sym] = c
        for sym in {sg.sym for sg in sigs}:                 # a newer signal supersedes the symbol's older candidate
            if sym in by_sym:
                self._rnd_best[sym] = by_sym[sym]
            else:
                self._rnd_best.pop(sym, None)
        live = [c for c in self._rnd_best.values() if self._idx.get(c.sig.sym) == c.sig.idx]
        self._rnd_snap = max(self._rnd_snap, len(live))     # shadow: pool size a snapshot ranking would see
        if self._rnd_n >= self.minute_top_n:
            return
        label = self._win_label(win)
        if now < win * self.rank_window + self.rank_delay:
            self._last_decision = f"window {label}: collecting ({len(self._rnd_best)} symbols ranked)"
            return
        if self.rank_mode == "bar":
            ranked = sorted(self._bar_pool.values(), key=self._cand_key)     # information of the bar that just closed
            pool = [fc for fc in (self._bar_refresh(c) for c in ranked) if fc is not None]
        else:
            pool = []
            for c in self._rnd_best.values():               # only live candidates: no newer tick, not late, not muted
                s_ = c.sig.sym
                why = ("stale" if self._idx.get(s_) != c.sig.idx else "muted/held" if (s_ in self._muted or s_ in self._hold)
                       else "late" if self._late(c.sig) else None)
                if why:
                    self._rnd_rej[s_] = why
                else:
                    pool.append(c)
            pool.sort(key=self._cand_key)                   # level 2
        if not pool:
            return
        picks: List[Tuple[Cand, float]] = []
        for c in pool:
            if self._rnd_n + len(picks) >= self.minute_top_n or self._blocked():
                break
            if self.max_open and len(self._open) + self._inflight + len(picks) >= self.max_open:
                break
            tk = (c.sig.sym, c.sig.idx + c.h)
            if tk in self._doubt_targets or self._tcount.get(tk, 0) >= self.max_per_target:
                self._rnd_rej[c.sig.sym] = "target_closed"
                continue                                    # an in-doubt buy keeps its target closed
            stake = self._next_stake()
            if stake is None:
                self._last_decision = "skip: no stake available"
                break
            if not self.money.reserve(stake):
                self._last_decision = "skip: balance cannot pay the stake"
                break
            self._tcount[tk] = self._tcount.get(tk, 0) + 1
            picks.append((c, stake))
        if not picks:
            return
        self._rnd_n += len(picks)
        self._rank_shadow(pool, picks, f"window {label}")
        runner = [f"{x.sig.sym} {mp.COVERS[x.ci][0]}{mp.COVERS[x.ci][1]}" for x in pool[len(picks):len(picks) + 2]]
        head = (f"MINUTE RANK {label[:5]} UTC" if self.rank_window == 60 else f"WINDOW RANK {label} UTC") + f" [{self.rank_mode}]"
        logger.info("%s: %d symbols ranked -> BUY %s (runners-up: %s)", head, len(pool),
                    ", ".join(f"{c.sig.sym} {mp.COVERS[c.ci][0]}{mp.COVERS[c.ci][1]} h={c.h} score={c.score:.3f} p={c.p:.3f}"
                              for c, _ in picks), ", ".join(runner) or "-")
        for c, stake in picks:
            self._inflight += 1
            t = asyncio.create_task(self._buy_one(c, stake))
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)

    async def _buy_one(self, c: Cand, stake: float) -> None:
        sig, h, cand = c.sig, c.h, mp.COVERS[c.ci]
        sym = sig.sym
        tk = (sym, sig.idx + h, c.arm) if c.arm else (sym, sig.idx + h)
        released = False
        st = {"ok": False}
        handed = False                                      # in doubt: the reconcile task now owns reservation + slot
        try:
            err: Optional[Exception] = None
            resp = None
            t_sent = time.time()
            async with self._sem:
                if self._late(sig):                         # waited too long for a permit
                    self._skip_late(sig, "buy")
                    return
                t0 = time.perf_counter()
                t_sent = time.time()
                try:
                    price = round(stake * (1.0 + self.price_tol) + 1e-9, 2)
                    resp = await self._buy_direct(sym, cand, price, stake, h, timeout=self.buy_timeout)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    err = exc
                self._note_rtt(time.perf_counter() - t0, sym)
            if err is not None:
                if self._buy_in_doubt(err):                 # frame went out, outcome unknown: do NOT free anything yet
                    handed = True
                    self._start_reconcile(c, stake, tk, t_sent, err)
                    return
                await self._on_api_error("buy", err)        # definitely refused: old failure path (finally frees the slot)
                return
            buy = (resp or {}).get("buy") or {}
            self.money.note_buy(buy)
            self.money.release(stake)
            released = True
            await self._register_buy(c, stake, buy, resp, st)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("buy flow error %s: %s", sym, exc, exc_info=True)
        finally:
            if not handed:
                self._buy_finish(c, stake, tk, released, st["ok"])

    @staticmethod
    def _buy_in_doubt(exc: BaseException) -> bool:
        """Timeout / disconnect after the frame was written = the buy MAY have executed. API error replies are definite."""
        return isinstance(exc, TimeoutError) or bool(getattr(exc, "sent", False))

    def _buy_finish(self, c: Cand, stake: float, tk: tuple, released: bool, ok: bool, win: Optional[int] = None) -> None:
        """Release the reservation, the in-flight count and (when the buy did not happen) the slot."""
        if not released:
            self.money.release(stake)
        self._inflight -= 1
        if c.arm == "test":
            self._inflight_test = max(0, self._inflight_test - 1)
        if c.arm:
            self._test_pending = max(0.0, self._test_pending - stake)
        if not ok:
            self._tcount[tk] = max(0, self._tcount.get(tk, 1) - 1)
            same = (self._win_id() == self._rnd_minute) if win is None else (win == self._rnd_minute)
            if self.minute_rank and c.arm is None and same:
                self._rnd_n = max(0, self._rnd_n - 1)      # the minute's pick failed: another candidate may be tried

    def _remember_cid(self, cid: str) -> None:
        self._known_cids[cid] = None
        while len(self._known_cids) > 5000:
            self._known_cids.popitem(last=False)

    async def _register_buy(self, c: Cand, stake: float, buy: dict, resp, st: dict, recovered: bool = False, source: str = "") -> None:
        """Success path: track the contract, ledger + log, subscribe. st['ok'] is set as soon as it is in self._open."""
        sig, h, cand = c.sig, c.h, mp.COVERS[c.ci]
        sym = sig.sym
        cid = str(buy.get("contract_id", ""))
        if not cid:
            logger.warning("BUY returned no contract_id: %s", resp)
            return
        price_paid = float(buy.get("buy_price") or stake)
        payout = float(buy.get("payout") or 0.0)
        ratio = payout / price_paid if payout > 0 and price_paid > 0 else c.r
        st["ok"] = True
        self._remember_cid(cid)
        slip = None
        if not recovered and c.r:                           # payout-slip diagnostics: measurement only
            slip = ratio - c.r
            self._slips.append(slip)
            self._slip_n += 1
            if ratio < self.min_ratio:
                self._slip_low += 1
        q_age = (time.time() - c.qts) if c.qts else None
        self._open[cid] = {"sym": sym, "cand": cand, "stake": price_paid, "ratio": ratio,
                           "opened_at": float(buy.get("purchase_time") or time.time()),
                           "decision_epoch": sig.epoch, "t0": time.time(), "h": h, "sig_idx": sig.idx,
                           "p_cal": c.p, "p_lb": c.lb, "score": c.score, "pred_digits": (), "pred_ranked": (),
                           "arm": c.arm or ("test" if self.test_mode else "mh"), "buy_ts": time.time(),
                           **(c.extra or {})}
        if recovered:
            self._open[cid]["recovered"] = source
        if self.ledger is not None:
            extra = {"recovered": 1} if recovered else {}
            self.ledger.event("buy", cid=cid, sym=sym, idx=sig.idx, h=h, arm=c.arm or "mh", contract=f"{cand[0]}{cand[1]}",
                              stake=price_paid, ratio=round(ratio, 4), rtt=round(self._rtt_for(sym), 4),
                              age=round(time.time() - sig.recv, 4),
                              payout_slip=None if slip is None else round(slip, 4), **extra)
        tg = f"{cand[0]}{cand[1]}"
        cl_s = ""
        if c.cl is not None:
            self._cl_cover[tg] += 1
            self._cl_width[c.cl[2]] += 1
            cl_s = " cluster=%s mass=%.2f width=%d pick=%s" % (list(c.cl[0]), c.cl[1], c.cl[2], self.pick)
        self._last_decision = f"BUY {sym} {tg} h={h} p={c.p:.3f} lb={c.lb:.3f} r={ratio:.3f} edge_lb={c.edge:+.3f}"
        logger.info("MH TRADE %s %s %s h=%d p_cal=%.3f p_lb=%.3f paid=%.2f payout=%.2f ratio=%.3f edge_lb=%+.3f "
                    "score=%.4f q=%.2f shortcode=%s", cid, sym, tg, h, c.p, c.lb, price_paid, payout, ratio, c.edge,
                    c.score, self._q.get(sym, 0.0), str(buy.get("shortcode", "?")) + cl_s)
        if slip is not None:
            logger.info("MH SLIP %s quote_ratio=%.3f paid_ratio=%.3f slip=%+.3f quote_age=%s", cid, c.r, ratio, slip,
                        "n/a" if q_age is None else f"{q_age:.0f}s")
        self.client._contract_symbol_map[cid] = sym
        try:
            await self.client.subscribe_contract(
                cid, lambda msg, _c=cid: asyncio.create_task(self._on_contract_result(_c, msg)), symbol=sym)
        except Exception as exc:
            logger.warning("subscribe_contract(%s) failed (settle loop will poll): %s", cid, exc)

    # ── in-doubt buys: reconcile instead of guessing ─────────────────────────
    def _start_reconcile(self, c: Cand, stake: float, tk: tuple, t_sent: float, err: BaseException) -> None:
        req_id = getattr(err, "req_id", None)
        self._doubt_targets.add(tk)                         # no second candidate for this (symbol, target tick)
        logger.warning("MH BUY IN DOUBT %s %s%s h=%d req_id=%s (%s: %s): reservation and slot held, reconciling",
                       c.sig.sym, mp.COVERS[c.ci][0], mp.COVERS[c.ci][1], c.h, req_id, type(err).__name__, str(err)[:80])
        t = asyncio.create_task(self._reconcile_in_doubt(c, stake, tk, req_id, t_sent))
        self._reconciles.add(t)
        self._doubt_open.add(t)
        t.add_done_callback(lambda x: (self._reconciles.discard(x), self._doubt_open.discard(x)))

    async def _lookup_open_or_closed(self, c: Cand, stake: float, t_sent: float):
        """portfolio (open) + profit_table (already settled). -> (row, source) | (None, 'absent') | (None, 'unknown')."""
        cand = mp.COVERS[c.ci]
        want = {"sym": c.sig.sym, "type": pred.contract_type(cand), "barrier": cand[1], "ticks": c.h, "stake": stake,
                "max_price": round(stake * (1.0 + self.price_tol) + 1e-9, 2), "t_lo": t_sent - 2.0, "t_sent": t_sent}
        for attempt in (1, 2):
            try:
                rows, closed = await asyncio.gather(self.client.get_portfolio(strict=True, timeout=5.0),
                                                    self.client.get_profit_table(limit=30, strict=True, timeout=5.0))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("MH RECONCILE query failed (attempt %d): %s", attempt, exc)
                if attempt == 1:
                    await asyncio.sleep(self.reconcile_retry)
                continue
            want["t_hi"] = time.time() + 2.0
            m = match_in_doubt(rows, want, self._known_cids)
            if m is not None:
                return m, "portfolio"
            m = match_in_doubt(closed, want, self._known_cids)
            if m is not None:
                return m, "profit_table"
            return None, "absent"
        return None, "unknown"

    async def _reconcile_in_doubt(self, c: Cand, stake: float, tk: tuple, req_id, t_sent: float) -> None:
        sym, h, cand = c.sig.sym, c.h, mp.COVERS[c.ci]
        win = self._rnd_minute
        st = {"ok": False}
        released = False
        me_task = asyncio.current_task()
        ident = dict(req_id=req_id, sym=sym, idx=c.sig.idx, h=h, contract=f"{cand[0]}{cand[1]}", stake=stake, t_sent=round(t_sent, 3))

        def late_reply():
            late = self.client.pop_late_buy(req_id) if req_id is not None else None
            return late["buy"] if late and (late.get("buy") or {}).get("contract_id") else None

        try:
            buy, source = None, ""
            t_end = time.monotonic() + self.reconcile_wait
            while True:                                     # (a) the late reply
                buy = late_reply()
                if buy is not None:
                    source = "late_reply"
                    break
                if time.monotonic() >= t_end:
                    break
                await asyncio.sleep(0.05)
            if buy is None:                                 # (b)+(c) ask Deriv (portfolio, profit_table), one retry
                row, source = await self._lookup_open_or_closed(c, stake, t_sent)
                if row is not None:
                    buy = {"contract_id": row.get("contract_id"), "buy_price": row.get("buy_price"), "payout": row.get("payout"),
                           "purchase_time": row.get("purchase_time"), "shortcode": row.get("shortcode")}
                elif source == "absent":
                    buy = late_reply()
                    if buy is not None:
                        source = "late_reply"
            if buy is not None:                             # RECOVERED: same success path
                self._doubt_open.discard(me_task)
                self.money.release(stake)                   # Deriv's balance already reflects the buy; no note_buy (no double deduction)
                released = True
                logger.warning("MH BUY RECOVERED %s %s%s h=%d req_id=%s cid=%s via %s", sym, cand[0], cand[1], h, req_id,
                               buy.get("contract_id"), source)
                await self._register_buy(c, stake, buy, None, st, recovered=True, source=source)
                self._doubt_targets.discard(tk)
                if source in ("profit_table", "late_reply") and st["ok"]:
                    cid = str(buy.get("contract_id"))
                    poc = await self.client.force_check_contract(cid)    # may already be settled
                    if poc:
                        await self._on_contract_result(cid, {"proposal_open_contract": poc})
                return
            self._doubt_open.discard(me_task)
            if source == "absent":                          # confirmed: behave exactly like a refused buy
                logger.warning("MH BUY CONFIRMED ABSENT %s %s%s h=%d req_id=%s: slot freed", sym, cand[0], cand[1], h, req_id)
                self._doubt_targets.discard(tk)
                return                                      # finally: _buy_finish(ok=False) frees reservation + slot
            # ambiguous: slot stays closed; reservation released only after a safety delay
            logger.error("MH BUY IN DOUBT UNRESOLVED %s %s%s h=%d req_id=%s: slot stays closed", sym, cand[0], cand[1], h, req_id)
            if self.ledger is not None:
                self.ledger.event("buy_in_doubt_unresolved", **ident)
            st["ok"] = True                                 # ok=True keeps _tcount / _rnd_n counted (slot closed)
            await asyncio.sleep(self.reconcile_safety)
            buy = late_reply()
            if buy is not None:
                st["ok"] = False
                self.money.release(stake)
                released = True
                logger.warning("MH BUY RECOVERED %s req_id=%s cid=%s via late_reply_after_delay", sym, req_id, buy.get("contract_id"))
                await self._register_buy(c, stake, buy, None, st, recovered=True, source="late_reply")
                cid = str(buy.get("contract_id"))
                poc = await self.client.force_check_contract(cid)
                if poc:
                    await self._on_contract_result(cid, {"proposal_open_contract": poc})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("reconcile error %s: %s", sym, exc, exc_info=True)
            st["ok"] = True                                 # unknown outcome: never reopen the slot
        finally:
            self._doubt_open.discard(me_task)
            self._buy_finish(c, stake, tk, released, st["ok"], win)

    # ── boot: adopt contracts Deriv still holds for us ───────────────────────
    async def _adopt_orphans(self) -> None:
        if not self.adopt_orphans:
            return
        try:
            rows = await self.client.get_portfolio(strict=True, timeout=10.0)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("MH ADOPT: portfolio unavailable at boot (%s)", exc)
            return
        n = 0
        for r in rows:
            cid = str(r.get("contract_id", ""))
            sc = parse_shortcode(r.get("shortcode")) or {}
            typ = r.get("contract_type") or sc.get("type")
            sym = r.get("symbol") or sc.get("sym")
            if not cid or cid in self._open or cid in self._known_cids or typ not in ("DIGITOVER", "DIGITUNDER"):
                continue
            try:
                bar = int(sc.get("barrier"))
                bp = float(r.get("buy_price") or 0.0)
                pay = float(r.get("payout") or 0.0)
                opened = float(r.get("purchase_time") or time.time())
            except (TypeError, ValueError):
                continue
            cand = ("OVER" if typ == "DIGITOVER" else "UNDER", bar)
            self._remember_cid(cid)
            self._open[cid] = {"sym": sym, "cand": cand, "stake": bp, "ratio": pay / bp if bp > 0 and pay > 0 else 1.9,
                               "opened_at": opened, "decision_epoch": None, "t0": time.time(), "h": None, "sig_idx": 0,
                               "p_cal": 0.0, "p_lb": 0.0, "score": 0.0, "pred_digits": (), "pred_ranked": (),
                               "arm": "orphan", "buy_ts": time.time()}
            self.client._contract_symbol_map[cid] = sym
            try:
                await self.client.subscribe_contract(
                    cid, lambda msg, _c=cid: asyncio.create_task(self._on_contract_result(_c, msg)), symbol=sym)
            except Exception as exc:
                logger.warning("subscribe_contract(%s) failed (settle loop will poll): %s", cid, exc)
            if self.ledger is not None:
                self.ledger.event("orphan_adopted", cid=cid, sym=sym, contract=f"{cand[0]}{cand[1]}", stake=bp)
            logger.warning("MH ADOPT ORPHAN %s %s %s%d stake=%.2f (open on Deriv, not tracked here)", cid, sym, cand[0], cand[1], bp)
            n += 1
        logger.info("MH ADOPT: %d orphan contract(s) adopted at boot", n)

    # ── TEST MODE: frozen zone rule, shadow scoring, control arm ─────────────
    def _test_step(self, sym: str, mhs: mp.MHSymbol) -> None:
        """One tick: model -> zone totals -> frozen decision -> ledger row BEFORE the outcome -> signal (if it fires)."""
        hs = mhs.hs.get(1)
        if hs is None or hs.cur is None:
            self._sig.pop(sym, None)
            return
        p = hs.cur[1]
        ci_o, ci_u = mp.COVERS.index(exp_mode.OVER5), mp.COVERS.index(exp_mode.UNDER4)
        cal = hs.cal
        cal_o = (lambda x: float(cal[ci_o](x)[0])) if cal else None
        cal_u = (lambda x: float(cal[ci_u](x)[0])) if cal else None
        thr = float(_cfg("TEST_ZONE_THRESHOLD", 0.45))
        zd = exp_mode.zone_decision(p, cal_o, cal_u, thr)
        fire, reason = zd.fire, zd.reason
        gate = bool(_cfg("TEST_APPLY_GATE", False))
        if fire and gate and (not hs.tradable or hs.muted()):          # optional significance gate + horizon mute
            fire, reason = False, "skip_gate"
        elif fire and bool(_cfg("TEST_HONOUR_MUTE", True)) and sym in self._muted:   # per-symbol breaker mute
            fire, reason = False, "skip_muted"
        idx = self._idx[sym]
        self._shadow[sym] = (idx, int(zd.top), exp_mode.best_zone(zd), bool(fire))   # scored when tick idx+1 arrives
        if self.ledger is not None and (fire or bool(_cfg("LEDGER_LOG_ALL_TICKS", True))):
            self.ledger.prediction(epoch=self._last_epoch.get(sym), sym=sym, idx=idx, h=1,
                                   probs=",".join(f"{x:.5f}" for x in p), zone_over=zd.zo, zone_under=zd.zu,
                                   cal_over=zd.co, cal_under=zd.cu, threshold=thr, decision=reason,
                                   cfg_hash=exp_mode.config_hash())
        if not fire:
            self._sig.pop(sym, None)
            return
        k = int(_cfg("TEST_RESCORE_HMAX", 5))
        p_by_h = {h: [float(x) for x in st.cur[1]] for h, st in mhs.hs.items() if h <= k and st.cur is not None}
        ci = mp.COVERS.index(zd.cand)
        sig = MHSignal(sym, idx, self._recv.get(sym, time.time()), self._last_epoch.get(sym),
                       {1: [(ci, float(zd.p_cal), float(zd.p_cal), float(zd.p_cal))]}, {})
        sig.test = {"zd": zd, "p_by_h": p_by_h}
        self._sig[sym] = sig
        self._scan_event.set()

    def _exp_on_digit(self, sym: str, i: int, digit: int) -> None:
        """Every tick: stream diagnostics + shadow outcome of the prediction made on the previous tick (no money)."""
        self.diag.add(sym, digit)
        pend = self._shadow.pop(sym, None)
        if not pend or pend[0] != i - 1:
            return
        _, top, zone, fired = pend
        top_hit, zh = (top == digit), exp_mode.zone_hit(zone, digit)
        EXP.add_shadow(sym, top_hit, zh, fired)
        if self.ledger is not None and (fired or bool(_cfg("LEDGER_LOG_ALL_TICKS", True))):
            self.ledger.outcome(sym=sym, idx=i, digit=int(digit), top_hit=int(top_hit), zone=zone, zone_hit=int(zh), fired=int(fired))

    async def _scan_test(self, sigs: List[MHSignal]) -> None:
        ci_o, ci_u = mp.COVERS.index(exp_mode.OVER5), mp.COVERS.index(exp_mode.UNDER4)
        control = bool(_cfg("TEST_CONTROL", True))
        need, seen = [], set()
        for sig in sigs:
            for ci in ((ci_o, ci_u) if control else (sig.cands[1][0][0],)):
                k = (sig.sym, ci, 1)
                if k not in seen and self._ratio_get(*k) is None:
                    seen.add(k)
                    need.append(k)
        need = need[: self._max_ratio_fetch]
        if need and time.monotonic() >= self._rl_cool_until:
            await asyncio.gather(*(self._ratio_fetch(*k) for k in need), return_exceptions=True)
        for sym in {s.sym for s in sigs}:                   # forget old target counters
            cut = self._idx.get(sym, 0) - 30
            for k in [k for k in self._tcount if k[0] == sym and k[1] < cut]:
                self._tcount.pop(k, None)
            self._doubt_targets = {k for k in self._doubt_targets if not (k[0] == sym and k[1] < cut)}
        sigs = sorted(sigs, key=lambda s: (-s.cands[1][0][1], s.sym))
        stake = self.stake                                   # ONE flat stake, no money-rules sizing
        picks: List[Tuple[Cand, float]] = []
        n_test_new = 0
        cap = int(_cfg("TEST_MAX_TRADES", 4000))
        for sig in sigs:
            ci, p, _lb, _m = sig.cands[1][0]
            r = self._ratio_get(sig.sym, ci, 1)
            if r is None or r < 1.0:
                continue
            arms = [("test", ci, p, r)]
            if control:
                cci = mp.COVERS.index(exp_mode.control_cover(str(_cfg("TEST_CONTROL_MODE", "random")), self._rng))
                cr = self._ratio_get(sig.sym, cci, 1)
                if cr is not None and cr >= 1.0:
                    arms.append(("ctl", cci, exp_mode.BASE_RATE, cr))
            n_need = len(arms)
            if self._blocked() or self._late(sig):
                if self._late(sig):
                    self._skip_late(sig, "scan2")
                continue
            if self.max_open and len(self._open) + self._inflight + len(picks) + n_need > self.max_open:
                break
            if EXP.n_test() + self._open_test() + self._inflight_test + n_test_new + 1 > cap:
                break
            if any(self._tcount.get((sig.sym, sig.idx + 1, a[0]), 0) >= 1 for a in arms):
                continue
            if self.client.balance - self._test_pending < stake * n_need:
                self._last_decision = "skip: demo balance below flat stake"
                break
            for arm, aci, ap, ar in arms:
                c = Cand(sig, 1, aci, ap, ap, ar, ap * ar - 1.0)
                c.arm = arm
                c.score = ap
                c.extra = {"p_by_h": sig.test["p_by_h"], "zone_over": sig.test["zd"].zo, "zone_under": sig.test["zd"].zu,
                           "cal_zone": sig.test["zd"].p_cal, "signal_cfg": exp_mode.config_hash()}
                self._tcount[(sig.sym, sig.idx + 1, arm)] = 1
                self._test_pending += stake
                if arm == "test":
                    self._inflight_test += 1
                    n_test_new += 1
                picks.append((c, stake))
        for c, st in picks:
            self._inflight += 1
            t = asyncio.create_task(self._buy_one(c, st))
            self._tasks.add(t)
            t.add_done_callback(self._tasks.discard)
        if picks:
            self._last_decision = f"TEST scan {self._scans}: {len(picks)} order(s) from {len(sigs)} signal(s)"

    # ── ledger / restore / cycle accounting ──────────────────────────────────
    async def _exp_boot(self) -> None:
        import exp_ledger
        self.ledger = await asyncio.to_thread(lambda: exp_ledger.get_ledger())
        try:
            await asyncio.to_thread(self._exp_restore)
        except Exception as exc:
            logger.error("LEDGER restore failed (starting from zero): %s", exc, exc_info=True)
        if EXP.start_balance is None:
            try:
                EXP.start_balance = float(self.client.balance)
                self.ledger.event("start_balance", value=EXP.start_balance)
            except Exception:
                pass
        self.ledger.event("boot", cfg_hash=exp_mode.config_hash(), pid=os.getpid())

    def _exp_restore(self) -> None:
        led = self.ledger
        now = time.time()
        n = 0
        for r in led.settlements():
            arm = r["arm"]
            row = {k: r[k] for k in ("ts", "arm", "sym", "won", "profit", "stake", "ratio", "h", "realized_h", "align", "p_cal", "p_rescored")}
            row["won"] = bool(row["won"])
            if arm in ("test", "ctl"):
                EXP.add_settlement(row)
            if arm == "ctl":
                continue
            n += 1
            h, won, p = int(r["h"] or 1), bool(r["won"]), float(r["p_cal"] or 0.0)
            ratio = max(float(r["ratio"] or 1.0), 1.0001)
            self._n_trades += 1
            self._n_wins += int(won)
            self._be_sum += 1.0 / ratio
            self._total_pnl += float(r["profit"] or 0.0)
            self._stakes_won += float(r["profit"] or 0.0) / max(float(r["stake"] or self.stake), 1e-9)
            self._set_trades += 1
            self._hs_n[h] += 1
            self._hs_w[h] += int(won)
            self._hs_p[h] += p
            try:
                kind, bar = r["contract"][:-1], int(r["contract"][-1])
                pt = pred.true_probability((kind, bar))
                self._fair_mean += pt * ratio - 1.0
                self._fair_var += pt * (1.0 - pt) * ratio * ratio
            except Exception:
                pass
            if arm == "test":
                self._brk.setdefault(r["sym"], collections.deque(maxlen=80)).append((1.0 if won else 0.0, p))
        ttl = float(_cfg("MH_MUTE_TTL_SECONDS", 0))
        muted = {}
        for e in led.events("mute") + led.events("unmute"):
            if e["kind"] == "mute":
                muted[e["data"].get("sym")] = e["ts"]
            else:
                muted.pop(e["data"].get("sym"), None)
        for sym, ts in muted.items():
            if sym and (ttl <= 0 or now - ts < ttl):
                self._muted.add(sym)
                self._mute_ts[sym] = ts
        sh = led.last_event("shadow_snapshot")
        if sh:
            EXP.load_shadow(sh["data"])
        sb = led.last_event("start_balance")
        if sb:
            EXP.start_balance = float(sb["data"].get("value"))
        fv = led.last_event("final_verdict")
        if fv:
            EXP.final, EXP.done_reason = fv["data"], str(fv["data"].get("reason", "finished"))
        dg = led.last_event("donkey_guard_state")
        if dg and dg["data"].get("state"):
            os.environ["DONKEY_GUARD_STATE"] = dg["data"]["state"]
            try:
                import donkey_guard
                donkey_guard.get_guard().load_from_env()
            except Exception:
                pass
        logger.info("LEDGER RESTORE: %d settled trade(s) (test arm %d, control %d), muted=%s, final=%s, shadow ticks=%d",
                    n, EXP.n_test(), len(EXP.ctl), sorted(self._muted) or "-", bool(EXP.final), EXP.shadow.get("ticks", 0))

    async def _exp_ready(self) -> None:
        """Boot finished: log how long this redeploy cycle cost (uptime of the previous instance vs downtime)."""
        import redeploy_guard
        now = time.time()
        led = self.ledger
        prev_ready, trig = led.last_event("ready"), led.last_event("deploy_triggered")
        boot_s = now - getattr(restart_scheduler, "_process_start_time", self._boot_t0)
        fit_s = now - self._boot_t0
        data = {"boot_seconds": round(boot_s, 1), "fit_seconds": round(fit_s, 1)}
        if trig and (not prev_ready or trig["ts"] > prev_ready["ts"]):
            down = now - trig["ts"]
            up = (trig["ts"] - prev_ready["ts"]) if prev_ready else None
            Tm = [self._T.get(s, 2.0) for s in self.symbols]
            lost = sum(down / max(t, 0.5) for t in Tm)
            data.update(downtime_seconds=round(down, 1), prev_uptime_seconds=None if up is None else round(up, 1),
                        trading_fraction=None if up is None else round(up / (up + down), 4), ticks_lost_est=int(lost))
            logger.warning("REDEPLOY CYCLE: previous instance uptime %s | downtime %.0fs (process boot %.0fs of which "
                           "history+model fit %.0fs) | trading fraction %s | ~%d symbol-ticks not evaluated",
                           "n/a" if up is None else f"{up:.0f}s", down, boot_s, fit_s,
                           "n/a" if up is None else f"{100 * up / (up + down):.1f}%", int(lost))
        else:
            logger.info("BOOT: process %.0fs, history+fit %.0fs (no earlier deploy_triggered event in the ledger)", boot_s, fit_s)
        led.event("ready", **data)
        EXP.cycle = data
        redeploy_guard.mark_engine_ready()

    async def _before_redeploy(self) -> None:
        if self._doubt_open:                                # in-doubt buys still being reconciled: bounded wait
            limit = self.reconcile_wait + self.reconcile_retry + 6.0
            logger.warning("REDEPLOY: waiting up to %.0fs for %d in-doubt buy reconcile(s)", limit, len(self._doubt_open))
            await asyncio.wait(list(self._doubt_open), timeout=limit)
        led = self.ledger
        if led is not None:
            for _t in list(self._doubt_open):               # still unknown after the wait
                led.event("abandoned_at_redeploy", cid=None, in_doubt=1)
        if led is None:
            return
        for cid, info in list(self._open.items()):
            led.event("abandoned_at_redeploy", cid=cid, sym=info.get("sym"), arm=info.get("arm"), h=info.get("h"))
        led.event("shadow_snapshot", **EXP.shadow_snapshot())
        try:
            from donkey_guard import get_guard
            led.event("donkey_guard_state", state=get_guard().export_state())
        except Exception:
            pass
        led.event("deploy_triggered", open_contracts=len(self._open))
        await asyncio.to_thread(led.flush, 10.0)

    def _rtt_report(self) -> dict:
        out = {}
        for sym in self.symbols:
            a, b = self._rtt_q(sym, 0.5), self._rtt_q(sym, 0.95)
            if a is not None:
                out[sym] = {"p50": round(a, 4), "p95": round(b, 4), "n": len(self._rtt_hist[sym]), "T": self._T.get(sym)}
        return out

    def _epoch_report(self) -> dict:
        """realized_h comes from mapping the contract's exit epoch to a tick index; epochs have 1 s resolution.
        A symbol is flagged when duplicate-epoch ticks, unmappable exits or irregular gaps make that unreliable."""
        out = {}
        for sym in self.symbols:
            nt, dup = self._ntick[sym] + self._dup[sym], self._dup[sym]
            q = list(self._epq.get(sym, ()))
            T = self._T.get(sym, 2.0)
            gaps = [b - a for a, b in zip(q, q[1:])]
            irr = (sum(1 for g in gaps if g != round(T)) / len(gaps)) if gaps else 0.0
            sn, un = self._set_n[sym], self._unk_h[sym]
            why = []
            if nt >= 200 and dup / nt > float(_cfg("TEST_EPOCH_DUP_MAX", 0.005)):
                why.append(f"dup-epoch ticks {100 * dup / nt:.2f}%")
            if sn >= 20 and un / sn > 0.05:
                why.append(f"{100 * un / sn:.0f}% exit ticks not mappable")
            if T <= 1.5 and irr > 0.02:
                why.append(f"{100 * irr:.1f}% irregular gaps at T={T:.0f}s")
            out[sym] = {"T": T, "dup_rate": round(dup / nt, 5) if nt else None, "irregular_gaps": round(irr, 4),
                        "settled": sn, "unmapped": un, "unreliable": bool(why), "why": why}
        return out

    def _exp_publish(self) -> None:
        EXP.rtt = self._rtt_report()
        EXP.epoch = self._epoch_report()
        EXP.ledger_stats = self.ledger.stats() if self.ledger else {}
        EXP.engine = {"balance": round(self.client.balance, 2), "open": len(self._open), "symbols": len(self.symbols),
                      "muted": sorted(self._muted), "stop": EXP.stop_reason(self._open_test()),
                      "redeploy_in": None if not restart_scheduler._due_at else round(restart_scheduler._due_at - time.time())}
        EXP.publish()

    async def _exp_loop(self) -> None:
        last_pub = last_diag = last_shadow = 0.0
        while True:
            await asyncio.sleep(5)
            try:
                now = time.time()
                if self.test_mode:
                    open_test = self._open_test()
                    reason = EXP.stop_reason(open_test)
                    if reason and not EXP.is_done():
                        self._stop_t0 = self._stop_t0 or now
                        if open_test == 0 or now - self._stop_t0 > 120:
                            final = EXP.finalize(reason)
                            logger.warning(EXP.banner())
                            self.ledger.event("final_verdict", **final)
                            await asyncio.to_thread(self.ledger.flush, 10.0)
                    if now - last_diag >= 30:
                        last_diag = now
                        EXP.diag = self.diag.snapshot()
                if now - last_shadow >= 60:
                    last_shadow = now
                    self.ledger.event("shadow_snapshot", **EXP.shadow_snapshot())
                if now - last_pub >= 10:
                    last_pub = now
                    self._exp_publish()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("exp loop error: %s", exc, exc_info=True)

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
        arm = info.get("arm") or ("test" if self.test_mode else "mh")
        realized = None
        try:                                                # late = settled on a later tick than intended
            ex = poc.get("exit_tick_time")
            m = self._ep2idx.get(sym, {})
            if ex is not None and int(float(ex)) in m:
                realized = m[int(float(ex))] - int(info["sig_idx"])
                if arm != "ctl":
                    late = 1.0 if realized > h else 0.0
                    self._q[sym] = 0.95 * self._q.get(sym, 0.1) + 0.05 * late
        except Exception:
            pass
        align = exp_mode.label_alignment(realized, h)
        self._set_n[sym] += 1
        if realized is None:
            self._unk_h[sym] += 1
        exit_digit = None
        try:
            raw = poc.get("exit_tick")
            raw = poc.get("exit_tick_display_value") if raw is None else raw
            exit_digit = pred.last_digit(float(raw), self._decimals[sym])
        except Exception:
            pass
        p_resc = None
        if align == "LATE" and arm == "test" and info.get("p_by_h"):   # re-score against the horizon that actually settled
            try:
                pk = (info["p_by_h"].get(realized) or info["p_by_h"].get(str(realized)))
                if pk is not None:
                    ci = mp.COVERS.index(cand)
                    raw_p = float(np.asarray(pk) @ mp.W[:, ci])
                    hk = self.mh[sym].hs.get(realized)
                    p_resc = float(hk.cal[ci](raw_p)[0]) if (hk is not None and hk.cal) else raw_p
            except Exception:
                p_resc = None
        stake_v = float(info.get("stake") or self.stake)
        ratio_v = float(info.get("ratio") or 0.0)
        tag = f"{cand[0]}{cand[1]}"
        if self.ledger is not None:
            self.ledger.settlement(cid=cid, sym=sym, sig_idx=int(info["sig_idx"]), h=h, realized_h=realized, align=align,
                                   arm=arm, contract=tag, stake=stake_v, ratio=ratio_v, profit=profit, won=int(won),
                                   exit_digit=exit_digit, p_cal=p, p_rescored=p_resc,
                                   rtt=round(self._rtt_for(sym), 4), cfg_hash=exp_mode.config_hash())
        if arm in ("test", "ctl"):
            EXP.add_settlement({"ts": time.time(), "arm": arm, "sym": sym, "won": won, "profit": profit, "stake": stake_v,
                                "ratio": ratio_v, "h": h, "realized_h": realized, "align": align, "p_cal": p, "p_rescored": p_resc})
        logger.info("MH RESULT %s %s %s h=%d %s %s arm=%s realized_h=%s q=%.2f p_cal=%.3f profit=%+.2f%s", cid, sym, tag, h,
                    "WIN" if won else "LOSS", align, arm, realized, self._q.get(sym, 0.1), p, profit,
                    "" if p_resc is None else f" p_rescored={p_resc:.3f}")
        if arm == "ctl":                                    # control arm: evidence only, never feeds the model's breaker/stats
            return
        self._set_trades += 1
        self._hs_n[h] += 1
        self._hs_w[h] += int(won)
        self._hs_p[h] += p
        dq = self._brk.setdefault(sym, collections.deque(maxlen=80))
        dq.append((1.0 if won else 0.0, p))
        if sym not in self._muted and len(dq) >= 40:
            var = sum(pp * (1 - pp) for _, pp in dq)
            z = sum(w - pp for w, pp in dq) / (var ** 0.5) if var > 0 else 0.0
            if z < -2.0:
                self._muted.add(sym)
                self._mute_ts[sym] = time.time()
                self._sig.pop(sym, None)
                if self.ledger is not None:
                    self.ledger.event("mute", sym=sym, z=round(z, 3), n=len(dq))
                logger.warning("MH BREAKER: %s muted (persisted in the ledger; z=%.2f over %d trades)", sym, z, len(dq))
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

    # ── optional in-process refit (MH_INPROC_REFIT_SECONDS > 0): what the 7-minute redeploy did implicitly ──
    async def _inproc_refit_loop(self):
        while True:
            await asyncio.sleep(self.inproc_refit)
            if not self.client.is_connected:
                continue
            for sym in list(self.symbols):
                try:
                    await self._refit_symbol_bg(sym)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error("MH REFIT %s failed (old model kept): %s", sym, exc)
                await asyncio.sleep(1.0)                  # spread the history downloads

    async def _refit_symbol_bg(self, sym: str) -> bool:
        """Build a NEW MHSymbol from fresh history off the event loop, replay the ticks buffered meanwhile, swap atomically
        (symbol in _hold; no await between replay and publish). Any failure or gap keeps the old model: never no model."""
        old = self.mh.get(sym)
        if old is None or sym in self._hold or not old.ready():
            return False
        t0 = time.time()
        self._bg_buf[sym] = collections.deque(maxlen=20000)       # record BEFORE the history request goes out
        try:
            ticks = await self.client.get_tick_history(sym, int(_cfg("MH_HISTORY_TICKS", 15000)))
            if len(ticks) < 600:
                raise RuntimeError(f"only {len(ticks)} ticks returned")
            dec = self._decimals[sym]
            digits = pred.digits_from_quotes([t["quote"] for t in ticks], dec)
            new = mp.MHSymbol(sym, self.hmax, 10, float(_cfg("MH_DECAY", 0.998)))
            await new.fit(digits, ticks[-1]["epoch"], deadline=time.time() + float(_cfg("MH_BOOT_BUDGET_SECONDS", 75)), offload=True)
            if not new.hs or not set(old.hs) <= set(new.hs):
                raise RuntimeError("new model has fewer horizons than the old one (time budget)")
            old = self.mh.get(sym)                                 # may have been replaced by a resync meanwhile
            if old is None or sym in self._hold:
                return False
            self._hold.add(sym)                                    # from here to the publish there is no await
            try:
                self._sig.pop(sym, None)
                seen, n = set(), 0
                for ep, qt in sorted(self._bg_buf.get(sym, ())):
                    if ep > new.last_epoch and ep not in seen:
                        seen.add(ep)
                        new.on_tick(pred.last_digit(qt, dec), ep)
                        n += 1
                if old.last_epoch is not None and new.last_epoch != old.last_epoch:
                    raise RuntimeError(f"replay gap (new {new.last_epoch} vs live {old.last_epoch})")
                self.mh[sym] = new
            finally:
                self._hold.discard(sym)
            logger.info("MH REFIT %s: swapped, %d ticks replayed, %.1fs, tradable_h=%s", sym, n, time.time() - t0,
                        [h for h, hs in new.hs.items() if hs.tradable])
            return True
        finally:
            self._bg_buf.pop(sym, None)

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
        if self.cluster:
            return sum(1 for m in self.mh.values() if m.ready())     # symbols with a fitted MH state
        return sum(1 for m in self.mh.values() for h, hs in m.hs.items() if hs.tradable and h <= self.hmax and not hs.muted())

    def report_text(self) -> str:
        T = self._n_trades
        trade = (f"trades {T}: win {100.0 * self._n_wins / T:.1f}% vs break-even {100.0 * self._be_sum / T:.1f}% "
                 f"| P&L {self._total_pnl:+.2f}") if T else "trades 0"
        per_h = " ".join(f"h{h}:{self._hs_n[h]}t/{self._hs_w[h]}w exc{self._hs_w[h] - self._hs_p[h]:+.1f}"
                         for h in range(1, self.hmax + 1) if self._hs_n[h]) or "no settled trades"
        qs = ",".join(f"{s}:{self._q.get(s, 0.1):.2f}" for s in self.symbols)
        total = len(self.symbols) if self.cluster else sum(1 for m in self.mh.values() for h in m.hs if h <= self.hmax)
        cl = ""
        if self.cluster:
            cv = " ".join(f"{k}:{v}" for k, v in sorted(self._cl_cover.items())) or "-"
            wd = " ".join(f"w{k}:{v}" for k, v in sorted(self._cl_width.items())) or "-"
            cl = (f"CLUSTER formed {self._cl_formed} spread-out {self._cl_spread} floor-rejected {self._cl_floor} | "
                  f"covers {cv} | widths {wd} | ")
        if self.cluster:
            cl += f"gate/mute-refused {self._cl_gate} edge-refused {self._cl_edge} oos-refused {self._cl_oos} | "
        if self.test_mode:
            v = EXP.verdict()
            tn = EXP.summary()["three_numbers"]
            pc = lambda x: "-" if x is None else f"{100 * x:.1f}%"
            cl += (f"TEST[{exp_mode.config_hash()}] n={v['n']}/{int(_cfg('TEST_MAX_TRADES', 4000))} state={v['state']} "
                   f"shadow={pc(tn['shadow_hit_rate'])} aligned={pc(tn['aligned']['win_rate'])}({tn['aligned']['n']}) "
                   f"late={pc(tn['late']['win_rate'])}({tn['late']['n']}) ctl={EXP.summary()['control']['n']} "
                   f"skipped_late={sum(EXP.skipped_late.values())} | ")
        if self._slips:
            sl = sorted(self._slips)
            cl += (f"SLIP mean {sum(sl) / len(sl):+.3f} p95 {sl[min(len(sl) - 1, int(0.95 * len(sl)))]:+.3f} "
                   f"n={self._slip_n} paid<{self.min_ratio:.2f}: {self._slip_low} | ")
        return (f"SET REPORT | MH {trade} | {cl}per-h {per_h} | tradable (sym,h) {self._n_tradable()}/{total} | "
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
