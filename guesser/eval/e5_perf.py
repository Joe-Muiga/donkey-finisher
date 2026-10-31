"""E5 - performance: per-tick latency (p50/p99), memory per symbol, boot-to-first-trade with simulated paged history."""
import asyncio, gc, json, sys, time, tracemalloc
import numpy as np
from guesser import sim, policy as pol
from guesser.core import SymbolGuesser, GuesserConfig


def per_tick(n=20000):
    d, q = sim.fair_pcg(1, n)
    g = SymbolGuesser("P", GuesserConfig())
    g.fit(d[:3000].tolist(), None if q is None else None)
    lat, vlat = [], []
    rf = lambda c: pol.model_ratio(len(pol.win_set(c)), 0.05)
    for i in range(3000, n):
        t = time.perf_counter()
        g.on_tick(int(d[i]), 1000.0 + (i % 50) * 0.01, float(i))
        lat.append(time.perf_counter() - t)
        if i % 10 == 0:
            t = time.perf_counter(); g.view(rf); vlat.append(time.perf_counter() - t)
    a = np.array(lat) * 1e3; v = np.array(vlat) * 1e3
    return {"ticks": len(a), "on_tick_ms_p50": float(np.percentile(a, 50)), "on_tick_ms_p99": float(np.percentile(a, 99)), "on_tick_ms_max": float(a.max()),
            "view_ms_p50": float(np.percentile(v, 50)), "view_ms_p99": float(np.percentile(v, 99))}


def memory():
    tracemalloc.start()
    gc.collect()
    s0 = tracemalloc.take_snapshot()
    gs = []
    for k in range(5):
        g = SymbolGuesser(f"M{k}", GuesserConfig())
        d, q = sim.fair_pcg(k, 3000)
        g.fit(d.tolist(), [1000.0 + (i % 50) * 0.01 for i in range(3000)])
        gs.append(g)
    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    d, _ = sim.fair_pcg(9, 3000)
    g = SymbolGuesser("Z", GuesserConfig()); g.fit(d.tolist())
    b = g.mix.bank
    arr = sum(getattr(b, n).nbytes for n in ("cF", "cW", "T1", "T2", "T3", "TL", "cDL", "TP", "cm", "cn", "TR", "Tpar", "Thi", "TPa", "TPb", "hist", "inc_hist", "carry_hist", "have_price", "run_hist"))
    # growth check: memory must not grow with ticks (tables are fixed-size, history is a ring)
    n1 = arr
    d2, _ = sim.fair_pcg(10, 30000); g2 = SymbolGuesser("Z2", GuesserConfig()); g2.fit(d2.tolist())
    b2 = g2.mix.bank
    arr2 = sum(getattr(b2, n).nbytes for n in ("cF", "cW", "T1", "T2", "T3", "TL", "cDL", "TP", "cm", "cn", "TR", "Tpar", "Thi", "TPa", "TPb", "hist", "inc_hist", "carry_hist", "have_price", "run_hist"))
    return {"traced_bytes_per_symbol_incl_python_objects": cur / 5, "table_bytes_per_symbol": int(arr), "table_bytes_after_30k_ticks": int(arr2),
            "bounded": arr == arr2, "tail_deque_max_digits": 2000}


async def boot(n_symbols=20, rtt=0.30, hist=5000, tick_gap=1.0):
    sys.path.insert(0, ".")
    import test_guesser as tg
    tg._stub_missing_modules()
    import config, collections
    config.GUESSER_MODE = True
    config.GUESSER_HISTORY_TICKS = hist
    from guesser.engine import GuesserEngine

    class SlowClient(tg.FakeClient):
        async def get_tick_history(self, sym, n):
            pages = -(-n // 1000)
            await asyncio.sleep(rtt * pages)                      # paged history, one RTT per 1000 ticks
            return await super().get_tick_history(sym, n)

        async def _send(self, req, timeout=10):
            await asyncio.sleep(rtt)
            return await super()._send(req, timeout)
    eng = GuesserEngine()
    eng.client = SlowClient(np.random.default_rng(3).integers(0, 10, hist).tolist())
    eng.symbols = [f"S{i}" for i in range(n_symbols)]
    eng._events = {s: asyncio.Event() for s in eng.symbols}
    eng._live = {s: collections.deque(maxlen=300) for s in eng.symbols}
    eng._last_tick_wall = {s: time.time() for s in eng.symbols}
    eng.money.balance = eng.client.balance
    eng._seen_conn = eng._conn_count()
    eng._blocked = lambda: False
    t0 = time.time()
    done = {}

    async def one(s):
        await eng._fit_symbol(s, initial=True)
        done[s] = time.time() - t0
    await asyncio.gather(*(one(s) for s in eng.symbols))
    ready_first, ready_all = min(done.values()), max(done.values())
    # first eligible trade: first symbol's next tick + one scan (proposal RTT + buy RTT)
    s = min(done, key=done.get)
    ep = eng.predictors[s].last_epoch + tick_gap
    eng._on_tick(s, {"quote": 1000.0 + 0.05, "epoch": ep})
    t1 = time.time()
    await eng._scan_once()
    first_trade = (time.time() - t0)
    n_buys = len([r for r in eng.client.sent if "buy" in r])
    return {"symbols": n_symbols, "rtt_s": rtt, "history_ticks": hist, "first_symbol_ready_s": ready_first, "all_symbols_ready_s": ready_all,
            "first_trade_after_boot_s_excl_waiting_for_next_tick": first_trade, "buys_in_first_scan": n_buys,
            "note": "wall-clock with simulated 0.3 s RTT per page/request; CPU fit of 5000 ticks per symbol runs in a worker thread (one core here)"}


if __name__ == "__main__":
    out = {"per_tick": per_tick(), "memory": memory()}
    print(out, flush=True)
    out["boot"] = asyncio.run(boot())
    print(out["boot"])
    json.dump(out, open("docs/guesser/results/e5_perf.json", "w"), indent=1, default=float)
