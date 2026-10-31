"""E4 - ablations: drop one expert family at a time; prior on U in {0.5, 0.9, 0.99}; fixed-share 1/5000.
E1-lite (fair streams: max log BF, so any threshold alpha can be read off) and E2-lite (detection delay at ln 100 on three planted streams)."""
import argparse, json, math, statistics, time
import numpy as np
from guesser import sim, experts as ex
from guesser.core import SymbolGuesser, GuesserConfig

FAMS = ["Fw", "M", "L", "DL", "P", "C", "R", "B", "PI"]


def variants():
    allv = [n for n in ex.NAMES]
    V = {"baseline": {}}
    for f in FAMS:
        V[f"drop {f}"] = {"enabled": tuple(n for n in allv if ex.family(n) != f)}
    for p in (0.5, 0.99):
        V[f"prior_null={p}"] = {"prior_null": p}
    V["fixed_share=1/5000"] = {"share": 1 / 5000}
    return V


def null_run(kw, streams, ticks):
    mx = []
    for s in range(streams):
        d, _ = sim.fair_pcg(4000 + s, ticks)
        g = SymbolGuesser("A", GuesserConfig(**kw))
        m = -1e9
        for i, y in enumerate(d.tolist()):
            g.on_tick(y, None, float(i))
            m = max(m, g.mix.log_e)
        mx.append(m)
    return mx


def power_run(kw, nmax, seeds):
    out = {}
    cases = {"bias .12": lambda s: sim.bias(s, nmax, 0.12), "lagcopy5 .2": lambda s: sim.lagcopy(s, nmax, 5, 0.2),
             "markov2 .1": lambda s: sim.markov2(s, nmax, 0.10)}
    for nm, fn in cases.items():
        ds = []
        for s in range(seeds):
            d, q = fn(800 + s)
            g = SymbolGuesser("A", GuesserConfig(**kw))
            t = None
            for i, y in enumerate(d.tolist()):
                g.on_tick(y, None, float(i))
                if g.mix.cumulative_log_e() >= math.log(100):
                    t = i
                    break
            ds.append(t)
        out[nm] = ds
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--streams", type=int, default=30); ap.add_argument("--ticks", type=int, default=10000)
    ap.add_argument("--nmax", type=int, default=8000); ap.add_argument("--seeds", type=int, default=3)
    ap.add_argument("--only", default=""); ap.add_argument("--out", default="docs/guesser/results/e4_ablation.json")
    a = ap.parse_args()
    import os
    res = json.load(open(a.out)) if os.path.exists(a.out) else {}
    for name, kw in variants().items():
        if a.only and name not in a.only.split('|'):
            continue
        t0 = time.time()
        mx = null_run(kw, a.streams, a.ticks)
        pw = power_run(kw, a.nmax, a.seeds)
        res[name] = {"null_max_log_e": mx, "null_ever_ln100": sum(m >= math.log(100) for m in mx), "null_ever_ln20": sum(m >= math.log(20) for m in mx),
                     "null_streams": len(mx), "null_max": max(mx), "power_delay_ticks": pw}
        print(name, res[name]["null_ever_ln100"], res[name]["null_max"], pw, f"{time.time() - t0:.0f}s", flush=True)
    res["_meta"] = vars(a)
    json.dump(res, open(a.out, "w"), indent=1, default=float)
