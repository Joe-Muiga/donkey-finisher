"""E2 - power and detection delay on planted structure, guesser vs the existing V2 stack (rng_core.stat_model.StatModel e-process).
Detection = first tick where log BF >= ln(100) (alpha=0.01, one stream) and >= ln(2000) (Bonferroni over S=20 symbols).
Usage: python -m guesser.eval.e2_power --seeds 3 --nmax 15000 --out docs/guesser/results/e2_power.json [--no-v2]"""
import argparse, json, math, statistics, sys, time
import numpy as np
from guesser import sim
from guesser.core import SymbolGuesser, GuesserConfig

T_A, T_B = math.log(100.0), math.log(2000.0)


def families(nmax):
    F = []
    for p in (0.105, 0.11, 0.12):
        F.append((f"bias P(7)={p}", "bias", lambda s, n, p=p: sim.bias(s, n, p), True))
    for t in (0.02, 0.05):
        F.append((f"markov1 tilt={t}", "markov1", lambda s, n, t=t: sim.markov1(s, n, t), True))
    for t in (0.05, 0.10):
        F.append((f"markov2 tilt={t}", "markov2", lambda s, n, t=t: sim.markov2(s, n, t), True))
    for lag, p in ((5, 0.05), (5, 0.1), (5, 0.2), (5, 0.3), (1, 0.1), (17, 0.1), (32, 0.1), (48, 0.2)):
        F.append((f"lagcopy lag={lag} p={p}", "lagcopy", lambda s, n, lag=lag, p=p: sim.lagcopy(s, n, lag, p), lag <= 31))
    for per in (2, 3, 7, 10, 31):
        F.append((f"periodic p={per} noise=0.5", "periodic", lambda s, n, per=per: sim.periodic(s, n, per, 0.5), per <= 31))
    for q in (0.1, 0.3):
        F.append((f"delta lag2 (+3) q={q}", "delta", lambda s, n, q=q: sim.delta2(s, n, q), True))
    F.append(("regime switch@3000 lagcopy5 p=.3", "regime", lambda s, n: sim.regime(s, n, 3000, lambda a, b: sim.lagcopy(a, b, 5, 0.3)), True))
    for rho in (0.5, 0.9):
        F.append((f"price-increment AR rho={rho}", "price", lambda s, n, rho=rho: sim.price_inc(s, n, rho), True))
    F.append(("weak generator xorshift32 mod10", "weak", lambda s, n: sim.weak(s, n), True))
    return F


def run_guesser(D, Q, nmax, after=2000):
    g = SymbolGuesser("E2", GuesserConfig())
    dA = dB = None
    stop = None
    d = D.tolist()
    t3_0 = n0 = 0
    for i in range(min(nmax, len(d))):
        g.on_tick(d[i], None if Q is None else float(Q[i]), float(i))
        le = g.mix.cumulative_log_e() if g.mix.runs > 1 else g.mix.log_e
        if dA is None and le >= T_A:
            dA = i; t3_0, n0 = g.top3, g.n_sh
        if dB is None and le >= T_B:
            dB = i
        if dA is not None and (i - dA >= after) and dB is not None or (dA is not None and i - dA >= after + 3000):
            break
    hit3 = (g.top3 - t3_0) / max(1, g.n_sh - n0) if dA is not None else None
    return dA, dB, hit3


def run_v2(D, nmax, after=2000):
    from rng_core.stat_model import StatModel
    st = StatModel(hmax=1)
    dA = dB = None
    d = D.tolist()
    prev = None
    h = n = 0
    for i in range(min(nmax, len(d))):
        if prev is not None and dA is not None:
            n += 1
            h += int(d[i] in prev)
        st.on_digit(int(d[i]))
        le = st.log_e(1)
        if dA is None and le >= T_A:
            dA = i
        if dB is None and le >= T_B:
            dB = i
        alt = st.dist_alt(1)
        prev = None if alt is None else set(int(x) for x in np.argsort(-alt, kind="stable")[:3])
        if dA is not None and i - dA >= after:
            break
    return dA, dB, (h / n if n else None)


def summarize(vals, nmax):
    det = [v for v in vals if v is not None]
    return {"detected": f"{len(det)}/{len(vals)}", "median": (statistics.median(det) if det and len(det) * 2 > len(vals) else None),
            "p90": (sorted(det)[min(len(det) - 1, math.ceil(0.9 * len(vals)) - 1)] if len(det) >= math.ceil(0.9 * len(vals)) else None),
            "all": vals}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=3); ap.add_argument("--nmax", type=int, default=15000)
    ap.add_argument("--no-v2", action="store_true"); ap.add_argument("--only", default="")
    ap.add_argument("--out", default="docs/guesser/results/e2_power.json")
    a = ap.parse_args()
    import os
    out = json.load(open(a.out)) if os.path.exists(a.out) else {}
    for name, fam, fn, v2ok in families(a.nmax):
        if a.only and not any(o in name for o in a.only.split('|')):
            continue
        rows = {"guesser": [], "guesser_bonf": [], "g_top3": [], "v2": [], "v2_bonf": [], "v2_top3": []}
        t0 = time.time()
        for s in range(a.seeds):
            D, Q = fn(300 + s, a.nmax)
            dA, dB, h3 = run_guesser(D, Q, a.nmax)
            rows["guesser"].append(dA); rows["guesser_bonf"].append(dB); rows["g_top3"].append(h3)
            if not a.no_v2:
                vA, vB, v3 = run_v2(D, a.nmax)
                rows["v2"].append(vA); rows["v2_bonf"].append(vB); rows["v2_top3"].append(v3)
        r = {"family": fam, "v2_supports": v2ok, "nmax": a.nmax,
             "guesser_ln100": summarize(rows["guesser"], a.nmax), "guesser_bonf20": summarize(rows["guesser_bonf"], a.nmax),
             "guesser_top3_after": [x for x in rows["g_top3"] if x is not None]}
        if not a.no_v2:
            r.update({"v2_ln100": summarize(rows["v2"], a.nmax), "v2_bonf20": summarize(rows["v2_bonf"], a.nmax),
                      "v2_top3_after": [x for x in rows["v2_top3"] if x is not None]})
        out[name] = r
        print(name, "guesser", r["guesser_ln100"]["detected"], r["guesser_ln100"]["median"], "| v2", r.get("v2_ln100", {}).get("detected"),
              r.get("v2_ln100", {}).get("median"), f"{time.time() - t0:.0f}s", flush=True)
    out["_meta"] = vars(a)
    json.dump(out, open(a.out, "w"), indent=1, default=float)
