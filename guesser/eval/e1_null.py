"""E1 - null calibration.  Fair streams (PCG64, SHA-256 DRBG, os.urandom) through SymbolGuesser; records per stream the running
max of log BF, log BF at checkpoints (for session-level e-BH), shadow top-1/top-3 hits, mean log-loss gain, calibration curve.
Usage: python -m guesser.eval.e1_null --streams 100 --ticks 25000 --out docs/guesser/results/e1_null.json"""
import argparse, json, math, sys, time
import numpy as np
from guesser import sim, evidence as evd
from guesser.core import SymbolGuesser, GuesserConfig, wilson
from guesser.calibrate import SetCalibrator

GENS = {"pcg64": sim.fair_pcg, "sha256_drbg": sim.fair_drbg, "urandom": sim.fair_urandom}


def run(streams, ticks, session, ck, share=0.0, prior_null=0.9, enabled=None, gens=None):
    out = {}
    for gname in (gens or GENS):
        fn = GENS[gname]
        cal = SetCalibrator()
        rows = []
        t0 = time.time()
        for s in range(streams):
            d, _ = fn(1000 + s, ticks)
            g = SymbolGuesser("N", GuesserConfig(share=share, prior_null=prior_null, enabled=enabled), cal)
            mx, cks = -1e9, []
            for i, y in enumerate(d.tolist()):
                g.on_tick(y, None, float(i))
                if g.mix.log_e > mx:
                    mx = g.mix.log_e
                if (i + 1) % ck == 0:
                    cks.append(g.mix.cumulative_log_e())
            st = g.stats()
            rows.append({"max_log_e": mx, "cks": cks, "n": st["n"], "top1": g.top1, "top3": g.top3, "ll_sum": g.ll_sum,
                         "final": g.mix.log_e, "runs": g.mix.runs})
        n = sum(r["n"] for r in rows)
        t1 = sum(r["top1"] for r in rows); t3 = sum(r["top3"] for r in rows)
        ll = sum(r["ll_sum"] for r in rows) / n
        sd1 = math.sqrt(0.1 * 0.9 / n); sd3 = math.sqrt(0.3 * 0.7 / n)
        a = 0.01
        ever = sum(r["max_log_e"] >= math.log(1 / a) for r in rows)
        ever_b = sum(r["max_log_e"] >= math.log(session / a) for r in rows)
        ever5 = sum(r["max_log_e"] >= math.log(1 / 0.05) for r in rows)
        # sessions: groups of `session` streams; Bonferroni 'ever' and e-BH at every checkpoint
        ns = len(rows) // session
        sess_bonf = sess_ebh = 0
        for k in range(ns):
            grp = rows[k * session:(k + 1) * session]
            sess_bonf += any(r["max_log_e"] >= math.log(session / a) for r in grp)
            hit = False
            for c in range(len(grp[0]["cks"])):
                if evd.ebh([r["cks"][c] for r in grp], a):
                    hit = True
            sess_ebh += hit
        # worst final log-loss: negative gain means the mixture LOSES vs uniform (expected under H0)
        out[gname] = {
            "streams": len(rows), "ticks_per_stream": ticks, "session_size": session, "scored": n,
            "top1": t1 / n, "top1_z": (t1 / n - 0.1) / sd1, "top3": t3 / n, "top3_z": (t3 / n - 0.3) / sd3,
            "mean_ll_gain_nats": ll,
            "ever_reach_ln100_per_stream": ever, "ever_wilson95": wilson(ever, len(rows)),
            "ever_reach_ln(session/alpha)_per_stream": ever_b,
            "ever_reach_ln20_per_stream": ever5,
            "sessions": ns, "sessions_with_bonferroni_T2": sess_bonf, "sessions_with_ebh_T2_any_checkpoint": sess_ebh,
            "max_log_e_over_streams": max(r["max_log_e"] for r in rows),
            "restarts_total": sum(r["runs"] - 1 for r in rows),
            "calibration_k3": cal.curve(0, 3), "calibration_k1": cal.curve(0, 1), "seconds": round(time.time() - t0, 1)}
        print(gname, {k: v for k, v in out[gname].items() if not k.startswith("calibration")}, flush=True)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--streams", type=int, default=100); ap.add_argument("--ticks", type=int, default=25000)
    ap.add_argument("--session", type=int, default=20); ap.add_argument("--ck", type=int, default=500)
    ap.add_argument("--share", type=float, default=0.0); ap.add_argument("--prior", type=float, default=0.9)
    ap.add_argument("--gens", default="pcg64,sha256_drbg,urandom"); ap.add_argument("--out", default="docs/guesser/results/e1_null.json")
    a = ap.parse_args()
    import os
    res = json.load(open(a.out)) if os.path.exists(a.out) else {}
    res.update(run(a.streams, a.ticks, a.session, a.ck, a.share, a.prior, gens=a.gens.split(',')))
    res["_meta"] = vars(a)
    json.dump(res, open(a.out, "w"), indent=1, default=float)
