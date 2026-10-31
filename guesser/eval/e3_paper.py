"""E3 - paper-trading simulator: Deriv-like payout r=(1-m)*10/k, one-tick contracts, latency 0/1/2 ticks, min stake 0.35,
max stake 50, 1 tick/second, 1 hour per run (3600 ticks).  Policies: evidence | probe | bold | money_rules (17.5% of balance each
tick, no evidence gate) | flat (min stake each tick, no gate).  Bold-play check: success probability vs start/target."""
import argparse, json, math, statistics, time
import numpy as np
from guesser import sim


def agg(rs):
    tr = [r.trades for r in rs]
    wins = sum(r.wins for r in rs); n = sum(r.trades for r in rs)
    be = sum(r.breakeven_sum for r in rs)
    pnl = sorted(r.pnl for r in rs)
    q = lambda p: pnl[min(len(pnl) - 1, int(p * len(pnl)))]
    ft = [r.first_trade_tick for r in rs if r.first_trade_tick is not None]
    return {"runs": len(rs), "trades_per_hour_mean": statistics.mean(tr) if tr else 0, "win_rate": wins / n if n else None,
            "breakeven": be / n if n else None, "pnl_mean": statistics.mean(pnl), "pnl_p05": q(0.05), "pnl_median": q(0.5), "pnl_p95": q(0.95),
            "reach_2x_frac": sum(r.target_tick is not None for r in rs) / len(rs),
            "median_time_to_target_ticks": (statistics.median([r.target_tick for r in rs if r.target_tick is not None]) if any(r.target_tick is not None for r in rs) else None),
            "probe_trades_mean": statistics.mean(r.probe_trades for r in rs), "probe_net_mean": statistics.mean(r.probe_net for r in rs),
            "probe_net_worst": min(r.probe_net for r in rs), "evidence_trades_mean": statistics.mean(r.evidence_trades for r in rs),
            "first_trade_tick_median": statistics.median(ft) if ft else None, "first_t2_tick_median": (statistics.median([r.first_t2_tick for r in rs if r.first_t2_tick is not None]) if any(r.first_t2_tick is not None for r in rs) else None),
            "bust_frac": sum(r.busted for r in rs) / len(rs), "end_mean": statistics.mean(r.end for r in rs)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=8); ap.add_argument("--ticks", type=int, default=3600)
    ap.add_argument("--bold-runs", type=int, default=200); ap.add_argument("--sections", default="fair,latency,planted,bold")
    ap.add_argument("--out", default="docs/guesser/results/e3_paper.json")
    a = ap.parse_args()
    import os
    out = json.load(open(a.out)) if os.path.exists(a.out) else {"fair": {}, "planted": {}, "latency": {}, "bold": {}}
    S = a.sections.split(",")
    t0 = time.time()
    # fair streams: margin sweep, latency 0
    for m in ((0.02, 0.05, 0.08) if "fair" in S else ()):
        for pol in ("evidence", "probe", "bold", "money_rules", "flat"):
            rs = [sim.paper_trade(*sim.fair_pcg(70 + s, a.ticks), pol, margin=m, seed=s) for s in range(a.seeds)]
            out["fair"][f"m={m} {pol}"] = agg(rs)
        print("fair m", m, f"{time.time() - t0:.0f}s", flush=True)
    # latency sweep (fair, m=0.05)
    for lat in ((0, 1, 2) if "latency" in S else ()):
        for pol in ("evidence", "probe"):
            rs = [sim.paper_trade(*sim.fair_pcg(90 + s, a.ticks), pol, margin=0.05, latency=lat, seed=s) for s in range(a.seeds)]
            out["latency"][f"lat={lat} {pol}"] = agg(rs)
    # planted structure
    plant = {"lagcopy lag5 p=.2": lambda s: sim.lagcopy(500 + s, a.ticks, 5, 0.2), "bias P(7)=.12": lambda s: sim.bias(500 + s, a.ticks, 0.12),
             "markov2 tilt=.1": lambda s: sim.markov2(500 + s, a.ticks, 0.10)}
    for nm, fn in (plant.items() if "planted" in S else ()):
        for pol in ("evidence", "probe", "money_rules", "flat"):
            rs = [sim.paper_trade(*fn(s), pol, margin=0.05, seed=s) for s in range(max(4, a.seeds // 2))]
            out["planted"][f"{nm} {pol}"] = agg(rs)
        print("planted", nm, f"{time.time() - t0:.0f}s", flush=True)
    # bold play: fair stream, negative EV; success prob of doubling (start 100 -> target 200) vs flat staking and vs theory
    for m in ((0.02, 0.05, 0.08) if "bold" in S else ()):
        res = {}
        for pol in ("bold", "flat"):
            ok = 0
            N = a.bold_runs if pol == "bold" else max(40, a.bold_runs // 5)
            for s in range(N):
                d, q = sim.fair_pcg(900 + s, 3000 if pol == "bold" else 3600)
                r = sim.paper_trade(d, q, pol, margin=m, target=200.0, max_stake=1000.0, stop_at_target=True, seed=s)
                ok += r.target_tick is not None
            res[pol] = {"success": ok / N, "runs": N}
        # theory (continuous bold play at fair-ish odds): P ~ start/target * (1 - O(m)); an upper bound is start/target = 0.5
        res["upper_bound_start_over_target"] = 0.5
        out["bold"][f"m={m}"] = res
        print("bold", m, res, flush=True)
    out["_meta"] = vars(a)
    json.dump(out, open(a.out, "w"), indent=1, default=float)
