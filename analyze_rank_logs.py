"""analyze_rank_logs.py - how often do mass and edge ranking disagree, and is the board thinning?

Usage:  python analyze_rank_logs.py render.log [more.log ...]      (or pipe the log on stdin)
Reads the `RANK SHADOW window ...` lines (one per pick round) and the `WINDOW RANK ...: no admissible candidate` lines.
"""
import collections, re, sys

SHADOW = re.compile(r"RANK SHADOW window (\S+) pool=(\d+) key=(\w+) mass_top=(\S+) edge_top=(\S+) agree=(\d)"
                    r".*?(?:trigger=(\w+))?\s*$")
NONE = re.compile(r"WINDOW RANK (\S+): no admissible candidate \| pool snapshot=(\d+) bar=(\d+) \| rejected: (.*)")
BOUGHT = re.compile(r"WINDOW RANK (\S+): bought (\d+) \| pool snapshot=(\d+) bar=(\d+)")


def main(paths):
    lines = []
    for p in paths:
        with open(p, errors="replace") as f:
            lines += f.read().splitlines()
    if not paths:
        lines = sys.stdin.read().splitlines()
    rounds, agree, trig, pools = 0, 0, collections.Counter(), collections.Counter()
    by_trig = collections.defaultdict(lambda: [0, 0])
    none_n, none_rej = 0, collections.Counter()
    bought_n = 0
    for ln in lines:
        m = SHADOW.search(ln)
        if m:
            _w, pool, _k, _mt, _et, ag, tg = m.groups()
            rounds += 1
            agree += int(ag)
            tg = tg or "-"
            trig[tg] += 1
            by_trig[tg][0] += 1
            by_trig[tg][1] += int(ag)
            pools[min(int(pool), 10)] += 1
            continue
        m = NONE.search(ln)
        if m:
            none_n += 1
            for kv in m.group(4).split(","):
                if "=" in kv:
                    k, v = kv.strip().rsplit("=", 1)
                    none_rej[k] += int(v) if v.isdigit() else 0
            continue
        if BOUGHT.search(ln):
            bought_n += 1
    if not rounds:
        print("no RANK SHADOW lines found")
        return
    print(f"pick rounds: {rounds}")
    print(f"mass-top == edge-top: {agree}/{rounds} = {agree / rounds:.1%}   (disagree {rounds - agree} = {(rounds - agree) / rounds:.1%})")
    for k, (n, a) in sorted(by_trig.items()):
        print(f"  trigger={k:9s} rounds {n:5d}  agree {a / n:.1%}")
    print("pool size at pick (10 = 10+):", dict(sorted(pools.items())))
    early = trig.get("early", 0)
    print(f"early-trigger share: {early}/{rounds} = {early / rounds:.1%}   (low share = pool rarely reaches MH_EARLY_POOL_MIN)")
    tot_win = bought_n + none_n
    if tot_win:
        print(f"windows with a scan: {tot_win}, no admissible candidate: {none_n} = {none_n / tot_win:.1%}")
    if none_rej:
        print("rejections in empty windows:", dict(none_rej.most_common()))


if __name__ == "__main__":
    main(sys.argv[1:])
