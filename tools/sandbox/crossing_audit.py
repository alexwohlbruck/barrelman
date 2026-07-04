"""tools.sandbox.crossing_audit — avoidable merge/split double-flip sweep.

Companion to bundle_audit.py, on the ORDERING side. bundle_audit finds
corridors that SHOULD share a ribbon but don't; this finds ribbons that DO
share a corridor but cross the same line-pair TWICE — the signature of an
avoidable double-flip:

  two lines A, B MERGE onto a shared corridor at node M (crossing there),
  run BUNDLED together through the corridor, then SPLIT at node S (crossing
  AGAIN) — where reordering A, B's relative slot ALONG the shared corridor
  would remove BOTH crossings at no net cost. If the merge node's order and
  the split node's order were coordinated (the split's required exit order
  propagated back through the corridor to the merge), the pair would join
  without crossing and split without crossing: 0 achievable, 2 realized.

This is the Eastern Parkway 2/3 x 4/5 pattern the user flagged. It is NOT
every pair that crosses twice: a pair can legitimately cross at two nodes
when the two crossings are independent (different corridors) or when
removing one forces the other (a genuine objective tradeoff — flipping the
shared block does not reduce the global score). We flag ONLY the strictly
avoidable ones: a contiguous shared corridor bounded by two crossing nodes
of the SAME pair, where flipping the pair's order along that whole corridor
strictly lowers the MLNCM-S objective (i.e. removes crossings the current
solution left in). That is exactly a double-flip the optimizer should have
caught.

The audit reads the CURRENT stored slots (the emitted `transit_graph_*`
build, via lineorder.model.load_build + the solved slots) and reports, per
city: the avoidable-double-flip COUNT and the crossings they carry, with
locations. Run before/after an optimizer change: the count must drop (a
genuine avoidable double-flip like Eastern Parkway 2->0), and the network
total must not rise.

  uv run --with-requirements tools/sandbox/requirements.txt \
      python -m tools.sandbox.crossing_audit
  uv run --with-requirements tools/sandbox/requirements.txt \
      python -m tools.sandbox.crossing_audit --build-key nyc:subway-v3 --json
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

DEFAULT_DSN = "postgresql://barrelman:barrelman@localhost:5434/barrelman"
BUILD_KEYS = ("nyc:subway-v3", "chicago:l-v3")


# ── crossing detection (mirrors lineorder.score term-for-term) ────────────


def _pair_crossings_at(g, reg, sol, nid):
    """Set of frozenset({a, b}) line-uid pairs that incur a same-segment or
    split (diff-segment) crossing at node nid under solution sol — exactly
    the pairs score_node counts a crossing for."""
    from lineorder.model import ord_arrive, ord_leave
    from lineorder.score import _before, _positions

    inc = g.order.get(nid, [])
    out = set()
    if len(inc) < 2:
        return out
    arrive = {e: _positions(reg, ord_arrive(g, sol, e, nid)) for e in inc}
    leave = {e: _positions(reg, ord_leave(g, sol, e, nid)) for e in inc}
    cw = {e: {f: k for k, f in enumerate(g.clockwise_from(nid, e))}
          for e in inc}
    # same-segment: A,B share two incident edges and flip between them
    for e, f in itertools.combinations(inc, 2):
        if g.edges[e].dummy or g.edges[f].dummy:
            continue
        shared = set(g.edges[e].lines) & set(g.edges[f].lines)
        for a, b in itertools.combinations(sorted(shared), 2):
            if _before(arrive[e], a, b) != _before(leave[f], a, b):
                out.add(frozenset((a, b)))
    # split: A,B share e, continue into different edges fA != fB, and their
    # order in e disagrees with the clockwise rank of fA, fB
    for e in inc:
        ee = g.edges[e]
        if ee.dummy:
            continue
        cont = {u: [f for f in inc if f != e and not g.edges[f].dummy
                    and u in g.edges[f].lines] for u in ee.lines}
        for a, b in itertools.combinations(sorted(ee.lines), 2):
            for fa in cont[a]:
                for fb in cont[b]:
                    if fa == fb:
                        continue
                    if _before(arrive[e], a, b) != (cw[e][fa] < cw[e][fb]):
                        out.add(frozenset((a, b)))
    return out


# ── the sweep ─────────────────────────────────────────────────────────────


def sweep(build_key, dsn=DEFAULT_DSN):
    """Solve the build and find every strictly-avoidable merge/split
    double-flip. Returns a card dict."""
    from lineorder.model import load_build
    from lineorder.score import score
    from lineorder.solve import SolveConfig, solve_instance

    inst = load_build(build_key, dsn)
    g, reg = inst.graph, inst.registry

    def rname(u):
        ln = reg.get(u)
        return getattr(ln, "short_name", None) or getattr(ln, "route_id",
                                                           str(u))

    out = solve_instance(inst, SolveConfig(seed=0, jobs=1))
    sol = dict(out.full_solution)
    w = out.reduction.weights
    base = score(g, reg, sol, w).weighted

    # crossing nodes per pair
    pair_nodes = defaultdict(set)
    for nid in g.nodes:
        for pr in _pair_crossings_at(g, reg, sol, nid):
            pair_nodes[pr].add(nid)

    # shared-corridor walk: for a pair crossing at >= 2 nodes, follow the
    # subgraph of edges carrying BOTH lines; a contiguous corridor whose
    # interior nodes carry exactly the two shared edges (the pair runs
    # bundled, no branch of A or B) and whose two ENDS both cross for the
    # pair is a double-flip candidate. Flipping the pair's order along the
    # whole corridor and re-scoring decides if it is avoidable.
    avoidable = []
    checked = set()
    for pr, xnodes in pair_nodes.items():
        if len(xnodes) < 2:
            continue
        a, b = tuple(pr)
        she = {eid for eid, e in g.edges.items()
               if a in e.lines and b in e.lines}
        sinc = defaultdict(list)
        for eid in she:
            e = g.edges[eid]
            sinc[e.u].append(eid)
            sinc[e.v].append(eid)
        for M in xnodes:
            for e0 in sinc[M]:
                path = [e0]
                cur = M
                nxt = g.other(e0, cur)
                ok = True
                # walk the bundled shared corridor: interior nodes carry
                # exactly the two shared edges (pair stays together)
                while len(sinc[nxt]) == 2 and nxt not in xnodes:
                    nexts = [x for x in sinc[nxt] if x != path[-1]]
                    if not nexts:
                        ok = False
                        break
                    path.append(nexts[0])
                    cur = nxt
                    nxt = g.other(nexts[0], cur)
                S = nxt
                if not ok or S == M or S not in xnodes:
                    continue
                key = frozenset((pr, M, S))
                if key in checked:
                    continue
                checked.add(key)
                # flip the pair's relative order along the whole corridor
                trial = dict(sol)
                for eid in path:
                    perm = list(trial[eid])
                    ia, ib = perm.index(a), perm.index(b)
                    perm[ia], perm[ib] = perm[ib], perm[ia]
                    trial[eid] = tuple(perm)
                new = score(g, reg, trial, w).weighted
                if new < base - 1e-9:
                    n1, n2 = g.nodes[M], g.nodes[S]
                    avoidable.append({
                        "line_a": rname(a), "line_b": rname(b),
                        "merge_at": [round(n1.x, 5), round(n1.y, 5)],
                        "split_at": [round(n2.x, 5), round(n2.y, 5)],
                        "corridor_edges": len(path),
                        "score_drop": round(base - new, 1),
                    })
    avoidable.sort(key=lambda r: -r["score_drop"])
    return {"build_key": build_key, "nodes": len(g.nodes),
            "edges": len(g.edges), "objective": base,
            "avoidable": avoidable, "avoidable_count": len(avoidable),
            "avoidable_score": round(sum(r["score_drop"] for r in avoidable),
                                     1)}


def render(card):
    L = []
    bk = card["build_key"]
    L.append("=" * 68)
    L.append(f" CROSSING AUDIT — {bk}   ({card['edges']} edges, "
             f"objective {card['objective']:.0f})")
    L.append("=" * 68)
    L.append(f" AVOIDABLE merge/split double-flips: {card['avoidable_count']}")
    L.append(f"   recoverable objective if reordered: "
             f"{card['avoidable_score']}")
    L.append("-" * 68)
    if card["avoidable"]:
        L.append(" offenders (pair crosses at merge AND split, flip fixes "
                 "both):")
        for r in card["avoidable"][:16]:
            L.append(f"   {r['line_a']:>3} x {r['line_b']:<3}  "
                     f"merge {r['merge_at']} split {r['split_at']}  "
                     f"corridor {r['corridor_edges']} edges  "
                     f"-{r['score_drop']}")
    else:
        L.append(" (none — every twice-crossed pair is independent or a "
                 "genuine objective tradeoff, not an avoidable flip)")
    L.append("=" * 68)
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="Avoidable merge/split double-flip sweep")
    ap.add_argument("--build-key", action="append",
                    help="default: nyc:subway-v3 and chicago:l-v3")
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    keys = args.build_key or list(BUILD_KEYS)
    cards = [sweep(bk, args.dsn) for bk in keys]
    if args.json:
        print(json.dumps(cards, indent=2, default=float))
    else:
        for c in cards:
            print(render(c))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
