"""lineorder.solve — phase-B solver cascade for MLNCM-S.

Per-component cascade mirroring LOOM's CombOptimizer (Bast/Brosi/
Storandt TSAS 2019; Brosi/Bast 2024 planet-scale config):

  max |L(e)| == 1                 -> identity (nothing to order)
  search space < exhaustive cap   -> exhaustive enumeration (optimal)
  else                            -> OR-Tools CP-SAT exact model
  CP-SAT timeout / unavailable    -> greedy-with-lookahead + simulated
                                     annealing polish (T0=1000, Ti=T0/i,
                                     neighborhood = swap one adjacent
                                     pair on one edge), seeded with the
                                     CP-SAT incumbent when there is one

CP-SAT model (mirrors score.py term for term):
  * one integer position var per (edge, line), AddAllDifferent per edge;
  * one reified order boolean per (edge, unordered line pair) —
    "a before b" in the edge's STORAGE frame (left-to-right u -> v);
    arrive/leave frames at a node are the storage literal, negated when
    the traveler direction flips it (reversing a permutation negates
    every pairwise before);
  * same-segment crossing = XOR of the two frame literals;
  * split (diff-segment) crossing has a CONSTANT right-hand side (the
    clockwise rank comparison of the two continuation edges, Lemma 4.4)
    so it is just a literal, no auxiliary variable;
  * separation: adjacency booleans |pos(a)-pos(b)| == 1 per edge (block
    spans of P2 pseudo-lines stay contiguous, so expanded adjacency ==
    permutation adjacency); separation = XOR of the two adjacency
    booleans, plus the both-adjacent-but-flipped double-separation term
    (weight 2) when a pseudo-line is involved (score._sep_count);
  * objective = weighted sum with score.py's node weights evaluated on
    v*, scaled to integers.

CLI (report-only diagnostics; writing slots is lineorder.apply's job):
  uv run --with-requirements lineorder/requirements.txt \
      python -m lineorder.solve --build-key chicago:l-v3
"""

from __future__ import annotations

import itertools
import math
import os
import random
import time
from dataclasses import dataclass, field

from .model import DEFAULT_DSN, Instance
from .reconstruct import reconstruct
from .reduce import Reduction, reduce_graph
from .score import (Score, Weights, brute_force, canonical_solution, score,
                    score_node, search_space)

OBJ_SCALE = 1000  # weight -> integer objective coefficient


@dataclass(frozen=True)
class SolveConfig:
    exhaustive_max_space: int = 500
    cpsat_time_limit: float = 30.0
    cpsat_workers: int = 8
    anneal_iters: int = 30_000
    anneal_t0: float = 1000.0
    seed: int = 0
    jobs: int = 0  # 0 = auto (cpu count), 1 = sequential


@dataclass
class ComponentResult:
    index: int
    method: str      # identity | exhaustive | cpsat | cpsat+anneal | greedy+anneal
    status: str      # optimal | heuristic
    n_edges: int
    space: int
    canonical: float  # score of the canonical (sorted-uid) order, NOT the
                      # provisional DB slots (those don't map through the
                      # reduction); the CLI summary line has provisional
    after: float
    wall: float
    solution: dict = field(default_factory=dict)


# ------------------------------------------------------------- CP-SAT

def _aff(lit):
    """Affine 0/1 expression for a (BoolVar, negated) literal."""
    var, neg = lit
    return 1 - var if neg else var


def _blit(lit):
    var, neg = lit
    return var.Not() if neg else var


def solve_cpsat(g, reg, w: Weights, edges=None, nodes=None,
                cfg: SolveConfig | None = None, hint: dict | None = None):
    """Exact CP-SAT model over the given edge/node subset. Returns
    (solution or None, status) with status in optimal|feasible|unknown.
    The subset must be node-closed: every edge incident to a scored
    node belongs to the subset (true for reduction components)."""
    from ortools.sat.python import cp_model

    cfg = cfg or SolveConfig()
    edges = list(edges if edges is not None else g.edges)
    if nodes is None:
        nodes = set()
        for eid in edges:
            nodes.update((g.edges[eid].u, g.edges[eid].v))
    edge_set = set(edges)

    model = cp_model.CpModel()
    pos: dict = {}
    before: dict = {}
    perm_edges = [eid for eid in edges
                  if len(g.edges[eid].lines) > 1 and not g.edges[eid].dummy]
    for eid in perm_edges:
        lines = g.edges[eid].lines
        k = len(lines)
        for l in lines:
            pos[(eid, l)] = model.NewIntVar(0, k - 1, f"p_{eid}_{l}")
        model.AddAllDifferent([pos[(eid, l)] for l in lines])
        for a, b in itertools.combinations(sorted(lines), 2):
            bv = model.NewBoolVar(f"b_{eid}_{a}_{b}")
            model.Add(pos[(eid, a)] < pos[(eid, b)]).OnlyEnforceIf(bv)
            model.Add(pos[(eid, a)] > pos[(eid, b)]).OnlyEnforceIf(bv.Not())
            before[(eid, a, b)] = bv

    def sbefore(eid, a, b):
        """Literal: 'a before b' in eid's storage frame."""
        return (before[(eid, a, b)], False) if a < b \
            else (before[(eid, b, a)], True)

    def frame(eid, a, b, flip: bool):
        var, neg = sbefore(eid, a, b)
        return (var, neg ^ flip)

    adj: dict = {}

    def adjacent(eid, a, b):
        key = (eid, min(a, b), max(a, b))
        if key not in adj:
            k = len(g.edges[eid].lines)
            d = model.NewIntVar(1, k - 1, f"d_{key}")
            model.AddAbsEquality(d, pos[(eid, key[1])] - pos[(eid, key[2])])
            av = model.NewBoolVar(f"adj_{key}")
            model.Add(d == 1).OnlyEnforceIf(av)
            model.Add(d != 1).OnlyEnforceIf(av.Not())
            adj[key] = av
        return adj[key]

    def xor(la, lb, name):
        """c == (la != lb) for two literals."""
        c = model.NewBoolVar(name)
        model.Add(_aff(la) + _aff(lb) == 1).OnlyEnforceIf(c)
        model.Add(_aff(la) == _aff(lb)).OnlyEnforceIf(c.Not())
        return c

    terms = []  # (int coefficient, literal)
    for nid in nodes:
        inc = g.order.get(nid, [])
        if len(inc) < 2:
            continue
        if any(e not in edge_set for e in inc):
            raise ValueError(f"node {nid} has incident edges outside the "
                             f"solved subset (subset not node-closed)")
        ws = round(w.w_same(g, nid) * OBJ_SCALE)
        wd = round(w.w_diff(g, nid) * OBJ_SCALE)
        wp = round(w.w_sep(g, nid) * OBJ_SCALE)
        arrive_flip = {e: g.edges[e].v != nid for e in inc}
        cw_rank = {e: {f: k for k, f in enumerate(g.clockwise_from(nid, e))}
                   for e in inc}

        for e, f in itertools.combinations(inc, 2):
            ee, ef = g.edges[e], g.edges[f]
            if ee.dummy or ef.dummy:
                continue
            shared = sorted(set(ee.lines) & set(ef.lines))
            for a, b in itertools.combinations(shared, 2):
                la = frame(e, a, b, arrive_flip[e])
                lb = frame(f, a, b, not arrive_flip[f])  # leave frame
                c = xor(la, lb, f"x_{nid}_{e}_{f}_{a}_{b}")
                m = reg.mult(a) * reg.mult(b)
                terms.append((ws * m, (c, False)))
                ae, af = adjacent(e, a, b), adjacent(f, a, b)
                s1 = xor((ae, False), (af, False),
                         f"s_{nid}_{e}_{f}_{a}_{b}")
                terms.append((wp, (s1, False)))
                if reg.mult(a) > 1 or reg.mult(b) > 1:
                    s2 = model.NewBoolVar(f"s2_{nid}_{e}_{f}_{a}_{b}")
                    model.AddBoolAnd([ae, af, c]).OnlyEnforceIf(s2)
                    model.AddBoolOr([ae.Not(), af.Not(), c.Not()]) \
                        .OnlyEnforceIf(s2.Not())
                    terms.append((2 * wp, (s2, False)))

        for e in inc:
            ee = g.edges[e]
            if ee.dummy or len(ee.lines) < 2:
                continue
            cont = {uid: [f for f in inc
                          if f != e and not g.edges[f].dummy
                          and uid in g.edges[f].lines]
                    for uid in ee.lines}
            for a, b in itertools.combinations(sorted(ee.lines), 2):
                m = reg.mult(a) * reg.mult(b)
                for fa in cont[a]:
                    for fb in cont[b]:
                        if fa == fb:
                            continue
                        rhs = cw_rank[e][fa] < cw_rank[e][fb]
                        var, neg = frame(e, a, b, arrive_flip[e])
                        # crossing iff before_arrive != rhs
                        terms.append((wd * m, (var, neg ^ rhs)))

    model.Minimize(sum(coef * _aff(lit) for coef, lit in terms))

    if hint:
        for eid in perm_edges:
            if eid in hint:
                for i, l in enumerate(hint[eid]):
                    model.AddHint(pos[(eid, l)], i)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = cfg.cpsat_time_limit
    solver.parameters.num_workers = cfg.cpsat_workers
    solver.parameters.random_seed = cfg.seed
    # deterministic parallel search: free-for-all worker portfolios return
    # whichever optimum lands first (same score, different slots run to
    # run) — interleaving makes the search reproducible so apply/exam can
    # assert identical slots across reruns
    solver.parameters.interleave_search = True
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return None, "unknown"

    sol = {}
    for eid in edges:
        e = g.edges[eid]
        if eid in perm_edges:
            sol[eid] = tuple(sorted(
                e.lines, key=lambda l: solver.Value(pos[(eid, l)])))
        else:
            sol[eid] = tuple(e.lines)
    if __debug__:
        sc = score(g, reg, sol, w, nodes)
        obj = solver.ObjectiveValue() / OBJ_SCALE
        assert abs(sc.weighted - obj) < 1e-6, (
            f"CP-SAT objective {obj} != score.py {sc.weighted}")
    return sol, ("optimal" if status == cp_model.OPTIMAL else "feasible")


# ---------------------------------------------- greedy with lookahead

def _pair_pref(g, w: Weights, eid: int, a: int, b: int):
    """Preferred 'a before b' (in eid's storage frame) for one line pair:
    chase the pair's common path from both ends of eid to the first
    ordering-inducing node (where the two lines diverge, Lemma 4.4).
    If the two ends disagree, the pair must cross once somewhere — keep
    the pricier end's preference so the crossing lands at the cheaper
    endpoint (greedy-with-lookahead, Brosi/Bast 2024). Returns
    (weight, before) or None when no end induces an order."""
    prefs = []
    for forward in (True, False):
        cur = eid
        n = g.edges[cur].v if forward else g.edges[cur].u
        for _ in range(len(g.edges) + 1):
            fa = [x for x in g.line_edges_at(n, a) if x != cur]
            fb = [x for x in g.line_edges_at(n, b) if x != cur]
            if len(fa) != 1 or len(fb) != 1:
                break  # a line terminates or branches: no preference
            fa, fb = fa[0], fb[0]
            if fa != fb:
                rank = {f: k for k, f in
                        enumerate(g.clockwise_from(n, cur))}
                t = rank[fa] < rank[fb]  # desired order, traveler frame
                # the traveler frame is continuous along the common
                # path; eid's storage frame equals it iff `forward`
                # (the chase started traveling u -> v)
                prefs.append((w.w_diff(g, n), t if forward else not t))
                break
            cur = fa
            leaving_u = g.edges[cur].u == n
            n = g.edges[cur].v if leaving_u else g.edges[cur].u
    if not prefs:
        return None
    if len(prefs) == 1 or prefs[0][1] == prefs[1][1]:
        return max(prefs)
    return max(prefs, key=lambda p: p[0])  # pricier end wins


def greedy_order(g, reg, w: Weights, edges=None) -> dict:
    """Initial solution: per edge, weighted insertion honoring pairwise
    lookahead preferences."""
    edges = list(edges if edges is not None else g.edges)
    sol = {}
    for eid in edges:
        e = g.edges[eid]
        lines = sorted(e.lines)
        if len(lines) <= 1 or e.dummy:
            sol[eid] = tuple(e.lines)
            continue
        pref = {}
        for a, b in itertools.combinations(lines, 2):
            p = _pair_pref(g, w, eid, a, b)
            if p is not None:
                pref[(a, b)] = p
        order: list[int] = []
        for l in lines:
            best_i, best_cost = 0, None
            for i in range(len(order) + 1):
                cost = 0.0
                for j, o in enumerate(order):
                    a, b = (l, o) if l < o else (o, l)
                    if (a, b) not in pref:
                        continue
                    wgt, ab = pref[(a, b)]
                    l_before_o = i <= j
                    a_before_b = l_before_o if a == l else not l_before_o
                    if a_before_b != ab:
                        cost += wgt
                if best_cost is None or cost < best_cost:
                    best_i, best_cost = i, cost
            order.insert(best_i, l)
        sol[eid] = tuple(order)
    return sol


# --------------------------------------------------------- annealing

def anneal(g, reg, w: Weights, start: dict, edges=None, nodes=None,
           cfg: SolveConfig | None = None, rng: random.Random | None = None):
    """Simulated-annealing polish (Brosi/Bast 2024 config: T0 = 1000,
    Ti = T0/i, neighborhood = swap one adjacent pair on one edge).
    Returns (best solution seen, its Score) — never worse than start."""
    cfg = cfg or SolveConfig()
    rng = rng or random.Random(cfg.seed)
    edges = list(edges if edges is not None else g.edges)
    if nodes is None:
        nodes = set()
        for eid in edges:
            nodes.update((g.edges[eid].u, g.edges[eid].v))
    cur = {eid: tuple(start[eid]) for eid in edges}
    multi = [eid for eid in edges
             if len(g.edges[eid].lines) > 1 and not g.edges[eid].dummy]
    if not multi:
        return dict(cur), score(g, reg, cur, w, nodes)

    def local(nids):
        return sum(score_node(g, reg, cur, n, w).weighted for n in nids)

    total = score(g, reg, cur, w, nodes).weighted
    best, best_total = dict(cur), total
    for i in range(1, cfg.anneal_iters + 1):
        t = cfg.anneal_t0 / i
        eid = multi[rng.randrange(len(multi))]
        perm = list(cur[eid])
        j = rng.randrange(len(perm) - 1)
        e = g.edges[eid]
        ends = (e.u, e.v)
        before_w = local(ends)
        perm[j], perm[j + 1] = perm[j + 1], perm[j]
        old = cur[eid]
        cur[eid] = tuple(perm)
        delta = local(ends) - before_w
        if delta <= 0 or (delta / t < 50
                          and rng.random() < math.exp(-delta / t)):
            total += delta
            if total < best_total - 1e-9:
                best, best_total = dict(cur), total
        else:
            cur[eid] = old
    return best, score(g, reg, best, w, nodes)


# ----------------------------------------------------------- cascade

def solve_component(g, reg, w: Weights, comp_nodes, comp_edges,
                    cfg: SolveConfig, index: int = 0) -> ComponentResult:
    t0 = time.perf_counter()
    comp_edges = sorted(comp_edges)
    space = search_space(g, comp_edges)
    canonical = score(g, reg, canonical_solution(g, comp_edges), w,
                      comp_nodes).weighted
    maxcard = max((len(g.edges[e].lines) for e in comp_edges), default=0)

    if maxcard <= 1:
        sol, method, status = canonical_solution(g, comp_edges), \
            "identity", "optimal"
    elif space < cfg.exhaustive_max_space:
        sol, _ = brute_force(g, reg, w, edges=comp_edges, nodes=comp_nodes,
                             max_space=cfg.exhaustive_max_space)
        method, status = "exhaustive", "optimal"
    else:
        hint = greedy_order(g, reg, w, comp_edges)
        incumbent, st = None, "unavailable"
        try:
            incumbent, st = solve_cpsat(g, reg, w, comp_edges, comp_nodes,
                                        cfg, hint=hint)
        except ImportError:
            pass
        if st == "optimal":
            sol, method, status = incumbent, "cpsat", "optimal"
        else:
            seed_sol = incumbent if incumbent is not None else hint
            rng = random.Random(cfg.seed * 7919 + index)
            sol, _ = anneal(g, reg, w, seed_sol, comp_edges, comp_nodes,
                            cfg, rng)
            method = "cpsat+anneal" if incumbent is not None \
                else "greedy+anneal"
            status = "heuristic"

    after = score(g, reg, sol, w, comp_nodes).weighted
    return ComponentResult(index=index, method=method, status=status,
                           n_edges=len(comp_edges), space=space,
                           canonical=canonical, after=after,
                           wall=time.perf_counter() - t0, solution=sol)


def _worker(args):
    g, reg, w, nodes, edges, cfg, idx = args
    return solve_component(g, reg, w, nodes, edges, cfg, idx)


def solve_reduction(red: Reduction, cfg: SolveConfig | None = None):
    """Solve every component of a reduction. Returns
    (reduced solution over all edges, [ComponentResult])."""
    cfg = cfg or SolveConfig()
    g, reg, w = red.graph, red.registry, red.weights
    comps = red.components()
    results: list[ComponentResult] = []
    reduced_sol: dict = {}

    pending = []
    for i, c in enumerate(comps):
        if c.search_space(g) <= 1:
            sol = canonical_solution(g, sorted(c.edges))
            reduced_sol.update(sol)
            results.append(ComponentResult(
                index=i, method="identity", status="optimal",
                n_edges=len(c.edges), space=1,
                canonical=score(g, reg, sol, w, c.nodes).weighted,
                after=score(g, reg, sol, w, c.nodes).weighted,
                wall=0.0, solution=sol))
        else:
            pending.append((i, c))

    jobs = cfg.jobs if cfg.jobs > 0 else (os.cpu_count() or 1)
    if len(pending) > 1 and jobs > 1:
        from concurrent.futures import ProcessPoolExecutor
        args = [(g, reg, w, c.nodes, c.edges, cfg, i) for i, c in pending]
        with ProcessPoolExecutor(max_workers=min(jobs, len(pending))) as ex:
            for res in ex.map(_worker, args):
                results.append(res)
                reduced_sol.update(res.solution)
    else:
        for i, c in pending:
            res = solve_component(g, reg, w, c.nodes, c.edges, cfg, i)
            results.append(res)
            reduced_sol.update(res.solution)

    results.sort(key=lambda r: r.index)
    return reduced_sol, results


@dataclass
class SolveOutcome:
    instance: Instance
    reduction: Reduction
    results: list
    full_solution: dict          # original-graph edge -> line permutation
    before: Score                # provisional slots on the original graph
    after: Score


def solve_instance(inst: Instance, cfg: SolveConfig | None = None,
                   weights: Weights | None = None) -> SolveOutcome:
    cfg = cfg or SolveConfig()
    g = inst.graph
    w = weights or Weights.for_graph(g)
    red = reduce_graph(inst, w)
    reduced_sol, results = solve_reduction(red, cfg)
    full = reconstruct(red, reduced_sol)

    provisional = {eid: inst.provisional.get(eid, g.edges[eid].lines)
                   for eid in g.edges}
    before = score(g, red.registry, provisional, w)
    after = score(g, red.registry, full, w)
    comp_total = sum(r.after for r in results)
    assert abs(after.weighted - (comp_total + red.fixed_cost)) < 1e-6, (
        f"accounting broken: original {after.weighted} != components "
        f"{comp_total} + fixed {red.fixed_cost}")
    return SolveOutcome(inst, red, results, full, before, after)


# ----------------------------------------------------------- reporting

def crossing_report(g, reg, sol: dict, w: Weights):
    """Nodes of the (original) graph with nonzero cost under sol:
    [(nid, label, x, y, Score)] sorted by weighted cost descending."""
    out = []
    for nid in g.nodes:
        s = score_node(g, reg, sol, nid, w)
        if s.weighted > 0:
            o = g.orig_nodes[g.nodes[nid].orig]
            n = g.nodes[nid]
            out.append((nid, o.label, n.x, n.y, s))
    return sorted(out, key=lambda r: -r[4].weighted)


def _fmt_space(n: int) -> str:
    return str(n) if n < 10_000_000 else f"{float(n):.3g}"


def print_outcome(out: SolveOutcome, tag: str = "solve"):
    """Human report for a SolveOutcome (shared by solve + apply CLIs)."""
    red = out.reduction
    print(f"[{tag}] reduced to {len(red.graph.edges)} edges in "
          f"{len(out.results)} components "
          f"(rules {dict(red.stats)}, fixed cost {red.fixed_cost})")
    print(f"\n{'comp':>4} {'method':<14} {'edges':>5} {'space':>10} "
          f"{'canonical':>9} {'after':>9} {'time':>8}")
    for r in out.results:
        print(f"{r.index:>4} {r.method:<14} {r.n_edges:>5} "
              f"{_fmt_space(r.space):>10} {r.canonical:>9.1f} "
              f"{r.after:>9.1f} {r.wall:>7.2f}s")

    print("('optimal' = optimal over the corridor-stable subspace; "
          "see lineorder.reduce)")

    b, a = out.before, out.after
    print(f"\nscore (original graph, weighted): provisional "
          f"{b.weighted:.1f} -> optimized {a.weighted:.1f}")
    print(f"crossings same-seg {b.crossings_same} -> {a.crossings_same}, "
          f"diff-seg {b.crossings_diff} -> {a.crossings_diff}, "
          f"separations {b.separations} -> {a.separations}")

    rep = crossing_report(out.instance.graph, red.registry,
                          out.full_solution, red.weights)
    if rep:
        print("\nresidual cost locations:")
        for nid, label, x, y, s in rep:
            print(f"  {label or '(unnamed)'} ({x:.6f}, {y:.6f}): "
                  f"same={s.crossings_same} diff={s.crossings_diff} "
                  f"sep={s.separations} weighted={s.weighted:.1f}")
    else:
        print("\nno residual crossings or separations")


# ------------------------------------------------------------------ CLI

def main(argv=None):
    import argparse

    from .model import load_build

    ap = argparse.ArgumentParser(
        prog="python -m lineorder.solve",
        description="Order the lines of a transit_graph build (MLNCM-S) "
                    "and report — never writes; see python -m lineorder.apply.")
    ap.add_argument("--build-key", required=True)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--time-limit", type=float, default=30.0,
                    help="CP-SAT seconds per component (default 30)")
    ap.add_argument("--anneal-iters", type=int, default=30_000)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--jobs", type=int, default=0,
                    help="parallel component workers (0 = cpu count)")
    args = ap.parse_args(argv)

    cfg = SolveConfig(cpsat_time_limit=args.time_limit,
                      anneal_iters=args.anneal_iters,
                      seed=args.seed, jobs=args.jobs)

    t0 = time.perf_counter()
    inst = load_build(args.build_key, args.dsn)
    g = inst.graph
    print(f"[solve] {args.build_key}: {len(g.nodes)} nodes, "
          f"{len(g.edges)} edges, search space = "
          f"{_fmt_space(search_space(g))}")

    out = solve_instance(inst, cfg)
    print_outcome(out, tag="solve")
    print(f"\n[solve] report only — write slots with "
          f"python -m lineorder.apply --build-key {args.build_key} "
          f"({time.perf_counter() - t0:.2f}s total)")


if __name__ == "__main__":
    main()
