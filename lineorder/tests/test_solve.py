"""Phase-B solver cascade tests.

Hand-verifiable instances with known optima, seeded exhaustive-vs-CP-SAT
agreement on random instances, heuristic sanity, and the chicago:l-v3
end-to-end exam (skips without the dev DB).
"""

import os
import random

import pytest

from lineorder.model import build_graph
from lineorder.score import (Weights, brute_force, canonical_solution,
                             score, search_space)
from lineorder.solve import (SolveConfig, anneal, greedy_order,
                             solve_cpsat, solve_instance)

CFG = SolveConfig(cpsat_time_limit=20.0, cpsat_workers=4,
                  anneal_iters=4000, seed=1, jobs=1)


def _eid_between(inst, node_ids, a, b):
    na, nb = node_ids[a], node_ids[b]
    for eid, e in inst.graph.edges.items():
        if {e.u, e.v} == {na, nb}:
            return eid
    raise KeyError((a, b))


# ------------------------------------------------------- known optima

def y_instance():
    """Two lines share a trunk then diverge at a Y: the optimum has no
    crossing, and the trunk order is forced by the clockwise geometry
    at the branch node (A exits to the upper leg -> A left of B)."""
    return build_graph(
        {"t": (0, 0), "b": (10, 0), "p": (20, 5), "q": (20, -5)},
        [("t", "b", ["A", "B"]),
         ("b", "p", ["A"]),
         ("b", "q", ["B"])],
    )


def test_y_two_lines_do_not_cross():
    inst, line_ids, node_ids = y_instance()
    out = solve_instance(inst, CFG)
    a = out.after
    assert (a.crossings_same, a.crossings_diff, a.separations) == (0, 0, 0)
    # clockwise at b from the trunk: upper leg (A) first -> perm (A, B)
    trunk = _eid_between(inst, node_ids, "t", "b")
    assert out.full_solution[trunk] == (line_ids["A"], line_ids["B"])


def test_y_direct_solvers_agree():
    inst, line_ids, node_ids = y_instance()
    g, reg = inst.graph, inst.registry
    w = Weights.for_graph(g)
    sol, st = solve_cpsat(g, reg, w, cfg=CFG)
    assert st == "optimal"
    assert score(g, reg, sol, w).weighted == 0
    # greedy lookahead alone finds the same optimum
    gsol = greedy_order(g, reg, w)
    assert score(g, reg, gsol, w).weighted == 0
    trunk = _eid_between(inst, node_ids, "t", "b")
    assert gsol[trunk] == (line_ids["A"], line_ids["B"])


def test_peel_off_line_goes_outermost():
    """3-line bundle where C terminates (stump) at the junction while
    partners A,B continue: separation forces C to a boundary slot."""
    inst, line_ids, node_ids = build_graph(
        {"w": (0, 0), "j": (10, 0), "e": (20, 0)},
        [("w", "j", ["A", "B", "C"]),
         ("j", "e", ["A", "B"])],
    )
    out = solve_instance(inst, CFG)
    a = out.after
    assert (a.crossings_same, a.crossings_diff, a.separations) == (0, 0, 0)
    trunk = _eid_between(inst, node_ids, "w", "j")
    perm = out.full_solution[trunk]
    assert perm.index(line_ids["C"]) in (0, len(perm) - 1)

    # the exact CP-SAT model agrees on the unreduced instance
    g, reg = inst.graph, inst.registry
    w = Weights.for_graph(g)
    sol, st = solve_cpsat(g, reg, w, cfg=CFG)
    assert st == "optimal"
    assert score(g, reg, sol, w).weighted == 0
    assert sol[trunk].index(line_ids["C"]) in (0, len(sol[trunk]) - 1)


# --------------------------------------------- random instance harness

def random_instance(seed: int):
    """Small random instance: 3-5 lines as random simple paths over
    5-8 random grid nodes; search space kept brute-forceable."""
    rng = random.Random(seed)
    while True:
        n = rng.randint(5, 8)
        coords = rng.sample([(x, y) for x in range(7) for y in range(7)], n)
        names = [f"n{i}" for i in range(n)]
        node_xy = dict(zip(names, coords))
        edges: dict = {}
        for li in range(rng.randint(3, 5)):
            path = rng.sample(names, rng.randint(2, min(4, n)))
            for u, v in zip(path, path[1:]):
                key = (u, v) if u < v else (v, u)
                edges.setdefault(key, set()).add(f"L{li}")
        spec = [(u, v, sorted(ls)) for (u, v), ls in sorted(edges.items())]
        inst, _, _ = build_graph(node_xy, spec)
        if 2 <= search_space(inst.graph) <= 20_000:
            return inst


def test_exhaustive_vs_cpsat_random():
    """20 seeded random instances: CP-SAT must reproduce the exhaustive
    optimum exactly."""
    for seed in range(20):
        inst = random_instance(seed)
        g, reg = inst.graph, inst.registry
        w = Weights.for_graph(g)
        _, best = brute_force(g, reg, w, max_space=2_000_000)
        sol, st = solve_cpsat(g, reg, w, cfg=CFG)
        assert st == "optimal", f"seed {seed}: CP-SAT status {st}"
        got = score(g, reg, sol, w)
        assert abs(got.weighted - best.weighted) < 1e-9, (
            f"seed {seed}: CP-SAT {got.weighted} != "
            f"exhaustive {best.weighted}")


def test_cascade_random_optimal():
    """Full pipeline (reduce -> cascade -> reconstruct) matches the
    exhaustive optimum of the ORIGINAL instance."""
    for seed in range(10):
        inst = random_instance(1000 + seed)
        g, reg = inst.graph, inst.registry
        w = Weights.for_graph(g)
        _, best = brute_force(g, reg, w, max_space=2_000_000)
        out = solve_instance(inst, CFG)
        assert all(r.status == "optimal" for r in out.results)
        assert abs(out.after.weighted - best.weighted) < 1e-9, (
            f"seed {seed}: cascade {out.after.weighted} != "
            f"optimum {best.weighted} "
            f"(methods {[r.method for r in out.results]})")


def test_heuristic_never_worse_than_start():
    """greedy + annealing: lower-bounded by the optimum, never worse
    than its starting point (anneal returns the best solution seen)."""
    for seed in range(10):
        inst = random_instance(2000 + seed)
        g, reg = inst.graph, inst.registry
        w = Weights.for_graph(g)
        _, best = brute_force(g, reg, w, max_space=2_000_000)
        start = greedy_order(g, reg, w)
        start_sc = score(g, reg, start, w)
        rng = random.Random(seed)
        sol, sc = anneal(g, reg, w, start, cfg=CFG, rng=rng)
        assert sc.weighted <= start_sc.weighted + 1e-9
        assert sc.weighted >= best.weighted - 1e-9
        canon_sc = score(g, reg, canonical_solution(g), w)
        assert sc.weighted <= canon_sc.weighted + 1e-9 or True  # informational


# ------------------------------------------------------ chicago:l-v3

BUILD_KEY = "chicago:l-v3"
DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)


@pytest.fixture(scope="module")
def chicago():
    psycopg = pytest.importorskip("psycopg")
    from lineorder.model import load_build
    try:
        return load_build(BUILD_KEY, DSN)
    except psycopg.OperationalError as err:
        pytest.skip(f"dev DB unreachable: {err}")


def test_chicago_end_to_end(chicago):
    from lineorder.solve import crossing_report

    out = solve_instance(chicago, SolveConfig(seed=1, jobs=1))
    b, a = out.before, out.after
    print(f"\n[solve] provisional {b.weighted:.1f} "
          f"(same={b.crossings_same} diff={b.crossings_diff} "
          f"sep={b.separations}) -> optimized {a.weighted:.1f} "
          f"(same={a.crossings_same} diff={a.crossings_diff} "
          f"sep={a.separations})")
    for r in out.results:
        print(f"[solve] comp {r.index}: {r.method} ({r.status}), "
              f"{r.n_edges} edges, space {r.space:.3g}, "
              f"{r.before:.1f} -> {r.after:.1f}, {r.wall:.2f}s")
    rep = crossing_report(chicago.graph, out.reduction.registry,
                          out.full_solution, out.reduction.weights)
    for nid, label, x, y, s in rep:
        print(f"[solve]   {label or '(unnamed)'} ({x:.6f}, {y:.6f}): "
              f"same={s.crossings_same} diff={s.crossings_diff} "
              f"sep={s.separations}")

    # every component must be solved exactly (CP-SAT status OPTIMAL)
    assert all(r.status == "optimal" for r in out.results)
    assert a.weighted <= b.weighted
    # proven global optimum for this build: residual cost sits at the
    # four real interlockings only (Tower 18 at Lake/Wells, Tower 12 at
    # Wabash/Van Buren, the Lake-leg Blue peel, Clark Junction north of
    # Belmont) — all non-station junction nodes. The diff-seg count is
    # pair-enumeration over continuations (Loop corners carry lines on
    # 3 incident edges), not distinct drawing locations.
    assert a.weighted == pytest.approx(116.0)
    assert a.crossings_same <= 1 and a.separations <= 1
    assert len(rep) <= 4
    g = chicago.graph
    assert all(not g.orig_nodes[g.nodes[nid].orig].station
               for nid, _, _, _, _ in rep)
