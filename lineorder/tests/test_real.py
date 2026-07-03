"""Real-data exam: chicago:l-v3 (CTA rail, 154 edges / 220 edge_lines /
8 routes — one crossing-bleed claim fewer since the linegraph refit +
generalized bleed suppression) loads from PostGIS, reduces, and
round-trips.

Requires the dev DB (postgresql://barrelman:barrelman@localhost:5434);
skips if unreachable. Run:
  uv run --with-requirements lineorder/requirements.txt \
      python -m pytest lineorder/tests/test_real.py -v -s
"""

import os

import pytest

from lineorder.reconstruct import reconstruct
from lineorder.reduce import reduce_graph
from lineorder.score import Score, Weights, brute_force, score

BUILD_KEY = "chicago:l-v3"
DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)
COMP_BRUTE_CAP = 2_000_000


@pytest.fixture(scope="module")
def inst():
    psycopg = pytest.importorskip("psycopg")
    from lineorder.model import load_build
    try:
        return load_build(BUILD_KEY, DSN)
    except psycopg.OperationalError as err:
        pytest.skip(f"dev DB unreachable: {err}")


def test_load_dimensions(inst):
    g = inst.graph
    assert len(g.edges) == 154
    assert sum(len(e.lines) for e in g.edges.values()) == 220
    assert g.max_cardinality() == 6  # Loop legs
    routes = {(l.feed_id, l.route_id) for l in inst.registry
              if hasattr(l, "feed_id")}
    assert len(routes) == 8
    # topology sanity: clockwise orders cover every edge end twice
    ends = sum(len(v) for v in g.order.values())
    assert ends == 2 * len(g.edges)


def test_reduce_and_roundtrip(inst):
    g = inst.graph
    w = Weights.for_graph(g)
    red = reduce_graph(inst, w)
    comps = red.components()
    print(f"\n[real] rules fired: {dict(red.stats)}")
    print(f"[real] reduced: {len(red.graph.nodes)} nodes, "
          f"{len(red.graph.edges)} edges, max |L| = "
          f"{red.graph.max_cardinality()}, fixed_cost = {red.fixed_cost}")
    nontrivial = [c for c in comps if c.search_space(red.graph) > 1]
    inventory = [(len(c.edges), c.max_cardinality(red.graph),
                  c.search_space(red.graph)) for c in nontrivial]
    print(f"[real] components: {len(comps)} total, "
          f"{len(nontrivial)} nontrivial: {inventory}")

    # expect few components and small cardinalities after reduction
    assert len(nontrivial) <= 12
    assert all(c.max_cardinality(red.graph) <= 6 for c in comps)

    reduced_sol = {}
    comp_total = Score()
    skipped = 0
    for c in comps:
        space = c.search_space(red.graph)
        if space > COMP_BRUTE_CAP:
            # accounting holds for ANY solution; use canonical order
            for eid in c.edges:
                reduced_sol[eid] = red.graph.edges[eid].lines
            comp_total = comp_total + score(
                red.graph, red.registry, reduced_sol, w, nodes=c.nodes)
            skipped += 1
            continue
        sol, sc = brute_force(red.graph, red.registry, w,
                              edges=c.edges, nodes=c.nodes,
                              max_space=COMP_BRUTE_CAP)
        reduced_sol.update(sol)
        comp_total = comp_total + sc
    print(f"[real] brute-forced {len(comps) - skipped}/{len(comps)} "
          f"components (skipped {skipped} oversized)")

    full = reconstruct(red, reduced_sol)
    assert set(full) == set(g.edges)
    assert sum(len(p) for p in full.values()) == 220

    orig = score(g, red.registry, full, w)
    print(f"[real] original-graph score: {orig}")
    assert abs(orig.weighted
               - (comp_total.weighted + red.fixed_cost)) < 1e-6
