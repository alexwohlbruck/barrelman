"""Real-data exam: chicago:l-v3 (CTA rail, 145 edges / 223 edge_lines /
8 routes — the DETERMINISTIC committed-source build; see the pin history in
test_load_dimensions) loads from PostGIS, reduces, and round-trips.

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
    # round 19 (cross-family gap 10->22): the wider gap splits the Tower 18
    # multi-family interlocking finer, 154 -> 157 edges / 220 -> 229
    # edge_lines (loop exam holds the Loop bundles + Tower 18 unchanged).
    # round 21 (transitive cross-family bundling): the North Side P/Red now
    # bundles onto the Brown's shared centerline (the sweep's largest Chicago
    # miss, ~2.5 km); those lines share one ribbon.
    # Re-synced to the DETERMINISTIC committed-source build: a fresh
    # `linegraph.build --feed 29 --force` reproducibly emits 155 edges / 230
    # edge_lines (the round-21 pins 167/252 were from a transient build the
    # committed source no longer reproduces — pre-existing drift, independent
    # of the FIX 1 same-family bundle change which is Chicago-byte-identical;
    # the North Side Brn/P/Red bundle is present and all geometry exams PASS).
    # Re-pinned 155 -> 144 edges / 230 -> 220 edge_lines after PAR-12 stop
    # conflation: moving CTA stops onto their OSM platforms (Ashland +29 m,
    # etc.) shifted station-split nodes, merging a few short station segments
    # and turning the Ashland junction into a deg-2 composition change.
    # Re-pinned 144 -> 167 edges / 220 -> 253 edge_lines in round 24
    # (junction-anchored merge start).
    # Re-pinned 167 -> 145 edges / 253 -> 223 edge_lines — PAR-12 CACHE-DIGEST
    # FIX. The round-24 pin of 167/253 was taken against a STALE corridor
    # cache: the old waygraph_digest hashed only the route SHAPES, so a cache
    # built before the round-22/23 conflation + anti-hop re-match was reused
    # (the round-21 "transient" 167 topology, ~22 spurious corridor fragments,
    # never rebuilt clean). The DETERMINISTIC committed-source build — clean
    # cache, waygraph_digest v17 which also hashes route COLOUR + the
    # pattern->route mapping + STOP positions — reproducibly emits 145 edges /
    # 223 edge_lines. Two consecutive clean-cache rebuilds are byte-identical
    # and reproduce these DB rows (linegraph/tests/test_determinism.py). All
    # user fixes survive the clean rebuild: the Clark/Lake Blue join into the
    # 6-line Lake St bundle (loop_exam node 14 deg=3 {Blue,Brn,G,Org,P,Pink}),
    # O'Hare Blue on the OSM platform, anti-hop CTA 0 switches; chicago:l LOOM
    # baseline md5-identical. These are the deterministic committed-source
    # values.
    assert len(g.edges) == 145
    assert sum(len(e.lines) for e in g.edges.values()) == 223
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
    assert sum(len(p) for p in full.values()) == 223  # r19:220->229; r21:229->252; committed-source resync:252->230; PAR-12 conflation:230->220; r24 junction-anchored Blue-Loop bundle:220->253; PAR-12 cache-digest fix (clean deterministic rebuild, v17 digest):253->223

    orig = score(g, red.registry, full, w)
    print(f"[real] original-graph score: {orig}")
    assert abs(orig.weighted
               - (comp_total.weighted + red.fixed_cost)) < 1e-6
