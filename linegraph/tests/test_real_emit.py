"""Real-data phase B exam: CTA feed 29 rail, attributed + emitted to the DB.

Runs the FULL phase B path via build.enrich_graph — the same path the
CLI --emit takes: shape-evidence geometry refit (linegraph.refit),
station snapping (edge splitting on the refit centerline), route
attribution, line-less pruning — then the transit_graph_* emit under a
THROWAWAY build_key 'chicago:l-v3-test' (deleted on teardown so the suite
never touches the live chicago:l-v3) in the dev DB, and checks:

  - every pattern with a shape attributes with <2% unmatched samples,
  - Loop-window edges carry plausible multi-route sets (the elevated
    legs bundle >=4 routes; the Dearborn/State subways stay single-route
    {Blue} / {Red} sets — the Tower 18 non-merge property, now visible
    in the attribution), with the distinct sets printed,
  - >=90% of CTA rail station complexes label a node, including
    Clark/Lake and Washington/Wabash,
  - the transit_graph_builds ledger row for chicago:l-v3 is written and
    the DB rows round-trip the in-memory graph,
  - the LOOM baseline build 'chicago:l' is byte-count untouched
    (counts captured before the emit, asserted after).

Requires the dev DB (postgresql://barrelman:barrelman@localhost:5434);
skips if unreachable. Run:
  uv run --with-requirements linegraph/requirements.txt \
      python -m pytest linegraph/tests/test_real_emit.py -v -s
"""

import os
import sys
import time
from collections import Counter

import pytest
from shapesnap.match import load_patterns

from linegraph.build import (REPO_ROOT, build_linegraph, dedup_shapes,
                             enrich_graph)
from linegraph.emit import emit_build
from linegraph.stations import load_station_complexes

FEED_PROCESSED = REPO_ROOT / "data" / "gtfs-processed" / "29.zip"
FEED_RAW = REPO_ROOT / "data" / "gtfs" / "29.zip"

MERGE_WIDTH = 18.0
RES = 2.0
# HERMETIC (task 16): this exam runs the LEGACY raster phase-B path
# (build_linegraph + enrich_graph), which is NOT how the live chicago:l-v3
# is built (that is the way-graph corridor engine). Emitting under the live
# key clobbered the authoritative build every time pytest ran. Emit under a
# throwaway key instead and delete-and-replace it on teardown, so running
# the suite never corrupts the live chicago:l-v3 (or the LOOM chicago:l).
BUILD_KEY = "chicago:l-v3-test"
BASELINE_KEY = "chicago:l"
DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)

# elevated Loop + interior subways (Wells / Lake / Wabash / Van Buren)
LOOP_WINDOW = (-87.6355, 41.8755, -87.6245, 41.8875)
# strictly INSIDE the legs: only the Dearborn (Blue) and State (Red)
# subways run here N-S. NOTE the Lake St leg is excluded on purpose —
# the Dearborn subway genuinely runs UNDER the Lake elevated west of
# Dearborn (Clark/Lake), so those edges bundle Blue WITH the elevated
# routes by design (plan-view parallel within MERGE_WIDTH fuses).
LOOP_INTERIOR = (-87.6330, 41.8772, -87.6266, 41.8852)


def _connect():
    psycopg = pytest.importorskip("psycopg")
    try:
        return psycopg.connect(DSN, connect_timeout=5)
    except Exception as err:  # pragma: no cover
        pytest.skip(f"dev DB unreachable: {err}")


def _graph_counts(cur, build_key):
    cur.execute(
        """SELECT
             (SELECT count(*) FROM transit_graph_nodes WHERE build_key = %(k)s),
             (SELECT count(*) FROM transit_graph_nodes
               WHERE build_key = %(k)s AND station_label IS NOT NULL),
             (SELECT count(*) FROM transit_graph_edges WHERE build_key = %(k)s),
             (SELECT count(*) FROM transit_graph_edge_lines el
               JOIN transit_graph_edges e ON e.id = el.edge_id
              WHERE e.build_key = %(k)s),
             (SELECT count(*) FROM transit_graph_builds WHERE build_key = %(k)s)
        """,
        {"k": build_key},
    )
    nodes, labeled, edges, edge_lines, builds = cur.fetchone()
    return {"nodes": nodes, "labeled": labeled, "edges": edges,
            "edge_lines": edge_lines, "builds": builds}


@pytest.fixture(scope="module")
def emitted():
    if FEED_PROCESSED.exists():
        zip_path = FEED_PROCESSED
    elif FEED_RAW.exists():
        print(f"\nWARNING: using RAW {FEED_RAW} (not OSM-matched)", file=sys.stderr)
        zip_path = FEED_RAW
    else:
        pytest.skip("no feed 29 zip (processed or raw)")

    conn = _connect()
    with conn.cursor() as cur:
        baseline_before = _graph_counts(cur, BASELINE_KEY)

    t0 = time.perf_counter()
    patterns = load_patterns(zip_path, modes={"rail"})
    shapes, n_skipped = dedup_shapes(patterns)
    lg = build_linegraph(
        shapes, MERGE_WIDTH, RES,
        build_key=BUILD_KEY, feed_id="29", mode="rail", verbose=False,
    )

    # the same phase-B path the CLI --emit takes (refit ON by default)
    stop_ids = {sid for p in patterns for sid in p.stop_ids}
    complexes = load_station_complexes(zip_path, stop_ids)
    lg, snap, edge_routes, stats = enrich_graph(
        lg, patterns, zip_path, "29", verbose=False
    )
    counts = emit_build(
        lg, edge_routes, snap.labels, build_key=BUILD_KEY,
        feed_id="29", mode="rail", dsn=DSN,
    )
    print(
        f"\n[emit] {len(patterns)} patterns ({n_skipped} shapeless) -> "
        f"{len(lg.nodes)} nodes / {len(lg.edges)} edges after station split, "
        f"emitted {counts} in {time.perf_counter() - t0:.1f}s total"
    )
    yield {
        "lg": lg, "snap": snap, "complexes": complexes, "stats": stats,
        "edge_routes": edge_routes, "counts": counts, "conn": conn,
        "baseline_before": baseline_before,
    }
    # teardown: drop the throwaway build so the suite leaves no trace in the
    # DB (edge_lines cascade off transit_graph_edges).
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transit_graph_nodes WHERE build_key = %s",
                        (BUILD_KEY,))
            cur.execute("DELETE FROM transit_graph_edges WHERE build_key = %s",
                        (BUILD_KEY,))
            cur.execute("DELETE FROM transit_graph_builds WHERE build_key = %s",
                        (BUILD_KEY,))
        conn.commit()
    finally:
        conn.close()


def test_every_pattern_attributes_under_2pct_unmatched(emitted):
    stats = [s for s in emitted["stats"] if s.n_samples]
    assert stats, "no attributable patterns"
    worst = max(stats, key=lambda s: s.unmatched_fraction)
    print(
        f"[attr] {len(stats)} patterns, worst unmatched "
        f"{worst.unmatched_fraction:.2%} ({worst.pattern_key}, "
        f"{worst.n_unmatched}/{worst.n_samples})"
    )
    for s in stats:
        assert s.unmatched_fraction < 0.02, (
            s.pattern_key, s.n_unmatched, s.n_samples
        )
        assert s.n_edges > 0, s.pattern_key


def test_loop_edges_carry_plausible_route_sets(emitted):
    lg, edge_routes = emitted["lg"], emitted["edge_routes"]
    w, s, e, n = LOOP_WINDOW
    sets = Counter()
    for pos, edge in enumerate(lg.edges):
        if not any(w < lon < e and s < lat < n for lon, lat in edge.coords):
            continue
        names = frozenset(
            info.route_short_name or info.route_id  # CTA rail: short is blank
            for info in edge_routes.get(pos, {}).values()
        )
        sets[names] += 1
    print(f"[loop] distinct route sets in window ({sum(sets.values())} edges):")
    for names, cnt in sorted(sets.items(), key=lambda kv: (-len(kv[0]), kv[0])):
        print(f"  {cnt:3d} x {{{', '.join(sorted(names)) or '-'}}}")

    assert sets, "no edges in the Loop window"
    # elevated legs bundle >=4 routes (Brown/Green/Orange/Pink/Purple)
    assert any(len(names) >= 4 for names in sets), sets
    # subways stay single-route through the interior (Tower 18 property)
    assert frozenset({"Blue"}) in sets, sets
    assert frozenset({"Red"}) in sets, sets
    # nothing in the window is unattributed
    assert frozenset() not in sets, "unattributed edge inside the Loop"

    # Tower 18 attribution exam: N-S corridors strictly inside the legs
    # are the Dearborn/State subways — pure {Blue} / {Red}, never
    # co-attributed with the elevated routes.
    iw, is_, ie, in_ = LOOP_INTERIOR
    interior_checked = 0
    for pos, edge in enumerate(lg.edges):
        inside = [
            (lon, lat) for lon, lat in edge.coords
            if iw < lon < ie and is_ < lat < in_
        ]
        if len(inside) < 2:
            continue
        interior_checked += 1
        names = frozenset(
            info.route_short_name or info.route_id
            for info in edge_routes.get(pos, {}).values()
        )
        assert names in (frozenset({"Blue"}), frozenset({"Red"})), (
            f"Loop-interior edge {edge.edge_id} carries {set(names)} — "
            f"subway merged/co-attributed with elevated (LOOM's failure)"
        )
    assert interior_checked >= 2, "expected both interior subway corridors"


def test_stations_90pct_labeled_including_loop_anchors(emitted):
    snap, complexes = emitted["snap"], emitted["complexes"]
    frac = len(snap.labeled) / len(complexes)
    labels = {label for _, label in snap.labels.values()}
    sample = sorted(labels)[:8]
    print(
        f"[stations] {len(snap.labeled)}/{len(complexes)} complexes labeled "
        f"({frac:.1%}), {snap.n_split_nodes} split nodes; sample: {sample}"
    )
    for comp, reason, dist in snap.unlabeled:
        print(f"[stations] unlabeled: {comp.station_id} '{comp.label}' "
              f"({reason}{'' if dist is None else f', {dist:.0f} m'})")
    assert frac >= 0.90, frac
    assert "Clark/Lake" in labels
    assert "Washington/Wabash" in labels


def test_emitted_rows_round_trip_and_ledger_written(emitted):
    lg, counts = emitted["lg"], emitted["counts"]
    with emitted["conn"].cursor() as cur:
        db = _graph_counts(cur, BUILD_KEY)
        assert db["nodes"] == len(lg.nodes) == counts["nodes"]
        assert db["edges"] == len(lg.edges) == counts["edges"]
        assert db["edge_lines"] == counts["edge_lines"] == sum(
            len(r) for r in emitted["edge_routes"].values()
        )
        assert db["labeled"] == len(emitted["snap"].labels)
        assert db["builds"] == 1

        cur.execute(
            "SELECT feed_id, mode, route_type FROM transit_graph_builds"
            " WHERE build_key = %s", (BUILD_KEY,),
        )
        assert cur.fetchone() == ("29", "rail", 1)

        # provisional slots: contiguous 0..n-1, ordered by route_id
        cur.execute(
            """SELECT count(*) FROM (
                 SELECT el.edge_id,
                        array_agg(el.slot ORDER BY el.route_id) AS slots
                 FROM transit_graph_edge_lines el
                 JOIN transit_graph_edges e ON e.id = el.edge_id
                 WHERE e.build_key = %s GROUP BY el.edge_id
               ) g
               WHERE g.slots <> (SELECT array_agg(i) FROM
                     generate_series(0, array_length(g.slots, 1) - 1) i)""",
            (BUILD_KEY,),
        )
        assert cur.fetchone()[0] == 0, "slots must be route_id-sorted 0..n-1"


def test_loom_baseline_untouched(emitted):
    with emitted["conn"].cursor() as cur:
        after = _graph_counts(cur, BASELINE_KEY)
    print(f"[baseline] {BASELINE_KEY}: before={emitted['baseline_before']} "
          f"after={after}")
    assert after == emitted["baseline_before"]
    assert after["nodes"] > 0, "LOOM baseline must remain present"
