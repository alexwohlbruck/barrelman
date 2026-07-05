"""Build-reproducibility guard (PAR-12): a clean-cache rebuild is
DETERMINISTIC and reproduces the live DB.

This is verification-integrity insurance against the class of bug this
test was written for: the way-graph corridor cache digest
(`waygraph.waygraph_digest`) once hashed only the route SHAPES, not the
route colours / pattern->route mapping / GTFS stop positions that also
change the emitted graph. A conflation / colour / route change then reused
a STALE corridor cache, so the live `chicago:l-v3` (167 edges) silently
diverged from what the committed source deterministically builds
(145 edges). The digest now folds in every graph-affecting input
(CONFIG_FORMAT_VERSION 17), so this can't recur — and this test proves it:

  1. DETERMINISM: two rebuilds FROM A CLEAN CACHE (cache deleted so the
     digest is exercised from scratch) produce byte-identical geometry —
     identical node/edge/edge_line counts AND identical md5 of the sorted
     edge + node geometry.
  2. DB REPRODUCTION: that fresh rebuild's counts match the live
     transit_graph_* rows for the same build_key — the DB IS the
     committed-source deterministic build, not a transient stale state.

In-memory only: it never emits, so running it never mutates the DB. It
builds the way-graph corridors (clean cache) + station snapping — the same
graph `linegraph.build --emit` writes — and fingerprints `lg` directly.

Requires the dev DB (skips if unreachable) and both feed zips. Run:
  uv run --with-requirements linegraph/requirements.txt \
      python -m pytest linegraph/tests/test_determinism.py -v -s
"""

from __future__ import annotations

import hashlib
import os

import pytest
from shapesnap.match import load_patterns

from linegraph.build import (REPO_ROOT, build_waygraph_linegraph,
                             dedup_shapes, resolve_feed_zip,
                             waygraph_edge_routes)
from linegraph.model import default_cache_path
from linegraph.stations import load_station_complexes, snap_stations

DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)

# (feed_id, mode, live build_key). Both cities the v3 pipeline emits.
CITIES = [
    ("29", "rail", "chicago:l-v3"),
    ("5", "rail", "nyc:subway-v3"),
]


def _fingerprint(lg, edge_routes):
    """Deterministic (counts, md5) fingerprint of a station-snapped lg.

    Geometry hashed from lg.coords rounded to 1e-6 deg (the digest/emit
    rounding), sorted, so it is independent of edge/node ordering.
    """
    eh = hashlib.md5()
    for g in sorted(
        ";".join(f"{lon:.6f},{lat:.6f}" for lon, lat in e.coords)
        for e in lg.edges
    ):
        eh.update(g.encode())
        eh.update(b"|")
    nh = hashlib.md5()
    for g in sorted(f"{n.lon:.6f},{n.lat:.6f}" for n in lg.nodes):
        nh.update(g.encode())
        nh.update(b"|")
    edge_lines = sum(len(r) for r in edge_routes.values())
    return {
        "nodes": len(lg.nodes),
        "edges": len(lg.edges),
        "edge_lines": edge_lines,
        "edge_md5": eh.hexdigest(),
        "node_md5": nh.hexdigest(),
    }


def _clean_rebuild(feed_id, mode):
    """Delete the way-graph cache, rebuild corridors from scratch, snap
    stations. Returns (fingerprint dict). In-memory; never emits."""
    zip_path = resolve_feed_zip(feed_id)
    patterns = load_patterns(zip_path, modes={mode})
    shapes, _ = dedup_shapes(patterns)

    cache = default_cache_path(feed_id, mode).with_name(
        f"{feed_id}.{mode}.waygraph.pkl.gz")
    for p in (cache, cache.with_name(cache.name + ".notes.json")):
        try:
            p.unlink()
        except FileNotFoundError:
            pass

    lg, _notes = build_waygraph_linegraph(
        patterns, shapes, feed_id, mode, f"determinism:{feed_id}",
        force=True, use_cache=True, verbose=False)
    stop_ids = {sid for p in patterns for sid in p.stop_ids}
    complexes = load_station_complexes(zip_path, stop_ids)
    lg, _snap = snap_stations(lg, complexes)
    edge_routes = waygraph_edge_routes(lg, zip_path, feed_id)
    return _fingerprint(lg, edge_routes)


def _db_counts(build_key):
    psycopg = pytest.importorskip("psycopg")
    try:
        conn = psycopg.connect(DSN, connect_timeout=5)
    except Exception as err:  # pragma: no cover
        pytest.skip(f"dev DB unreachable: {err}")
    with conn, conn.cursor() as cur:
        cur.execute(
            """SELECT
                 (SELECT count(*) FROM transit_graph_nodes WHERE build_key=%(k)s),
                 (SELECT count(*) FROM transit_graph_edges WHERE build_key=%(k)s),
                 (SELECT count(*) FROM transit_graph_edge_lines el
                    JOIN transit_graph_edges e ON e.id=el.edge_id
                   WHERE e.build_key=%(k)s)""",
            {"k": build_key},
        )
        nodes, edges, edge_lines = cur.fetchone()
    conn.close()
    return {"nodes": nodes, "edges": edges, "edge_lines": edge_lines}


@pytest.mark.parametrize("feed_id,mode,build_key", CITIES)
def test_clean_rebuild_is_deterministic_and_reproduces_db(feed_id, mode,
                                                          build_key):
    if not (REPO_ROOT / "data" / "gtfs-processed" / f"{feed_id}.zip").exists():
        pytest.skip(f"no feed {feed_id} processed zip")

    a = _clean_rebuild(feed_id, mode)
    b = _clean_rebuild(feed_id, mode)
    print(f"\n[determinism] {build_key} rebuild#1 {a}")
    print(f"[determinism] {build_key} rebuild#2 {b}")
    # 1. two clean-cache rebuilds are byte-identical
    assert a == b, (build_key, a, b)

    # 2. the fresh build reproduces the live DB (counts) — the DB is the
    #    committed-source deterministic build, not a stale state
    db = _db_counts(build_key)
    print(f"[determinism] {build_key} live DB {db}")
    assert a["nodes"] == db["nodes"], (build_key, "nodes", a, db)
    assert a["edges"] == db["edges"], (build_key, "edges", a, db)
    assert a["edge_lines"] == db["edge_lines"], (build_key, "edge_lines", a, db)
