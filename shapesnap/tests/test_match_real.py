"""Real-data exam: CTA feed 29 Brown + Blue on the Chicago rail graph.

The stage-3 acceptance property (docs/transit-pipeline-v3.md): Brown runs
the ELEVATED Loop, Blue runs the Dearborn SUBWAY beneath it — matched
paths must never cross-contaminate levels (pfaedle's exact failure).

Asserted here:
  - every exam pattern matches hmm_dense with all gates passed,
  - Brown's path uses NO subway edges (tunnel / layer<0) in the Loop bbox,
  - Blue's path uses NO elevated edges (bridge / layer>=1) along Dearborn,
  - per-shape point count stays <= a few hundred post-simplification.

Known data note (investigated, not a matcher fault): near the O'Hare
terminal the CTA shape sits 50-120 m from OSM's tunnel alignment for
~500 m, so Blue legitimately records one break there, bridged with the
original shape segment. OSM has the track (O'Hare Branch, ways 12254162
/ 680270914 / ...); the geometries just disagree beyond the candidate
radius.

Run (slow-ish — parses the 360 MB stop_times.txt once; auto-skips
without a Chicago pbf/graph or data/gtfs/29.zip):
  uv run --with-requirements shapesnap/requirements.txt \
      python -m pytest shapesnap/tests/test_match_real.py -v -s
"""

import time
from collections import Counter
from pathlib import Path

import pytest

from shapesnap.candidates import MatchGraph
from shapesnap.graph import REPO_ROOT, build_graph, is_stale, load_graph, save_graph
from shapesnap.match import MatchConfig, load_patterns, match_pattern

# Chicago region crop (config/regions.json chicago bbox)
CHI_BBOX = (-87.95, 41.64, -87.52, 42.07)
# the elevated Loop exam window
LOOP_BBOX = (-87.64, 41.87, -87.62, 41.89)
# the Dearborn subway corridor
DEARBORN_BBOX = (-87.6335, 41.875, -87.6285, 41.886)

FEED_ZIP = REPO_ROOT / "data" / "gtfs" / "29.zip"
GRAPH_CACHE = REPO_ROOT / "data" / "shapesnap" / "il-chicago.rail.graph.pkl.gz"
PBF_CANDIDATES = (
    REPO_ROOT / "data" / "il.osm.pbf",
    REPO_ROOT / "data" / "region.osm.pbf",
)

EXAM_ROUTES = {"Brn", "Blue"}
PATTERNS_PER_ROUTE_DIR = 1  # top pattern by trip count per (route, direction)


def _rail_graph():
    if GRAPH_CACHE.exists():
        try:
            g = load_graph(GRAPH_CACHE)
            if not is_stale(g):
                return g
        except Exception:
            pass
    for pbf in PBF_CANDIDATES:
        if pbf.exists():
            g = build_graph(pbf, "rail", bbox=CHI_BBOX)
            save_graph(g, GRAPH_CACHE)
            return g
    return None


@pytest.fixture(scope="module")
def exam():
    if not FEED_ZIP.exists():
        pytest.skip("no data/gtfs/29.zip")
    graph = _rail_graph()
    if graph is None:
        pytest.skip("no Chicago pbf / rail graph cache")
    mg = MatchGraph(graph)

    t0 = time.perf_counter()
    patterns = load_patterns(FEED_ZIP, route_ids=EXAM_ROUTES)
    t_load = time.perf_counter() - t0

    picked: list = []
    for p in patterns:  # already sorted by -trip_count
        key = (p.route_id, p.direction_id)
        n_key = sum(1 for q in picked if (q.route_id, q.direction_id) == key)
        if n_key < PATTERNS_PER_ROUTE_DIR and p.trip_count >= 10:
            picked.append(p)

    cfg = MatchConfig()
    results = {}
    for p in picked:
        results[p.key] = (p, match_pattern(mg, p, cfg))
    print(f"\n[exam] {len(patterns)} patterns loaded in {t_load:.1f}s; matched {len(picked)}")
    for key, (p, r) in results.items():
        print(
            f"[exam] {key} route={p.route_id} dir={p.direction_id} trips={p.trip_count} "
            f"method={r.method} conf={r.confidence} breaks={r.stats['breaks']} "
            f"pts={r.stats.get('output_points')} gates={r.gates.as_dict() if r.gates else None} "
            f"runtime={r.stats['runtime_s']}s"
        )
    return graph, mg, results


def _layer(tags) -> int:
    try:
        return int(tags.get("layer", "0"))
    except ValueError:
        return 0


def _is_subway(tags) -> bool:
    return tags.get("tunnel") in ("yes", "building_passage") or _layer(tags) < 0


def _is_elevated(tags) -> bool:
    if _is_subway(tags):
        return False
    bridge = tags.get("bridge")
    return (bridge is not None and bridge != "no") or _layer(tags) >= 1


def _in_bbox(edge, bbox) -> bool:
    minlon, minlat, maxlon, maxlat = bbox
    return any(
        minlon <= lon <= maxlon and minlat <= lat <= maxlat
        for lon, lat in edge.geometry
    )


def _level_histogram(graph, edge_idxs) -> Counter:
    hist = Counter()
    for i in edge_idxs:
        t = graph.edges[i].tags
        level = "subway" if _is_subway(t) else "elevated" if _is_elevated(t) else "surface"
        hist[(t.get("railway"), level)] += 1
    return hist


def _route_results(results, route_id):
    return [(p, r) for (p, r) in results.values() if p.route_id == route_id]


def test_exam_patterns_match_dense(exam):
    _, _, results = exam
    assert results, "expected exam patterns"
    routes = {p.route_id for p, _ in results.values()}
    assert routes == EXAM_ROUTES
    for p, r in results.values():
        assert r.method == "hmm_dense", (p.key, r.method, r.stats)
        assert r.gates is not None and r.gates.passed, (p.key, r.gates.as_dict())
        assert r.confidence > 0.3, (p.key, r.confidence)


def test_brown_stays_on_elevated_loop(exam):
    """Brown must never touch subway edges inside the Loop bbox."""
    graph, _, results = exam
    for p, r in _route_results(results, "Brn"):
        loop_edges = [i for i in r.edges_used if _in_bbox(graph.edges[i], LOOP_BBOX)]
        assert loop_edges, "Brown must traverse the Loop window"
        hist = _level_histogram(graph, loop_edges)
        print(f"[exam] Brown {p.key} Loop-bbox edge histogram: {dict(hist)}")
        offenders = [
            (graph.edges[i].way_id, graph.edges[i].tags)
            for i in loop_edges
            if _is_subway(graph.edges[i].tags)
        ]
        assert not offenders, f"Brown used subway edges in the Loop: {offenders}"
        assert r.stats["breaks"] == 0, r.stats


def test_blue_stays_in_dearborn_subway(exam):
    """Blue must never touch elevated edges in the Dearborn corridor."""
    graph, _, results = exam
    for p, r in _route_results(results, "Blue"):
        dear_edges = [i for i in r.edges_used if _in_bbox(graph.edges[i], DEARBORN_BBOX)]
        assert dear_edges, "Blue must traverse the Dearborn window"
        hist = _level_histogram(graph, dear_edges)
        print(f"[exam] Blue {p.key} Dearborn-bbox edge histogram: {dict(hist)}")
        offenders = [
            (graph.edges[i].way_id, graph.edges[i].tags)
            for i in dear_edges
            if _is_elevated(graph.edges[i].tags)
        ]
        assert not offenders, f"Blue used elevated edges on Dearborn: {offenders}"
        # single allowed break: the O'Hare terminal shape-vs-OSM offset (see module docstring)
        assert r.stats["breaks"] <= 1, r.stats


def test_full_path_histograms_and_point_budget(exam):
    graph, _, results = exam
    for p, r in results.values():
        hist = _level_histogram(graph, r.edges_used)
        print(f"[exam] {p.key} full-path histogram: {dict(hist)}")
        pts = r.stats.get("output_points")
        assert pts is not None and 50 <= pts <= 600, (p.key, pts)
        # density sanity vs the pfaedle blowup: ~1 point per >=50 m on average
        assert r.stats["output_len_m"] / pts > 50, (p.key, pts, r.stats["output_len_m"])
