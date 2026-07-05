"""Real-data exam: MTA subway feed 5 on the NYC rail graph (v3 milestone 5).

What NYC tests that Chicago (test_match_real.py) cannot:

  COLOUR COLLAPSE — N/Q/R/W all share route_color #F6BC26 and 1/2/3 share
  #D82233, and the OSM NYCS route relations carry those exact colours. A
  colour-equality relation match therefore decorates the WHOLE trunk
  family, turning the relation prior into a constant — and, because the
  MTA publishes one identical trunk centerline per family (route 1's and
  route 3's shapes are the same polyline on the 66–72 St corridor),
  emission alone cannot tell a local track from an express track 5 m
  away. The tiered RouteMatcher (identity ref/name > colour-only) is
  what keeps express/local separation alive; this exam pins it.

  4-TRACK EXPRESS/LOCAL — OSM maps each track as its own way ~3–6 m
  apart. Local-route matches must ride different ways than express-route
  matches:
    - IRT Broadway–7th Av, 66–72 St: 1 local ('1','2'-decorated ways) vs
      2/3 express ('2','3' ways),
    - BMT Broadway, Prince St–8 St: N/Q express ('N','Q' ways) vs R/W
      local ('R','W' ways).

  VERTICAL SANITY — the Broadway/7th Av trunks are tunnel through these
  Manhattan windows (no elevated way within them may be ridden); the
  Astoria line is elevated (no tunnel way may be ridden).

Known agency-data notes (investigated, not matcher faults):
  - 96 St (2nd Av) terminal: the MTA shape for Q (and the N/R trips that
    terminate there) ends 103 m short of the stop coordinate, while the
    OSM track passes within ~2 m of it. The stop-snap gate correctly
    refuses the dense match; under the on-OSM policy the fallback chain
    then re-matches the pattern in the sparse regime and the rescue
    lands on the Second Av tunnel track (hmm_sparse_rescue, 0 agency
    meters) — the agency shape is no longer the arbiter of a pattern it
    itself mis-draws.
  - Joralemon St Tunnel (4/5 under the East River): the MTA shape runs
    37–64 m from OSM's tunnel alignment (way 797157484 + partners) for
    ~700 m, beyond the 50 m dense radius at its peak. Historically the
    matcher broke and bridged ~390–420 m of original shape there — the
    braid of ~30 per-pattern bridges wobbled the rendered 4/5 ribbon
    under the river. Since the gap retry
    (MatchConfig.gap_retry_radius_mult) the gap re-matches at 2x radius
    and reconnects on the IRT Lexington tunnel way seamlessly (breaks 0,
    bridged 0, recorded under stats.gap_retries).
  - R/0/a5589774 (32 trips/day, shape R..N78R): between Canal St and
    Prince St the shape is drawn through the Manhattan-Bridge approach
    and the Lexington Av corridor (long stop-to-stop chords plus a
    self-reversal just past Canal St) — 40+ m from the R's own Broadway
    local track but <15 m from foreign tracks (N/Q bridge, 4/5/6), so
    unpenalized emission would division-hop. The matcher excises the
    foreign-identity observation run (widened to the enclosing stop
    anchors) and spans the gap with a network path on the R's own track;
    foreign-identity candidates in mixed layers pay the heavy emission
    prior. Pinned in test_r_canal_degenerate_shape_excised; the residual
    is a parallel-track wobble on the City Hall chord (Hausdorff ~15 m
    to the top R/0 pattern, inside the stage-4 merge width).

Run (auto-skips without a NYC pbf/graph or data/gtfs/5.zip):
  uv run --with-requirements shapesnap/requirements.txt \
      python -m pytest shapesnap/tests/test_match_nyc.py -v -s
"""

import time
from collections import Counter

import pytest
from shapely.geometry import LineString, box

from shapesnap.candidates import MatchGraph
from shapesnap.graph import REPO_ROOT, build_graph, is_stale, load_graph, save_graph
from shapesnap.match import MatchConfig, load_patterns, match_pattern

# NYC crop (config/shapesnap.json feed 5: feed-5 shape bounds + ~10 km)
NYC_BBOX = (-74.37, 40.42, -73.64, 41.0)
# IRT Broadway–7th Av, 66 St–72 St (4-track tunnel: 1 local, 2/3 express)
SEVENTH_AVE_BBOX = (-73.985, 40.772, -73.978, 40.780)
# BMT Broadway, Prince St–8 St (4-track tunnel: N/Q express, R/W local)
BROADWAY_BBOX = (-73.9995, 40.722, -73.992, 40.731)
# BMT Astoria line along 31st St, Queens (elevated: N/W)
ASTORIA_BBOX = (-73.928, 40.760, -73.912, 40.772)

FEED_ZIP = REPO_ROOT / "data" / "gtfs" / "5.zip"
GRAPH_CACHE = REPO_ROOT / "data" / "shapesnap" / "ny-nyc.rail.graph.pkl.gz"
PBF = REPO_ROOT / "data" / "ny.osm.pbf"

EXAM_ROUTES = {"1", "2", "3", "N", "Q", "R", "W"}
# Q's top patterns hit the documented 96 St terminal quirk and are
# sparse-RESCUED under the on-OSM policy; asserted apart
DENSE_ROUTES = EXAM_ROUTES - {"Q"}
# non-top patterns matched IN ADDITION to the per-(route, direction) top
# ones: the Canal St degenerate-shape excision pin (module docstring)
PINNED_KEYS = {"R/0/a5589774"}


def _rail_graph():
    if GRAPH_CACHE.exists():
        try:
            g = load_graph(GRAPH_CACHE)
            if not is_stale(g):
                return g
        except Exception:
            pass
    if PBF.exists():
        g = build_graph(PBF, "rail", bbox=NYC_BBOX)
        save_graph(g, GRAPH_CACHE)
        return g
    return None


@pytest.fixture(scope="module")
def exam():
    if not FEED_ZIP.exists():
        pytest.skip("no data/gtfs/5.zip")
    graph = _rail_graph()
    if graph is None:
        pytest.skip("no NYC pbf / rail graph cache")
    mg = MatchGraph(graph)

    t0 = time.perf_counter()
    patterns = load_patterns(FEED_ZIP, route_ids=EXAM_ROUTES)
    t_load = time.perf_counter() - t0

    picked: dict = {}  # (route, direction) -> top pattern by trip count
    for p in patterns:  # already sorted by -trip_count
        picked.setdefault((p.route_id, p.direction_id), p)
    todo = [p for _k, p in sorted(picked.items())]
    todo += [p for p in patterns if p.key in PINNED_KEYS and p not in todo]

    cfg = MatchConfig()
    results = {}  # pattern key -> (Pattern, MatchResult)
    for p in todo:
        results[p.key] = (p, match_pattern(mg, p, cfg))
    print(f"\n[exam] {len(patterns)} patterns loaded in {t_load:.1f}s; matched {len(results)}")
    for p, r in results.values():
        print(
            f"[exam] {p.key} trips={p.trip_count} method={r.method} "
            f"conf={r.confidence} breaks={r.stats['breaks']} "
            f"tier={r.stats.get('relation_match_tier')} "
            f"dropped={r.stats.get('dropped_obs')} "
            f"gates={r.gates.as_dict() if r.gates else None}"
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


def _window_ways(graph, r, bbox) -> dict:
    """way_id -> sorted relation refs, over edges the decode used in bbox."""
    out: dict = {}
    for i in r.edges_used:
        e = graph.edges[i]
        if _in_bbox(e, bbox):
            out[e.way_id] = sorted({rr["ref"] for rr in e.route_refs if rr["ref"]})
    return out


def _route(results, rid):
    return [(p, r) for (p, r) in results.values() if p.route_id == rid]


def test_exam_patterns_match_dense(exam):
    _, _, results = exam
    assert {p.route_id for p, _r in results.values()} == EXAM_ROUTES
    for p, r in results.values():
        if p.route_id not in DENSE_ROUTES:
            continue
        assert r.method == "hmm_dense", (p.key, r.method, r.stats)
        assert r.gates is not None and r.gates.passed, (p.key, r.gates.as_dict())
        assert r.confidence > 0.3, (p.key, r.confidence)
        # colour collapse pin: identity relation matching must be active
        assert r.stats.get("relation_match_tier") == 2, (p.key, r.stats)


def test_q_96st_terminal_reanchored_on_osm(exam):
    """Q's shape ends 103 m short of the 96 St stop (agency data). Terminal
    re-anchoring (MatchConfig.reanchor_max_m — the O'Hare fix) extends the
    shape end to the stop, which sits ~1.5 m from the OSM track, so the
    resample reaches the platform and the pattern DENSE-matches 100% on OSM
    (agency_m = 0) — strictly better than the prior sparse rescue. The
    pattern's output must stay fully on OSM with every stop snapped."""
    _, _, results = exam
    for p, r in _route(results, "Q"):
        assert r.method == "hmm_dense", (p.key, r.method)
        # the terminal was re-anchored (shape end extended to the short
        # terminal stop) — this is what put the tail back on the OSM track
        assert r.stats.get("reanchored"), (p.key, r.stats.get("reanchored"))
        g = r.gates.as_dict()
        assert g["passed"] and g["max_stop_dist_m"] < 50, (p.key, g)
        assert r.stats["n_empty_layers"] == 0, (p.key, r.stats)
        assert r.stats["agency_m"] == 0.0, (p.key, r.stats)
        assert r.stats["on_osm_m"] > 20_000, (p.key, r.stats)


def test_seventh_ave_express_local_separation(exam):
    """66–72 St: 1 (local) and 3 (express) must ride disjoint ways; the
    1 may only ride ways whose relations include the 1."""
    graph, _, results = exam
    local_ways: set = set()
    express_ways: set = set()
    for p, r in _route(results, "1"):
        w = _window_ways(graph, r, SEVENTH_AVE_BBOX)
        assert w, f"{p.key} must traverse the 66-72 St window"
        print(f"[exam] 1 {p.key} 7th-Av ways: {w}")
        for way, refs in w.items():
            assert "1" in refs, (p.key, way, refs)
        local_ways.update(w)
    for rid in ("2", "3"):
        for p, r in _route(results, rid):
            w = _window_ways(graph, r, SEVENTH_AVE_BBOX)
            assert w, f"{p.key} must traverse the 66-72 St window"
            print(f"[exam] {rid} {p.key} 7th-Av ways: {w}")
            for way, refs in w.items():
                assert "1" not in refs, (p.key, way, refs)
            express_ways.update(w)
    assert not (local_ways & express_ways), (local_ways, express_ways)


def test_broadway_express_local_separation(exam):
    """Prince St–8 St: N (express; Q asserted via its decode too) rides
    'N','Q' ways; R/W ride the local ways — disjoint sets."""
    graph, _, results = exam
    express_ways: set = set()
    local_ways: set = set()
    for rid in ("N", "Q"):
        for p, r in _route(results, rid):
            w = _window_ways(graph, r, BROADWAY_BBOX)
            assert w, f"{p.key} must traverse the Broadway window"
            print(f"[exam] {rid} {p.key} Broadway ways: {w}")
            for way, refs in w.items():
                assert "R" not in refs and "W" not in refs, (p.key, way, refs)
            express_ways.update(w)
    for rid in ("R", "W"):
        for p, r in _route(results, rid):
            w = _window_ways(graph, r, BROADWAY_BBOX)
            assert w, f"{p.key} must traverse the Broadway window"
            print(f"[exam] {rid} {p.key} Broadway ways: {w}")
            for way, refs in w.items():
                assert "R" in refs or "W" in refs, (p.key, way, refs)
            local_ways.update(w)
    assert not (express_ways & local_ways), (express_ways, local_ways)


def test_vertical_sanity(exam):
    """Manhattan trunk windows are tunnel-only; Astoria is elevated-only."""
    graph, _, results = exam

    def levels(r, bbox) -> Counter:
        c: Counter = Counter()
        for i in r.edges_used:
            e = graph.edges[i]
            if _in_bbox(e, bbox):
                c["subway" if _is_subway(e.tags) else
                  "elevated" if _is_elevated(e.tags) else "surface"] += 1
        return c

    for rid, bbox, label in (
        ("1", SEVENTH_AVE_BBOX, "7th Av"), ("2", SEVENTH_AVE_BBOX, "7th Av"),
        ("3", SEVENTH_AVE_BBOX, "7th Av"), ("N", BROADWAY_BBOX, "Broadway"),
        ("Q", BROADWAY_BBOX, "Broadway"), ("R", BROADWAY_BBOX, "Broadway"),
        ("W", BROADWAY_BBOX, "Broadway"),
    ):
        for p, r in _route(results, rid):
            c = levels(r, bbox)
            assert c and set(c) == {"subway"}, (p.key, label, dict(c))
    for rid in ("N", "W"):
        for p, r in _route(results, rid):
            c = levels(r, ASTORIA_BBOX)
            assert c and set(c) == {"elevated"}, (p.key, "Astoria", dict(c))


def test_r_canal_degenerate_shape_excised(exam):
    """Third worst-pattern investigation (module docstring): the R shape
    drawn through the Manhattan-Bridge approach / Lexington corridor must
    be excised, never division-hopped. Asserts (1) the foreign-run
    excision fired with a clean decode, (2) no foreign-identity way is
    ridden anywhere, (3) the geometry agrees with the top R/0 pattern
    through lower Manhattan within the stage-4 merge width."""
    graph, mg, results = exam
    assert "R/0/a5589774" in results, "feed 5 artifact changed under the pin"
    p, r = results["R/0/a5589774"]
    assert r.method == "hmm_dense", (r.method, r.stats)
    assert r.stats["dropped_obs"] > 0 and r.stats["dropped_runs"] >= 1, r.stats
    assert r.stats["breaks"] == 0, r.stats
    for i in r.edges_used:
        e = graph.edges[i]
        refs = {rr["ref"] for rr in e.route_refs if rr["ref"]}
        assert not refs or "R" in refs, (e.way_id, sorted(refs))
    main = max(
        (
            pr for pr in results.values()
            if pr[0].route_id == "R" and pr[0].direction_id == 0
            and pr[0].key != p.key
        ),
        key=lambda pr: pr[0].trip_count,
    )
    # City Hall–Canal (the excursion + chord zone) and Canal–8 St: same
    # trunk as the main decode; the residual parallel-track wobble on the
    # City Hall chord measures ~15 m (merge width 15–20 m)
    for bb in (
        (-74.012, 40.710, -73.998, 40.7215),
        (-74.002, 40.7185, -73.990, 40.7315),
    ):
        (x0, y0), (x1, y1) = mg.project_lonlat([(bb[0], bb[1]), (bb[2], bb[3])])
        w = box(x0, y0, x1, y1)
        a = LineString(mg.project_lonlat(r.coords)).intersection(w)
        b = LineString(mg.project_lonlat(main[1].coords)).intersection(w)
        assert not a.is_empty and not b.is_empty, bb
        d = a.hausdorff_distance(b)
        print(f"[exam] R pin window {bb}: hausdorff {d:.1f} m")
        assert d < 20.0, (bb, d)
