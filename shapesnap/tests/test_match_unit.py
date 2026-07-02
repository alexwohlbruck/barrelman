"""Unit tests for shapesnap.match / candidates / gates on a synthetic graph.

Scenarios (fixtures/match_scenarios.osm):
  A. parallel ways with NO shared nodes; a shape hugging way 401 must
     match way 401 end-to-end (never its 40 m-away twin 402),
  B. a hole mid-route: the matcher must break, match the sub-traces —
     the gap RETRY at 2x radius must attempt and fail, the GRAPH BRIDGE
     must find no route (no topological connection exists) — and bridge
     the hole with the ORIGINAL shape segment (bridge_kind="agency",
     counted in stats.agency_m), never fabricate a crossing,
  C. a shape offset ~40 m from the only way: candidates exist but the
     coverage gate fails -> the on-OSM fallback chain re-matches the
     pattern in the SPARSE regime (stops sit on the mapped track) ->
     method=hmm_sparse_rescue, output 100% on OSM. With the stops moved
     beyond the sparse radius the rescue is infeasible ->
     method=passthrough_agency, original returned unchanged, loud log,
  D. a long continuous way with the shape bulging ~70 m off it for a
     stretch (past the 50 m dense radius, inside the 2x retry radius):
     the gap retry must reconnect on the way's own track with no break
     recorded (the 4/5 Joralemon St Tunnel offset in miniature),
  E. two approach rails joined ONLY through a yard-penalized U-detour:
     the shape crosses the gap straight, the retry fails (no feasible
     expanded chain through the yard arms within the per-hop cutoff),
     and the bounded GRAPH BRIDGE between the pinned anchors must
     splice the U (bridge_kind="graph") — output 100% on OSM.

Plus regime-B station emission features (StationIndex wired through
match_pattern): name bonus, platform-ref bonus via Pattern.stop_platforms,
stations-passed-without-stopping penalty; and degenerate-input gates.

Run (repo convention):
  uv run --with-requirements shapesnap/requirements.txt \
      python -m pytest shapesnap/tests/test_match_unit.py -v
"""

from pathlib import Path

import pytest
from shapely.geometry import LineString

from shapesnap.candidates import (
    MatchGraph,
    RouteMatcher,
    Station,
    StationIndex,
    cheap_ted,
    name_similarity,
)
from shapesnap.gates import GateConfig, evaluate_gates
from shapesnap.graph import build_graph
from shapesnap.match import (
    MatchConfig,
    Pattern,
    geometry_hash,
    load_patterns,  # noqa: F401  (import smoke)
    match_pattern,
)

FIXTURE = Path(__file__).parent / "fixtures" / "match_scenarios.osm"


@pytest.fixture(scope="module")
def mg():
    return MatchGraph(build_graph(FIXTURE, "rail"))


def edges_of_way(mg, way_id):
    return {i for i, e in enumerate(mg.graph.edges) if e.way_id == way_id}


def mk_pattern(shape, stops, names=None, platforms=None,
               short="T1", long="Test Line", color="123456"):
    return Pattern(
        route_id="T1",
        direction_id=0,
        stop_ids=tuple(f"s{i}" for i in range(len(stops))),
        stop_coords=stops,
        stop_names=names or [f"Stop {i}" for i in range(len(stops))],
        trip_count=1,
        shape_id="sh1" if shape else None,
        shape=shape,
        route_short_name=short,
        route_long_name=long,
        route_color=color,
        route_type=1,
        stop_platforms=platforms or [],
    )


def lons(a, b, step):
    out, x = [], a
    while (step > 0 and x <= b + 1e-9) or (step < 0 and x >= b - 1e-9):
        out.append(round(x, 7))
        x += step
    return out


# ── A: parallel ways, no cross-bleed ─────────────────────────────────────────


def test_parallel_ways_match_stays_on_hugged_way(mg):
    shape = [(x, 41.88003) for x in lons(-87.6400, -87.6300, 0.0005)]
    stops = [(-87.6400, 41.8800), (-87.6300, 41.8800)]
    r = match_pattern(mg, mk_pattern(shape, stops))

    assert r.method == "hmm_dense"
    assert r.stats["breaks"] == 0
    assert r.gates is not None and r.gates.passed
    assert set(r.edges_used), "must use graph edges"
    assert set(r.edges_used) <= edges_of_way(mg, 401), (
        f"matched onto the wrong parallel track: {r.edges_used}"
    )
    assert not (set(r.edges_used) & edges_of_way(mg, 402))
    # end-to-end: output spans the full way length (~829 m)
    assert r.stats["output_len_m"] > 780
    assert r.confidence > 0.5


def test_parallel_ways_sparse_regime(mg):
    """Regime B: stop coordinates only (no shape) stay on the hugged way."""
    stops = [(-87.6398, 41.8800), (-87.6370, 41.8800),
             (-87.6330, 41.8800), (-87.6302, 41.8800)]
    r = match_pattern(mg, mk_pattern(None, stops))

    assert r.method == "hmm_sparse"
    assert r.stats["regime"] == "sparse"
    assert set(r.edges_used) <= edges_of_way(mg, 401)
    assert r.gates is not None and r.gates.passed
    assert r.stats["breaks"] == 0


# ── regime B station emission features (StationIndex through match_pattern) ──

# stops ~20 m SOUTH of way 401 (nonzero snap distance -> nonzero emission
# cost that the station bonuses can then erase)
OFFSET_STOPS = [(-87.6398, 41.87982), (-87.6370, 41.87982),
                (-87.6330, 41.87982), (-87.6302, 41.87982)]


def test_station_name_bonus_raises_sparse_confidence(mg):
    names = ["Alpha", "Bravo", "Charlie", "Delta"]
    base = match_pattern(mg, mk_pattern(None, OFFSET_STOPS, names=names))
    # matching-named OSM stations sit ON way 401 next to each stop
    idx = StationIndex(
        [Station(lon, 41.8800, name, "") for (lon, _), name in zip(OFFSET_STOPS, names)],
        mg,
    )
    boosted = match_pattern(mg, mk_pattern(None, OFFSET_STOPS, names=names), station_idx=idx)
    assert base.method == boosted.method == "hmm_sparse"
    assert set(boosted.edges_used) <= edges_of_way(mg, 401)
    assert boosted.confidence > base.confidence, (base.confidence, boosted.confidence)


def test_platform_ref_bonus_via_stop_platforms(mg):
    """platform_code carried on the Pattern must reach best_name_bonus."""
    blank = [""] * len(OFFSET_STOPS)  # no name signal: only the ref can match
    plats = ["A", "B", "C", "D"]
    base = match_pattern(mg, mk_pattern(None, OFFSET_STOPS, names=blank, platforms=blank))
    idx = StationIndex(
        [Station(lon, 41.8800, "", ref) for (lon, _), ref in zip(OFFSET_STOPS, plats)],
        mg,
    )
    boosted = match_pattern(
        mg, mk_pattern(None, OFFSET_STOPS, names=blank, platforms=plats), station_idx=idx
    )
    assert base.method == boosted.method == "hmm_sparse"
    assert boosted.confidence > base.confidence, (base.confidence, boosted.confidence)


def test_station_passed_without_stopping_penalty(mg):
    stops = [(-87.6398, 41.8800), (-87.6302, 41.8800)]  # on way 401, ends only
    base = match_pattern(mg, mk_pattern(None, stops))
    # a station mid-way along 401, far (>2x pass radius) from both stops
    idx = StationIndex([Station(-87.6350, 41.8800, "Middleton", "")], mg)
    penalized = match_pattern(mg, mk_pattern(None, stops), station_idx=idx)
    assert base.method == penalized.method == "hmm_sparse"
    assert set(penalized.edges_used) <= edges_of_way(mg, 401)
    assert penalized.confidence < base.confidence, (base.confidence, penalized.confidence)


# ── B: break + bridge, never force ───────────────────────────────────────────


def test_gap_breaks_and_bridges_with_original(mg):
    shape = [(x, 41.8830) for x in lons(-87.6400, -87.6300, 0.0005)]
    stops = [(-87.6400, 41.8830), (-87.6300, 41.8830)]
    r = match_pattern(mg, mk_pattern(shape, stops))

    assert r.method == "hmm_dense"
    assert r.stats["breaks"] == 1, r.stats
    assert len(r.stats["gaps"]) == 1
    gap = r.stats["gaps"][0]
    # no topological connection at any budget: the graph bridge must find
    # nothing and the gap falls back to the original geometry
    assert gap["bridge_kind"] == "agency", gap
    assert 50 < gap["bridged_m"] < 300, gap
    # both sides matched, nothing forced across the hole
    used_ways = {mg.graph.edges[i].way_id for i in r.edges_used}
    assert used_ways == {403, 404}
    # the bridge keeps the output continuous across the hole (gates pass)
    assert r.gates is not None and r.gates.passed
    # on-OSM accounting: the agency meters are exactly the bridged span
    assert r.stats["agency_m"] == gap["bridged_m"], r.stats
    assert r.stats["graph_bridged_m"] == 0.0, r.stats
    assert r.stats["on_osm_m"] > 0
    assert (
        abs(r.stats["on_osm_m"] + r.stats["agency_m"] - r.stats["output_len_m"]) < 5.0
    ), r.stats
    # break halves the confidence
    assert r.confidence < 0.6


# ── D: gap retry — offset track recovered, absent track still bridges ────────


def scenario_d_shape():
    """Hug way 406 except a ~70 m southward bulge (-87.6560..-87.6540)."""
    return [
        (x, 41.88937 if -87.6560 <= x <= -87.6540 else 41.8900)
        for x in lons(-87.6900, -87.5900, 0.0005)
    ]


def test_gap_retry_recovers_offset_track(mg):
    """(a) The true track sits ~70 m from the shape for a stretch: past
    the 50 m dense radius (break today) but inside the 2x retry radius.
    The retry must splice a continuous path on way 406 — no break, no
    bridge, the gap recorded as a successful retry."""
    shape = scenario_d_shape()
    stops = [(-87.6900, 41.8900), (-87.5900, 41.8900)]
    r = match_pattern(mg, mk_pattern(shape, stops))

    assert r.method == "hmm_dense", (r.method, r.stats)
    assert r.gates is not None and r.gates.passed, r.gates.as_dict()
    assert r.stats["breaks"] == 0, r.stats
    assert r.stats["gaps"] == [] and r.stats["bridged_m"] == 0, r.stats
    retries = r.stats.get("gap_retries")
    assert retries and len(retries) == 1, r.stats
    assert retries[0]["ok"] is True and retries[0]["radius_m"] == 100.0, retries
    # the whole decode rides way 406 — bulge included
    assert set(r.edges_used) <= edges_of_way(mg, 406), r.edges_used
    # ...and the output geometry stays ON the track through the bulge
    # (never the agency bulge): worst vertex-to-track distance ~0
    track = LineString(
        mg.project_lonlat(mg.graph.edges[next(iter(edges_of_way(mg, 406)))].geometry)
    )
    out = mg.project_lonlat(r.coords)
    from shapely.geometry import Point
    worst = max(track.distance(Point(xy)) for xy in out)
    assert worst < 1.0, f"output strays {worst:.1f} m off the retried track"
    # no break -> confidence not halved
    assert r.confidence > 0.5, r.confidence


def test_gap_retry_absent_track_still_bridges(mg):
    """(b) Scenario B's hole has no topological crossing at ANY radius:
    the retry must attempt (interior observations exist, expanded layers
    are non-empty on both flanks) and fail feasibility, the graph bridge
    must find no route — bridged with the original segment exactly as
    without the retry, and the agency meters equal the gap length."""
    shape = [(x, 41.8830) for x in lons(-87.6400, -87.6300, 0.0005)]
    stops = [(-87.6400, 41.8830), (-87.6300, 41.8830)]
    r = match_pattern(mg, mk_pattern(shape, stops))

    assert r.method == "hmm_dense"
    assert r.stats["breaks"] == 1, r.stats
    assert len(r.stats["gaps"]) == 1 and r.stats["bridged_m"] > 50, r.stats
    retries = r.stats.get("gap_retries")
    assert retries and len(retries) == 1, r.stats
    assert retries[0]["ok"] is False, retries
    assert r.stats["gaps"][0]["bridge_kind"] == "agency", r.stats["gaps"]
    assert r.stats["agency_m"] == r.stats["gaps"][0]["bridged_m"], r.stats
    assert {mg.graph.edges[i].way_id for i in r.edges_used} == {403, 404}
    assert r.gates is not None and r.gates.passed


def test_gap_retry_disabled_graph_bridge_takes_over(mg):
    """Config-driven: gap_retry_radius_mult <= 1 disables the retry, so
    the scenario-D bulge BREAKS — but the on-OSM policy then routes the
    gap through the graph (both anchors sit on continuous way 406), so
    the bridge is a graph splice, not agency geometry."""
    shape = scenario_d_shape()
    stops = [(-87.6900, 41.8900), (-87.5900, 41.8900)]
    cfg = MatchConfig(gap_retry_radius_mult=1.0)
    r = match_pattern(mg, mk_pattern(shape, stops), cfg)

    assert r.method == "hmm_dense", (r.method, r.stats)
    assert r.stats["breaks"] == 1, r.stats
    assert "gap_retries" not in r.stats, r.stats
    assert len(r.stats["gaps"]) == 1, r.stats
    assert r.stats["gaps"][0]["bridge_kind"] == "graph", r.stats["gaps"]
    assert r.stats["bridged_m"] == 0.0 and r.stats["agency_m"] == 0.0, r.stats
    assert r.stats["graph_bridged_m"] > 100, r.stats
    assert set(r.edges_used) <= edges_of_way(mg, 406), r.edges_used


def test_gap_retry_and_graph_bridge_both_disabled_reproduce_break(mg):
    """Config-driven: with the retry AND the graph bridge disabled the
    pre-policy break+agency-bridge behavior is fully restored."""
    shape = scenario_d_shape()
    stops = [(-87.6900, 41.8900), (-87.5900, 41.8900)]
    cfg = MatchConfig(gap_retry_radius_mult=1.0, bridge_route_cutoff_factor=0.0)
    r = match_pattern(mg, mk_pattern(shape, stops), cfg)

    assert r.method == "hmm_dense", (r.method, r.stats)
    assert r.stats["breaks"] == 1, r.stats
    assert r.stats["gaps"][0]["bridge_kind"] == "agency", r.stats["gaps"]
    assert r.stats["bridged_m"] > 100, r.stats
    assert r.stats["agency_m"] == r.stats["bridged_m"], r.stats
    assert "gap_retries" not in r.stats, r.stats


# ── E: failed retry gap, graph-routed bridge ─────────────────────────────────


E_WAYS = (410, 411, 412, 413, 414)


def scenario_e_pattern():
    """Straight shape across the ways-410/411 gap (the only connection is
    the yard-arm U 412/413/414, ~66 m south — past the 50 m radius,
    inside the 100 m retry radius but infeasible per hop)."""
    shape = [(x, 41.8960) for x in lons(-87.7100, -87.5850, 0.0005)]
    stops = [(-87.7100, 41.8960), (-87.5850, 41.8960)]
    return mk_pattern(shape, stops)


def test_failed_retry_gap_graph_bridge(mg):
    """The retry attempts and fails (no feasible expanded chain through
    the yard arms), then the bounded graph route between the pinned
    anchors splices the U: bridge_kind="graph", zero agency meters, the
    whole output rides fixture ways."""
    r = match_pattern(mg, scenario_e_pattern())

    assert r.method == "hmm_dense", (r.method, r.stats)
    assert r.gates is not None and r.gates.passed, r.gates.as_dict()
    assert r.stats["breaks"] == 1, r.stats
    retries = r.stats.get("gap_retries")
    assert retries and len(retries) == 1 and retries[0]["ok"] is False, r.stats
    assert len(r.stats["gaps"]) == 1, r.stats
    gap = r.stats["gaps"][0]
    assert gap["bridge_kind"] == "graph", gap
    assert 500 < gap["routed_m"] < 1200, gap
    # on-OSM accounting: everything is OSM geometry
    assert r.stats["agency_m"] == 0.0 and r.stats["bridged_m"] == 0.0, r.stats
    assert r.stats["graph_bridged_m"] == gap["routed_m"], r.stats
    assert (
        abs(r.stats["on_osm_m"] - r.stats["output_len_m"]) < 5.0
    ), r.stats
    # the splice rides the fixture ways — U included — and nothing else
    used_ways = {mg.graph.edges[i].way_id for i in r.edges_used}
    assert used_ways == set(E_WAYS), used_ways
    from shapely.ops import unary_union
    track = unary_union([
        LineString(mg.project_lonlat(mg.graph.edges[i].geometry))
        for i in range(len(mg.graph.edges))
        if mg.graph.edges[i].way_id in E_WAYS
    ])
    from shapely.geometry import Point
    worst = max(track.distance(Point(xy)) for xy in mg.project_lonlat(r.coords))
    assert worst < 1.0, f"output strays {worst:.1f} m off OSM ways"


def test_graph_bridge_disabled_restores_agency_bridge(mg):
    """Config-driven: bridge_route_cutoff_factor <= 0 disables routing;
    the scenario-E gap falls back to the original geometry and the
    agency meters are accounted."""
    cfg = MatchConfig(bridge_route_cutoff_factor=0.0)
    r = match_pattern(mg, scenario_e_pattern(), cfg)

    assert r.method == "hmm_dense", (r.method, r.stats)
    assert r.stats["breaks"] == 1, r.stats
    gap = r.stats["gaps"][0]
    assert gap["bridge_kind"] == "agency", gap
    assert r.stats["agency_m"] == gap["bridged_m"] > 300, r.stats
    assert r.stats["graph_bridged_m"] == 0.0, r.stats
    # without the route the U's bottom is never ridden (the anchors may
    # legitimately snap onto an arm top — it shares the approach node)
    used_ways = {mg.graph.edges[i].way_id for i in r.edges_used}
    assert {410, 411} <= used_ways <= {410, 411, 412, 414}, used_ways
    assert 413 not in used_ways, used_ways


# ── C: gate failure -> on-OSM fallback chain ─────────────────────────────────


def test_gates_fail_sparse_rescue_recovers_on_osm(mg):
    """The shape sits ~40 m off way 405 (coverage gate fails) but the
    stops are within the sparse radius of the mapped track: the fallback
    chain must re-match in the sparse regime and return the OSM path —
    never the agency geometry."""
    shape = [(x, 41.88564) for x in lons(-87.6400, -87.6300, 0.0005)]
    stops = [shape[0], shape[-1]]
    r = match_pattern(mg, mk_pattern(shape, stops))

    assert r.method == "hmm_sparse_rescue", (r.method, r.stats)
    assert r.stats["regime"] == "sparse"
    assert r.gates is not None and r.gates.passed, r.gates.as_dict()
    # the dense attempt (and why it failed) is recorded on the rescue
    dense = r.stats.get("dense_attempt")
    assert dense and dense["method"] == "passthrough_agency", r.stats
    assert any("coverage" in f for f in dense["gates"]["failures"]), dense
    # the output is way 405, fully on OSM
    assert set(r.edges_used) <= edges_of_way(mg, 405), r.edges_used
    assert r.stats["agency_m"] == 0.0, r.stats
    assert abs(r.stats["on_osm_m"] - r.stats["output_len_m"]) < 5.0, r.stats
    assert r.coords != shape


def test_sparse_rescue_infeasible_keeps_agency_passthrough(mg, capfd):
    """Same failing shape, but the stops sit ~220 m from every way —
    beyond the sparse radius, so the rescue sees empty stop layers and
    must refuse (a chord bridge through the stop coordinate would fake
    the stop-snap gate): method=passthrough_agency, original geometry
    byte-identical, loud stderr log, agency_m == the whole output."""
    shape = [(x, 41.88564) for x in lons(-87.6400, -87.6300, 0.0005)]
    stops = [(-87.6400, 41.8880), (-87.6300, 41.8880)]
    r = match_pattern(mg, mk_pattern(shape, stops))

    assert r.method == "passthrough_agency", (r.method, r.stats)
    assert r.gates is not None and not r.gates.passed
    assert r.coords == shape, "passthrough_agency must return the ORIGINAL geometry"
    assert r.stats["on_osm_m"] == 0.0 and r.stats["agency_m"] > 700, r.stats
    err = capfd.readouterr().err
    assert "PASSTHROUGH_AGENCY" in err, err
    assert "sparse rescue failed" in err, err


def test_sparse_rescue_disabled_keeps_agency_passthrough(mg, capfd):
    """Config-driven: sparse_rescue=False restores the pre-policy
    gates-fail behavior (original geometry back, no rescue attempt)."""
    shape = [(x, 41.88564) for x in lons(-87.6400, -87.6300, 0.0005)]
    stops = [shape[0], shape[-1]]
    cfg = MatchConfig(sparse_rescue=False)
    r = match_pattern(mg, mk_pattern(shape, stops), cfg)

    assert r.method == "passthrough_agency", (r.method, r.stats)
    assert r.coords == shape
    assert "sparse rescue disabled" in capfd.readouterr().err


def test_off_network_shape_is_passthrough(mg):
    shape = [(x, 41.9200) for x in lons(-87.6400, -87.6300, 0.0005)]
    r = match_pattern(mg, mk_pattern(shape, [shape[0], shape[-1]]))
    assert r.method == "passthrough"
    assert r.coords == shape
    assert r.stats["on_osm_m"] == 0.0 and r.stats["agency_m"] > 0, r.stats


# ── candidates / helpers ─────────────────────────────────────────────────────


def test_candidates_radius_and_direction(mg):
    x, y = mg.project_lonlat([(-87.6350, 41.8800)])[0]
    cands = mg.candidates(x, y, radius=50.0, k=8)
    ways = {mg.graph.edges[c.edge].way_id for c in cands}
    assert ways == {401, 402}
    # two-way rail -> both directions offered
    dirs = {(mg.graph.edges[c.edge].way_id, c.dir) for c in cands}
    assert (401, 1) in dirs and (401, -1) in dirs
    tight = mg.candidates(x, y, radius=10.0, k=8)
    assert {mg.graph.edges[c.edge].way_id for c in tight} == {401}


def test_route_matcher_rules():
    edge_like = type("E", (), {})()
    edge_like.route_refs = [
        {"ref": "Brown", "name": "CTA Brown Line: Kimball → Loop", "colour": "#62361b"}
    ]
    # colour match (CTA rail: short_name is empty)
    assert RouteMatcher("", "Brown Line", "62361B").matches_edge(0, edge_like)
    # name-contains-long-name match
    assert RouteMatcher("", "Brown Line", "").matches_edge(1, edge_like)
    # ref == short_name match
    assert RouteMatcher("Brown", "", "").matches_edge(2, edge_like)
    # word-bounded short name: route "2" must NOT match "22 Clark"
    bus = type("E", (), {})()
    bus.route_refs = [{"ref": "22", "name": "22 Clark", "colour": None}]
    assert not RouteMatcher("2", "", "").matches_edge(0, bus)
    assert RouteMatcher("22", "", "").matches_edge(1, bus)
    # empty GTFS values never match
    assert not RouteMatcher("", "", "").matches_edge(3, edge_like)


def test_route_matcher_strength_tiers():
    """route_color is a FAMILY key on colour-collapsed networks (NYC:
    1/2/3 share #D82233 and the OSM relations carry that exact colour) —
    identity matches (ref/name) must outrank colour-only matches."""
    local = type("E", (), {})()
    local.route_refs = [
        {"ref": "1", "name": "NYCS - 1 Train: ...", "colour": "#D82233"},
        {"ref": "2", "name": "NYCS - 2 Train (late nights): ...", "colour": "#D82233"},
    ]
    express = type("E", (), {})()
    express.route_refs = [
        {"ref": "2", "name": "NYCS - 2 Train: ...", "colour": "#D82233"},
        {"ref": "3", "name": "NYCS - 3 Train: ...", "colour": "#d82233"},
    ]
    rm = RouteMatcher("1", "Broadway - 7 Avenue Local", "D82233")
    assert rm.match_strength(0, local) == 2      # ref match
    assert rm.match_strength(1, express) == 1    # colour-only (family key)
    # both still "match" for candidate admission
    assert rm.matches_edge(0, local) and rm.matches_edge(1, express)
    # colour stays a real signal when no identity data exists
    colour_only = type("E", (), {})()
    colour_only.route_refs = [{"ref": None, "name": None, "colour": "#D82233"}]
    assert RouteMatcher("1", "", "D82233").match_strength(0, colour_only) == 1


def test_route_matcher_is_foreign():
    """Foreign = identity-decorated with OTHER routes' relations only.
    Undecorated (yard) and colour-only-decorated edges are merely
    uninformative, never foreign; a same-colour-family edge carrying
    another route's ref IS foreign (NYC R vs the N/Q bridge tracks)."""
    rm = RouteMatcher("R", "Broadway Local", "F6BC26")
    own = type("E", (), {})()
    own.route_refs = [{"ref": "R", "name": "NYCS - R Train: ...", "colour": "#F6BC26"}]
    other_division = type("E", (), {})()
    other_division.route_refs = [
        {"ref": "4", "name": "NYCS - 4 Train: ...", "colour": "#00933C"},
        {"ref": "6", "name": "NYCS - 6 Train: ...", "colour": "#00933C"},
    ]
    same_family = type("E", (), {})()
    same_family.route_refs = [
        {"ref": "N", "name": "NYCS - N Train: ...", "colour": "#F6BC26"}
    ]
    yard = type("E", (), {})()
    yard.route_refs = []
    colour_only = type("E", (), {})()
    colour_only.route_refs = [{"ref": None, "name": None, "colour": "#F6BC26"}]
    assert not rm.is_foreign(0, own)
    assert rm.is_foreign(1, other_division)
    assert rm.is_foreign(2, same_family)
    assert not rm.is_foreign(3, yard)
    assert not rm.is_foreign(4, colour_only)


def test_name_similarity_and_ted():
    assert cheap_ted("kimball", "kimball") == 0
    assert cheap_ted("kimball", "kimbal") == 1
    assert name_similarity("Kimball", "Kimball (CTA Station)") == 1.0  # token subset
    assert name_similarity("Western Ave", "Western") == 1.0
    assert name_similarity("Kimball", "O'Hare") == 0.0


def test_geometry_hash_dedup():
    a = [(-87.64, 41.88), (-87.63, 41.88)]
    b = [(-87.6400000004, 41.8800000004), (-87.63, 41.88)]  # < 1e-6 apart
    c = [(-87.641, 41.88), (-87.63, 41.88)]
    assert geometry_hash(a) == geometry_hash(b)
    assert geometry_hash(a) != geometry_hash(c)


# ── gates: degenerate inputs must fail cleanly, never raise ──────────────────


def test_gates_zero_length_output_fails_cleanly():
    r = evaluate_gates(LineString([(0, 0), (0, 0)]), "rail", GateConfig())
    assert not r.passed
    assert r.failures == ["empty_output"]


def test_gates_zero_length_ref_line_no_crash():
    out = LineString([(0, 0), (100, 0)])
    r = evaluate_gates(
        out, "rail", GateConfig(), ref_line=LineString([(5, 5), (5, 5)]), dense=True
    )
    # degenerate ref: frechet / length-ratio gates skipped, no exception
    assert r.frechet_m is None and r.length_ratio is None
