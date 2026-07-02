"""Unit tests for shapesnap.match / candidates / gates on a synthetic graph.

Scenarios (fixtures/match_scenarios.osm):
  A. parallel ways with NO shared nodes; a shape hugging way 401 must
     match way 401 end-to-end (never its 40 m-away twin 402),
  B. a hole mid-route: the matcher must break, match the sub-traces and
     bridge the hole with the ORIGINAL shape segment (gap recorded),
  C. a shape offset ~40 m from the only way: candidates exist but the
     coverage gate fails -> method=fallback, original returned unchanged.

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
    assert 50 < gap["bridged_m"] < 300, gap
    # both sides matched, nothing forced across the hole
    used_ways = {mg.graph.edges[i].way_id for i in r.edges_used}
    assert used_ways == {403, 404}
    # the bridge keeps the output continuous across the hole (gates pass)
    assert r.gates is not None and r.gates.passed
    # break halves the confidence
    assert r.confidence < 0.6


# ── C: gate failure -> fallback, original unchanged ──────────────────────────


def test_bad_match_fails_gates_and_falls_back(mg):
    shape = [(x, 41.88564) for x in lons(-87.6400, -87.6300, 0.0005)]
    stops = [shape[0], shape[-1]]
    r = match_pattern(mg, mk_pattern(shape, stops))

    assert r.method == "fallback"
    assert r.gates is not None and not r.gates.passed
    assert any("coverage" in f for f in r.gates.failures), r.gates.failures
    assert r.coords == shape, "fallback must return the ORIGINAL geometry unchanged"


def test_off_network_shape_is_passthrough(mg):
    shape = [(x, 41.9200) for x in lons(-87.6400, -87.6300, 0.0005)]
    r = match_pattern(mg, mk_pattern(shape, [shape[0], shape[-1]]))
    assert r.method == "passthrough"
    assert r.coords == shape


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
