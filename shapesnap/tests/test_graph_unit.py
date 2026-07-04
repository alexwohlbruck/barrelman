"""Unit tests for shapesnap.graph on a synthetic OSM XML fixture.

The fixture models the Chicago Loop failure mode in miniature: an elevated
way and a subway way cross at IDENTICAL coordinates but share no node ids;
a portal way is the only topological connection. Topology must come from
node ids, never geometry.

Run (repo convention):
  uv run --with osmium --with pytest python -m pytest shapesnap/tests/test_graph_unit.py -v
"""

from pathlib import Path

import pytest

from shapesnap.graph import (
    build_graph,
    classify_bus,
    connected_components,
    default_cache_path,
    is_regular_service_track,
    load_graph,
    save_graph,
)

FIXTURE = Path(__file__).parent / "fixtures" / "loop_portal.osm"


@pytest.fixture(scope="module")
def rail():
    return build_graph(FIXTURE, "rail")


@pytest.fixture(scope="module")
def bus():
    return build_graph(FIXTURE, "bus")


@pytest.fixture(scope="module")
def ferry():
    return build_graph(FIXTURE, "ferry")


def ways_of(graph):
    return {e.way_id for e in graph.edges}


def edges_of_way(graph, way_id):
    return [e for e in graph.edges if e.way_id == way_id]


def nodes_of_way(graph, way_id):
    out = set()
    for e in edges_of_way(graph, way_id):
        out.add(e.from_node)
        out.add(e.to_node)
    return out


# ── rail: tag filters ────────────────────────────────────────────────────────


def test_rail_kept_and_excluded_ways(rail):
    assert ways_of(rail) == {101, 102, 103, 104, 107, 108}


def test_rail_penalties(rail):
    assert edges_of_way(rail, 104)[0].class_penalty == pytest.approx(4.0)   # yard
    assert edges_of_way(rail, 108)[0].class_penalty == pytest.approx(2.0)   # industrial
    assert edges_of_way(rail, 107)[0].class_penalty == pytest.approx(1.0)   # embedded
    assert edges_of_way(rail, 101)[0].class_penalty == pytest.approx(1.0)


def test_regular_service_predicate():
    """Display-geometry predicate: yard/siding/spur/crossover service and
    industrial/military/tourism usage are NON-regular (excluded from the
    pair/platform midline + reconciliation ground truth); a plain running
    track is regular. A missing key means the qualifier is absent."""
    assert is_regular_service_track({"railway": "subway"})
    assert is_regular_service_track({"railway": "rail", "service": "main"})
    for svc in ("yard", "siding", "spur", "crossover"):
        assert not is_regular_service_track({"railway": "rail",
                                             "service": svc}), svc
    for use in ("industrial", "military", "tourism"):
        assert not is_regular_service_track({"railway": "rail",
                                             "usage": use}), use


def test_yard_track_matchable_but_display_excluded(rail):
    """A yard track stays PENALIZED-BUT-AVAILABLE in matching (trains reverse
    over crossovers at terminals — its edges exist with penalty 4.0) yet is
    EXCLUDED from the display ground truth by the regular-service predicate.
    The two concerns are independent: filtering display geometry never
    removes a yard from the matching graph."""
    yard_edges = edges_of_way(rail, 104)
    assert yard_edges, "the yard way must still be in the matching graph"
    assert yard_edges[0].class_penalty == pytest.approx(4.0)  # penalized
    # ...but the same tags fail the display-geometry predicate
    assert not is_regular_service_track(dict(yard_edges[0].tags))


# ── rail: the structural-separation property ─────────────────────────────────


def test_geometric_crossing_creates_no_junction(rail):
    """Node 2 (elevated) and node 12 (subway) share coordinates; neither may
    become a graph node — the crossing must be invisible to topology."""
    assert 2 not in rail.nodes
    assert 12 not in rail.nodes
    # each parallel way stays one un-split edge with full geometry
    (e101,) = edges_of_way(rail, 101)
    (e102,) = edges_of_way(rail, 102)
    assert len(e101.geometry) == 3
    assert len(e102.geometry) == 3
    # identical coordinates, disjoint node ids
    assert e101.geometry[1] == e102.geometry[1]
    assert nodes_of_way(rail, 101) & nodes_of_way(rail, 102) == set()


def test_components_merge_only_through_portal(rail):
    full = connected_components(rail.edges)
    containing = [c for c in full if 1 in c]
    assert len(containing) == 1
    assert 11 in containing[0], "portal must connect elevated to subway"

    without_portal = [e for e in rail.edges if e.way_id != 103]
    comps = connected_components(without_portal)
    comp_elevated = next(c for c in comps if 1 in c)
    comp_subway = next(c for c in comps if 11 in c)
    assert comp_elevated is not comp_subway, (
        "removing the portal way must split elevated and subway apart"
    )
    # and the portal attaches ONLY at the tagged endpoints (3 and 11)
    portal_nodes = nodes_of_way(rail, 103)
    assert portal_nodes & comp_elevated == {3}
    assert portal_nodes & comp_subway == {11}


def test_edge_geometry_and_length(rail):
    (e101,) = edges_of_way(rail, 101)
    assert e101.from_node == 1 and e101.to_node == 3
    assert e101.length_m > 0
    assert e101.tags.get("bridge") == "yes"
    assert e101.tags.get("layer") == "1"
    (e102,) = edges_of_way(rail, 102)
    assert e102.tags.get("tunnel") == "yes"
    assert e102.tags.get("layer") == "-2"


def test_route_refs_skip_platform_members(rail):
    (e102,) = edges_of_way(rail, 102)
    assert {"ref": "Red", "name": "Red Line", "colour": "#c60c30"} in e102.route_refs
    (e101,) = edges_of_way(rail, 101)  # platform role member: no route ref
    assert e101.route_refs == []


# ── bus: access, oneway, restrictions ────────────────────────────────────────


def test_bus_kept_and_excluded_ways(bus):
    # 107 is a residential street (with embedded rails) so bus keeps it too
    assert ways_of(bus) == {201, 202, 204, 206, 208, 209, 107}


def test_bus_oneway_flags(bus):
    assert edges_of_way(bus, 201)[0].oneway == 1     # oneway=yes
    assert edges_of_way(bus, 202)[0].oneway == 0     # oneway:bus=no override
    assert edges_of_way(bus, 208)[0].oneway == -1    # oneway=-1
    assert edges_of_way(bus, 209)[0].oneway == 0


def test_classify_bus_reversed_contraflow():
    """oneway:bus=-1 / oneway:psv=-1 must override plain oneway with -1,
    not fall through to it."""
    assert classify_bus({"highway": "residential", "oneway:bus": "-1"})["oneway"] == -1
    assert (
        classify_bus({"highway": "residential", "oneway": "yes", "oneway:bus": "-1"})["oneway"]
        == -1
    )
    assert (
        classify_bus({"highway": "primary", "oneway": "no", "oneway:psv": "-1"})["oneway"]
        == -1
    )
    # the existing override semantics stay intact
    assert (
        classify_bus({"highway": "residential", "oneway": "-1", "oneway:bus": "no"})["oneway"]
        == 0
    )
    assert classify_bus({"highway": "residential", "oneway": "-1"})["oneway"] == -1


def test_bus_turn_restrictions(bus):
    rs = {(r.via_node, r.from_way, r.to_way): r for r in bus.restrictions}
    r1 = rs[(72, 201, 202)]
    assert r1.kind == "no_left_turn"
    assert r1.applies_to_psv is True
    r2 = rs[(73, 202, 209)]
    assert r2.kind == "no_right_turn"
    assert r2.applies_to_psv is False  # except=psv honored
    assert len(bus.restrictions) == 2


def test_bus_route_refs(bus):
    (e201,) = edges_of_way(bus, 201)
    assert any(r["ref"] == "22" and r["colour"] == "#0000ff" for r in e201.route_refs)


def test_bus_junction_splits_edges(bus):
    # node 73 is shared by ways 202 and 209 -> graph node
    assert 73 in bus.nodes
    assert nodes_of_way(bus, 202) == {72, 73}


# ── ferry ────────────────────────────────────────────────────────────────────


def test_ferry_tagged_and_relation_member_ways(ferry):
    assert ways_of(ferry) == {301, 302}
    (e302,) = edges_of_way(ferry, 302)
    assert any(r["ref"] == "F1" for r in e302.route_refs)
    # the two ferry legs share node 82 -> one component
    comps = connected_components(ferry.edges)
    assert len(comps) == 1


# ── bbox crop + cache round-trip ─────────────────────────────────────────────


def test_bbox_crop():
    g = build_graph(FIXTURE, "rail", bbox=(-87.6305, 41.8790, -87.6280, 41.8815))
    assert ways_of(g) == {101, 102, 103, 104}
    assert g.bbox == (-87.6305, 41.8790, -87.6280, 41.8815)


@pytest.mark.parametrize(
    "bad",
    [
        (-87.6280, 41.8790, -87.6305, 41.8815),  # inverted lon
        (-87.6305, 41.8815, -87.6280, 41.8790),  # inverted lat
        (-87.6305, 41.8790, -87.6305, 41.8815),  # degenerate (minlon == maxlon)
        (-87.6305, 41.8790, -87.6280),           # wrong arity
        ("-87.6", "oops", "-87.5", "41.9"),      # non-numeric
    ],
)
def test_build_graph_rejects_bad_bbox(bad):
    """A degenerate/inverted bbox must raise, never silently return an
    empty graph (mirrors the CLI's _parse_bbox)."""
    with pytest.raises(ValueError):
        build_graph(FIXTURE, "rail", bbox=bad)


def test_route_refs_not_shared_between_way_edges(tmp_path):
    """Every edge of a split way must own its route_refs list: a consumer
    mutating one edge's list must not mutate its siblings'."""
    osm = tmp_path / "split_route.osm"
    osm.write_text(
        """<?xml version='1.0' encoding='UTF-8'?>
<osm version="0.6" generator="shapesnap-test">
  <node id="1" lat="41.0000" lon="-87.0000"/>
  <node id="2" lat="41.0010" lon="-87.0000"/>
  <node id="3" lat="41.0020" lon="-87.0000"/>
  <node id="4" lat="41.0010" lon="-87.0010"/>
  <way id="10">
    <nd ref="1"/><nd ref="2"/><nd ref="3"/>
    <tag k="highway" v="residential"/>
  </way>
  <way id="11">
    <nd ref="2"/><nd ref="4"/>
    <tag k="highway" v="residential"/>
  </way>
  <relation id="20">
    <member type="way" ref="10" role=""/>
    <tag k="type" v="route"/>
    <tag k="route" v="bus"/>
    <tag k="ref" v="X9"/>
    <tag k="name" v="X9 Express"/>
    <tag k="colour" v="#123456"/>
  </relation>
</osm>
"""
    )
    g = build_graph(osm, "bus")
    e1, e2 = [e for e in g.edges if e.way_id == 10]  # split at junction node 2
    assert e1.route_refs == e2.route_refs
    assert any(r["ref"] == "X9" for r in e1.route_refs)
    assert e1.route_refs is not e2.route_refs
    e1.route_refs.append({"ref": "MUT", "name": None, "colour": None})
    assert all(r["ref"] != "MUT" for r in e2.route_refs)


def test_cache_round_trip(rail, tmp_path):
    out = tmp_path / "fixture.rail.graph.pkl.gz"
    save_graph(rail, out)
    loaded = load_graph(out, expect_pbf=FIXTURE)
    assert loaded.mode == "rail"
    assert len(loaded.edges) == len(rail.edges)
    assert len(loaded.nodes) == len(rail.nodes)
    assert loaded.source_size == FIXTURE.stat().st_size
    # staleness: lie about the source
    loaded.source_size += 1
    from shapesnap.graph import is_stale

    assert is_stale(loaded, FIXTURE)


def test_default_cache_path():
    p = default_cache_path("/x/data/il.osm.pbf", "rail")
    assert str(p) == "/x/data/shapesnap/il.rail.graph.pkl.gz"


def test_load_graph_unreadable_cache_raises_value_error(tmp_path):
    """Corrupt/garbage caches — and pre-f0933b8 caches pickled under
    __main__.* — must raise ValueError('… rebuild'), never an opaque
    unpickling error."""
    import gzip

    not_gzip = tmp_path / "notgzip.rail.graph.pkl.gz"
    not_gzip.write_bytes(b"this is not a gzip stream")
    with pytest.raises(ValueError, match="rebuild"):
        load_graph(not_gzip)

    not_pickle = tmp_path / "notpickle.rail.graph.pkl.gz"
    with gzip.open(not_pickle, "wb") as f:
        f.write(b"gzipped garbage, definitely not a pickle")
    with pytest.raises(ValueError, match="rebuild"):
        load_graph(not_pickle)

    # pre-f0933b8 CLI caches: dataclasses pickled as __main__.* — the
    # GLOBAL opcode below resolves against __main__, which has no such class
    orphan = tmp_path / "orphan.rail.graph.pkl.gz"
    with gzip.open(orphan, "wb") as f:
        f.write(b"c__main__\nDefinitelyNotARealGraphClass\n.")
    with pytest.raises(ValueError, match="rebuild"):
        load_graph(orphan)

    # a valid pickle of a non-graph object is also 'unreadable'
    import pickle

    wrong_obj = tmp_path / "wrongobj.rail.graph.pkl.gz"
    with gzip.open(wrong_obj, "wb") as f:
        pickle.dump({"not": "a ModeGraph"}, f)
    with pytest.raises(ValueError, match="rebuild"):
        load_graph(wrong_obj)

    # missing file stays a FileNotFoundError, not ValueError
    with pytest.raises(FileNotFoundError):
        load_graph(tmp_path / "missing.rail.graph.pkl.gz")


def test_cli_cache_loads_from_other_entry_points(tmp_path):
    """Regression: caches written by `python -m shapesnap.graph` must pickle
    dataclasses as shapesnap.graph.*, not __main__.*."""
    import subprocess
    import sys

    out = tmp_path / "cli.rail.graph.pkl.gz"
    repo_root = Path(__file__).resolve().parents[2]
    subprocess.run(
        [sys.executable, "-m", "shapesnap.graph",
         "--pbf", str(FIXTURE), "--mode", "rail", "--out", str(out)],
        check=True, cwd=repo_root, capture_output=True,
    )
    loaded = load_graph(out, expect_pbf=FIXTURE)
    assert type(loaded).__module__ == "shapesnap.graph"
    assert {e.way_id for e in loaded.edges} == {101, 102, 103, 104, 107, 108}
