"""Real-data exam: rail graph around Chicago, elevated-vs-subway separation.

Builds the rail matching graph from a local pbf that covers the Chicago Loop
(data/il.osm.pbf or data/region.osm.pbf, whichever header bbox contains it;
skips when neither is present). The acceptance property from
docs/transit-pipeline-v3.md stage 3: elevated Loop ways (bridge / layer>=1)
and subway ways (tunnel / layer<0) must share NO graph nodes inside the Loop
bbox — vertical separation is structural because topology comes only from
shared OSM node ids.

Run (slow — full pbf scan; skipped automatically when no Chicago pbf):
  uv run --with osmium --with pytest python -m pytest shapesnap/tests/test_graph_real.py -v -s
"""

import time
from pathlib import Path

import osmium
import pytest

from shapesnap.graph import REPO_ROOT, build_graph

# Chicago region crop (config/regions.json chicago bbox; north edge must
# clear the Purple line's Linden terminal at 42.0734)
CHI_BBOX = (-87.95, 41.64, -87.52, 42.09)
# The Loop exam window
LOOP_BBOX = (-87.64, 41.87, -87.62, 41.89)

PBF_CANDIDATES = (
    REPO_ROOT / "data" / "il.osm.pbf",
    REPO_ROOT / "data" / "region.osm.pbf",
)


def _covers(path: Path, bbox) -> bool:
    reader = osmium.io.Reader(str(path), osmium.osm.NOTHING)
    try:
        box = reader.header().box()
    finally:
        reader.close()
    if not box.valid():
        return True  # no header bbox: let the build decide
    return (
        box.bottom_left.lon <= bbox[0]
        and box.bottom_left.lat <= bbox[1]
        and box.top_right.lon >= bbox[2]
        and box.top_right.lat >= bbox[3]
    )


def _find_pbf():
    for cand in PBF_CANDIDATES:
        if cand.exists() and _covers(cand, LOOP_BBOX):
            return cand
    return None


@pytest.fixture(scope="module")
def chi_rail():
    pbf = _find_pbf()
    if pbf is None:
        pytest.skip("no local pbf covering the Chicago Loop")
    t0 = time.perf_counter()
    graph = build_graph(pbf, "rail", bbox=CHI_BBOX)
    elapsed = time.perf_counter() - t0
    print(
        f"\n[real] {pbf.name} rail graph (Chicago crop): "
        f"{len(graph.nodes)} nodes, {len(graph.edges)} edges, "
        f"{len(graph.restrictions)} restrictions in {elapsed:.1f}s"
    )
    return graph


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


def _in_loop(edge) -> bool:
    minlon, minlat, maxlon, maxlat = LOOP_BBOX
    return any(
        minlon <= lon <= maxlon and minlat <= lat <= maxlat
        for lon, lat in edge.geometry
    )


def test_graph_is_substantial(chi_rail):
    # Chicago rail (CTA + Metra + freight): well into the thousands
    assert len(chi_rail.nodes) > 1000
    assert len(chi_rail.edges) > 1000
    assert all(e.length_m >= 0 for e in chi_rail.edges)


def test_route_relations_attached(chi_rail):
    """CTA L route relations (Brown/Red/...) must decorate Loop edges."""
    refs = set()
    for e in chi_rail.edges:
        if _in_loop(e):
            for r in e.route_refs:
                if r["ref"]:
                    refs.add(r["ref"])
    print(f"[real] route refs on Loop edges: {sorted(refs)}")
    cta = {"Brown", "Purple", "Orange", "Green", "Pink", "Blue", "Red"}
    named = {ref for ref in refs for line in cta if line.lower() in ref.lower()}
    assert named, f"expected CTA line refs among Loop route_refs, got {sorted(refs)}"


def test_loop_elevated_subway_share_no_nodes(chi_rail):
    """THE exam: inside the Loop bbox, elevated and subway edge sets must be
    node-disjoint. Portals (where tunnels surface) live outside this window."""
    elevated = [e for e in chi_rail.edges if _in_loop(e) and _is_elevated(e.tags)]
    subway = [e for e in chi_rail.edges if _in_loop(e) and _is_subway(e.tags)]

    assert len(elevated) > 10, "expected the elevated Loop structure in the window"
    assert len(subway) > 10, "expected State/Dearborn subway tubes in the window"

    def node_set(edges):
        out = set()
        for e in edges:
            out.add(e.from_node)
            out.add(e.to_node)
        return out

    elev_nodes, sub_nodes = node_set(elevated), node_set(subway)
    shared = elev_nodes & sub_nodes

    sample = sorted({e.way_id for e in elevated})[:8]
    print(
        f"[real] loop window: {len(elevated)} elevated edges "
        f"({len(elev_nodes)} nodes), {len(subway)} subway edges "
        f"({len(sub_nodes)} nodes); sample elevated ways: {sample}"
    )
    if shared:
        evidence = [
            (n, [(e.way_id, e.tags) for e in elevated + subway
                 if n in (e.from_node, e.to_node)])
            for n in sorted(shared)
        ]
        pytest.fail(f"elevated and subway share graph nodes in the Loop: {evidence}")


def test_loop_elevated_carries_l_routes(chi_rail):
    """Elevated Loop edges should carry the Loop lines' route relations
    (Brown/Orange/Green/Pink/Purple), and subway edges Red/Blue — spot-check
    that relation decoration lands on the right vertical layer."""
    def refs_of(edges):
        out = set()
        for e in edges:
            for r in e.route_refs:
                if r["ref"]:
                    out.add(r["ref"].lower())
        return out

    elevated_refs = refs_of(
        e for e in chi_rail.edges if _in_loop(e) and _is_elevated(e.tags)
    )
    subway_refs = refs_of(
        e for e in chi_rail.edges if _in_loop(e) and _is_subway(e.tags)
    )
    print(f"[real] elevated refs: {sorted(elevated_refs)}")
    print(f"[real] subway refs: {sorted(subway_refs)}")

    assert any("brown" in r or "orange" in r or "green" in r for r in elevated_refs)
    assert any("red" in r or "blue" in r for r in subway_refs)
    # cross-contamination check: Red/Blue must not decorate the elevated Loop
    assert not any(r in ("red", "red line") for r in elevated_refs)
    assert not any(r in ("brown", "brown line") for r in subway_refs)
