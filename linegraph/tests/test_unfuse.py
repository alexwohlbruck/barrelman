"""Synthetic exams for the corridor unfuse (linegraph.unfuse).

Same authoring convention as test_refit.py: local meters around a UTM
anchor, through the real build path, then unfuse against the stamped
shapes with explicit family keys.

Contracts (PAR-12 v3, Brooklyn Bridge / Whitehall receipts):
  a. tangent kiss of two DIFFERENT-family corridors (V-shaped gap
     valley, min below the merge width, diverging past it well within
     ESCAPE_DIST on both sides) is split: no edge carries both
     families afterwards, each family's corridor hugs its own track
     through the ex-zone, and both corridors stay connected;
  b. the same kiss within ONE family builds no zone and changes
     nothing (the raster's fusion is the desired bundling);
  c. two families running parallel-but-separate for kilometers
     (Flatbush Av / Lake St in miniature) stay fused — sustained
     closeness on either side of the closest approach is a bundle;
  d. a genuine fork (families share the trunk, one diverges) stays
     fused — the shared side never escapes.
"""

import math

import numpy as np
from pyproj import Transformer
from shapely.geometry import LineString

from linegraph.build import build_linegraph
from linegraph.unfuse import unfuse_corridors

EPSG = 32616
X0, Y0 = 445_000.0, 4_636_000.0
_TO_WGS = Transformer.from_crs(EPSG, 4326, always_xy=True)

MERGE = 18.0
RES = 1.0


def lonlat(pts_local):
    lons, lats = _TO_WGS.transform(
        [p[0] + X0 for p in pts_local], [p[1] + Y0 for p in pts_local]
    )
    return list(zip(lons, lats))


def build(shapes_local):
    return build_linegraph(
        [lonlat(s) for s in shapes_local], MERGE, RES,
        build_key="test", feed_id="test", mode="rail", verbose=False,
    )


def dense_local(pts_local, step=10.0):
    line = LineString(pts_local)
    n = max(2, int(line.length / step))
    return [line.interpolate(i / n, normalized=True) for i in range(n + 1)]


def ridden_families(lg, shapes_local, fams):
    """family -> set(edge ids) via fresh coarse attribution."""
    from pyproj import Transformer as T

    from linegraph.attribute import EdgeSnapIndex, attribute_shape_xy

    to_xy = T.from_crs(4326, lg.epsg, always_xy=True)
    index = EdgeSnapIndex(lg)
    out: dict = {}
    for s, fam in zip(shapes_local, fams):
        ll = lonlat(s)
        xs, ys = to_xy.transform([c[0] for c in ll], [c[1] for c in ll])
        ridden, _, _, _ = attribute_shape_xy(index, list(zip(xs, ys)))
        out.setdefault(fam, set()).update(lg.edges[p].edge_id for p in ridden)
    return out


KISS_A = [(-800.0, 0.0), (800.0, 0.0)]
KISS_B = [(-800.0, 150.0), (-300.0, 60.0), (0.0, 8.0),
          (300.0, 60.0), (800.0, 150.0)]


def test_tangent_kiss_of_two_families_splits():
    lg = build([KISS_A, KISS_B])
    fused = [e for e in lg.edges]
    stats = unfuse_corridors(
        lg, [lonlat(KISS_A), lonlat(KISS_B)],
        [frozenset({"green"}), frozenset({"brown"})], verbose=False)
    assert stats.n_split == 1, (stats.n_zones, stats.skipped_zones,
                                stats.kept_zones)
    rid = ridden_families(lg, [KISS_A, KISS_B], ["green", "brown"])
    assert rid["green"] and rid["brown"]
    assert not (rid["green"] & rid["brown"]), rid
    # each family's corridor hugs its own track through the ex-zone
    # (pre-refit bound: the arms outside the zone keep skeleton geometry,
    # whose bow toward the erstwhile blob the later refit removes)
    by_id = {e.edge_id: e for e in lg.edges}
    for fam, track in (("green", KISS_A), ("brown", KISS_B)):
        track_line = LineString(track)
        for eid in rid[fam]:
            exy = [(x - X0, y - Y0) for x, y in by_id[eid].coords_xy]
            dev = max(track_line.distance(p) for p in dense_local(exy))
            assert dev < 6.0, (fam, eid, dev)
    # connectivity survives per family: one connected run each
    for fam in ("green", "brown"):
        nodes: dict = {}
        for eid in rid[fam]:
            e = by_id[eid]
            nodes.setdefault(e.from_node, set()).add(eid)
            nodes.setdefault(e.to_node, set()).add(eid)
        seen, stack = set(), [next(iter(rid[fam]))]
        while stack:
            cur = stack.pop()
            if cur in seen:
                continue
            seen.add(cur)
            e = by_id[cur]
            for nid in (e.from_node, e.to_node):
                stack.extend(nodes[nid] - seen)
        assert seen == rid[fam], (fam, seen, rid[fam])
    assert len(lg.edges) != len(fused) or stats.n_edges_added


def test_same_family_kiss_stays_fused():
    lg = build([KISS_A, KISS_B])
    n_edges = len(lg.edges)
    stats = unfuse_corridors(
        lg, [lonlat(KISS_A), lonlat(KISS_B)],
        [frozenset({"green"}), frozenset({"green"})], verbose=False)
    assert stats.n_zones == 0
    assert stats.n_split == 0
    assert len(lg.edges) == n_edges


def test_long_parallel_families_stay_fused():
    # 3 km side by side 8 m apart, diverging only at the far ends:
    # sustained closeness = bundle (Flatbush Av / Lake St in miniature)
    a = [(-2000.0, 0.0), (2000.0, 0.0)]
    b = [(-2000.0, 120.0), (-1700.0, 8.0), (1700.0, 8.0), (2000.0, 120.0)]
    lg = build([a, b])
    stats = unfuse_corridors(
        lg, [lonlat(a), lonlat(b)],
        [frozenset({"green"}), frozenset({"yellow"})], verbose=False)
    assert stats.n_split == 0, stats.split_zones
    assert stats.n_kept >= 1


def test_fork_stays_fused():
    ang = math.radians(30.0)
    trunk = [(-900.0, 0.0), (900.0, 0.0)]
    branch = [(-900.0, 0.0), (0.0, 0.0),
              (900.0 * math.cos(ang), 900.0 * math.sin(ang))]
    lg = build([trunk, branch])
    stats = unfuse_corridors(
        lg, [lonlat(trunk), lonlat(branch)],
        [frozenset({"blue"}), frozenset({"lime"})], verbose=False)
    assert stats.n_split == 0, stats.split_zones


def test_unfuse_is_deterministic():
    def run():
        lg = build([KISS_A, KISS_B])
        unfuse_corridors(
            lg, [lonlat(KISS_A), lonlat(KISS_B)],
            [frozenset({"green"}), frozenset({"brown"})], verbose=False)
        return (
            [(n.node_id, n.x, n.y) for n in lg.nodes],
            [(e.edge_id, e.from_node, e.to_node,
              tuple(map(tuple, e.coords_xy))) for e in lg.edges],
        )

    assert run() == run()
