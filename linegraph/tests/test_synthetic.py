"""Synthetic exams for the raster-skeleton-vectorize pipeline.

Tiny shape sets built in code (no OSM, no GTFS). Geometry is authored in
local meters around a Chicago-ish UTM-16N anchor and unprojected to
lon/lat, so the tests exercise the exact production path (projection,
snapped grid origin, stroking, skeletonization, vectorization, cleanup).

The five contracts (docs/transit-pipeline-v3.md stage 4):
  1. two parallel lines 8 m apart, MERGE_WIDTH 18  -> ONE line on the midline
  2. two lines crossing at 90 degrees              -> X: 4 edges, 1 junction,
     arms straight away from the crossing (the Tower 18 property)
  3. two parallel lines 40 m apart                 -> TWO separate lines
  4. a Y-split                                     -> 3 edges, 1 junction
  5. determinism: same input twice                 -> identical graph

Run:
  uv run --with-requirements linegraph/requirements.txt \
      python -m pytest linegraph/tests/test_synthetic.py -v
"""

import math

from pyproj import Transformer

from linegraph.build import build_linegraph

# anchor in UTM zone 16N (Chicago-ish) so pick_epsg() lands on 32616
EPSG = 32616
X0, Y0 = 445_000.0, 4_636_000.0
_TO_WGS = Transformer.from_crs(EPSG, 4326, always_xy=True)

MERGE = 18.0
RES = 1.0


def lonlat(pts_local):
    """Local meter coords -> lon/lat via the anchor."""
    lons, lats = _TO_WGS.transform(
        [p[0] + X0 for p in pts_local], [p[1] + Y0 for p in pts_local]
    )
    return list(zip(lons, lats))


def build(shapes_local, merge_width=MERGE, res=RES):
    return build_linegraph(
        [lonlat(s) for s in shapes_local], merge_width, res,
        build_key="test", feed_id="test", mode="rail", verbose=False,
    )


def local_xy(edge):
    return [(x - X0, y - Y0) for x, y in edge.coords_xy]


# ── 1. parallel lines within MERGE_WIDTH fuse to the midline ─────────────────


def test_parallel_8m_merges_to_one_midline():
    lg = build([
        [(0.0, 0.0), (600.0, 0.0)],
        [(0.0, 8.0), (600.0, 8.0)],
    ])
    assert len(lg.edges) == 1, [e.coords_xy for e in lg.edges]
    assert len(lg.nodes) == 2
    assert {n.kind for n in lg.nodes} == {"endpoint"}
    pts = local_xy(lg.edges[0])
    # ON the midline y = 4, within ~1.5 px
    for x, y in pts:
        assert abs(y - 4.0) <= 1.5 * RES, (x, y)
    xs = [p[0] for p in pts]
    # covers the body of the corridor (endcap contraction ~ stroke width ok)
    assert min(xs) <= 30 and max(xs) >= 570, (min(xs), max(xs))


# ── 2. 90-degree crossing = X junction, arms unmolested ──────────────────────


def test_perpendicular_crossing_is_x_junction():
    lg = build([
        [(-300.0, 0.0), (300.0, 0.0)],
        [(0.0, -300.0), (0.0, 300.0)],
    ])
    assert len(lg.edges) == 4, [(e.from_node, e.to_node) for e in lg.edges]
    junctions = [n for n in lg.nodes if n.degree > 2]
    ends = [n for n in lg.nodes if n.degree == 1]
    assert len(junctions) == 1 and junctions[0].degree == 4
    assert len(ends) == 4
    jx, jy = junctions[0].x - X0, junctions[0].y - Y0
    assert math.hypot(jx, jy) <= 5.0, (jx, jy)
    # all 4 edges meet at the junction node
    j = junctions[0].node_id
    assert all(j in (e.from_node, e.to_node) for e in lg.edges)
    # arms stay on their axes away from the crossing — no merged/diagonal
    # geometry (crossing-but-not-parallel must never exchange geometry)
    for e in lg.edges:
        for x, y in local_xy(e):
            if math.hypot(x, y) > 25.0:
                assert min(abs(x), abs(y)) <= 2.5 * RES, (x, y)


# ── 3. parallel lines beyond MERGE_WIDTH stay separate ───────────────────────


def test_parallel_40m_stays_two_lines():
    lg = build([
        [(0.0, 0.0), (600.0, 0.0)],
        [(0.0, 40.0), (600.0, 40.0)],
    ])
    assert len(lg.edges) == 2
    assert len(lg.nodes) == 4
    assert len(lg.components()) == 2
    lanes = set()
    for e in lg.edges:
        ys = [y for _, y in local_xy(e)]
        mean_y = sum(ys) / len(ys)
        lane = 0.0 if abs(mean_y) < 5 else 40.0
        lanes.add(lane)
        for y in ys:
            assert abs(y - lane) <= 1.5 * RES, (lane, y)
    assert lanes == {0.0, 40.0}


# ── 4. Y-split = 3 edges around 1 junction ───────────────────────────────────


def test_y_split_three_edges_one_junction():
    trunk = [(0.0, 0.0), (300.0, 0.0)]
    lg = build([
        trunk + [(600.0, 120.0)],
        trunk + [(600.0, -120.0)],
    ])
    assert len(lg.edges) == 3, [(e.from_node, e.to_node) for e in lg.edges]
    junctions = [n for n in lg.nodes if n.degree > 2]
    ends = [n for n in lg.nodes if n.degree == 1]
    assert len(junctions) == 1 and junctions[0].degree == 3
    assert len(ends) == 3
    # junction sits at the split, allowing the stroke-fusion pull-forward
    jx, jy = junctions[0].x - X0, junctions[0].y - Y0
    assert abs(jy) <= 5.0 and 300.0 - 5.0 <= jx <= 300.0 + 2.5 * MERGE, (jx, jy)
    assert len(lg.components()) == 1


# ── 5. determinism ───────────────────────────────────────────────────────────


def _fingerprint(lg):
    return (
        [(n.node_id, n.lon, n.lat, n.x, n.y, n.degree, n.kind) for n in lg.nodes],
        [(e.edge_id, e.from_node, e.to_node, e.px_len, e.length_m, tuple(e.coords))
         for e in lg.edges],
        lg.origin, lg.grid_shape, lg.input_digest, lg.epsg,
    )


def test_determinism_same_input_identical_graph():
    shapes = [
        [(0.0, 0.0), (300.0, 0.0), (600.0, 120.0)],
        [(0.0, 0.0), (300.0, 0.0), (600.0, -120.0)],
        [(-100.0, -200.0), (700.0, -200.0)],
        [(100.0, -500.0), (100.0, 300.0)],
    ]
    lg1 = build(shapes)
    lg2 = build(shapes)
    assert _fingerprint(lg1) == _fingerprint(lg2)
