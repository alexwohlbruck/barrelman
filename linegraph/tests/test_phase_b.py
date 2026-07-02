"""Synthetic exams for phase B: route attribution + station snapping.

Same authoring convention as test_synthetic.py: geometry in local meters
around a UTM-16N anchor, unprojected to lon/lat, through the REAL build
path — then attributed/labeled with hand-built Patterns and complexes.

Contracts (task spec):
  1. Y-split, route A rides trunk+left, route B trunk+right ->
     the trunk edge carries {A, B}; each branch carries exactly one.
  2. Station snap: a stop 40 m from a node labels it; a stop 400 m away
     labels nothing.
  3. Mid-corridor stations split the edge at the projection point —
     total geometry length is preserved (geometry never moves) and the
     new node is a labeled degree-2 "station" node.

Run:
  uv run --with-requirements linegraph/requirements.txt \
      python -m pytest linegraph/tests/test_phase_b.py -v
"""

import math

from pyproj import Transformer
from shapesnap.match import Pattern

from linegraph.attribute import attribute_patterns
from linegraph.build import build_linegraph
from linegraph.stations import StationComplex, snap_stations

EPSG = 32616
X0, Y0 = 445_000.0, 4_636_000.0
_TO_WGS = Transformer.from_crs(EPSG, 4326, always_xy=True)

MERGE = 18.0
RES = 1.0

TRUNK = [(0.0, 0.0), (300.0, 0.0)]
LEFT = TRUNK + [(600.0, 120.0)]
RIGHT = TRUNK + [(600.0, -120.0)]


def lonlat(pts_local):
    lons, lats = _TO_WGS.transform(
        [p[0] + X0 for p in pts_local], [p[1] + Y0 for p in pts_local]
    )
    return list(zip(lons, lats))


def make_pattern(route_id, shape_local, direction=0):
    return Pattern(
        route_id=route_id,
        direction_id=direction,
        stop_ids=(),
        stop_coords=[],
        stop_names=[],
        trip_count=1,
        shape_id=f"shp-{route_id}-{direction}",
        shape=lonlat(shape_local),
        route_short_name=route_id,
        route_long_name=f"Route {route_id}",
        route_color="ff0000" if route_id == "A" else "0000ff",
        route_type=1,
    )


def build_y():
    return build_linegraph(
        [lonlat(LEFT), lonlat(RIGHT)], MERGE, RES,
        build_key="test", feed_id="t", mode="rail", verbose=False,
    )


def classify_y_edges(lg):
    """-> (trunk_edge_pos, left_edge_pos, right_edge_pos) by geometry."""
    trunk = left = right = None
    for pos, e in enumerate(lg.edges):
        xs = [x - X0 for x, _ in e.coords_xy]
        ys = [y - Y0 for _, y in e.coords_xy]
        if max(xs) < 320.0 and all(abs(y) < 30.0 for y in ys):
            trunk = pos
        elif max(ys) > 60.0:
            left = pos
        elif min(ys) < -60.0:
            right = pos
    assert None not in (trunk, left, right), [e.coords_xy for e in lg.edges]
    return trunk, left, right


# ── 1. attribution: shared trunk carries both routes ─────────────────────────


def test_y_split_attribution_trunk_carries_both():
    lg = build_y()
    patterns = [
        make_pattern("A", LEFT, 0),
        make_pattern("A", LEFT[::-1], 1),   # opposite direction merges
        make_pattern("B", RIGHT, 0),
    ]
    edge_routes, stats = attribute_patterns(lg, patterns, feed_id="t")

    trunk, left, right = classify_y_edges(lg)
    ids = lambda pos: {rid for _, rid in edge_routes.get(pos, {})}
    assert ids(trunk) == {"A", "B"}, edge_routes
    assert ids(left) == {"A"}
    assert ids(right) == {"B"}

    for s in stats:
        assert s.n_samples > 10
        assert s.unmatched_fraction < 0.02, (s.pattern_key, s.unmatched_fraction)

    # merged across directions: A appears once per edge, with its colour
    info = edge_routes[trunk][("t", "A")]
    assert info.route_color == "ff0000"
    assert info.route_type == 1


# ── 2. station snap: 40 m labels the node, 400 m labels nothing ──────────────


def test_station_snap_40m_labels_400m_does_not():
    lg = build_y()
    end = min(
        (n for n in lg.nodes if n.degree == 1),
        key=lambda n: math.hypot(n.x - X0, n.y - Y0),
    )  # trunk-start endpoint
    # 40 m beyond the endpoint along the trunk axis -> nearest skeleton
    # point IS the node; 400 m off the trunk -> beyond MAX_SNAP_M.
    near = lonlat([(end.x - X0 - 40.0, end.y - Y0)])[0]
    far = lonlat([(150.0, 400.0)])[0]
    complexes = [
        StationComplex("S1", "Near Station", near[0], near[1], 2),
        StationComplex("S2", "Far Station", far[0], far[1], 1),
    ]
    n_edges_before = len(lg.edges)
    lg, snap = snap_stations(lg, complexes)

    assert snap.labels.get(end.node_id) == ("S1", "Near Station")
    assert [c.station_id for c, _, _ in snap.labeled] == ["S1"]
    assert [(c.station_id, r) for c, r, _ in snap.unlabeled] == [("S2", "too_far")]
    # node snap, no split: graph topology unchanged
    assert len(lg.edges) == n_edges_before
    assert snap.n_split_nodes == 0


# ── 3. mid-corridor station splits the edge on the line ─────────────────────


def test_mid_corridor_station_splits_edge_without_moving_geometry():
    lg = build_y()
    trunk, _, _ = classify_y_edges(lg)
    total_before = lg.total_length_m()
    trunk_len = lg.edges[trunk].length_m
    n_edges = len(lg.edges)

    stop = lonlat([(150.0, 30.0)])[0]  # 30 m off the trunk midpoint
    lg, snap = snap_stations(
        lg, [StationComplex("S3", "Mid Station", stop[0], stop[1], 2)]
    )

    assert snap.n_split_nodes == 1
    assert len(lg.edges) == n_edges + 1
    (comp, node_id, dist) = snap.labeled[0]
    assert comp.station_id == "S3" and 25.0 <= dist <= 35.0
    node = next(n for n in lg.nodes if n.node_id == node_id)
    assert node.kind == "station" and node.degree == 2
    # the node sits ON the trunk (y ~ 0), NOT at the stop (y = 30)
    assert abs(node.y - Y0) <= 2.0, node.y - Y0
    assert abs(node.x - X0 - 150.0) <= 5.0
    # geometry preserved: pieces sum to the old trunk, total unchanged
    assert math.isclose(lg.total_length_m(), total_before, rel_tol=1e-6)
    pieces = [e for e in lg.edges if node_id in (e.from_node, e.to_node)]
    assert len(pieces) == 2
    assert math.isclose(sum(e.length_m for e in pieces), trunk_len, rel_tol=1e-6)


# ── 4. stations-then-attribution: split pieces both carry the routes ─────────


def test_attribution_after_station_split_covers_both_pieces():
    lg = build_y()
    stop = lonlat([(150.0, 10.0)])[0]
    lg, snap = snap_stations(
        lg, [StationComplex("S3", "Mid Station", stop[0], stop[1], 2)]
    )
    patterns = [make_pattern("A", LEFT), make_pattern("B", RIGHT)]
    edge_routes, stats = attribute_patterns(lg, patterns, feed_id="t")

    station_node = next(iter(snap.labels))
    pieces = [
        pos for pos, e in enumerate(lg.edges)
        if station_node in (e.from_node, e.to_node)
    ]
    assert len(pieces) == 2
    for pos in pieces:
        assert {rid for _, rid in edge_routes[pos]} == {"A", "B"}, pos
    for s in stats:
        assert s.unmatched_fraction < 0.02
