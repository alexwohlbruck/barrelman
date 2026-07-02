#!/usr/bin/env python3
"""linegraph.attribute — map pattern shapes onto skeleton edges (phase B.1).

Each pattern's matched shape (the SAME shapes the raster was built from)
is resampled every ~SAMPLE_M meters; every sample snaps to its nearest
skeleton edge (STRtree, vectorized query_nearest). The per-sample edge
sequence is collapsed into an edge chain, crossing bleed is suppressed
(short redundant runs picked up while passing straight through a
junction — see _chain_edges), and single-edge gaps (an edge shorter
than the sample spacing hopped over between two samples) are filled
through graph adjacency. The union of chain edges is the set of edges
the pattern rides.

Snap radius: the skeleton was built FROM these shapes, so samples are
near it by construction — but not ON it: parallel-track fusing puts the
centerline up to ~MERGE_WIDTH/2 off each source track, and junction
contraction / endcap erosion (vector.cleanup_graph works at 1–2 x
MERGE_WIDTH) locally displaces geometry further. Default radius
2 x MERGE_WIDTH covers all of that while still rejecting samples the
skeleton genuinely does not represent. Unmatched fraction is reported
per pattern and should be ~0.

Converse deviation gate: snapping guarantees samples are near SOME
edge, not that every claimed edge is near the pattern's shape — the
adjacency bridge fill (and one-sample junction bleed the run filter
misses) can pull in edges the route never rides, painting phantom
ribbons hundreds of metres off the route's own track (NYC: R on the
7's Queens Blvd elevated, B on the White Plains Rd corridor). After
chaining, any edge whose densified geometry deviates more than
DEVIATION_GATE_M from the pattern's shape ANYWHERE is excised from
that pattern (an edge genuinely ridden stays within snap radius plus
local junction displacement everywhere, so the gate never fires on
it). Excisions are counted per pattern in the stats.

Output: per skeleton edge, the merged-across-directions set of routes
riding it, carrying (feed_id, route_id, route_short_name, route_type,
route_color, route_text_color). route_text_color is not part of
shapesnap's Pattern, so it is read straight from the zip's routes.txt
(load_routes_meta).
"""

from __future__ import annotations

import csv
import io
import math
import zipfile
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path

import numpy as np
import shapely
from pyproj import Transformer
from shapely import STRtree
from shapely.geometry import LineString

DEFAULT_SAMPLE_M = 15.0
# max deviation of a claimed edge from the pattern's own shape; beyond
# this the edge is excised from the pattern (see module docstring)
DEVIATION_GATE_M = 50.0
EDGE_SAMPLE_M = 25.0  # densification step for the gate's edge probes


@dataclass(slots=True, frozen=True)
class RouteInfo:
    feed_id: str
    route_id: str
    route_short_name: str
    route_type: int
    route_color: str
    route_text_color: str


@dataclass(slots=True)
class PatternAttribution:
    pattern_key: str
    route_id: str
    n_samples: int
    n_unmatched: int
    n_edges: int
    n_excised: int = 0  # chain edges dropped by the deviation gate

    @property
    def unmatched_fraction(self) -> float:
        return self.n_unmatched / self.n_samples if self.n_samples else 1.0


def load_routes_meta(zip_path) -> dict:
    """routes.txt -> {route_id: {short, type, color, text_color}}.

    Pattern objects carry short name / type / color already; this exists
    for route_text_color (and as the single source should the two ever
    disagree — routes.txt wins).
    """
    meta: dict = {}
    with zipfile.ZipFile(Path(zip_path)) as zf:
        with zf.open("routes.txt") as f:
            for r in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                meta[r["route_id"]] = {
                    "short": (r.get("route_short_name") or "").strip(),
                    "type": int(r.get("route_type") or 3),
                    "color": (r.get("route_color") or "").strip(),
                    "text_color": (r.get("route_text_color") or "").strip(),
                }
    return meta


def sample_polyline_xy(coords_xy, step_m: float) -> np.ndarray:
    """Resample a projected polyline at <= step_m spacing (endpoints kept)."""
    pts = np.asarray(coords_xy, dtype=float)
    if len(pts) < 2:
        return pts
    seg = np.hypot(*(pts[1:] - pts[:-1]).T)
    cum = np.concatenate([[0.0], np.cumsum(seg)])
    total = float(cum[-1])
    if total <= 0.0:
        return pts[:1]
    n = max(1, math.ceil(total / step_m))
    targets = np.linspace(0.0, total, n + 1)
    return np.column_stack(
        [np.interp(targets, cum, pts[:, 0]), np.interp(targets, cum, pts[:, 1])]
    )


def _edge_adjacency(lg):
    """(edge nodes list, node -> set(edge positions)) over lg.edges order."""
    edge_nodes = [(e.from_node, e.to_node) for e in lg.edges]
    node_edges: dict = {}
    for i, (a, b) in enumerate(edge_nodes):
        node_edges.setdefault(a, set()).add(i)
        node_edges.setdefault(b, set()).add(i)
    return edge_nodes, node_edges


class EdgeSnapIndex:
    """STRtree + adjacency + densified edge probes over lg.edges.

    Shared by every consumer of the per-shape attribution core
    (attribute_patterns and linegraph.refit's coarse pass) so the
    sample-snap-chain-gate logic exists exactly once. Holds positional
    references into lg.edges — build one per graph STATE and discard it
    whenever edges are split, pruned, or re-geometried.
    """

    def __init__(self, lg):
        self.lg = lg
        self.tree = STRtree([LineString(e.coords_xy) for e in lg.edges])
        self.edge_nodes, self.node_edges = _edge_adjacency(lg)
        # densified edge probes for the converse deviation gate, built
        # lazily (an edge is probed only once a chain claims it)
        self._probes: dict = {}

    def probes_for(self, eid: int):
        pts = self._probes.get(eid)
        if pts is None:
            samples = sample_polyline_xy(self.lg.edges[eid].coords_xy,
                                         EDGE_SAMPLE_M)
            pts = shapely.points(samples[:, 0], samples[:, 1])
            self._probes[eid] = pts
        return pts


def attribute_shape_xy(index: EdgeSnapIndex, shape_xy, *,
                       sample_m: float = DEFAULT_SAMPLE_M,
                       snap_radius_m: float | None = None,
                       deviation_gate_m: float = DEVIATION_GATE_M):
    """One projected shape through the full sample-snap-chain-gate core.

    Returns (ridden edge positions, n_samples, n_unmatched, n_excised).
    """
    lg = index.lg
    if snap_radius_m is None:
        snap_radius_m = 2.0 * lg.merge_width_m
    samples = sample_polyline_xy(shape_xy, sample_m)
    points = shapely.points(samples[:, 0], samples[:, 1])
    nearest = np.full(len(points), -1, dtype=np.int64)
    pairs = index.tree.query_nearest(
        points, max_distance=snap_radius_m, all_matches=False
    )
    nearest[pairs[0]] = pairs[1]

    chain = _chain_edges(
        [int(e) for e in nearest[nearest >= 0]], lg,
        index.edge_nodes, index.node_edges,
    )

    # converse gate: excise edges the shape never comes near — max
    # deviation over the DENSIFIED edge, not just vertices
    shape_line = LineString(shape_xy)
    shapely.prepare(shape_line)
    ridden, n_excised = [], 0
    for eid in sorted(set(chain)):
        dmax = float(shapely.distance(index.probes_for(eid), shape_line).max())
        if dmax <= deviation_gate_m:
            ridden.append(eid)
        else:
            n_excised += 1
    return ridden, len(points), int((nearest < 0).sum()), n_excised


BLEED_SUPPORT = 2  # runs this short at a junction are crossing artifacts


def _chain_edges(seq, lg, edge_nodes, node_edges):
    """Collapse a per-sample edge sequence into a connected chain.

    Consecutive duplicates collapse into runs. A short run (<=
    BLEED_SUPPORT samples) whose surrounding runs are already mutually
    adjacent is CROSSING BLEED — at a plan-view crossing the junction
    node swallows ~MERGE_WIDTH of geometry, so one or two samples of a
    route passing straight through can momentarily sit nearest to a
    perpendicular arm it never rides (the subway picking up Loop
    elevated edges). Redundant short runs are dropped; a genuinely
    ridden short edge is never redundant (its neighbors only connect
    THROUGH it), so it survives.

    Then a single edge skipped between two samples (shorter than the
    sample spacing) is re-inserted when exactly reachable through one
    shared-node hop. Longer gaps are left as chain breaks — the
    unmatched fraction, not the chain, is the QA signal.
    """
    runs = [(e, len(list(g))) for e, g in groupby(seq)]
    if not runs:
        return []

    def linked(i, j):
        a, b = runs[i][0], runs[j][0]
        return a == b or bool(set(edge_nodes[a]) & set(edge_nodes[b]))

    chain = [
        e for i, (e, cnt) in enumerate(runs)
        if not (cnt <= BLEED_SUPPORT and 0 < i < len(runs) - 1
                and linked(i - 1, i + 1))
    ]
    if not chain:
        return []
    filled = [chain[0]]
    for nxt in chain[1:]:
        cur = filled[-1]
        if nxt == cur:
            continue
        ca, cb = edge_nodes[cur]
        na, nb = edge_nodes[nxt]
        if {ca, cb} & {na, nb}:
            filled.append(nxt)
            continue
        touching_cur = node_edges.get(ca, set()) | node_edges.get(cb, set())
        touching_nxt = node_edges.get(na, set()) | node_edges.get(nb, set())
        bridge = sorted(
            (touching_cur & touching_nxt) - {cur, nxt},
            key=lambda i: (lg.edges[i].length_m, i),
        )
        if bridge:
            filled.append(bridge[0])
        filled.append(nxt)
    return filled


def attribute_patterns(lg, patterns, feed_id: str, routes_meta=None, *,
                       sample_m: float = DEFAULT_SAMPLE_M,
                       snap_radius_m: float | None = None,
                       deviation_gate_m: float = DEVIATION_GATE_M):
    """Attribute every pattern's shape to skeleton edges.

    Returns (edge_routes, stats):
      edge_routes: {edge position: {(feed_id, route_id): RouteInfo}} —
                   merged across directions/patterns of the same route.
      stats:       [PatternAttribution] in the given pattern order
                   (shapeless patterns get n_samples=0, fraction 1.0).
    """
    routes_meta = routes_meta or {}
    to_xy = Transformer.from_crs(4326, lg.epsg, always_xy=True)
    index = EdgeSnapIndex(lg)

    edge_routes: dict = {}
    stats: list = []
    for p in patterns:
        if not p.shape or len(p.shape) < 2:
            stats.append(PatternAttribution(p.key, p.route_id, 0, 0, 0))
            continue
        xs, ys = to_xy.transform([c[0] for c in p.shape], [c[1] for c in p.shape])
        ridden, n_samples, n_unmatched, n_excised = attribute_shape_xy(
            index, list(zip(xs, ys)), sample_m=sample_m,
            snap_radius_m=snap_radius_m, deviation_gate_m=deviation_gate_m,
        )
        stats.append(
            PatternAttribution(p.key, p.route_id, n_samples, n_unmatched,
                               len(ridden), n_excised)
        )

        meta = routes_meta.get(p.route_id, {})
        info = RouteInfo(
            feed_id=feed_id,
            route_id=p.route_id,
            route_short_name=meta.get("short", p.route_short_name),
            route_type=int(meta.get("type", p.route_type)),
            route_color=meta.get("color", p.route_color),
            route_text_color=meta.get("text_color", ""),
        )
        for eid in ridden:
            edge_routes.setdefault(eid, {}).setdefault(
                (feed_id, p.route_id), info
            )
    return edge_routes, stats
