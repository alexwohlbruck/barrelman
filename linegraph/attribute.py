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
                       snap_radius_m: float | None = None):
    """Attribute every pattern's shape to skeleton edges.

    Returns (edge_routes, stats):
      edge_routes: {edge position: {(feed_id, route_id): RouteInfo}} —
                   merged across directions/patterns of the same route.
      stats:       [PatternAttribution] in the given pattern order
                   (shapeless patterns get n_samples=0, fraction 1.0).
    """
    routes_meta = routes_meta or {}
    if snap_radius_m is None:
        snap_radius_m = 2.0 * lg.merge_width_m

    to_xy = Transformer.from_crs(4326, lg.epsg, always_xy=True)
    tree = STRtree([LineString(e.coords_xy) for e in lg.edges])
    edge_nodes, node_edges = _edge_adjacency(lg)

    edge_routes: dict = {}
    stats: list = []
    for p in patterns:
        if not p.shape or len(p.shape) < 2:
            stats.append(PatternAttribution(p.key, p.route_id, 0, 0, 0))
            continue
        xs, ys = to_xy.transform([c[0] for c in p.shape], [c[1] for c in p.shape])
        samples = sample_polyline_xy(list(zip(xs, ys)), sample_m)
        points = shapely.points(samples[:, 0], samples[:, 1])
        nearest = np.full(len(points), -1, dtype=np.int64)
        pairs = tree.query_nearest(
            points, max_distance=snap_radius_m, all_matches=False
        )
        nearest[pairs[0]] = pairs[1]

        chain = _chain_edges(
            [int(e) for e in nearest[nearest >= 0]], lg, edge_nodes, node_edges
        )
        ridden = sorted(set(chain))
        n_unmatched = int((nearest < 0).sum())
        stats.append(
            PatternAttribution(p.key, p.route_id, len(points), n_unmatched,
                               len(ridden))
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
