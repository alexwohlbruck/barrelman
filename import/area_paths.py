"""
Straight walking lines across an open pedestrian space.

The space is a polygon in metres, holes included. A shortest walk across it
runs straight between where it enters and leaves, bending only around the
corners that stick into it, so it is enough to connect those points to each
other wherever the line between them stays inside: a reduced visibility graph.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable

import numpy as np
from shapely import LineString, Point, Polygon, STRtree, contains_xy, distance, points as as_points, prepare
from shapely.geometry.polygon import orient

# Room for rounding on a line that runs along the space's edge; never further outside than this.
SLACK_M = 0.002
# A point this close to a line lies on it.
ON_LINE_M = 0.05
# People keep a step or two off a wall or a planter, so lines keep this far off the edge where there is room.
PAD_M = 1.5
# Where the space is too narrow for that: arm's length.
CLEARANCE_M = 0.5
# How fast a line from a way in, which starts on the edge, must move off it: metres of clearance per metre walked.
EASE = 0.25
# Within the clearance, so a round fountain turns a handful of corners rather than dozens.
SIMPLIFY_M = 0.4
MIN_LINE_M = 0.5
# Walking around anything smaller (a planter, a kiosk) changes a walk by well under a metre.
MIN_OBSTACLE_M2 = 12.0
# Corners shallower than this are tracing noise, not something to walk around.
MIN_TURN_RAD = math.radians(4)

XY = tuple[float, float]


@dataclass(frozen=True)
class Vertex:
    xy: XY
    # A corner's neighbours along its ring; None for a way in, which can be left in any direction.
    prev: XY | None = None
    next: XY | None = None
    clearance: float = 0.0


def _cross(o: XY, a: XY, b: XY) -> float:
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


def _turn(a: XY, b: XY, c: XY) -> float:
    h1 = math.atan2(b[1] - a[1], b[0] - a[0])
    h2 = math.atan2(c[1] - b[1], c[0] - b[0])
    return abs((h2 - h1 + math.pi) % (2 * math.pi) - math.pi)


def corners(space: Polygon, clearance: float = 0.0) -> list[Vertex]:
    """Vertices where the space's edge turns into it: the only places a shortest walk bends."""
    space = orient(space, 1.0)
    found = []
    for ring in (space.exterior, *space.interiors):
        pts = list(ring.coords)[:-1]
        for i, b in enumerate(pts):
            a, c = pts[i - 1], pts[(i + 1) % len(pts)]
            if _cross(a, b, c) < 0 and _turn(a, b, c) >= MIN_TURN_RAD:
                found.append(Vertex(b, a, c, clearance))
    return found


def _tangent(v: Vertex, other: XY) -> bool:
    """A line leaving a corner is only worth walking if it grazes the corner rather than cutting back past it."""
    if v.prev is None:
        return True
    return _cross(v.xy, other, v.prev) * _cross(v.xy, other, v.next) >= 0


def without_planters(space: Polygon) -> Polygon:
    """The space a walk may use: obstacles too small to walk around are left in it."""
    return Polygon(space.exterior, [h for h in space.interiors if Polygon(h).area >= MIN_OBSTACLE_M2])


def _eroded_corners(space: Polygon, clearance: float) -> list[Vertex]:
    clear = space.buffer(-clearance, join_style="mitre", mitre_limit=2.0).simplify(SIMPLIFY_M)
    return [v for part in getattr(clear, "geoms", [clear]) if isinstance(part, Polygon) for v in corners(part, clearance)]


class _Padding:
    """Whether a line keeps PAD_M off the space's edge, or as far as its ends and the space's width allow."""

    def __init__(self, space: Polygon):
        self.edge = space.boundary
        self.roomy = space.buffer(-(PAD_M - 0.01))
        # Passages narrower than twice the pad, and their mouths.
        self.tight = space.difference(space.buffer(-PAD_M).buffer(PAD_M)).buffer(PAD_M)
        prepare(self.roomy)
        prepare(self.tight)

    def kept(self, a: Vertex, b: Vertex, line: LineString) -> bool:
        if self.roomy.covers(line):
            return True
        close = line.difference(self.roomy)
        if close.is_empty:
            return True
        steps = max(2, int(math.ceil(close.length / 0.25)) + 1)
        at = np.concatenate([np.linspace(0, part.length, steps) + line.project(Point(part.coords[0]))
                             for part in getattr(close, "geoms", [close])])
        at = np.clip(at, 0, line.length)
        need = np.minimum(PAD_M, np.minimum(a.clearance + EASE * at, b.clearance + EASE * (line.length - at)))
        xy = np.array([line.interpolate(s).coords[0] for s in at])
        need = np.where(contains_xy(self.tight, xy[:, 0], xy[:, 1]), np.minimum(need, CLEARANCE_M), need)
        return bool(np.all(distance(self.edge, as_points(xy)) >= need - 0.01))


def lines(space: Polygon, anchors: Iterable[XY], barriers: Iterable[LineString] = (), max_points: int = 400) -> list[tuple[XY, XY]] | None:
    """
    Every straight line worth walking across `space` between its anchors (ways
    in, ends of walls) and its corners. None past `max_points` points.
    """
    space = without_planters(space)
    padding = _Padding(space)
    points: dict[XY, Vertex] = {v.xy: v for v in _eroded_corners(space, PAD_M)}
    points.update((v.xy, v) for v in _eroded_corners(space, CLEARANCE_M) if padding.tight.contains(Point(v.xy)))
    for xy in anchors:
        points[xy] = Vertex(xy)
    if len(points) > max_points:
        return None
    vertices = list(points.values())

    room = space.buffer(SLACK_M)
    prepare(room)
    point_tree = STRtree([Point(v.xy) for v in vertices])
    walls = list(barriers)
    wall_tree = STRtree(walls) if walls else None

    found = []
    for i, a in enumerate(vertices):
        for b in vertices[i + 1:]:
            if not (_tangent(a, b.xy) and _tangent(b, a.xy)):
                continue
            line = LineString([a.xy, b.xy])
            if line.length < MIN_LINE_M:
                continue
            # A line through another point is the two lines either side of it.
            if any(j != i and vertices[j] is not b for j in point_tree.query(line, predicate='dwithin', distance=ON_LINE_M)):
                continue
            if not room.covers(line):
                continue
            if wall_tree is not None and any(walls[k].crosses(line) for k in wall_tree.query(line)):
                continue
            if not padding.kept(a, b, line):
                continue
            found.append((a.xy, b.xy))
    return found
