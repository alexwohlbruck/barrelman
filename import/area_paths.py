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

from shapely import LineString, Point, Polygon, STRtree, prepare
from shapely.geometry.polygon import orient

SLACK_M = 0.05
# People pass a wall or a fountain at arm's length, so corners sit this far off them.
CLEARANCE_M = 0.5
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


def _cross(o: XY, a: XY, b: XY) -> float:
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


def _turn(a: XY, b: XY, c: XY) -> float:
    h1 = math.atan2(b[1] - a[1], b[0] - a[0])
    h2 = math.atan2(c[1] - b[1], c[0] - b[0])
    return abs((h2 - h1 + math.pi) % (2 * math.pi) - math.pi)


def corners(space: Polygon) -> list[Vertex]:
    """Vertices where the space's edge turns into it: the only places a shortest walk bends."""
    space = orient(space, 1.0)
    found = []
    for ring in (space.exterior, *space.interiors):
        pts = list(ring.coords)[:-1]
        for i, b in enumerate(pts):
            a, c = pts[i - 1], pts[(i + 1) % len(pts)]
            if _cross(a, b, c) < 0 and _turn(a, b, c) >= MIN_TURN_RAD:
                found.append(Vertex(b, a, c))
    return found


def _tangent(v: Vertex, other: XY) -> bool:
    """A line leaving a corner is only worth walking if it grazes the corner rather than cutting back past it."""
    if v.prev is None:
        return True
    return _cross(v.xy, other, v.prev) * _cross(v.xy, other, v.next) >= 0


def lines(space: Polygon, anchors: Iterable[XY], barriers: Iterable[LineString] = (), max_points: int = 400) -> list[tuple[XY, XY]] | None:
    """
    Every straight line worth walking across `space` between its anchors (ways
    in, ends of walls) and its corners. None past `max_points` points.
    """
    space = Polygon(space.exterior, [h for h in space.interiors if Polygon(h).area >= MIN_OBSTACLE_M2])
    clear = space.buffer(-CLEARANCE_M, join_style="mitre", mitre_limit=2.0).simplify(SIMPLIFY_M)
    points: dict[XY, Vertex] = {v.xy: v for part in getattr(clear, "geoms", [clear]) if isinstance(part, Polygon) for v in corners(part)}
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
            if any(j != i and vertices[j] is not b for j in point_tree.query(line, predicate='dwithin', distance=SLACK_M)):
                continue
            if not room.covers(line):
                continue
            if wall_tree is not None and any(walls[k].crosses(line) for k in wall_tree.query(line)):
                continue
            found.append((a.xy, b.xy))
    return found
