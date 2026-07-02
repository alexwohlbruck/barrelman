"""Shared assertions for segments tests (metric helpers on lon/lat)."""

from __future__ import annotations

import math

MX = 111319.4908   # metres per degree lon at the equator
MY = 110574.2727   # metres per degree lat


def to_m(coords):
    return [(lon * MX, lat * MY) for lon, lat in coords]


def seg_len_m(seg) -> float:
    xy = to_m(seg.coords)
    return sum(math.dist(a, b) for a, b in zip(xy, xy[1:]))


def endpoints(seg):
    xy = to_m(seg.coords)
    return xy[0], xy[-1]


def find(segments, *, kind=None, color_key=None, sites=None):
    out = []
    for s in segments:
        if kind and s.kind != kind:
            continue
        if color_key and s.color_key != color_key:
            continue
        if sites is not None and tuple(s.sites) != tuple(sites):
            continue
        out.append(s)
    return out


def max_spacing_m(seg) -> float:
    xy = to_m(seg.coords)
    return max(math.dist(a, b) for a, b in zip(xy, xy[1:]))


def circumradius(a, b, c) -> float:
    la, lb, lc = math.dist(b, c), math.dist(a, c), math.dist(a, b)
    area2 = abs((b[0] - a[0]) * (c[1] - a[1])
                - (c[0] - a[0]) * (b[1] - a[1]))
    if area2 < 1e-9:
        return float("inf")
    return la * lb * lc / (2.0 * area2)


def min_radius_near(seg, center_m, within_m: float) -> float:
    """Minimum circumradius over consecutive vertex triples whose middle
    vertex lies within `within_m` of center_m."""
    xy = to_m(seg.coords)
    best = float("inf")
    for a, b, c in zip(xy, xy[1:], xy[2:]):
        if math.dist(b, center_m) <= within_m:
            best = min(best, circumradius(a, b, c))
    return best


def offset_at_shared_endpoint(transition, steady, tol_m: float = 0.5):
    """If the transition and a steady feature share an endpoint, return
    (transition_offset_at_that_end, expected_from_steady) where expected
    carries the direction-aware sign (a steady stored against the
    transition's travel direction flips its offset sign). None when no
    endpoint is shared."""
    t_xy = to_m(transition.coords)
    s_xy = to_m(steady.coords)
    for t_end, off in (("start", transition.off_from_px),
                       ("end", transition.off_to_px)):
        tp = t_xy[0] if t_end == "start" else t_xy[-1]
        # transition travel direction at that endpoint
        td = ((t_xy[1][0] - t_xy[0][0]), (t_xy[1][1] - t_xy[0][1])) \
            if t_end == "start" else \
            ((t_xy[-1][0] - t_xy[-2][0]), (t_xy[-1][1] - t_xy[-2][1]))
        for s_end in ("start", "end"):
            sp = s_xy[0] if s_end == "start" else s_xy[-1]
            if math.dist(tp, sp) > tol_m:
                continue
            sd = ((s_xy[1][0] - s_xy[0][0]), (s_xy[1][1] - s_xy[0][1])) \
                if s_end == "start" else \
                ((s_xy[-1][0] - s_xy[-2][0]), (s_xy[-1][1] - s_xy[-2][1]))
            dot = td[0] * sd[0] + td[1] * sd[1]
            sign = 1.0 if dot > 0 else -1.0
            return off, sign * steady.offset_px
    return None
