"""tools.sandbox.render — the CLIENT render, reproduced exactly.

Reproduces the parchment client's transit-line rendering for a window at a
given zoom, so a matplotlib panel is what the map actually draws:

  * per-vertex miter-joined perpendicular OFFSET (the MapLibre fork's
    a_line_offset shader; miter capped at the client miter-limit of 2 ->
    bevel), from segments.exam.loop_visual.offset_polyline;
  * the offset magnitude is the CLIENT expression, not the raw property:
      steady:      line-offset = zoomScaledOffset(offset_px)
      transition:  line-offset = zoomScaledOffset(
                     interpolate(cubic-bezier(.4,0,.6,1), line-progress,
                                 0->off_from_px, 1->off_to_px))
    where zoomScaledOffset(v) = v * gapScale(zoom), and gapScale is the
    client's zoom gap-squeeze (interpolate linear zoom 11->0.5, 14->1.0,
    CLAMPED outside). This is the single source of the on-screen px gap.
  * line-offset is in SCREEN PX; converting to the ground metres a
    matplotlib axis needs is `px * m_per_px(zoom, lat)` where
      m_per_px = 78271.51696 / 2**zoom * cos(lat).

All of the above mirrors server/src/constants/default-layers/transit.ts
(STEADY_OFFSET / TRANSITION_OFFSET, zoomScaledOffset, Z_GAP_SQUEEZED=11,
Z_GAP_FULL=14, GAP_LOW_SCALE=0.5) and loop_visual.py (Z=15 hard-coded there,
generalised to any zoom here).
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from segments.exam.loop_visual import (  # noqa: E402
    EASE_V3, cum_fractions, offset_polyline)

# client transit.ts constants (keep in sync)
Z_GAP_SQUEEZED = 11.0
Z_GAP_FULL = 14.0
GAP_LOW_SCALE = 0.5
# client RAIL line-width stops (transit.ts RAIL_WIDTH, non-ferry branch):
# zoom 9->1.1, 12->2.2, 14->3.0, 16->4.5 px. The bundled OFFSET_WIDTH is
# thinner (10->1.0, 13->2.0, 15->3.0, 16->3.6) — bundles use that.
_OFFSET_WIDTH_STOPS = [(10, 1.0), (13, 2.0), (15, 3.0), (16, 3.6)]


def m_per_px(zoom: float, lat: float) -> float:
    """Ground metres per screen pixel at Web-Mercator `zoom` and latitude."""
    return 78271.51696 / 2 ** zoom * math.cos(math.radians(lat))


def _interp(stops, x):
    """MapLibre `interpolate ['linear']` with clamped ends."""
    if x <= stops[0][0]:
        return stops[0][1]
    if x >= stops[-1][0]:
        return stops[-1][1]
    for (x0, y0), (x1, y1) in zip(stops, stops[1:]):
        if x0 <= x <= x1:
            t = (x - x0) / (x1 - x0)
            return y0 + (y1 - y0) * t
    return stops[-1][1]


def gap_scale(zoom: float) -> float:
    """zoomScaledOffset's multiplier: interpolate linear zoom 11->0.5,
    14->1.0, clamped. At z>=14 -> 1.0 (full baked gap); at z<=11 -> 0.5."""
    return _interp([(Z_GAP_SQUEEZED, GAP_LOW_SCALE), (Z_GAP_FULL, 1.0)], zoom)


def offset_width_px(zoom: float) -> float:
    """The client bundled-ribbon line-width (OFFSET_WIDTH), non-hover, px."""
    return _interp(_OFFSET_WIDTH_STOPS, zoom)


def steady_offsets_px(offset_px: float, n: int, zoom: float) -> list:
    """Per-vertex CLIENT line-offset (px) for a steady feature."""
    return [offset_px * gap_scale(zoom)] * n


def transition_offsets_px(off_from: float, off_to: float, xy: list,
                          zoom: float) -> list:
    """Per-vertex CLIENT line-offset (px) for a transition feature:
    cubic-bezier eased off_from->off_to along line-progress, then the
    zoom gap-scale applied to the whole thing (matches transit.ts
    TRANSITION_OFFSET = zoomScaledOffset(TRANSITION_PROGRESS_OFFSET))."""
    scale = gap_scale(zoom)
    fr = cum_fractions(xy)
    return [(off_from + (off_to - off_from) * EASE_V3(f)) * scale for f in fr]


def render_offset_xy(kind, offset_px, off_from, off_to, xy, zoom, lat):
    """Apply the client offset to an xy (metres) polyline and return the
    OFFSET polyline in metres — exactly what the map paints.

    `xy` is the feature centerline in local-plane metres. Returns the
    per-vertex offset polyline (metres) after the fork's miter offset."""
    mpp = m_per_px(zoom, lat)
    if kind == "steady":
        offs_px = steady_offsets_px(offset_px, len(xy), zoom)
    else:
        offs_px = transition_offsets_px(off_from, off_to, xy, zoom)
    offs_m = [o * mpp for o in offs_px]
    return offset_polyline(xy, offs_m)
