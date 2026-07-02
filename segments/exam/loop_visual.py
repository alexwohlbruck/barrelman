#!/usr/bin/env python3
"""Loop before/after visual (stage 6, phase C).

Two-panel matplotlib render of the Chicago Loop window at a simulated
z15, both panels drawn by the SAME per-vertex offset machinery the
MapLibre fork uses (miter-joined perpendicular offset per vertex, in
screen px converted at m/px = 78271.51696 / 2^15 * cos(lat)):

  LEFT  — v3 semantic segments (transit_line_segments, chicago:l-v3):
          steady features at constant offset_px; transition features
          interpolate off_from_px -> off_to_px along the feature's
          line-progress fraction through a cubic-bezier(.4, 0, .6, 1)
          easing — exactly the client expression
          ['interpolate', ['cubic-bezier', .4, 0, .6, 1],
           ['line-progress'], 0, ['get','off_from_px'],
           1, ['get','off_to_px']].
  RIGHT — the REJECTED v2 model (transit_lines_centerline, chicago:l):
          merged (color_key, slot, line_count) runs, offset multiplied
          by the linear 0/0.15/0.85/1 line-progress taper — run
          boundaries fall at arbitrary slot flips, taper zones scale
          with run length, ribbons pinch mid-corridor.

Read-only. Run:

  uv run --with-requirements segments/requirements.txt \
      python segments/exam/loop_visual.py --out /path/to/loop_v3_vs_v2.png
"""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from segments.corridors import DEFAULT_DSN  # noqa: E402

BUILD_V3 = "chicago:l-v3"
BUILD_V2 = "chicago:l"

LOOP_WINDOW = (-87.6355, 41.8755, -87.6245, 41.8875)  # w, s, e, n
Z = 15
LINE_WIDTH_PX = 3.2
FETCH_PAD_M = 400.0   # fetch beyond the window so entering ribbons draw


def m_per_px(lat: float) -> float:
    return 78271.51696 / 2 ** Z * math.cos(math.radians(lat))


def cubic_bezier_ease(p1x, p1y, p2x, p2y):
    """y(x) for the CSS cubic-bezier easing (endpoints (0,0), (1,1)) —
    the same curve MapLibre's interpolate expression evaluates."""
    def bez(t, a, b):
        u = 1.0 - t
        return 3 * u * u * t * a + 3 * u * t * t * b + t * t * t

    def ease(x):
        if x <= 0.0:
            return 0.0
        if x >= 1.0:
            return 1.0
        lo, hi = 0.0, 1.0
        for _ in range(40):  # bisection on x(t), monotone
            mid = (lo + hi) / 2
            if bez(mid, p1x, p2x) < x:
                lo = mid
            else:
                hi = mid
        return bez((lo + hi) / 2, p1y, p2y)

    return ease


EASE_V3 = cubic_bezier_ease(.4, 0, .6, 1)


def taper_v2(f: float) -> float:
    """v2's rejected linear taper: 0@0, 1@0.15, 1@0.85, 0@1."""
    if f < 0.15:
        return f / 0.15
    if f > 0.85:
        return (1.0 - f) / 0.15
    return 1.0


def offset_polyline(xy, offsets_m):
    """Per-vertex perpendicular offset with miter joins — what the fork's
    shader does with the per-vertex a_line_offset attribute. Positive
    offsets go RIGHT of travel. xy: [(x, y)] metres; offsets_m: per
    vertex. Miter scale is capped at MapLibre's default miter-limit of
    2 (joints sharper than ~120 deg turn bevel in the client — the
    incoming segment's plain normal stands in for the bevel here), so
    sharp joints never spike longer than the real render."""
    n = len(xy)
    dirs = []
    for a, b in zip(xy, xy[1:]):
        d = (b[0] - a[0], b[1] - a[1])
        ln = math.hypot(*d) or 1.0
        dirs.append((d[0] / ln, d[1] / ln))
    out = []
    for i in range(n):
        if i == 0:
            d = dirs[0]
            normal = (d[1], -d[0])
            scale = 1.0
        elif i == n - 1:
            d = dirs[-1]
            normal = (d[1], -d[0])
            scale = 1.0
        else:
            d1, d2 = dirs[i - 1], dirs[i]
            m = (d1[0] + d2[0], d1[1] + d2[1])
            ln = math.hypot(*m)
            if ln < 1e-9:  # 180-degree reversal: fall back to d1 normal
                normal, scale = (d1[1], -d1[0]), 1.0
            else:
                m = (m[0] / ln, m[1] / ln)
                cos_half = m[0] * d2[0] + m[1] * d2[1]
                if cos_half < 0.5:  # miter scale > 2: MapLibre bevels
                    normal, scale = (d1[1], -d1[0]), 1.0
                else:
                    normal = (m[1], -m[0])
                    scale = 1.0 / cos_half
        out.append((xy[i][0] + normal[0] * offsets_m[i] * scale,
                    xy[i][1] + normal[1] * offsets_m[i] * scale))
    return out


def cum_fractions(xy):
    cum = [0.0]
    for a, b in zip(xy, xy[1:]):
        cum.append(cum[-1] + math.dist(a, b))
    total = cum[-1] or 1.0
    return [c / total for c in cum]


def fetch_v3(cur, proj, envelope):
    # the z15 receipt reads the default band (60 m transitions)
    from segments.segment import SegmentConfig
    band = max(mz for mz, _ in SegmentConfig().bands)
    cur.execute(
        """SELECT seg_id, kind, route_color, offset_px, off_from_px,
                  off_to_px, ST_AsGeoJSON(geom)
           FROM transit_line_segments
           WHERE build_key = %s AND band_minzoom = %s
             AND geom && ST_MakeEnvelope(%s,%s,%s,%s,4326)
           ORDER BY seg_id""", (BUILD_V3, band, *envelope))
    import json
    feats = []
    for seg_id, kind, color, off, offa, offb, gj in cur.fetchall():
        ll = json.loads(gj)["coordinates"]
        xy = proj.to_xy(ll)
        feats.append((seg_id, kind, color, off, offa, offb, xy))
    return feats


def fetch_v2(cur, proj, envelope):
    """Full merged runs (progress is a fraction of the WHOLE run — the
    v2 failure mode needs the full geometry even far outside the
    window, exactly like the client saw it)."""
    cur.execute(
        """SELECT fid, route_color, slot, line_count,
                  ST_AsGeoJSON(ST_Transform(geom3857, 4326))
           FROM transit_lines_centerline
           WHERE build_key = %s
             AND geom3857 && ST_Transform(
                   ST_MakeEnvelope(%s,%s,%s,%s,4326), 3857)
           ORDER BY fid""", (BUILD_V2, *envelope))
    import json
    feats = []
    for fid, color, slot, count, gj in cur.fetchall():
        ll = json.loads(gj)["coordinates"]
        xy = proj.to_xy(ll)
        feats.append((fid, color, slot, count, xy))
    return feats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Loop v3-vs-v2 render")
    ap.add_argument("--out", required=True)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    args = ap.parse_args(argv)

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import psycopg

    from segments.segment import LocalProj

    w, s, e, n = LOOP_WINDOW
    lat0, lon0 = (s + n) / 2, (w + e) / 2
    proj = LocalProj(lon0, lat0)
    mpp = m_per_px(lat0)
    pad_deg = FETCH_PAD_M / 111000.0
    envelope = (w - pad_deg, s - pad_deg, e + pad_deg, n + pad_deg)

    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        v3 = fetch_v3(cur, proj, envelope)
        v2 = fetch_v2(cur, proj, envelope)
    print(f"v3 features in window: {len(v3)}; v2 runs: {len(v2)}; "
          f"m/px @ z{Z} = {mpp:.4f}")

    (x0, y0), (x1, y1) = proj.to_xy([(w, s)])[0], proj.to_xy([(e, n)])[0]
    win_w, win_h = x1 - x0, y1 - y0

    px_target = 1200            # per panel width
    dpi = 100
    fig_w_in = 2 * px_target / dpi
    fig_h_in = (px_target * win_h / win_w) / dpi
    fig, axes = plt.subplots(1, 2, figsize=(fig_w_in, fig_h_in), dpi=dpi)
    fig.subplots_adjust(left=0.005, right=0.995, bottom=0.005, top=0.94,
                        wspace=0.01)

    # line width: LINE_WIDTH_PX screen px -> ground metres -> points
    panel_px_per_m = px_target / win_w
    lw_pt = LINE_WIDTH_PX * mpp * panel_px_per_m * 72.0 / dpi

    def draw(ax, polylines, title):
        ax.set_facecolor("#f7f6f2")
        for color, pts in polylines:
            xs = [p[0] for p in pts]
            ys = [p[1] for p in pts]
            ax.plot(xs, ys, color=color, linewidth=lw_pt,
                    solid_capstyle="butt", solid_joinstyle="round",
                    zorder=2)
        ax.set_xlim(x0, x1)
        ax.set_ylim(y0, y1)
        ax.set_aspect("equal")
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_title(title, fontsize=13, pad=8)

    def color_of(hexcol):
        return f"#{hexcol}" if hexcol else "#888888"

    lines_v3 = []
    for seg_id, kind, color, off, offa, offb, xy in v3:
        if kind == "steady":
            offs = [off * mpp] * len(xy)
        else:
            fr = cum_fractions(xy)
            offs = [(offa + (offb - offa) * EASE_V3(f)) * mpp for f in fr]
        lines_v3.append((color_of(color), offset_polyline(xy, offs)))

    lines_v2 = []
    for fid, color, slot, count, xy in v2:
        base = (slot - (count - 1) / 2.0) * 4.4
        fr = cum_fractions(xy)
        offs = [base * taper_v2(f) * mpp for f in fr]
        lines_v2.append((color_of(color), offset_polyline(xy, offs)))

    draw(axes[0], lines_v3,
         f"v3 — semantic segments (steady + fixed-60 m transitions, "
         f"cubic-bezier .4/0/.6/1), {BUILD_V3}, z{Z}")
    draw(axes[1], lines_v2,
         f"v2 REJECTED — merged runs + 0/0.15/0.85/1 linear taper, "
         f"{BUILD_V2}, z{Z}")

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=dpi, facecolor="white")
    print(f"wrote {out} ({int(fig_w_in * dpi)}x{int(fig_h_in * dpi)} px)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
