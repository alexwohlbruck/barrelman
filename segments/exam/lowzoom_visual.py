#!/usr/bin/env python3
"""Low-zoom band receipt (stage 6, zoom-banded transitions).

Single-panel matplotlib render of a transit_line_segments BAND at a
simulated low zoom, drawn by the SAME per-vertex offset machinery as
loop_visual.py (miter-joined perpendicular offsets, MapLibre miter-limit
bevel, transitions eased cubic-bezier(.4, 0, .6, 1) along line-progress).
The panel width in pixels equals window width / (m/px at the simulated
zoom) — a faithful screenshot-scale rendering. Offsets are multiplied by
--gap-scale to match the client's low-zoom ribbon-gap squeeze (half
spacing at z11 -> full at z14; the task receipt uses 0.5).

Windows:
  loop            — the Chicago Loop area at city scale (chicago:l-v3)
  lower-manhattan — lower Manhattan + downtown Brooklyn (nyc:subway-v3)

Read-only. Run:

  uv run --with-requirements segments/requirements.txt \\
      python segments/exam/lowzoom_visual.py --window loop \\
      --out data/exam/chicago-loop-z12-band480.png
  uv run --with-requirements segments/requirements.txt \\
      python segments/exam/lowzoom_visual.py --window lower-manhattan \\
      --out data/exam/nyc-lower-manhattan-z12-band480.png
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from segments.corridors import DEFAULT_DSN                       # noqa: E402
from segments.exam.loop_visual import (EASE_V3, LINE_WIDTH_PX,   # noqa: E402
                                       cum_fractions, offset_polyline)

WINDOWS = {
    # window -> (build_key, (w, s, e, n))
    "loop":            ("chicago:l-v3",
                        (-87.72, 41.83, -87.56, 41.935)),
    "lower-manhattan": ("nyc:subway-v3",
                        (-74.055, 40.665, -73.92, 40.772)),
}
FETCH_PAD_M = 800.0


def m_per_px_at(z: float, lat: float) -> float:
    return 78271.51696 / 2 ** z * math.cos(math.radians(lat))


def fetch_band(cur, proj, build, band_minzoom, envelope):
    cur.execute(
        """SELECT seg_id, kind, route_color, offset_px, off_from_px,
                  off_to_px, ST_AsGeoJSON(geom)
           FROM transit_line_segments
           WHERE build_key = %s AND band_minzoom = %s
             AND geom && ST_MakeEnvelope(%s,%s,%s,%s,4326)
           ORDER BY seg_id""", (build, band_minzoom, *envelope))
    feats = []
    for seg_id, kind, color, off, offa, offb, gj in cur.fetchall():
        ll = json.loads(gj)["coordinates"]
        feats.append((seg_id, kind, color, off, offa, offb,
                      proj.to_xy(ll)))
    return feats


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="low-zoom band render")
    ap.add_argument("--window", choices=sorted(WINDOWS), required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--zoom", type=float, default=12.0,
                    help="simulated zoom (m/px + panel size)")
    ap.add_argument("--band", type=int, default=0,
                    help="band_minzoom to render (default: the 480 m band)")
    ap.add_argument("--gap-scale", type=float, default=0.5,
                    help="offset multiplier matching the client's low-zoom "
                         "ribbon-gap squeeze")
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    args = ap.parse_args(argv)

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import psycopg

    from segments.segment import LocalProj

    build, (w, s, e, n) = WINDOWS[args.window]
    lat0, lon0 = (s + n) / 2, (w + e) / 2
    proj = LocalProj(lon0, lat0)
    mpp = m_per_px_at(args.zoom, lat0)
    pad_deg = FETCH_PAD_M / 111000.0
    envelope = (w - pad_deg, s - pad_deg, e + pad_deg, n + pad_deg)

    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        feats = fetch_band(cur, proj, build, args.band, envelope)
    print(f"{len(feats)} band-{args.band} features in the {args.window} "
          f"window; m/px @ z{args.zoom:g} = {mpp:.3f}; "
          f"gap scale {args.gap_scale}")

    (x0, y0), (x1, y1) = proj.to_xy([(w, s)])[0], proj.to_xy([(e, n)])[0]
    win_w, win_h = x1 - x0, y1 - y0
    px_w = win_w / mpp            # faithful screenshot scale
    dpi = 100
    fig_w_in = px_w / dpi
    fig_h_in = (px_w * win_h / win_w) / dpi
    fig, ax = plt.subplots(figsize=(fig_w_in, fig_h_in), dpi=dpi)
    fig.subplots_adjust(left=0.004, right=0.996, bottom=0.004, top=0.955)

    panel_px_per_m = px_w / win_w
    lw_pt = LINE_WIDTH_PX * mpp * panel_px_per_m * 72.0 / dpi

    ax.set_facecolor("#f7f6f2")
    for seg_id, kind, color, off, offa, offb, xy in feats:
        if kind == "steady":
            offs = [off * args.gap_scale * mpp] * len(xy)
        else:
            fr = cum_fractions(xy)
            offs = [(offa + (offb - offa) * EASE_V3(f))
                    * args.gap_scale * mpp for f in fr]
        pts = offset_polyline(xy, offs)
        ax.plot([p[0] for p in pts], [p[1] for p in pts],
                color=f"#{color}" if color else "#888888",
                linewidth=lw_pt, solid_capstyle="butt",
                solid_joinstyle="round", zorder=2)
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(f"{args.window} — {build}, band z{args.band} "
                 f"(480 m transitions), simulated z{args.zoom:g}, "
                 f"gap x{args.gap_scale}", fontsize=10, pad=6)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=dpi, facecolor="white")
    print(f"wrote {out} ({int(fig_w_in * dpi)}x{int(fig_h_in * dpi)} px)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
