#!/usr/bin/env python3
"""NYC visual receipts (milestone 5).

Single-panel matplotlib renders of nyc:subway-v3 windows at a simulated
z15, drawn by the SAME per-vertex offset machinery as loop_visual.py
(miter-joined perpendicular offsets, MapLibre miter-limit bevel,
transitions eased cubic-bezier(.4, 0, .6, 1) along line-progress):

  broadway — the Broadway yellow trunk, Times Sq -> Canal St: N/Q/R/W
             collapse to one ribbon beside the 7th Av (1/2/3), 6th Av
             (B/D/F/M) and 8th Av (A/C/E) trunks
  dekalb   — the DeKalb Av / Flatbush Av junction complex

Read-only. Run (once per window):

  uv run --with-requirements segments/requirements.txt \
      python segments/exam/nyc_visual.py --window broadway \
      --out data/exam/nyc-broadway.png
  uv run --with-requirements segments/requirements.txt \
      python segments/exam/nyc_visual.py --window dekalb \
      --out data/exam/nyc-dekalb.png
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from segments.corridors import DEFAULT_DSN                    # noqa: E402
from segments.exam.loop_visual import (EASE_V3, LINE_WIDTH_PX,  # noqa: E402
                                       Z, cum_fractions, m_per_px,
                                       offset_polyline)

BUILD = "nyc:subway-v3"
FETCH_PAD_M = 400.0

WINDOWS = {
    # w, s, e, n
    "broadway": (-74.008, 40.7165, -73.980, 40.757),
    "dekalb":   (-73.992, 40.6825, -73.9735, 40.6975),
    # PAR-12 refit receipts: through-tracks must not bend at crossings
    "west14":   (-74.010, 40.7295, -73.988, 40.7455),
    "nevins":   (-73.993, 40.680, -73.971, 40.696),
    # PAR-12 v3 fused-corridor receipts (unfuse + refit fixes)
    "brooklyn-bridge": (-74.012, 40.708, -73.998, 40.7185),
    "bowling-green":   (-74.019, 40.7005, -74.009, 40.708),
    "whitehall":       (-74.019, 40.698, -74.006, 40.7065),
    "lafayette-av":    (-73.982, 40.6825, -73.9665, 40.6905),
    # PAR-12 way-graph corridor receipts (the five user sites)
    "rector":          (-74.018, 40.702, -74.009, 40.709),
    "joralemon":       (-74.008, 40.690, -73.987, 40.705),
    "west4":           (-74.008, 40.727, -73.996, 40.736),
    "grand-st":        (-74.001, 40.7135, -73.989, 40.7225),
    # PAR-12 merge-boundary receipts (second live review)
    "dekalb-merge":    (-73.9935, 40.684, -73.976, 40.7005),
    "borough-hall":    (-73.999, 40.687, -73.983, 40.699),
    "9av":             (-74.014, 40.6255, -73.996, 40.641),
    "15st-prospect":   (-73.988, 40.654, -73.972, 40.666),
    "fork-seams":      (-74.021, 40.636, -74.003, 40.651),
}
TITLES = {
    "broadway": "Broadway yellow trunk, Times Sq -> Canal St — "
                "N/Q/R/W one ribbon",
    "dekalb":   "DeKalb Av / Flatbush Av junction complex",
    "west14":   "14 St crossings — L under the 6/7/8th Av trunks",
    "nevins":   "Nevins St — 2/3 + 4/5 convergence toward Atlantic Av",
    "brooklyn-bridge": "Brooklyn Bridge-City Hall — 4/5/6 beside J/Z "
                       "(Chambers St), platform-separated corridors",
    "bowling-green":   "Bowling Green — 4/5 ribbon on the island "
                       "platform centerline",
    "whitehall":       "Whitehall/South Ferry — 4/5 x N/R/W x 1 tube "
                       "crossings",
    "lafayette-av":    "Lafayette Av — G merges the A/C Fulton corridor "
                       "(straight through-path)",
    "rector":          "Rector St -> South Ferry — 1 beside R/W, no "
                       "kissing outside shared track",
    "joralemon":       "Joralemon St tube — 4/5 smooth under the East "
                       "River",
    "west4":           "W 4 St — blue/orange stack + Christopher St 1 "
                       "platform centering",
    "grand-st":        "Grand St / Bowery — B/D on the Chrystie St "
                       "connector",
    "dekalb-merge":    "DeKalb Av interlocking — orange/yellow merge "
                       "boundaries vs the real interweave",
    "borough-hall":    "Borough Hall — R (Montague) passes around the "
                       "2/3, never captured",
    "9av":             "9 Av / Sea Beach cut — D/W + N bundles hold "
                       "through breathing track spacing",
    "15st-prospect":   "15 St-Prospect Park — F/G pair through the "
                       "station throat",
    "fork-seams":      "4th Av 59 St — bundle-to-track handoffs stay "
                       "tangent (no half-gap step)",
}


def fetch(cur, proj, build, envelope):
    # the z15 receipts read the default band (60 m transitions)
    from segments.segment import SegmentConfig
    band = max(mz for mz, _ in SegmentConfig().bands)
    cur.execute(
        """SELECT seg_id, kind, route_color, offset_px, off_from_px,
                  off_to_px, ST_AsGeoJSON(geom)
           FROM transit_line_segments
           WHERE build_key = %s AND band_minzoom = %s
             AND geom && ST_MakeEnvelope(%s,%s,%s,%s,4326)
           ORDER BY seg_id""", (build, band, *envelope))
    feats = []
    for seg_id, kind, color, off, offa, offb, gj in cur.fetchall():
        ll = json.loads(gj)["coordinates"]
        feats.append((seg_id, kind, color, off, offa, offb,
                      proj.to_xy(ll)))
    return feats


def fetch_platforms(cur, proj, envelope):
    """OSM platform polygons (context underlay for the receipt windows)."""
    cur.execute(
        """SELECT ST_AsGeoJSON(geom) FROM transit_platforms
           WHERE geom_type = 'area'
             AND geom && ST_MakeEnvelope(%s,%s,%s,%s,4326)""", envelope)
    polys = []
    for (gj,) in cur.fetchall():
        g = json.loads(gj)
        rings = ([g["coordinates"]] if g["type"] == "Polygon"
                 else g["coordinates"])
        for ring in rings:
            if ring and len(ring[0]) >= 3:
                polys.append(proj.to_xy(ring[0]))
    return polys


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="NYC window render")
    ap.add_argument("--window", choices=sorted(WINDOWS), required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--build-key", default=BUILD)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    args = ap.parse_args(argv)

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import psycopg

    from segments.segment import LocalProj

    w, s, e, n = WINDOWS[args.window]
    lat0, lon0 = (s + n) / 2, (w + e) / 2
    proj = LocalProj(lon0, lat0)
    mpp = m_per_px(lat0)
    pad_deg = FETCH_PAD_M / 111000.0
    envelope = (w - pad_deg, s - pad_deg, e + pad_deg, n + pad_deg)

    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        feats = fetch(cur, proj, args.build_key, envelope)
        platforms = fetch_platforms(cur, proj, envelope)
    print(f"{len(feats)} features, {len(platforms)} platform polygons in "
          f"the {args.window} window; m/px @ z{Z} = {mpp:.4f}")

    (x0, y0), (x1, y1) = proj.to_xy([(w, s)])[0], proj.to_xy([(e, n)])[0]
    win_w, win_h = x1 - x0, y1 - y0

    px_target = 1100
    dpi = 100
    fig_w_in = px_target / dpi
    fig_h_in = (px_target * win_h / win_w) / dpi
    fig, ax = plt.subplots(figsize=(fig_w_in, fig_h_in), dpi=dpi)
    fig.subplots_adjust(left=0.005, right=0.995, bottom=0.005, top=0.965)

    panel_px_per_m = px_target / win_w
    lw_pt = LINE_WIDTH_PX * mpp * panel_px_per_m * 72.0 / dpi

    ax.set_facecolor("#f7f6f2")
    for ring in platforms:
        ax.fill([p[0] for p in ring], [p[1] for p in ring],
                facecolor="#d9d3c8", edgecolor="#c4bcae",
                linewidth=0.6, zorder=1)
    for seg_id, kind, color, off, offa, offb, xy in feats:
        if kind == "steady":
            offs = [off * mpp] * len(xy)
        else:
            fr = cum_fractions(xy)
            offs = [(offa + (offb - offa) * EASE_V3(f)) * mpp for f in fr]
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
    ax.set_title(f"{TITLES[args.window]} — {args.build_key}, z{Z}",
                 fontsize=12, pad=8)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=dpi, facecolor="white")
    print(f"wrote {out} ({int(fig_w_in * dpi)}x{int(fig_h_in * dpi)} px)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
