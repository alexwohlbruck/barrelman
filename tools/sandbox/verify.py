"""tools.sandbox.verify — the verify harness (Part 2).

Reads the CURRENT DB (no reprocess), and per site:
  * pulls emitted transit_line_segments (band_minzoom default 15) in bbox,
  * pulls the real OSM REGULAR-SERVICE track (geo_places railway, the
    round-19 service/usage filter) as light-grey context,
  * pulls station points (transit_graph_nodes) for centring checks,
  * RENDERS each ribbon with the client offset applied (tools.sandbox.render),
  * MEASURES the on-screen px gap / separation / centring / straightness,
  * writes a per-site PNG (data/exam/sandbox/<key>.png) titled with the
    measured verdict, assembles a contact-sheet grid PNG, and emits a
    verdict JSON (data/exam/sandbox/verdicts.json).

Read-only against the DB. Run:

  uv run --with-requirements segments/requirements.txt \
      python -m tools.sandbox.verify
  uv run --with-requirements segments/requirements.txt \
      python -m tools.sandbox.verify --site dekalb --band 15
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from segments.corridors import DEFAULT_DSN            # noqa: E402
from segments.segment import GAP_PX                   # noqa: E402
from tools.sandbox import render as R                 # noqa: E402
from tools.sandbox.sites import SITES, get            # noqa: E402

# regular-service ground-truth predicate (mirrors tools.scorecard / round 19)
RAIL_WAY_TAGS = ("subway", "rail", "light_rail", "tram",
                 "narrow_gauge", "monorail", "funicular")
NON_REGULAR_SERVICE = ("yard", "siding", "spur", "crossover")
NON_REGULAR_USAGE = ("industrial", "military", "tourism")

FETCH_PAD_M = 400.0
DEFAULT_BAND = 15
OUT_DIR = Path("data/exam/sandbox")

# verdict thresholds
GAP_FAIL_FACTOR = 1.5      # measured gap > this * expected -> FAIL (bundle)
COINCIDENT_TOL_M = 6.0     # pre-offset centerlines within this = "bundled"
KISS_PX = 12.0             # separate: crossing-band width in px (a kiss dips
#                            under this only briefly; a bundle stays under)
SEPARATE_RUN_M = 200.0     # separate: a co-run longer than this within the
#                            slot band means the families FUSED (bad); a
#                            kiss/cross shares only a short stretch
CENTER_TOL_M = 12.0        # centered: ribbon within this of platform centroid
STRAIGHT_TOL_M = 12.0      # straight: through-deviation cap from the chord
INTRA_PARALLEL_BAND_M = 20.0   # nyc_exam duplicate-centerline test: two same-
INTRA_PARALLEL_MIN_M = 100.0   # colour features parallel within 20 m for
#                            >100 m are a duplicate ribbon (real failure)


# ── data access ──────────────────────────────────────────────────────────

def fetch_segments(cur, build_key, band, envelope):
    cur.execute(
        """SELECT seg_id, kind, route_color, route_short_names,
                  offset_px, off_from_px, off_to_px, line_count,
                  ST_AsGeoJSON(geom)
           FROM transit_line_segments
           WHERE build_key=%s AND band_minzoom=%s
             AND geom && ST_MakeEnvelope(%s,%s,%s,%s,4326)
           ORDER BY seg_id""", (build_key, band, *envelope))
    return cur.fetchall()


def fetch_track(cur, envelope):
    """Real OSM regular-service rail track (round-19 filter). geo_places
    first (NYC); falls back to mm_edges (Chicago QA dump)."""
    env = "ST_MakeEnvelope(%s,%s,%s,%s,4326)"
    cur.execute(
        f"""SELECT ST_AsGeoJSON(geom) FROM geo_places
            WHERE tags->>'railway' = ANY(%s)
              AND ST_GeometryType(geom)='ST_LineString'
              AND (tags->>'service' IS NULL OR NOT tags->>'service' = ANY(%s))
              AND (tags->>'usage'   IS NULL OR NOT tags->>'usage'   = ANY(%s))
              AND geom && {env}""",
        (list(RAIL_WAY_TAGS), list(NON_REGULAR_SERVICE),
         list(NON_REGULAR_USAGE), *envelope))
    rows = cur.fetchall()
    if rows:
        return [json.loads(r[0])["coordinates"] for r in rows], "geo_places"
    cur.execute(
        f"""SELECT ST_AsGeoJSON(geom) FROM mm_edges
            WHERE mode='rail' AND geom && {env}
              AND COALESCE((tags->>'class_penalty')::float,1.0) < 1.5""",
        envelope)
    rows = cur.fetchall()
    return ([json.loads(r[0])["coordinates"] for r in rows],
            "mm_edges") if rows else ([], "none")


def fetch_stations(cur, build_key, envelope):
    cur.execute(
        """SELECT station_label, ST_X(geom), ST_Y(geom)
           FROM transit_graph_nodes
           WHERE build_key=%s AND station_id IS NOT NULL
             AND geom && ST_MakeEnvelope(%s,%s,%s,%s,4326)""",
        (build_key, *envelope))
    return cur.fetchall()


# ── geometry helpers (local-plane metres) ────────────────────────────────

def _poly_min_dist(a_pts, b_pts, step=6):
    """Min distance (metres) between two xy polylines, sampled every `step`
    vertices of A against every segment of B (coarse but fine at the sub-px
    scale we judge). Returns (min_dist, argmin_point_on_A)."""
    from shapely.geometry import LineString, Point
    lb = LineString(b_pts)
    best = (float("inf"), None)
    for i in range(0, len(a_pts), step):
        d = lb.distance(Point(a_pts[i]))
        if d < best[0]:
            best = (d, a_pts[i])
    return best


def _sample_gap_px(a_off, b_off, mpp):
    """On-screen px gap between two RENDERED (offset) polylines: min ground
    distance sampled along A, converted to px. Uses the tighter of A->B and
    B->A so a short overlapping run is measured where they actually run
    parallel."""
    d_ab, _ = _poly_min_dist(a_off, b_off)
    d_ba, _ = _poly_min_dist(b_off, a_off)
    d = min(d_ab, d_ba)
    return d / mpp, d


def _shared_run_gap_px(a_off, b_off, mpp, near_m):
    """Median on-screen px gap over the run where A and B are within
    `near_m` metres (their shared/parallel stretch) — the number that
    tells 'tight bundle' from 'two ropes'. Returns (median_px, n_samples,
    min_px). Empty run -> (None, 0, None)."""
    from shapely.geometry import LineString, Point
    lb = LineString(b_off)
    gaps = []
    for p in a_off:
        d = lb.distance(Point(p))
        if d <= near_m:
            gaps.append(d / mpp)
    if not gaps:
        return None, 0, None
    gaps.sort()
    return gaps[len(gaps) // 2], len(gaps), gaps[0]


def _centerline_coincidence_m(a_xy, b_xy):
    """Median ground distance between two PRE-offset centerlines over their
    overlapping run (<=40 m) — the 'is the data actually bundled' test."""
    from shapely.geometry import LineString, Point
    lb = LineString(b_xy)
    ds = [lb.distance(Point(p)) for p in a_xy if lb.distance(Point(p)) <= 40.0]
    if not ds:
        return None
    ds.sort()
    return ds[len(ds) // 2]


def _parallel_run_m(a_xy, b_xy, band_m):
    """Longest CONTIGUOUS length (metres) of A's geometry that runs within
    `band_m` of B — the SUSTAINED co-run. Two separate lines that CROSS
    share only a point (~0 m contiguous), even if scattered near-touches
    across a long feature sum to a lot; a wrongly-bundled pair shares one
    unbroken run of hundreds of metres. Measures the longest UNBROKEN run,
    not the total, so crossings and approaches don't accumulate into a
    false bundle. (Mirrors nyc_exam's parallel-within-X-for->Y-m test.)"""
    from shapely.geometry import LineString, Point
    lb = LineString(b_xy)
    best = cur = 0.0
    for p, q in zip(a_xy, a_xy[1:]):
        mid = ((p[0] + q[0]) / 2, (p[1] + q[1]) / 2)
        if lb.distance(Point(mid)) <= band_m:
            cur += math.dist(p, q)
            best = max(best, cur)
        else:
            cur = 0.0
    return best


def _clip_to_bbox_xy(xy, proj, bbox):
    """Clip an xy (metres) polyline to the site bbox — so a co-run test
    judges only what's in the window, not a 5 km feature's far ends."""
    from shapely.geometry import LineString, box
    w, s, e, n = bbox
    (x0, y0), (x1, y1) = proj.to_xy([(w, s)])[0], proj.to_xy([(e, n)])[0]
    clip = LineString(xy).intersection(box(min(x0, x1), min(y0, y1),
                                           max(x0, x1), max(y0, y1)))
    if clip.is_empty:
        return []
    if clip.geom_type == "LineString":
        return list(clip.coords)
    # MultiLineString: return the longest part (the through-run in-window)
    parts = [list(g.coords) for g in clip.geoms]
    return max(parts, key=lambda c: LineString(c).length) if parts else []


def _through_deviation_m(xy, center_xy, half_m=150.0):
    """Max distance of the polyline from its straight chord, measured over
    the window centred on the site (the central +-half_m of the feature's
    portion nearest the site) — NOT the whole city-long feature. Clips the
    feature to the `half_m` band around the point on it nearest the site
    centre, then deviates that clip from its own chord."""
    from shapely.geometry import LineString, Point
    ls = LineString(xy)
    if ls.length < 20:
        return 0.0
    s0 = ls.project(Point(center_xy))         # arc-length nearest the site
    a = max(0.0, s0 - half_m)
    b = min(ls.length, s0 + half_m)
    from shapely.ops import substring
    clip = substring(ls, a, b)
    pts = list(clip.coords)
    if len(pts) < 3:
        return 0.0
    chord = LineString([pts[0], pts[-1]])
    return max(chord.distance(Point(p)) for p in pts)


# ── per-site rendering + measurement ─────────────────────────────────────

def _build_features(rows, proj):
    """[(seg_id, kind, color, routes, off, offa, offb, lc, xy_metres)]."""
    feats = []
    for (sid, kind, color, routes, off, offa, offb, lc, gj) in rows:
        ll = json.loads(gj)["coordinates"]
        feats.append((sid, kind, color, routes, off, offa, offb, lc,
                      proj.to_xy(ll)))
    return feats


def measure_site(site, feats, zoom, track_xy=None):
    """Compute the site's quantitative verdict from its features."""
    mpp = R.m_per_px(zoom, site.center[1])
    out = {"key": site.key, "expected": site.expected, "zoom": zoom,
           "mpp": round(mpp, 4), "gap_scale": round(R.gap_scale(zoom), 3),
           "expected_slot_gap_px": round(GAP_PX * R.gap_scale(zoom), 2)}

    # render every feature once (client offset applied)
    rendered = {}   # seg_id -> (color, off_polyline_metres, raw_xy)
    for (sid, kind, color, routes, off, offa, offb, lc, xy) in feats:
        off_poly = R.render_offset_xy(kind, off, offa, offb, xy, zoom,
                                      site.center[1])
        rendered[sid] = (color, off_poly, xy, lc)

    def feats_of_color(c):
        return [(sid, *rendered[sid])
                for (sid, k, col, *_rest) in
                [(f[0], f[1], f[2]) for f in feats] if col == c]

    by_color = {}
    off_lookup = {}   # seg_id -> signed offset_px (mean for transitions)
    for (sid, kind, color, routes, off, offa, offb, lc, xy) in feats:
        by_color.setdefault(color, []).append(sid)
        if kind == "steady":
            off_lookup[sid] = off or 0.0
        else:
            off_lookup[sid] = ((offa or 0.0) + (offb or 0.0)) / 2.0

    verdict = "PASS"
    measurements = []

    if site.expected in ("bundle", "separate"):
        for (ca, cb) in site.pairs:
            # gather rendered offset polylines for each family
            a_list = [(sid, rendered[sid]) for sid in by_color.get(ca, [])]
            b_list = [(sid, rendered[sid]) for sid in by_color.get(cb, [])]
            if ca == cb:
                # intra-family "one ribbon" test: any two DISTINCT steady
                # features of this colour that run parallel are a failure.
                pair_res = _intra_family(a_list, mpp)
                measurements.append(pair_res)
                if site.expected == "bundle" and pair_res.get("fail"):
                    verdict = "FAIL"
                continue
            if not a_list or not b_list:
                measurements.append({"pair": f"{ca}x{cb}",
                                     "note": "one family absent in window"})
                continue
            # Pick the representative (A-feature, B-feature) pair. For a
            # BUNDLE we want the two features that actually SHARE a
            # centerline (the bundle), so rank by centerline coincidence,
            # NOT by rendered gap (adjacent legs meeting at a corner render
            # close without being one bundle). For a SEPARATE site rank by
            # the tightest rendered approach (the kiss/cross point).
            cands = []
            for (sa, (cola, oa, xa, lca)) in a_list:
                for (sb, (colb, ob, xb, lcb)) in b_list:
                    near_m = max(30.0, 4 * mpp)
                    med_px, n, min_px = _shared_run_gap_px(oa, ob, mpp, near_m)
                    if med_px is None:
                        continue
                    coin = _centerline_coincidence_m(xa, xb)
                    # slot-derived expected on-screen gap: |offset_a-offset_b|
                    # px * gap_scale (bundle members that aren't adjacent
                    # slots sit N*4.4 px apart BY DESIGN).
                    exp_gap = (abs(off_lookup.get(sa, 0.0)
                                   - off_lookup.get(sb, 0.0))
                               * R.gap_scale(zoom))
                    cands.append({
                        "a": sa, "b": sb, "median_gap_px": round(med_px, 2),
                        "min_gap_px": round(min_px, 2), "n": n,
                        "centerline_coincidence_m":
                            None if coin is None else round(coin, 2),
                        "lc_a": lca, "lc_b": lcb,
                        "expected_gap_px": round(exp_gap, 2)})
            if not cands:
                measurements.append({"pair": f"{ca}x{cb}",
                                     "note": "no parallel run in window"})
                continue
            if site.expected == "bundle":
                # prefer a coincident-centerline pair (a real bundle); among
                # those the longest-shared (largest n)
                coincident = [c for c in cands
                              if c["centerline_coincidence_m"] is not None
                              and c["centerline_coincidence_m"]
                              <= COINCIDENT_TOL_M]
                best = (max(coincident, key=lambda c: c["n"]) if coincident
                        else min(cands, key=lambda c: c["median_gap_px"]))
            else:
                best = min(cands, key=lambda c: c["min_gap_px"])
            best["pair"] = f"{ca}x{cb}"
            if site.expected == "bundle":
                exp = best["expected_gap_px"]
                bundled = (best["centerline_coincidence_m"] is not None
                           and best["centerline_coincidence_m"]
                           <= COINCIDENT_TOL_M)
                # a bundle is "tight" when the rendered gap matches the
                # slot-derived expectation within tolerance (not wider)
                too_wide = best["median_gap_px"] > GAP_FAIL_FACTOR * max(exp,
                                                                         GAP_PX)
                best["data_bundled"] = bundled
                best["fail"] = bool(too_wide or not bundled)
                best["reason"] = ("centerlines not coincident (data not "
                                  "bundled)" if not bundled else
                                  "gap wider than 1.5x expected" if too_wide
                                  else "tight bundle")
                if best["fail"]:
                    verdict = "FAIL"
            else:  # separate — a KISS/CROSS touches briefly; a wrongly
                #   bundled pair runs PARALLEL within a slot width for a
                #   SUSTAINED contiguous length. Judge by the longest
                #   contiguous co-run WITHIN THE WINDOW, not the crossing-
                #   point min gap (~0 for any crossing) nor a 5 km feature's
                #   scattered near-touches.
                from segments.segment import LocalProj
                _proj = LocalProj(site.center[0], site.center[1])
                a_xy = _clip_to_bbox_xy(rendered[best["a"]][2], _proj,
                                        site.bbox)
                b_xy = _clip_to_bbox_xy(rendered[best["b"]][2], _proj,
                                        site.bbox)
                band = KISS_PX * mpp            # slot-scale band in metres
                run_m = (_parallel_run_m(a_xy, b_xy, band)
                         if a_xy and b_xy else 0.0)
                best["parallel_run_m"] = round(run_m, 1)
                best["parallel_band_m"] = round(band, 1)
                best.pop("median_gap_px", None)   # not the metric here
                fused = run_m > SEPARATE_RUN_M
                best["fail"] = bool(fused)
                best["reason"] = (f"fused: parallel {run_m:.0f} m > "
                                  f"{SEPARATE_RUN_M:.0f} m" if fused
                                  else f"separate (co-run {run_m:.0f} m, "
                                  f"min sep {best['min_gap_px']}px at cross)")
                if best["fail"]:
                    verdict = "FAIL"
            measurements.append(best)

    elif site.expected == "straight":
        # A "straight through-path" means the pipeline did not FABRICATE a
        # bend the real track doesn't have. Judge ONLY the site's named
        # through-route (through_color), and measure its steady features'
        # max stray from the REAL OSM regular-service track in-window: a
        # ribbon hugging real track curves only where the track curves
        # (fine); a fabricated bend leaves the track (fail). This is the
        # honest fidelity test — chord-deviation alone flags real track
        # curves as failures. Falls back to chord deviation if no track.
        from segments.segment import LocalProj
        from shapely.geometry import LineString, MultiLineString, Point
        proj = LocalProj(site.center[0], site.center[1])
        cxy = proj.to_xy([site.center])[0]
        judged = [sid for sid, (c, o, xy, lc) in rendered.items()
                  if c == site.through_color
                  and _kind_of(feats, sid) == "steady"]
        track = (MultiLineString([LineString(t) for t in track_xy
                                  if len(t) >= 2]) if track_xy else None)
        worst_stray = 0.0
        worst_dev = 0.0
        worst_sid = None
        for sid in judged:
            xy = _clip_to_bbox_xy(rendered[sid][2], proj, site.bbox)
            if len(xy) < 3:
                continue
            dev = _through_deviation_m(rendered[sid][2], cxy, half_m=150.0)
            worst_dev = max(worst_dev, dev)
            if track is not None:
                stray = max(track.distance(Point(p)) for p in xy)
                if stray > worst_stray:
                    worst_stray, worst_sid = stray, sid
        m = {"through_color": site.through_color, "judged_features": judged,
             "max_chord_deviation_m": round(worst_dev, 2),
             "tol_m": STRAIGHT_TOL_M, "window_half_m": 150}
        if track is not None:
            m["max_stray_from_osm_track_m"] = round(worst_stray, 2)
            m["worst_seg_id"] = worst_sid
            # a fabricated bend LEAVES the track; a real curve hugs it
            if worst_stray > STRAIGHT_TOL_M:
                verdict = "FAIL"
            m["reason"] = ("hugs real track (curve is real)"
                           if worst_stray <= STRAIGHT_TOL_M
                           else "strays from real track (fabricated bend)")
        else:
            if worst_dev > STRAIGHT_TOL_M:
                verdict = "FAIL"
        if not judged:
            m["note"] = "through_color absent in window"
        measurements.append(m)

    elif site.expected == "centered":
        # measured in main() where station points are available
        measurements.append({"note": "centring measured against station pts"})

    out["verdict"] = verdict
    out["measurements"] = measurements
    out["_rendered"] = rendered   # for the panel (stripped from JSON)
    return out


def _kind_of(feats, sid):
    for f in feats:
        if f[0] == sid:
            return f[1]
    return None


def _intra_family(a_list, mpp):
    """One-ribbon test (nyc_exam semantics): a family renders as ONE ribbon
    unless two DISTINCT steady features run PARALLEL within 20 m for >100 m
    (a duplicate centerline). Sequential corridor pieces (composition
    changes along the trunk) share only a divergence point and do NOT
    qualify; genuinely doubled track does. Reports the longest such
    parallel run."""
    worst = {"pair": "intra", "max_parallel_run_m": 0.0, "n_pairs": 0,
             "fail": False, "gap_px_of_worst": None}
    for i in range(len(a_list)):
        for j in range(i + 1, len(a_list)):
            _, (ca, oa, xa, lca) = a_list[i]
            _, (cb, ob, xb, lcb) = a_list[j]
            run_m = _parallel_run_m(xa, xb, INTRA_PARALLEL_BAND_M)
            if run_m < 1.0:
                continue
            worst["n_pairs"] += 1
            if run_m > worst["max_parallel_run_m"]:
                med_px, _n, _mn = _shared_run_gap_px(
                    oa, ob, mpp, INTRA_PARALLEL_BAND_M)
                worst["max_parallel_run_m"] = round(run_m, 1)
                worst["gap_px_of_worst"] = (None if med_px is None
                                            else round(med_px, 2))
    worst["fail"] = worst["max_parallel_run_m"] > INTRA_PARALLEL_MIN_M
    worst["reason"] = ("duplicate centerline (parallel %.0f m)"
                       % worst["max_parallel_run_m"] if worst["fail"]
                       else "single ribbon (no sustained duplicate)")
    return worst


def _centering(measurement_list, rendered, stations, site, zoom):
    """Fill the centered verdict: min distance from any ribbon vertex to the
    nearest station centroid in-window, vs CENTER_TOL_M."""
    from shapely.geometry import LineString, Point
    if not stations:
        return "PASS", [{"note": "no station points in window"}]
    # station matching the hint (fallback: all in-window stations)
    pts = [(lab, x, y) for (lab, x, y) in stations
           if site.platform_hint.lower() in (lab or "").lower()] or \
          [(lab, x, y) for (lab, x, y) in stations]
    from segments.segment import LocalProj
    proj = LocalProj(site.center[0], site.center[1])
    st_xy = proj.to_xy([(x, y) for (_l, x, y) in pts])
    worst = 0.0
    for sid, (color, off_poly, xy, lc) in rendered.items():
        ls = LineString(off_poly)
        for sp in st_xy:
            d = ls.distance(Point(sp))
            worst = max(worst, min([d], default=0.0)) if False else worst
    # measure: does the ribbon pass within CENTER_TOL_M of the station?
    nearest = float("inf")
    for sid, (color, off_poly, xy, lc) in rendered.items():
        ls = LineString(off_poly)
        for sp in st_xy:
            nearest = min(nearest, ls.distance(Point(sp)))
    verdict = "PASS" if nearest <= CENTER_TOL_M else "FAIL"
    return verdict, [{"nearest_ribbon_to_platform_m": round(nearest, 2),
                      "tol_m": CENTER_TOL_M, "stations": len(st_xy)}]


# ── panel drawing ────────────────────────────────────────────────────────

def draw_panel(ax, site, res, track_xy, stations_xy, zoom):
    lat0 = site.center[1]
    mpp = R.m_per_px(zoom, lat0)
    # window extent in metres
    from segments.segment import LocalProj
    proj = LocalProj(site.center[0], lat0)
    w, s, e, n = site.bbox
    (x0, y0), (x1, y1) = proj.to_xy([(w, s)])[0], proj.to_xy([(e, n)])[0]
    win_w = x1 - x0
    px_target = 900
    panel_px_per_m = px_target / win_w
    dpi = 100
    lw_pt = R.offset_width_px(zoom) * mpp * panel_px_per_m * 72.0 / dpi

    ax.set_facecolor("#f7f6f2")
    # OSM track context (light grey)
    for coords in track_xy:
        ax.plot([p[0] for p in coords], [p[1] for p in coords],
                color="#c9c4ba", linewidth=max(0.5, lw_pt * 0.7),
                solid_capstyle="round", zorder=1)
    # rendered ribbons (client offset applied)
    for sid, (color, off_poly, xy, lc) in res["_rendered"].items():
        ax.plot([p[0] for p in off_poly], [p[1] for p in off_poly],
                color=f"#{color}" if color else "#888888",
                linewidth=lw_pt, solid_capstyle="butt",
                solid_joinstyle="round", zorder=3)
    # station dots
    for sp in stations_xy:
        ax.plot(sp[0], sp[1], "o", color="#333", markersize=2.5, zorder=4)

    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect("equal")
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title(_panel_title(site, res, zoom), fontsize=8.5, pad=4)


def _panel_title(site, res, zoom):
    tag = res["verdict"]
    head = f"[{tag}] {site.name}"
    sub = f"z{zoom:g}  exp={site.expected}  slot-gap≈{res['expected_slot_gap_px']}px"
    detail = ""
    for m in res["measurements"]:
        if "median_gap_px" in m:
            detail = (f"  measured {m['median_gap_px']}px "
                      f"(coincidence {m.get('centerline_coincidence_m')}m)")
            break
        if "max_stray_from_osm_track_m" in m:
            detail = (f"  {m['through_color']} stray "
                      f"{m['max_stray_from_osm_track_m']}m")
            break
        if "max_chord_deviation_m" in m:
            detail = f"  chord-dev {m['max_chord_deviation_m']}m"
            break
        if "nearest_ribbon_to_platform_m" in m:
            detail = f"  ribbon→platform {m['nearest_ribbon_to_platform_m']}m"
            break
        if "parallel_run_m" in m:
            detail = f"  co-run {m['parallel_run_m']}m"
            break
        if "max_parallel_run_m" in m:
            detail = f"  dup-run {m['max_parallel_run_m']}m"
            break
    return f"{head}\n{sub}{detail}"


# ── main ─────────────────────────────────────────────────────────────────

def run(dsn, only, band):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import psycopg

    from segments.segment import LocalProj

    sites = [get(only)] if only else SITES
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    panels = []   # (site, res, track_xy, stations_xy, zoom)

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for site in sites:
            w, s, e, n = site.bbox
            pad = FETCH_PAD_M / 111000.0
            env = (w - pad, s - pad, e + pad, n + pad)
            proj = LocalProj(site.center[0], site.center[1])
            rows = fetch_segments(cur, site.build_key, band, env)
            feats = _build_features(rows, proj)
            track_ll, track_src = fetch_track(cur, env)
            track_xy = [proj.to_xy(c) for c in track_ll if len(c) >= 2]
            stations = fetch_stations(cur, site.build_key, env)
            stations_xy = proj.to_xy([(x, y) for (_l, x, y) in stations])

            res = measure_site(site, feats, site.zoom, track_xy=track_xy)
            res["n_features"] = len(feats)
            res["track_source"] = track_src
            if site.expected == "centered":
                v, m = _centering(res["measurements"], res["_rendered"],
                                  stations, site, site.zoom)
                res["verdict"], res["measurements"] = v, m
            results.append(res)
            panels.append((site, res, track_xy, stations_xy, site.zoom))
            print(f"[{res['verdict']:4}] {site.key:18} "
                  f"{_short_measure(res)}")

    # per-site PNGs
    for (site, res, track_xy, stations_xy, zoom) in panels:
        fig, ax = plt.subplots(figsize=(9, 7), dpi=100)
        fig.subplots_adjust(left=0.01, right=0.99, bottom=0.01, top=0.90)
        draw_panel(ax, site, res, track_xy, stations_xy, zoom)
        p = OUT_DIR / f"{site.key}.png"
        fig.savefig(p, dpi=100, facecolor="white")
        plt.close(fig)

    # contact sheet
    _contact_sheet(panels, plt)

    # verdict JSON (strip the heavy _rendered)
    clean = []
    for r in results:
        rr = {k: v for k, v in r.items() if k != "_rendered"}
        clean.append(rr)
    vp = OUT_DIR / "verdicts.json"
    vp.write_text(json.dumps(clean, indent=2, default=float))

    npass = sum(1 for r in results if r["verdict"] == "PASS")
    print(f"\n{npass}/{len(results)} PASS. contact sheet {OUT_DIR}/contact-sheet.png, "
          f"verdicts {vp}")
    return results


def _short_measure(res):
    for m in res["measurements"]:
        if "median_gap_px" in m:
            return (f"{m['pair']}: gap {m['median_gap_px']}px "
                    f"coincidence {m.get('centerline_coincidence_m')}m "
                    f"(exp {m.get('expected_gap_px')}px)")
        if "parallel_run_m" in m:
            return (f"{m['pair']}: co-run {m['parallel_run_m']}m "
                    f"(min sep {m['min_gap_px']}px at cross)")
        if "max_stray_from_osm_track_m" in m:
            return (f"{m['through_color']} stray {m['max_stray_from_osm_track_m']}m "
                    f"(chord-dev {m['max_chord_deviation_m']}m)")
        if "max_chord_deviation_m" in m:
            return f"chord-dev {m['max_chord_deviation_m']}m"
        if "nearest_ribbon_to_platform_m" in m:
            return f"ribbon→platform {m['nearest_ribbon_to_platform_m']}m"
        if "max_parallel_run_m" in m:
            return (f"intra dup-run {m['max_parallel_run_m']}m "
                    f"gap {m.get('gap_px_of_worst')}px")
    return ""


def _contact_sheet(panels, plt):
    ncols = 3
    nrows = math.ceil(len(panels) / ncols)
    fig, axes = plt.subplots(nrows, ncols, figsize=(ncols * 6, nrows * 4.6),
                             dpi=90)
    fig.subplots_adjust(left=0.005, right=0.995, bottom=0.005, top=0.965,
                        wspace=0.03, hspace=0.20)
    axes = axes.flatten() if hasattr(axes, "flatten") else [axes]
    for ax, (site, res, track_xy, stations_xy, zoom) in zip(axes, panels):
        draw_panel(ax, site, res, track_xy, stations_xy, zoom)
    for ax in axes[len(panels):]:
        ax.axis("off")
    fig.suptitle("Transit visual verification sandbox — client offset applied "
                 "@ site zoom", fontsize=13)
    fig.savefig(OUT_DIR / "contact-sheet.png", dpi=90, facecolor="white")
    plt.close(fig)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Visual verification sandbox")
    ap.add_argument("--site", help="single site key (default: all)")
    ap.add_argument("--band", type=int, default=DEFAULT_BAND)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    args = ap.parse_args(argv)
    run(args.dsn, args.site, args.band)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
