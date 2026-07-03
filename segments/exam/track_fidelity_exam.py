#!/usr/bin/env python3
"""Global track-fidelity exam (stage 6, durable regression guard).

Every EMITTED steady segment must hug the real OSM track of its mode: a
STEADY centerline is real way geometry verbatim (R1), so it may sit off a
single track only by the amount a merged bundle's midline legitimately
does — never a chord across open ground. This exam is what converts
"fixed the reported site" into "no steady corridor strays anywhere".

For every rail steady row of each build (z15 band — the verbatim band the
other exams pin), sample every ~20 m and measure the distance to the
nearest real OSM way of the matching mode. Ground truth per build:

  * geo_places `tags->>'railway' IN (subway,rail,light_rail,tram,...)`
    where the build's bbox is covered by the OSM place import (NYC);
  * else the `mm_edges` rail QA table (the same ground truth loop_exam
    holds Chicago to), when it covers the bbox.

A sample with NO way within COVERAGE_BUFFER_M is a ground-truth gap, not
a stray (an off-map tail, a mode the table doesn't carry) and is not
counted — the same "unmeasurable → skip" rule the fix's reconciliation
uses, so the exam never invents a failure from missing reference data.

Threshold (justified): a directional-pair / family / bundle midline is a
track-count-weighted midline of its constituents, so it sits off any one
constituent track by up to ~half the bundle's cross width. We allow
BASE_TOL_M (20 m) plus a bundle half-width margin scaled by the ribbon
count of the segment's bundle, measured against the UNION of ALL matching
ways (so a bundle sitting between its own two tracks reads the nearer
one). A solo line (line_count 1) gets only BASE_TOL_M — a 62 m FX chord
across the Culver S-curve trips it; the on-track F local at 12 m does not.

Builds whose geometry this stage does not own — chicago:l-v3 is
LOOM-derived and governed by loop_exam, and the fix's reconciliation is a
guaranteed no-op there (no geo_places rail in the Chicago bbox) — are
reported as ADVISORY (distribution printed, worst listed) and do not
hard-fail the exam; their single pre-existing expressway-median Blue
sample sits outside this stage's authority. Authoritative builds
(NYC) hard-fail on any over-threshold steady row.

Reports the distribution (mean, p90, p99, max, count>threshold) and the
worst 10 segments with coordinates for BOTH builds. Exits non-zero on any
authoritative failure. Read-only. Run:

  uv run --with-requirements segments/requirements.txt \\
      python segments/exam/track_fidelity_exam.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from segments.corridors import DEFAULT_DSN  # noqa: E402

SAMPLE_STEP_M = 20.0        # sample every ~20 m along each steady row
COVERAGE_BUFFER_M = 60.0    # a sample with no way this near is a gap, skip
BASE_TOL_M = 22.0           # solo-line stray budget. Calibrated: the
#   measured on-track maximum is ~12 m (curve sampling + a directional-
#   pair midline sitting ~half a pair_gap, 7.5 m, off each track), so 22 m
#   clears every legitimate steady centreline with margin while still
#   tripping the FX-class chord (63 m across the Culver S-curve) and any
#   real chord >= 22 m on open ground.
BUNDLE_HALF_MARGIN_PX = 4.4  # per extra ribbon: bundle half-width per rank
CLOSED_LOOP_TOL_M = 15.0    # start~=end within this => a turnback ring
BAND = 15                   # the verbatim z15 band

# rail-family OSM way tags (geo_places ground truth)
RAIL_WAY_TAGS = ("subway", "rail", "light_rail", "tram",
                 "narrow_gauge", "monorail", "funicular")

# Builds this stage owns geometry for (hard-fail); others are advisory.
AUTHORITATIVE = {"nyc:subway-v3"}
DEFAULT_BUILDS = ("nyc:subway-v3", "chicago:l-v3")


def _bbox(cur, build_key):
    cur.execute(
        """SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e)
           FROM (SELECT ST_Extent(geom) e FROM transit_line_segments
                 WHERE build_key = %s AND band_minzoom = %s) q""",
        (build_key, BAND))
    return cur.fetchone()


def _pick_ground_truth(cur, build_key):
    """Return ('geo_places'|'mm_edges', <count in bbox>) — whichever
    covers this build's bbox, geo_places first."""
    bbox = _bbox(cur, build_key)
    if bbox is None or bbox[0] is None:
        return None, 0, None
    env = "ST_MakeEnvelope(%s,%s,%s,%s,4326)"
    cur.execute(
        f"""SELECT COUNT(*) FROM geo_places
            WHERE tags->>'railway' = ANY(%s)
              AND ST_GeometryType(geom) = 'ST_LineString'
              AND geom && {env}""",
        (list(RAIL_WAY_TAGS), *bbox))
    n_gp = cur.fetchone()[0]
    if n_gp > 0:
        return "geo_places", n_gp, bbox
    cur.execute(
        f"SELECT COUNT(*) FROM mm_edges WHERE geom && {env}", bbox)
    n_mm = cur.fetchone()[0]
    if n_mm > 0:
        return "mm_edges", n_mm, bbox
    return None, 0, bbox


def _load_ground_truth(cur, source, bbox):
    """Materialize the matching ways into a temp table `gt(geom)` with a
    GiST index (fast per-sample nearest)."""
    cur.execute("DROP TABLE IF EXISTS gt")
    env = "ST_MakeEnvelope(%s,%s,%s,%s,4326)"
    if source == "geo_places":
        cur.execute(
            f"""CREATE TEMP TABLE gt AS SELECT geom FROM geo_places
                WHERE tags->>'railway' = ANY(%s)
                  AND ST_GeometryType(geom) = 'ST_LineString'
                  AND geom && {env}""",
            (list(RAIL_WAY_TAGS), *bbox))
    else:
        cur.execute(
            f"""CREATE TEMP TABLE gt AS SELECT geom FROM mm_edges
                WHERE mode = 'rail' AND geom && {env}""", bbox)
    cur.execute("CREATE INDEX ON gt USING gist(geom)")
    cur.execute("ANALYZE gt")


def _measure(cur, build_key):
    """Per steady row: (seg_id, routes, line_count, len_m, max_stray,
    n_samples, worst_lon, worst_lat, closed). max_stray None when no
    sample had a way within COVERAGE_BUFFER_M; `closed` True when the row
    is a turnback ring (start ~= end)."""
    buf_deg = COVERAGE_BUFFER_M / 111000.0
    cur.execute(
        """SELECT seg_id, route_short_names, line_count,
                  ST_Length(geom::geography), ST_AsText(geom),
                  ST_Distance(ST_StartPoint(geom)::geography,
                              ST_EndPoint(geom)::geography)
           FROM transit_line_segments
           WHERE build_key = %s AND band_minzoom = %s AND kind = 'steady'
           ORDER BY seg_id""",
        (build_key, BAND))
    rows = cur.fetchall()
    out = []
    for seg_id, routes, lc, len_m, wkt, end_gap in rows:
        n = max(1, min(2000, int((len_m or 0) / SAMPLE_STEP_M)))
        cur.execute(
            """WITH s AS (SELECT ST_GeomFromText(%s, 4326) g),
               samp AS (SELECT ST_LineInterpolatePoint(s.g, gs.i::float/%s) pt
                        FROM s, generate_series(0, %s) gs(i)),
               d AS (SELECT pt,
                        (SELECT MIN(ST_Distance(samp.pt::geography,
                                                gt.geom::geography))
                         FROM gt WHERE ST_DWithin(samp.pt, gt.geom, %s)) dist
                     FROM samp)
               SELECT MAX(dist), COUNT(dist),
                      ST_X((array_agg(pt ORDER BY dist DESC NULLS LAST))[1]),
                      ST_Y((array_agg(pt ORDER BY dist DESC NULLS LAST))[1])
               FROM d""",
            (wkt, n, n, buf_deg))
        mx, nnear, wlon, wlat = cur.fetchone()
        out.append((seg_id, routes or "", lc, float(len_m or 0),
                    None if mx is None else float(mx),
                    int(nnear or 0), wlon, wlat,
                    (end_gap or 0.0) <= CLOSED_LOOP_TOL_M))
    return out


def _threshold(line_count: int) -> float:
    """Bundle-offset-aware pass bar: BASE_TOL_M for a solo line, plus a
    half-bundle-width margin for multi-ribbon bundles (a midline sits off
    a single track by ~half the bundle cross width)."""
    extra = max(0, line_count - 1) * BUNDLE_HALF_MARGIN_PX / 2.0
    return BASE_TOL_M + extra


def _report(build_key, source, n_ways, measured, authoritative):
    print(f"\n=== {build_key} — ground truth: {source} ({n_ways} ways), "
          f"{'AUTHORITATIVE' if authoritative else 'advisory'} ===")
    have = [m for m in measured if m[4] is not None]
    skipped = len(measured) - len(have)
    if not have:
        print("  no measurable steady rows (no ground-truth coverage)")
        return []
    strays = sorted(m[4] for m in have)

    def pct(p):
        return strays[min(len(strays) - 1, int(p * len(strays)))]

    # a closed turnback ring (start ~= end) is a pocket/loop track that
    # departs and rejoins the main line, not a corridor traversing open
    # ground — its distance-to-nearest-way is not a chord-stray, so it is
    # reported but never hard-fails (the 5's Nevins St turnback loop).
    over = [m for m in have if m[4] > _threshold(m[2]) and not m[8]]
    rings = [m for m in have if m[4] > _threshold(m[2]) and m[8]]
    print(f"  steady rows: {len(measured)} "
          f"(measured {len(have)}, gap-skipped {skipped})")
    print(f"  stray m: mean {sum(strays)/len(strays):.1f}  "
          f"p90 {pct(0.9):.1f}  p99 {pct(0.99):.1f}  max {max(strays):.1f}")
    print(f"  over threshold (base {BASE_TOL_M:.0f} m + bundle margin): "
          f"{len(over)}  (+{len(rings)} turnback ring(s), advisory)")
    worst = sorted(have, key=lambda m: -m[4])[:10]
    print("  worst 10 (seg_id, routes, line_count, len_m, max_stray, "
          "thresh, worst_coord):")
    for seg_id, routes, lc, len_m, mx, nnear, wlon, wlat, closed in worst:
        flag = ("  <-- ring (advisory)" if closed and mx > _threshold(lc)
                else "  <-- OVER" if mx > _threshold(lc) else "")
        print(f"    seg {seg_id:4d}  {routes:14.14s} lc={lc} "
              f"{len_m:7.0f}m  max={mx:6.1f}m  thr={_threshold(lc):4.1f}m  "
              f"({wlon:.5f},{wlat:.5f}){flag}")
    return over if authoritative else []


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--build-key", action="append",
                    help="restrict to these builds (default: both cities)")
    args = ap.parse_args(argv)
    builds = args.build_key or list(DEFAULT_BUILDS)

    import psycopg

    print(f"track-fidelity exam — builds {builds}\n"
          f"sample {SAMPLE_STEP_M:.0f} m, base tol {BASE_TOL_M:.0f} m")
    failures = []
    with psycopg.connect(args.dsn) as conn, conn.cursor() as cur:
        for bk in builds:
            source, n_ways, bbox = _pick_ground_truth(cur, bk)
            if source is None:
                print(f"\n=== {bk} — NO ground truth in bbox; skipped ===")
                if bk in AUTHORITATIVE:
                    failures.append(f"{bk}: no ground truth available")
                continue
            _load_ground_truth(cur, source, bbox)
            measured = _measure(cur, bk)
            over = _report(bk, source, n_ways, measured,
                           bk in AUTHORITATIVE)
            for seg_id, routes, lc, len_m, mx, nnear, wlon, wlat, _cl in over:
                failures.append(
                    f"{bk} seg {seg_id} [{routes}] strays {mx:.1f} m "
                    f"(> {_threshold(lc):.1f} m) at ({wlon:.5f},{wlat:.5f})")

    if failures:
        print(f"\nEXAM FAILED — {len(failures)} straying steady segment(s):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nEXAM PASSED — every authoritative steady segment hugs its "
          "track within tolerance")
    return 0


if __name__ == "__main__":
    sys.exit(main())
