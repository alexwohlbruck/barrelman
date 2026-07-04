"""tools.scorecard — one-page quantitative scorecard for a transit build.

Runs the pipeline's quantitative checks and distils them to a single page so
a dial change can be measured: change a dial -> rebuild -> re-run -> compare.
Fast enough to run iteratively (the track-fidelity sampling is coarsened and
spatially predicated relative to the full exam).

  uv run --with-requirements segments/requirements.txt \
      python -m tools.scorecard --build-key nyc:subway-v3
  python -m tools.scorecard --build-key chicago:l-v3 --json

Metrics
  * junction through-deviation  — mean/p90/max of every degree>=3 junction's
    through-ribbon centerline deviation from the +-100 m chord (emitted
    geometry; the way-graph analogue of junction_exam).
  * track-fidelity stray        — max / count-over-threshold of steady rows'
    distance to the real OSM track (REGULAR-SERVICE ways only — yard/siding/
    spur/crossover excluded, mirroring the display-geometry ground truth),
    coarser sampling than track_fidelity_exam.
  * bundle counts               — cross-family / same-family / directional-
    pair merges (from the corridor build notes sidecar).
  * KISSING count               — short sub-threshold convergences the builder
    correctly did NOT bundle (the merge rejects) — surfaced so RAISING the
    bundle gap stays measurable.
  * corridor / feature counts   — emitted corridors, steady + transition
    features (z15 band).
  * on-OSM %                    — matched-shape metres on real OSM track.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DEFAULT_DSN = "postgresql://barrelman:barrelman@localhost:5434/barrelman"

# build_key -> (feed_id, mode) for the corridor-notes sidecar lookup
BUILD_FEED = {
    "nyc:subway-v3": ("5", "rail"),
    "chicago:l-v3": ("29", "rail"),
}

# regular-service predicate (display geometry): the ground-truth track set
# excludes non-running tracks — yard/siding/spur/crossover service and
# industrial/military/tourism usage — mirroring shapesnap.graph and
# segments.build.load_ways. A yard's parallel tracks otherwise pull the
# stray measurement toward track no train in service rides.
NON_REGULAR_SERVICE = ("yard", "siding", "spur", "crossover")
NON_REGULAR_USAGE = ("industrial", "military", "tourism")
RAIL_WAY_TAGS = ("subway", "rail", "light_rail", "tram",
                 "narrow_gauge", "monorail", "funicular")

BAND = 15
SAMPLE_STEP_M = 40.0        # coarser than track_fidelity_exam's 20 m
COVERAGE_BUFFER_M = 60.0
BASE_TOL_M = 22.0
BUNDLE_HALF_MARGIN_PX = 4.4
JUNCTION_HALF_M = 100.0     # chord half-length for through-deviation


# ── corridor-build notes (bundle / kissing counts) ──────────────────────────


def _notes_sidecar(build_key: str, dsn: str) -> dict | None:
    """Read the corridor build's notes JSON sidecar (bundle/kiss counts).

    Written by linegraph.build._save_notes_sidecar next to the waygraph
    cache. Missing/stale -> None (the scorecard still prints the DB metrics
    and flags the bundle section as unavailable — rebuild to populate)."""
    fm = BUILD_FEED.get(build_key)
    if fm is None:
        return None
    feed_id, mode = fm
    repo = Path(__file__).resolve().parents[1]
    cache = repo / "data" / "linegraph" / f"{feed_id}.{mode}.waygraph.pkl.gz"
    sidecar = Path(str(cache) + ".notes.json")
    if not sidecar.exists():
        return None
    try:
        return json.loads(sidecar.read_text())
    except Exception:
        return None


def _bundle_counts(notes: dict | None) -> dict:
    if notes is None:
        return {"available": False}
    merges = notes.get("merges", [])
    by_kind: dict = {"pair": 0, "family": 0, "cross": 0}
    for m in merges:
        by_kind[m.get("kind", "?")] = by_kind.get(m.get("kind", "?"), 0) + 1
    rejects = notes.get("rejects", [])
    # a KISS = a merge reject: a short sub-threshold convergence (or a
    # too-high relative bearing) the builder declined to bundle. Split by
    # reason so raising the gap is measurable.
    kiss_reasons: dict = {}
    for r in rejects:
        reason = r[0] if r else "?"
        kiss_reasons[reason] = kiss_reasons.get(reason, 0) + 1
    return {
        "available": True,
        "pair": by_kind.get("pair", 0),
        "family": by_kind.get("family", 0),
        "cross": by_kind.get("cross", 0),
        "kissing": len(rejects),
        "kiss_reasons": kiss_reasons,
        "n_absorbed": notes.get("n_absorbed", 0),
        "n_contracted": notes.get("n_contracted", 0),
        "n_offgraph": notes.get("n_offgraph", 0),
        "n_raw_corridors": notes.get("n_raw_corridors", 0),
    }


# ── DB metrics ──────────────────────────────────────────────────────────────


def _bbox(cur, build_key):
    cur.execute(
        """SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e)
           FROM (SELECT ST_Extent(geom) e FROM transit_line_segments
                 WHERE build_key = %s AND band_minzoom = %s) q""",
        (build_key, BAND))
    return cur.fetchone()


def _load_regular_track(cur, build_key, bbox):
    """Materialize REGULAR-SERVICE rail ways into a temp table `gt` with a
    GiST index. geo_places first (NYC), else mm_edges (Chicago). Returns the
    (source, n_ways) actually used, or (None, 0) when neither covers."""
    env = "ST_MakeEnvelope(%s,%s,%s,%s,4326)"
    cur.execute("DROP TABLE IF EXISTS gt")
    cur.execute(
        f"""SELECT COUNT(*) FROM geo_places
            WHERE tags->>'railway' = ANY(%s)
              AND ST_GeometryType(geom) = 'ST_LineString'
              AND (tags->>'service' IS NULL OR NOT tags->>'service' = ANY(%s))
              AND (tags->>'usage' IS NULL OR NOT tags->>'usage' = ANY(%s))
              AND geom && {env}""",
        (list(RAIL_WAY_TAGS), list(NON_REGULAR_SERVICE),
         list(NON_REGULAR_USAGE), *bbox))
    n_gp = cur.fetchone()[0]
    if n_gp > 0:
        cur.execute(
            f"""CREATE TEMP TABLE gt AS SELECT geom FROM geo_places
                WHERE tags->>'railway' = ANY(%s)
                  AND ST_GeometryType(geom) = 'ST_LineString'
                  AND (tags->>'service' IS NULL OR NOT tags->>'service' = ANY(%s))
                  AND (tags->>'usage' IS NULL OR NOT tags->>'usage' = ANY(%s))
                  AND geom && {env}""",
            (list(RAIL_WAY_TAGS), list(NON_REGULAR_SERVICE),
             list(NON_REGULAR_USAGE), *bbox))
        cur.execute("CREATE INDEX ON gt USING gist(geom)")
        cur.execute("ANALYZE gt")
        # gt_all = the same ways WITHOUT the regular-service filter, so a
        # steady row hugging a service track (a terminal crossover reverse,
        # a mis-tagged mainline) reads as on-service, not a stray — matching
        # track_fidelity_exam's advisory rule.
        cur.execute("DROP TABLE IF EXISTS gt_all")
        cur.execute(
            f"""CREATE TEMP TABLE gt_all AS SELECT geom FROM geo_places
                WHERE tags->>'railway' = ANY(%s)
                  AND ST_GeometryType(geom) = 'ST_LineString'
                  AND geom && {env}""",
            (list(RAIL_WAY_TAGS), *bbox))
        cur.execute("CREATE INDEX ON gt_all USING gist(geom)")
        cur.execute("ANALYZE gt_all")
        return "geo_places(regular)", n_gp
    cur.execute(f"SELECT COUNT(*) FROM mm_edges WHERE geom && {env}", bbox)
    n_mm = cur.fetchone()[0]
    if n_mm > 0:
        # mm_edges carries class_penalty in tags — regular-service ways have
        # penalty ~1.0 (yard 4.0, service 1.75-2.0, usage x2.0)
        cur.execute(
            f"""CREATE TEMP TABLE gt AS SELECT geom FROM mm_edges
                WHERE mode = 'rail' AND geom && {env}
                  AND COALESCE((tags->>'class_penalty')::float, 1.0) < 1.5""",
            bbox)
        cur.execute("CREATE INDEX ON gt USING gist(geom)")
        cur.execute("ANALYZE gt")
        cur.execute("DROP TABLE IF EXISTS gt_all")
        cur.execute(
            f"""CREATE TEMP TABLE gt_all AS SELECT geom FROM mm_edges
                WHERE mode = 'rail' AND geom && {env}""", bbox)
        cur.execute("CREATE INDEX ON gt_all USING gist(geom)")
        cur.execute("ANALYZE gt_all")
        return "mm_edges(regular)", n_mm
    return None, 0


def _threshold(line_count: int) -> float:
    return BASE_TOL_M + max(0, line_count - 1) * BUNDLE_HALF_MARGIN_PX / 2.0


def _track_fidelity(cur, build_key):
    """Coarse per-steady-row max stray to the nearest regular-service track.
    Returns dict with distribution + over-threshold count, or {'skipped':...}."""
    buf_deg = COVERAGE_BUFFER_M / 111000.0
    cur.execute(
        """SELECT seg_id, line_count, ST_Length(geom::geography),
                  ST_AsText(geom),
                  ST_Distance(ST_StartPoint(geom)::geography,
                              ST_EndPoint(geom)::geography)
           FROM transit_line_segments
           WHERE build_key = %s AND band_minzoom = %s AND kind = 'steady'
           ORDER BY seg_id""",
        (build_key, BAND))
    rows = cur.fetchall()
    strays, over = [], 0
    worst = (0.0, None, None)
    for seg_id, lc, len_m, wkt, end_gap in rows:
        n = max(1, min(1000, int((len_m or 0) / SAMPLE_STEP_M)))
        cur.execute(
            """WITH s AS (SELECT ST_GeomFromText(%s, 4326) g),
               samp AS (SELECT ST_LineInterpolatePoint(s.g, gs.i::float/%s) pt
                        FROM s, generate_series(0, %s) gs(i)),
               d AS (SELECT pt,
                       (SELECT MIN(ST_Distance(samp.pt::geography,
                                               gt.geom::geography))
                        FROM gt WHERE ST_DWithin(samp.pt, gt.geom, %s)) dist
                     FROM samp)
               SELECT MAX(dist),
                      ST_X((array_agg(pt ORDER BY dist DESC NULLS LAST))[1]),
                      ST_Y((array_agg(pt ORDER BY dist DESC NULLS LAST))[1])
               FROM d""",
            (wkt, n, n, buf_deg))
        mx, wlon, wlat = cur.fetchone()
        if mx is None:
            continue
        strays.append(float(mx))
        closed = (end_gap or 0.0) <= 15.0
        if float(mx) > _threshold(lc) and not closed:
            # on-service exemption (matches track_fidelity_exam): a worst
            # point hugging ANY track (gt_all) within BASE_TOL_M but beyond
            # the regular-track threshold is riding a service track the
            # train uses, not straying across open ground.
            on_service = False
            if wlon is not None:
                cur.execute(
                    """SELECT MIN(ST_Distance(
                           ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography,
                           geom::geography))
                       FROM gt_all
                       WHERE ST_DWithin(ST_SetSRID(ST_MakePoint(%s,%s),4326),
                                        geom, %s)""",
                    (wlon, wlat, wlon, wlat, buf_deg))
                d_all = cur.fetchone()[0]
                on_service = d_all is not None and float(d_all) <= BASE_TOL_M
            if not on_service:
                over += 1
                if float(mx) > worst[0]:
                    worst = (float(mx), wlon, wlat)
    if not strays:
        return {"skipped": "no regular-service track coverage in bbox"}
    strays.sort()

    def pct(p):
        return strays[min(len(strays) - 1, int(p * len(strays)))]

    return {
        "measured": len(strays), "mean": sum(strays) / len(strays),
        "p90": pct(0.9), "max": max(strays), "over_threshold": over,
        "worst": {"stray_m": round(worst[0], 1), "lon": worst[1],
                  "lat": worst[2]} if worst[1] is not None else None,
    }


def _junction_deviation(cur, build_key):
    """Through-ribbon deviation from the +-100 m chord at every degree>=3
    junction of the EMITTED graph. For each junction and each pair of
    incident edges continuing roughly straight through (interior turn near
    180 deg), measure the max distance of the joined +-100 m centerline
    from its straight chord. All geometry Python-side (one bulk fetch) so
    the scorecard stays fast enough for iterative use."""
    import numpy as np
    from pyproj import Transformer
    from shapely.geometry import LineString

    cur.execute(
        """SELECT ST_AsGeoJSON(n.geom)
           FROM transit_graph_nodes n
           WHERE n.build_key = %s AND n.station_id IS NULL""",
        (build_key,))
    import json as _json
    node_pts = [tuple(_json.loads(r[0])["coordinates"]) for r in cur.fetchall()]
    cur.execute(
        """SELECT ST_AsGeoJSON(geom) FROM transit_graph_edges
           WHERE build_key = %s""", (build_key,))
    edges = [_json.loads(r[0])["coordinates"] for r in cur.fetchall()]
    if not node_pts or not edges:
        return {"junctions": 0}
    lon0 = sum(p[0] for p in node_pts) / len(node_pts)
    lat0 = sum(p[1] for p in node_pts) / len(node_pts)
    crs = (f"+proj=aeqd +lat_0={lat0} +lon_0={lon0} +datum=WGS84 "
           f"+units=m +no_defs")
    fwd = Transformer.from_crs("EPSG:4326", crs, always_xy=True)

    def to_xy(coords):
        xs, ys = fwd.transform([c[0] for c in coords], [c[1] for c in coords])
        return list(zip(xs, ys))

    node_xy = {}
    for p in node_pts:
        node_xy[(round(p[0], 7), round(p[1], 7))] = fwd.transform(p[0], p[1])
    # arms per node endpoint (exact coordinate match, mirroring the loader)
    arms: dict = {}
    for coords in edges:
        xy = to_xy(coords)
        for endpt in (coords[0], coords[-1]):
            key = (round(endpt[0], 7), round(endpt[1], 7))
            if key in node_xy:
                arms.setdefault(key, []).append((xy, endpt))

    def clip_from(xy, at_start):
        """First JUNCTION_HALF_M of the arm measured from the node end."""
        pts = np.asarray(xy if at_start else xy[::-1])
        seg = np.hypot(*(pts[1:] - pts[:-1]).T)
        cum = np.concatenate([[0.0], np.cumsum(seg)])
        if cum[-1] <= JUNCTION_HALF_M:
            return pts
        n = int(np.searchsorted(cum, JUNCTION_HALF_M)) + 1
        out = pts[:n].copy()
        return out

    devs, worst = [], (0.0, None, None)
    for key, arm_list in arms.items():
        if len(arm_list) < 3:
            continue
        nxy = node_xy[key]
        clipped = []
        for xy, _endpt in arm_list:
            # orient so the node end is first
            first_close = np.hypot(xy[0][0] - nxy[0], xy[0][1] - nxy[1])
            last_close = np.hypot(xy[-1][0] - nxy[0], xy[-1][1] - nxy[1])
            clipped.append(clip_from(xy, first_close <= last_close))
        best = None
        for i in range(len(clipped)):
            for j in range(i + 1, len(clipped)):
                a = clipped[i][::-1]           # run INTO the node
                b = clipped[j]                 # run OUT of the node
                if len(a) < 2 or len(b) < 2:
                    continue
                # interior turn at the node
                va = np.array(a[-1]) - np.array(a[-2])
                vb = np.array(b[1]) - np.array(b[0])
                ang = math.degrees(math.atan2(
                    va[0] * vb[1] - va[1] * vb[0], va[0] * vb[0] + va[1] * vb[1]))
                # near-straight through pair only (|deflection| < 60 deg)
                if abs(ang) > 60.0:
                    continue
                thru = np.vstack([a, b[1:]])
                chord = LineString([thru[0], thru[-1]])
                d = max(chord.distance(_pt(p)) for p in thru)
                if best is None or d < best:
                    best = float(d)
        if best is not None:
            devs.append(best)
            if best > worst[0]:
                worst = (best, key[0], key[1])
    if not devs:
        return {"junctions": 0}
    devs.sort()

    def pct(p):
        return devs[min(len(devs) - 1, int(p * len(devs)))]

    return {
        "junctions": len(devs), "mean": sum(devs) / len(devs),
        "p90": pct(0.9), "max": max(devs),
        "worst": {"dev_m": round(worst[0], 1), "lon": worst[1],
                  "lat": worst[2]},
    }


def _pt(xy):
    from shapely.geometry import Point
    return Point(xy[0], xy[1])


def _corridor_count(cur, build_key):
    cur.execute(
        "SELECT COUNT(*) FROM transit_graph_edges WHERE build_key = %s",
        (build_key,))
    return cur.fetchone()[0]


def _feature_counts(cur, build_key):
    cur.execute(
        """SELECT kind, COUNT(*) FROM transit_line_segments
           WHERE build_key = %s AND band_minzoom = %s GROUP BY kind""",
        (build_key, BAND))
    d = dict(cur.fetchall())
    return {"steady": d.get("steady", 0), "transition": d.get("transition", 0)}


def _on_osm(cur, build_key):
    fm = BUILD_FEED.get(build_key)
    if fm is None:
        return None
    feed_id, _mode = fm
    cur.execute(
        """SELECT sum((stats->>'on_osm_m')::numeric),
                  sum((stats->>'output_len_m')::numeric)
           FROM matched_shapes WHERE feed_id = %s AND stats IS NOT NULL""",
        (feed_id,))
    on_osm, out_len = cur.fetchone()
    if not out_len:
        return None
    return round(100.0 * float(on_osm) / float(out_len), 3)


# ── assembly ────────────────────────────────────────────────────────────────


def scorecard(build_key: str, dsn: str) -> dict:
    import psycopg

    card: dict = {"build_key": build_key}
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        card["corridors"] = _corridor_count(cur, build_key)
        card["features"] = _feature_counts(cur, build_key)
        card["on_osm_pct"] = _on_osm(cur, build_key)
        card["junction_deviation"] = _junction_deviation(cur, build_key)
        bbox = _bbox(cur, build_key)
        if bbox and bbox[0] is not None:
            source, n_ways = _load_regular_track(cur, build_key, bbox)
            if source is None:
                card["track_fidelity"] = {"skipped": "no ground truth in bbox"}
            else:
                tf = _track_fidelity(cur, build_key)
                tf["ground_truth"] = f"{source} ({n_ways} ways)"
                card["track_fidelity"] = tf
        else:
            card["track_fidelity"] = {"skipped": "no z15 segments"}
    card["bundles"] = _bundle_counts(_notes_sidecar(build_key, dsn))
    return card


def render(card: dict) -> str:
    bk = card["build_key"]
    L: list[str] = []
    L.append("=" * 64)
    L.append(f" SCORECARD — {bk}")
    L.append("=" * 64)
    f = card["features"]
    L.append(f" corridors (graph edges) : {card['corridors']}")
    L.append(f" features (z15)          : steady {f['steady']}, "
             f"transition {f['transition']}")
    onp = card["on_osm_pct"]
    L.append(f" on-OSM %                : "
             f"{onp if onp is None else f'{onp:.3f}%'}")
    L.append("-" * 64)
    b = card["bundles"]
    if b.get("available"):
        L.append(f" bundles  cross-family {b['cross']}  same-family {b['family']}"
                 f"  directional-pair {b['pair']}")
        kr = b.get("kiss_reasons", {})
        kr_s = ", ".join(f"{k}={v}" for k, v in sorted(kr.items())) or "none"
        L.append(f" KISSING (not bundled)   : {b['kissing']}   [{kr_s}]")
        L.append(f" absorbed {b['n_absorbed']}  contracted {b['n_contracted']}"
                 f"  off-graph runs {b['n_offgraph']}  "
                 f"raw corridors {b['n_raw_corridors']}")
    else:
        L.append(" bundles                 : (no notes sidecar — rebuild "
                 "linegraph to populate)")
    L.append("-" * 64)
    jd = card["junction_deviation"]
    if jd.get("junctions"):
        w = jd["worst"]
        L.append(f" junction through-dev    : mean {jd['mean']:.2f}  "
                 f"p90 {jd['p90']:.2f}  max {jd['max']:.2f} m  "
                 f"({jd['junctions']} junctions)")
        L.append(f"   worst @ ({w['lon']:.5f},{w['lat']:.5f})  {w['dev_m']} m")
    else:
        L.append(" junction through-dev    : (no through-junctions)")
    tf = card["track_fidelity"]
    if tf.get("skipped"):
        L.append(f" track-fidelity stray    : skipped ({tf['skipped']})")
    else:
        L.append(f" track-fidelity stray    : mean {tf['mean']:.1f}  "
                 f"p90 {tf['p90']:.1f}  max {tf['max']:.1f} m  "
                 f"over-threshold {tf['over_threshold']}")
        L.append(f"   ground truth {tf['ground_truth']}, "
                 f"{tf['measured']} steady rows measured")
        if tf.get("worst"):
            w = tf["worst"]
            L.append(f"   worst @ ({w['lon']:.5f},{w['lat']:.5f})  "
                     f"{w['stray_m']} m")
    L.append("=" * 64)
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--build-key", action="append", required=True,
                    help="e.g. nyc:subway-v3 (repeatable)")
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    cards = [scorecard(bk, args.dsn) for bk in args.build_key]
    if args.json:
        print(json.dumps(cards, indent=2, default=float))
    else:
        for c in cards:
            print(render(c))
    return 0


if __name__ == "__main__":
    sys.exit(main())
