#!/usr/bin/env python3
"""shapesnap.run — feed-level pipeline CLI (transit pipeline v3, stage 3).

Matches every pattern of the requested mode classes onto cached OSM mode
graphs (shapesnap.graph / shapesnap.match) and REWRITES the processed
GTFS zip in place, so MOTIS, the DB import, and the display pipeline all
consume identical OSM-aligned geometry:

  shapes.txt   — matched geometries, deduped per feed by geometry hash
                 (ids ``snap_<hash12>``), plus every original shape still
                 referenced by untouched trips. Original rows for shapes
                 nothing references anymore are dropped.
  trips.txt    — trips of MATCHED patterns (hmm_dense / hmm_sparse /
                 hmm_sparse_rescue) are remapped to the deduped snap
                 shape ids. Trips of passthrough_agency / passthrough
                 patterns keep their original shape_id untouched.
  shape_dist_traveled — recomputed along the new geometry in the SAME
                 unit the feed used (inferred from the original
                 shapes.txt: meters / feet / km / miles), and the
                 stop_times.txt rows of remapped trips get matching stop
                 distances (monotonic projection onto the new shape) so
                 the dist reference stays consistent zip-wide.

Metadata lands in PostGIS (tables created idempotently):
  matched_shapes — one row per pattern: feed_id, route_id, direction_id,
                   pattern_id, shape_id, method, confidence, stats jsonb
                   (gates, levels_m breakdown, …), geom.
  shapesnap_runs — one row per run with the per-feed summary jsonb.

--dry-run: match + report only — no zip write, no DB write.

Config: config/shapesnap.json — per feed {enabled, modes, pbf, bbox,
graphStem}. ``enabled`` gates only the import-pipeline hook
(applyShapeRewrite in import/import-gtfs.ts); this CLI runs for any feed
you point it at. The zip is rewritten atomically (tmp + rename): a
crashed run never leaves a half-written artifact.

The last stdout line is machine-readable for the hook:
  [shapesnap] SUMMARY {json}

CLI (repo convention — uv, never system python):
  uv run --with-requirements shapesnap/requirements.txt \
      python -m shapesnap.run --feed 29 --zip data/gtfs-processed/29.zip
  # scripts/run-shapesnap.sh 29 wraps exactly this.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import math
import os
import shutil
import statistics
import sys
import time
import zipfile
from collections import Counter
from pathlib import Path

from shapely.geometry import LineString, Point
from shapely.ops import substring

from shapesnap.candidates import MatchGraph, StationIndex, load_stations
from shapesnap.graph import (
    MODE_CLASSES,
    REPO_ROOT,
    ModeGraph,
    build_graph,
    default_cache_path,
    is_stale,
    load_graph,
    save_graph,
)
from shapesnap.match import (
    MatchConfig,
    geometry_hash,
    load_patterns,
    match_pattern,
)

CONFIG_PATH = REPO_ROOT / "config" / "shapesnap.json"
DEFAULT_DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)
MATCHED_METHODS = ("hmm_dense", "hmm_sparse", "hmm_sparse_rescue")

# shape_dist_traveled unit inference: recorded total / geometric meters
UNIT_FACTORS = {"m": 1.0, "ft": 3.280839895, "km": 0.001, "mi": 0.000621371}
UNIT_TOLERANCE = 0.2  # relative; feeds' own shapes wiggle a few percent


# ── config ───────────────────────────────────────────────────────────────────


def load_feed_config(feed_id: str) -> dict:
    """Per-feed block from config/shapesnap.json ({} when absent)."""
    if not CONFIG_PATH.exists():
        return {}
    cfg = json.loads(CONFIG_PATH.read_text())
    return (cfg.get("feeds") or {}).get(feed_id, {}) or {}


def cache_path_for(pbf: Path, mode: str, stem: str | None) -> Path:
    if stem is None:
        return default_cache_path(pbf, mode)
    return pbf.parent / "shapesnap" / f"{stem}.{mode}.graph.pkl.gz"


def ensure_graph(pbf: Path, mode: str, stem: str | None, bbox) -> ModeGraph:
    """Load the mode graph cache; rebuild when missing/stale/bbox-changed."""
    cache = cache_path_for(pbf, mode, stem)
    want_bbox = tuple(bbox) if bbox else None
    if cache.exists():
        try:
            g = load_graph(cache)
            have_bbox = tuple(g.bbox) if g.bbox else None
            if have_bbox == want_bbox and not is_stale(g, pbf):
                print(
                    f"[shapesnap.run] graph cache hit: {cache.name} "
                    f"({len(g.edges)} edges)"
                )
                return g
            print(f"[shapesnap.run] graph cache stale/mismatched: {cache.name} — rebuilding")
        except Exception as err:  # unreadable/old format: rebuild
            print(f"[shapesnap.run] graph cache unreadable ({err}) — rebuilding")
    print(f"[shapesnap.run] building {mode} graph from {pbf} (bbox={want_bbox})")
    g = build_graph(pbf, mode, bbox=want_bbox)
    save_graph(g, cache)
    print(f"[shapesnap.run] cached {cache.name} ({len(g.edges)} edges)")
    return g


# ── geometry helpers ─────────────────────────────────────────────────────────


def cumulative_m(coords_xy) -> list:
    """Cumulative arc length (projected meters) at every vertex."""
    out = [0.0]
    for a, b in zip(coords_xy, coords_xy[1:]):
        out.append(out[-1] + math.hypot(b[0] - a[0], b[1] - a[1]))
    return out


def project_stops_monotonic(line: LineString, stops_xy) -> list:
    """Along-line position of every stop, never moving backwards.

    Nearest-forward projection handles loop patterns (a stop revisited
    later must land at a later arc position).
    """
    total = line.length
    pos, out = 0.0, []
    for xy in stops_xy:
        if pos >= total:
            out.append(total)
            continue
        tail = substring(line, pos, total)
        if tail.is_empty or tail.geom_type == "Point":
            out.append(total)
            pos = total
            continue
        pos = min(total, pos + max(0.0, tail.project(Point(xy))))
        out.append(pos)
    return out


def level_breakdown(graph: ModeGraph, edge_idxs) -> dict:
    """Matched meters by vertical class — the cross-contamination receipt."""
    out = {"elevated_m": 0.0, "subway_m": 0.0, "surface_m": 0.0}
    for i in edge_idxs:
        e = graph.edges[i]
        t = e.tags
        raw_layer = (t.get("layer") or "").strip()
        try:
            layer = int(raw_layer) if raw_layer else 0
        except ValueError:
            layer = 0
        if (t.get("tunnel") not in (None, "no")) or layer < 0:
            key = "subway_m"
        elif (t.get("bridge") not in (None, "no")) or layer >= 1:
            key = "elevated_m"
        else:
            key = "surface_m"
        out[key] += e.length_m
    return {k: round(v, 1) for k, v in out.items()}


# ── shapes.txt scan (row count + dist-unit inference) ────────────────────────


def scan_shapes(zf: zipfile.ZipFile) -> dict:
    """One pass over shapes.txt.

    Returns {present, rows, has_dist_col, has_dist_values, unit, factor,
    integral}. Unit inference compares each shape's recorded total
    shape_dist_traveled against its geometric length (equirectangular
    meters, points sorted by shape_pt_sequence — GTFS does not require
    file order to follow sequence order, and accumulating unordered rows
    as-read inflates the length and skews the ratio) and snaps the median
    ratio to m/ft/km/mi; an off-scale ratio keeps the feed's own median
    so rewritten values stay proportionally consistent with untouched
    ones.
    """
    info = {
        "present": False,
        "rows": 0,
        "has_dist_col": False,
        "has_dist_values": False,
        "unit": None,
        "factor": None,
        "integral": True,
    }
    if "shapes.txt" not in set(zf.namelist()):
        return info
    info["present"] = True

    # buffer points per shape so they can be sequence-sorted before the
    # length accumulation (file order is not guaranteed to be seq order)
    per: dict = {}  # shape_id -> [[(seq, lon, lat), ...], max_dist]
    with zf.open("shapes.txt") as f:
        rdr = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        info["has_dist_col"] = "shape_dist_traveled" in (rdr.fieldnames or [])
        for r in rdr:
            info["rows"] += 1
            try:
                lon, lat = float(r["shape_pt_lon"]), float(r["shape_pt_lat"])
            except (KeyError, TypeError, ValueError):
                continue
            s = per.get(r.get("shape_id"))
            if s is None:
                s = per[r.get("shape_id")] = [[], 0.0]
            try:
                seq = int(r.get("shape_pt_sequence") or "")
            except ValueError:
                seq = len(s[0])  # unparsable sequence: keep file order
            s[0].append((seq, lon, lat))
            if info["has_dist_col"]:
                v = (r.get("shape_dist_traveled") or "").strip()
                if v:
                    try:
                        d = float(v)
                    except ValueError:
                        continue
                    info["has_dist_values"] = True
                    if d != int(d):
                        info["integral"] = False
                    if d > s[1]:
                        s[1] = d

    if not info["has_dist_values"]:
        return info
    ratios = []
    for pts, max_dist in per.values():
        if max_dist <= 0 or len(pts) < 2:
            continue
        pts.sort(key=lambda p: p[0])  # stable: equal seqs keep file order
        length = 0.0
        p_lon, p_lat = pts[0][1], pts[0][2]
        for _, lon, lat in pts[1:]:
            lat0 = math.radians(p_lat)
            length += math.hypot(
                (lon - p_lon) * 111320.0 * math.cos(lat0), (lat - p_lat) * 110574.0
            )
            p_lon, p_lat = lon, lat
        if length > 200.0:
            ratios.append(max_dist / length)
    if not ratios:
        return info
    med = statistics.median(ratios)
    best = min(UNIT_FACTORS.items(), key=lambda kv: abs(math.log(med / kv[1])))
    if abs(med / best[1] - 1.0) <= UNIT_TOLERANCE:
        info["unit"], info["factor"] = best[0], best[1]
    else:
        info["unit"], info["factor"] = f"custom({med:.4g}/m)", med
    return info


def fmt_dist(meters: float, factor: float, integral: bool) -> str:
    v = meters * factor
    return str(int(round(v))) if integral else f"{v:.2f}"


# ── zip rewrite ──────────────────────────────────────────────────────────────


def rewrite_zip(
    zip_path: Path,
    remap: dict,        # trip_id -> snap shape id
    snap_shapes: dict,  # snap id -> (coords lonlat, cumulative meters)
    trip_dists: dict,   # trip_id -> [stop along-positions, meters]
) -> dict:
    """Rewrite shapes.txt / trips.txt / stop_times.txt atomically in place.

    Every other zip member is streamed through byte-identical. Returns
    rewrite stats for the run summary.
    """
    tmp = zip_path.with_name(zip_path.name + ".tmp")
    stats: dict = {}
    with zipfile.ZipFile(zip_path) as zin:
        shapes_info = scan_shapes(zin)
        factor, integral = shapes_info["factor"], shapes_info["integral"]
        if factor is None:
            # no usable original dist values: only emit dist when creating
            # shapes.txt from scratch (meters), never a new inconsistency
            factor, integral = 1.0, False
        emit_dist = shapes_info["has_dist_col"] or not shapes_info["present"]

        # trips.txt: remap + collect the shape ids untouched trips still use
        with zin.open("trips.txt") as f:
            rdr = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            trips_header = next(rdr)
            trips_rows = list(rdr)
        t_idx = trips_header.index("trip_id")
        if "shape_id" in trips_header:
            s_idx = trips_header.index("shape_id")
        else:
            trips_header = [*trips_header, "shape_id"]
            s_idx = len(trips_header) - 1
            trips_rows = [[*row, ""] for row in trips_rows]

        retained: set = set()
        n_remapped = 0
        for row in trips_rows:
            while len(row) <= s_idx:
                row.append("")
            new_sid = remap.get(row[t_idx])
            if new_sid is not None:
                row[s_idx] = new_sid
                n_remapped += 1
            elif row[s_idx]:
                retained.add(row[s_idx])

        st_has_dist = False
        if "stop_times.txt" in set(zin.namelist()):
            with zin.open("stop_times.txt") as f:
                st_header = (
                    io.TextIOWrapper(f, encoding="utf-8-sig").readline().strip()
                )
            st_has_dist = "shape_dist_traveled" in next(
                csv.reader([st_header])
            )
        rewrite_st = st_has_dist and bool(remap) and shapes_info["has_dist_values"]

        with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
            wrote_shapes = False
            for item in zin.infolist():
                name = item.filename
                if name == "trips.txt":
                    _write_csv(zout, name, trips_header, trips_rows)
                elif name == "shapes.txt":
                    stats.update(
                        _write_shapes(
                            zin, zout, retained, snap_shapes, factor, integral, emit_dist
                        )
                    )
                    wrote_shapes = True
                elif name == "stop_times.txt" and rewrite_st:
                    stats["stop_times_rows_updated"] = _write_stop_times(
                        zin, zout, remap, trip_dists, factor, integral
                    )
                else:
                    if item.is_dir():
                        continue
                    zi = zipfile.ZipInfo(name, date_time=item.date_time)
                    zi.compress_type = zipfile.ZIP_DEFLATED
                    with zin.open(item) as src, zout.open(zi, "w") as dst:
                        shutil.copyfileobj(src, dst, 1 << 20)
            if not wrote_shapes and snap_shapes:
                header = [
                    "shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence",
                ] + (["shape_dist_traveled"] if emit_dist else [])
                rows_after = _append_snap_rows(
                    zout, "shapes.txt", header, snap_shapes, factor, integral, emit_dist
                )
                stats.update(shapes_rows_before=0, shapes_rows_after=rows_after)
    tmp.replace(zip_path)

    stats.update(
        trips_remapped=n_remapped,
        retained_original_shapes=len(retained),
        snap_shapes_written=len(snap_shapes),
        dist_unit=shapes_info["unit"],
    )
    stats.setdefault("stop_times_rows_updated", 0)
    return stats


def _write_csv(zout, name, header, rows):
    with io.TextIOWrapper(
        zout.open(name, "w"), encoding="utf-8", newline=""
    ) as tw:
        w = csv.writer(tw)
        w.writerow(header)
        w.writerows(rows)


def _snap_rows(snap_shapes, factor, integral, emit_dist):
    for sid in sorted(snap_shapes):
        coords, cum = snap_shapes[sid]
        for i, (lon, lat) in enumerate(coords):
            row = [sid, f"{lat:.6f}", f"{lon:.6f}", str(i + 1)]
            if emit_dist:
                row.append(fmt_dist(cum[i], factor, integral))
            yield row


def _append_snap_rows(zout, name, header, snap_shapes, factor, integral, emit_dist):
    n = 0
    with io.TextIOWrapper(zout.open(name, "w"), encoding="utf-8", newline="") as tw:
        w = csv.writer(tw)
        w.writerow(header)
        for row in _snap_rows(snap_shapes, factor, integral, emit_dist):
            w.writerow(row)
            n += 1
    return n


def _write_shapes(zin, zout, retained, snap_shapes, factor, integral, emit_dist):
    """Stream original shapes.txt keeping still-referenced rows, then append
    the deduped snap shapes (fields mapped into the ORIGINAL header)."""
    before = after = 0
    with zin.open("shapes.txt") as f, io.TextIOWrapper(
        zout.open("shapes.txt", "w"), encoding="utf-8", newline=""
    ) as tw:
        rdr = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        header = next(rdr)
        w = csv.writer(tw)
        w.writerow(header)
        sid_i = header.index("shape_id")
        col = {name: header.index(name) for name in header}
        for row in rdr:
            before += 1
            if row[sid_i] in retained:
                w.writerow(row)
                after += 1
        std = ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"]
        if emit_dist and "shape_dist_traveled" in col:
            std.append("shape_dist_traveled")
        for srow in _snap_rows(snap_shapes, factor, integral, emit_dist):
            out = [""] * len(header)
            for field, value in zip(std, srow):
                out[col[field]] = value
            w.writerow(out)
            after += 1
    return {"shapes_rows_before": before, "shapes_rows_after": after}


def _write_stop_times(zin, zout, remap, trip_dists, factor, integral):
    """Stream stop_times.txt; rows of remapped trips get their
    shape_dist_traveled recomputed against the new shape (buffered per
    trip, sorted by stop_sequence, emitted after the untouched rows)."""
    buffered: dict = {}
    n_updated = 0
    with zin.open("stop_times.txt") as f, io.TextIOWrapper(
        zout.open("stop_times.txt", "w"), encoding="utf-8", newline=""
    ) as tw:
        rdr = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        header = next(rdr)
        w = csv.writer(tw)
        w.writerow(header)
        t_i = header.index("trip_id")
        seq_i = header.index("stop_sequence")
        d_i = header.index("shape_dist_traveled")
        for row in rdr:
            if row[t_i] in remap:
                buffered.setdefault(row[t_i], []).append(row)
            else:
                w.writerow(row)
        for tid, rows in buffered.items():
            rows.sort(key=lambda r: int(r[seq_i]))
            dists = trip_dists.get(tid)
            usable = dists is not None and len(dists) == len(rows)
            for k, row in enumerate(rows):
                if usable:
                    row[d_i] = fmt_dist(dists[k], factor, integral)
                    n_updated += 1
                w.writerow(row)
    return n_updated


# ── PostGIS metadata ─────────────────────────────────────────────────────────

MATCHED_SHAPES_DDL = """
CREATE TABLE IF NOT EXISTS matched_shapes (
    feed_id      text NOT NULL,
    route_id     text,
    direction_id smallint,
    pattern_id   text NOT NULL,
    shape_id     text,
    method       text NOT NULL,
    confidence   real,
    stats        jsonb,
    geom         geometry(LineString, 4326),
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (feed_id, pattern_id)
)"""

SHAPESNAP_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS shapesnap_runs (
    id         bigserial PRIMARY KEY,
    feed_id    text NOT NULL,
    modes      text[] NOT NULL,
    dry_run    boolean NOT NULL DEFAULT false,
    summary    jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
)"""


def write_db(
    dsn: str, feed_id: str, results: list, summary: dict, dry_run: bool,
    prune: bool = False,
):
    """Upsert per-pattern metadata + insert the run summary row.

    With prune=True (full, unfiltered runs only) also delete the feed's
    rows — for the modes this run covered — whose pattern no longer
    exists, so stale geometry never lingers after a feed update. Partial
    runs (--routes / --limit) must pass prune=False: their pattern list
    is incomplete by construction.
    """
    import psycopg
    from psycopg.types.json import Jsonb

    with psycopg.connect(dsn, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(MATCHED_SHAPES_DDL)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS matched_shapes_geom_idx "
            "ON matched_shapes USING gist (geom)"
        )
        cur.execute(SHAPESNAP_RUNS_DDL)
        for mode, p, r, shape_id in results:
            geom = None
            if len(r.coords) >= 2 and len({(round(x, 7), round(y, 7)) for x, y in r.coords}) >= 2:
                geom = (
                    "SRID=4326;LINESTRING("
                    + ",".join(f"{lon:.7f} {lat:.7f}" for lon, lat in r.coords)
                    + ")"
                )
            stats = {
                **r.stats,
                "mode": mode,
                "gates": r.gates.as_dict() if r.gates else None,
                "trip_count": p.trip_count,
                "route_short_name": p.route_short_name,
            }
            cur.execute(
                """INSERT INTO matched_shapes
                     (feed_id, route_id, direction_id, pattern_id, shape_id,
                      method, confidence, stats, geom, updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s, ST_GeomFromEWKT(%s), now())
                   ON CONFLICT (feed_id, pattern_id) DO UPDATE SET
                     route_id = EXCLUDED.route_id,
                     direction_id = EXCLUDED.direction_id,
                     shape_id = EXCLUDED.shape_id,
                     method = EXCLUDED.method,
                     confidence = EXCLUDED.confidence,
                     stats = EXCLUDED.stats,
                     geom = EXCLUDED.geom,
                     updated_at = now()""",
                (
                    feed_id, p.route_id, p.direction_id, p.key, shape_id,
                    r.method, r.confidence, Jsonb(stats), geom,
                ),
            )
        if prune:
            cur.execute(
                """DELETE FROM matched_shapes
                     WHERE feed_id = %s
                       AND stats->>'mode' = ANY(%s::text[])
                       AND NOT (pattern_id = ANY(%s::text[]))""",
                (feed_id, summary["modes"], [p.key for _, p, _, _ in results]),
            )
            if cur.rowcount:
                summary["stale_shapes_deleted"] = cur.rowcount
        cur.execute(
            "INSERT INTO shapesnap_runs (feed_id, modes, dry_run, summary) "
            "VALUES (%s,%s,%s,%s)",
            (feed_id, summary["modes"], dry_run, Jsonb(summary)),
        )
        conn.commit()


# ── cli ──────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m shapesnap.run",
        description="Match a feed's patterns onto OSM and rewrite its processed zip.",
    )
    ap.add_argument("--feed", required=True, help="feed id (e.g. 29)")
    ap.add_argument("--zip", type=Path, required=True, help="processed GTFS zip (rewritten in place)")
    ap.add_argument("--modes", default=None, help="comma list of rail,bus,ferry (default: config)")
    ap.add_argument("--dry-run", action="store_true", help="match + report; no zip/DB writes")
    ap.add_argument("--routes", default=None, help="comma-separated route_ids (debugging)")
    ap.add_argument("--limit", type=int, default=None, help="max patterns per mode (debugging)")
    ap.add_argument("--pbf", type=Path, default=None, help="override the configured OSM extract")
    ap.add_argument(
        "--db", default=None,
        help="PostGIS DSN for metadata (default $DATABASE_URL or the dev DB); "
             "'skip' disables the DB write",
    )
    args = ap.parse_args(argv)

    zip_path = args.zip if args.zip.is_absolute() else Path.cwd() / args.zip
    if not zip_path.exists():
        alt = REPO_ROOT / args.zip
        if alt.exists():
            zip_path = alt
        else:
            ap.error(f"zip not found: {args.zip}")

    fc = load_feed_config(args.feed)
    modes = (
        [m.strip() for m in args.modes.split(",") if m.strip()]
        if args.modes
        else list(fc.get("modes") or [])
    )
    if not modes:
        ap.error(f"feed {args.feed} has no modes in {CONFIG_PATH}; pass --modes")
    bad = [m for m in modes if m not in MODE_CLASSES]
    if bad:
        ap.error(f"unknown mode(s) {bad}; valid: {MODE_CLASSES}")
    pbf = args.pbf or Path(fc.get("pbf") or "data/region.osm.pbf")
    if not pbf.is_absolute():
        pbf = REPO_ROOT / pbf
    if not pbf.exists():
        ap.error(f"pbf not found: {pbf} (config/shapesnap.json feeds.{args.feed}.pbf)")
    bbox = fc.get("bbox")
    stem = fc.get("graphStem")
    route_ids = set(args.routes.split(",")) if args.routes else None

    t0 = time.perf_counter()
    cfg = MatchConfig()
    results: list = []       # (mode, Pattern, MatchResult, shape_id|None)
    snap_shapes: dict = {}   # snap id -> (coords lonlat, cumulative m)
    snap_by_hash: dict = {}
    remap: dict = {}         # trip_id -> snap id
    trip_dists: dict = {}    # trip_id -> stop along-positions (m)
    n_dup = 0

    for mode in modes:
        graph = ensure_graph(pbf, mode, stem, bbox)
        mg = MatchGraph(graph)
        stations = load_stations(pbf, mode)
        station_idx = StationIndex(stations, mg)
        print(f"[shapesnap.run] {mode}: station index {len(stations)} stations")

        patterns = load_patterns(zip_path, route_ids=route_ids, modes={mode})
        if args.limit:
            patterns = patterns[: args.limit]
        print(f"[shapesnap.run] {mode}: {len(patterns)} patterns to match")

        for p in patterns:
            r = match_pattern(mg, p, cfg, station_idx=station_idx)
            r.stats["levels_m"] = level_breakdown(graph, r.edges_used)
            shape_id = None
            if r.method in MATCHED_METHODS:
                h = geometry_hash(r.coords)
                shape_id = snap_by_hash.get(h)
                if shape_id is None:
                    shape_id = f"snap_{h[:12]}"
                    snap_by_hash[h] = shape_id
                    xy = mg.project_lonlat(r.coords)
                    snap_shapes[shape_id] = (list(r.coords), cumulative_m(xy))
                else:
                    n_dup += 1
                dists = project_stops_monotonic(
                    LineString(mg.project_lonlat(snap_shapes[shape_id][0])),
                    mg.project_lonlat(p.stop_coords),
                )
                for tid in p.trip_ids:
                    remap[tid] = shape_id
                    trip_dists[tid] = dists
            elif r.method == "passthrough_agency":
                shape_id = p.shape_id  # kept as-is in the zip
            g = r.gates.as_dict() if r.gates else {}
            agency_m = r.stats.get("agency_m") or 0.0
            print(
                f"  {p.key} trips={p.trip_count} stops={len(p.stop_ids)} "
                f"method={r.method} conf={r.confidence} "
                f"pts={r.stats.get('output_points')} breaks={r.stats.get('breaks')} "
                f"gates={g.get('failures') or 'ok'} t={r.stats.get('runtime_s')}s"
                + (f" agency_m={round(agency_m, 1)}" if agency_m > 0.05 else "")
                + (f" -> {shape_id}" if r.method in MATCHED_METHODS else "")
            )
            results.append((mode, p, r, shape_id))

    methods = Counter(r.method for _, _, r, _ in results)
    matched_n = sum(methods.get(m, 0) for m in MATCHED_METHODS)
    worst_frechet = max(
        (
            r.gates.frechet_m
            for _, _, r, _ in results
            if r.method in MATCHED_METHODS and r.gates and r.gates.frechet_m is not None
        ),
        default=None,
    )
    # the on-OSM acceptance metric: meters of OUTPUT geometry on OSM edges
    # vs spliced agency geometry, aggregated from the per-pattern stats;
    # every pattern below 100% is listed with why (its method + meters)
    on_osm_total = sum(r.stats.get("on_osm_m") or 0.0 for _, _, r, _ in results)
    agency_total = sum(r.stats.get("agency_m") or 0.0 for _, _, r, _ in results)
    off_osm = sorted(
        (
            {
                "pattern": p.key,
                "route": p.route_short_name or p.route_id,
                "method": r.method,
                "agency_m": round(r.stats.get("agency_m") or 0.0, 1),
                "on_osm_m": round(r.stats.get("on_osm_m") or 0.0, 1),
            }
            for _, p, r, _ in results
            if (r.stats.get("agency_m") or 0.0) > 0.05
        ),
        key=lambda e: -e["agency_m"],
    )
    summary: dict = {
        "feed_id": args.feed,
        "modes": modes,
        "dry_run": args.dry_run,
        "patterns": len(results),
        "matched": matched_n,
        "passthrough_agency": methods.get("passthrough_agency", 0),
        "passthrough": methods.get("passthrough", 0),
        "methods": dict(methods),
        "unique_matched_shapes": len(snap_shapes),
        "duplicate_geometries": n_dup,
        "trips_remapped": len(remap),
        "worst_frechet_m": None if worst_frechet is None else round(worst_frechet, 1),
        "matched_points": sum(len(c) for c, _ in snap_shapes.values()),
        "on_osm_m": round(on_osm_total, 1),
        "agency_m": round(agency_total, 1),
        "on_osm_pct": (
            round(100.0 * on_osm_total / (on_osm_total + agency_total), 3)
            if on_osm_total + agency_total > 0
            else None
        ),
        "patterns_off_osm": off_osm,
    }

    if args.dry_run:
        with zipfile.ZipFile(zip_path) as zf:
            info = scan_shapes(zf)
        summary["shapes_rows_before"] = info["rows"]
        summary["dist_unit"] = info["unit"]
        print("[shapesnap.run] dry run — zip and DB untouched")
    elif snap_shapes:
        summary.update(rewrite_zip(zip_path, remap, snap_shapes, trip_dists))
        print(
            f"[shapesnap.run] rewrote {zip_path.name}: shapes.txt "
            f"{summary['shapes_rows_before']} -> {summary['shapes_rows_after']} rows, "
            f"{summary['trips_remapped']} trips remapped, "
            f"{summary['stop_times_rows_updated']} stop_times rows updated "
            f"(dist unit: {summary['dist_unit']})"
        )
    else:
        print("[shapesnap.run] no matched patterns — zip left untouched")

    summary["runtime_s"] = round(time.perf_counter() - t0, 1)

    if not args.dry_run and args.db != "skip":
        dsn = args.db or DEFAULT_DSN
        # only a full run may prune stale matched_shapes rows: --routes /
        # --limit runs see a subset of the feed's patterns by construction
        full_run = route_ids is None and args.limit is None
        try:
            write_db(dsn, args.feed, results, summary, args.dry_run, prune=full_run)
            summary["db"] = "ok"
            print(f"[shapesnap.run] metadata: {len(results)} matched_shapes rows + 1 run row")
        except Exception as err:  # metadata is QA — never fail the rewrite over it
            summary["db"] = f"failed: {err}"
            print(f"[shapesnap.run] WARNING: PostGIS metadata write failed: {err}", file=sys.stderr)

    print(
        f"[shapesnap.run] feed {args.feed} ({','.join(modes)}): "
        f"{summary['patterns']} patterns — {summary['matched']} matched, "
        f"{summary['passthrough_agency']} passthrough_agency, "
        f"{summary['passthrough']} passthrough; "
        f"{summary['unique_matched_shapes']} unique shapes "
        f"({summary['duplicate_geometries']} dups); "
        f"worst Fréchet {summary['worst_frechet_m']} m; "
        f"{summary['runtime_s']}s"
    )
    pct = summary["on_osm_pct"]
    print(
        f"[shapesnap.run] on-OSM: "
        f"{'n/a' if pct is None else f'{pct}%'} of "
        f"{round(on_osm_total + agency_total, 1)} m output "
        f"({summary['agency_m']} m agency); "
        f"{len(off_osm)} patterns below 100%"
    )
    for e in off_osm[:20]:
        print(
            f"  off-OSM {e['pattern']} route={e['route']} method={e['method']} "
            f"agency_m={e['agency_m']} on_osm_m={e['on_osm_m']}"
        )
    if len(off_osm) > 20:
        print(f"  ... and {len(off_osm) - 20} more (full list in the run summary)")
    print(f"[shapesnap] SUMMARY {json.dumps(summary)}")
    return 0


if __name__ == "__main__":
    # canonical module name so pickled caches stay loadable (see graph.py)
    from shapesnap.run import main as _main

    sys.exit(_main())
