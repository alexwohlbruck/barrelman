"""tools.sandbox.rebuild — fast local-rebuild sandbox (Part 3).

Iterate a WaygraphConfig / SegmentConfig dial for ONE site WITHOUT
reprocessing all ~218 patterns. Given a site's bbox (+ a generous buffer),
it extracts only the feed patterns whose MATCHED SHAPE intersects the
buffered bbox, runs the real pipeline over that subset:

    load_patterns -> (bbox filter) -> build_waygraph_linegraph (cfg override,
    no cache) -> snap_stations -> emit_build(sandbox:<site>) ->
    lineorder.apply(sandbox:<site>) -> segments.build(sandbox:<site>, emit)

then re-verifies with tools.sandbox.verify against the scratch build_key and
prints the before/after measurement + writes a before/after render.

  uv run --with-requirements segments/requirements.txt \
      python -m tools.sandbox.rebuild --site dekalb
  uv run --with-requirements segments/requirements.txt \
      python -m tools.sandbox.rebuild --site dekalb --set cross_family_gap_m=22

TRUNCATION LIMITATION (read this)
  The rebuild is scoped to the patterns intersecting the BUFFERED bbox, so a
  corridor/merge whose evidence extends beyond the buffer is judged on a
  truncated pattern (a merge that needs 450 m of sustained co-run may be cut
  short if the co-run leaves the buffer). Use a generous buffer (default
  2 km) and treat the result as an INDICATOR, not the authority: the FULL
  build (`python -m linegraph.build ... && lineorder.apply && segments.build`)
  remains the ground truth. The scratch build_key `sandbox:<site>` is
  delete-and-replaced each run and is never the canonical geometry.
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from segments.corridors import DEFAULT_DSN            # noqa: E402
from tools.sandbox.sites import get                   # noqa: E402

# nyc:subway-v3 -> feed 5, chicago:l-v3 -> feed 29 (from tools.scorecard)
BUILD_FEED = {"nyc:subway-v3": ("5", "rail"), "chicago:l-v3": ("29", "rail")}
DEFAULT_BUFFER_M = 2000.0


def _bbox_buffered(site, buffer_m):
    w, s, e, n = site.bbox
    dlat = buffer_m / 111000.0
    import math
    dlon = buffer_m / (111000.0 * math.cos(math.radians(site.center[1])))
    return (w - dlon, s - dlat, e + dlon, n + dlat)


def _shape_in_bbox(shape_coords, bbox) -> bool:
    w, s, e, n = bbox
    for lon, lat in shape_coords:
        if w <= lon <= e and s <= lat <= n:
            return True
    return False


def _parse_overrides(kvs):
    """--set a=b --set c=d  ->  {a: coerced(b)}. Coerces to float when the
    value parses as a number, else keeps the string."""
    out = {}
    for kv in kvs or ():
        if "=" not in kv:
            raise SystemExit(f"--set expects key=value, got {kv!r}")
        k, v = kv.split("=", 1)
        try:
            out[k.strip()] = float(v)
        except ValueError:
            out[k.strip()] = v.strip()
    return out


def rebuild_site(site, overrides, buffer_m, dsn, verbose=True):
    """Run the truncated pipeline for one site to `sandbox:<key>`. Returns
    the scratch build_key."""
    from shapesnap.match import load_patterns
    from linegraph.build import (resolve_feed_zip, dedup_shapes,
                                 build_waygraph_linegraph,
                                 waygraph_edge_routes)
    from linegraph.emit import emit_build
    from linegraph.stations import load_station_complexes, snap_stations
    from linegraph.waygraph import WaygraphConfig

    feed_id, mode = BUILD_FEED[site.build_key]
    scratch = f"sandbox:{site.key}"
    bbox = _bbox_buffered(site, buffer_m)
    zip_path = resolve_feed_zip(feed_id)

    t0 = time.perf_counter()
    all_patterns = load_patterns(zip_path, modes={mode})
    # keep only patterns whose matched shape touches the buffered bbox
    patterns = [p for p in all_patterns
                if p.shape and _shape_in_bbox(p.shape, bbox)]
    if verbose:
        print(f"[rebuild] {site.key}: {len(patterns)}/{len(all_patterns)} "
              f"patterns intersect the {buffer_m:.0f} m-buffered bbox "
              f"({time.perf_counter()-t0:.1f}s)")
    if not patterns:
        raise SystemExit(f"no patterns intersect {site.key}'s bbox")

    shapes, _n_skipped = dedup_shapes(patterns)
    wg_fields = {f for f in WaygraphConfig.__dataclass_fields__}
    seg_over = {k: v for k, v in overrides.items() if k not in wg_fields}
    wg_over = {k: v for k, v in overrides.items() if k in wg_fields}
    cfg = WaygraphConfig(**wg_over) if wg_over else WaygraphConfig()
    if verbose and wg_over:
        print(f"[rebuild] WaygraphConfig overrides: {wg_over}")

    # corridors (no cache — a truncated subset must never poison the cache)
    lg, _notes = build_waygraph_linegraph(
        patterns, shapes, feed_id, mode, scratch, cfg=cfg,
        use_cache=False, force=True, verbose=verbose)

    # stations + attribution + emit (mirrors linegraph.build phase B)
    stop_ids = {sid for p in patterns for sid in p.stop_ids}
    complexes = load_station_complexes(zip_path, stop_ids)
    lg, snap = snap_stations(lg, complexes)
    edge_routes = waygraph_edge_routes(lg, zip_path, feed_id)
    emit_build(lg, edge_routes, snap.labels, build_key=scratch,
               feed_id=feed_id, mode=mode, dsn=dsn)
    if verbose:
        print(f"[rebuild] emitted transit_graph_* for {scratch}: "
              f"{len(lg.nodes)} nodes, {len(lg.edges)} edges")

    # lineorder slots
    from lineorder.writeback import apply_build
    apply_build(scratch, dsn)

    # segments -> transit_line_segments (all bands), delete-and-replace
    from segments.corridors import load_graph as seg_load_graph
    from segments.segment import (SegmentConfig, band_ranges, build_segments)
    from segments.build import load_shapes, load_ways
    from segments.emit import emit_segments

    scfg = SegmentConfig(**seg_over) if seg_over else SegmentConfig()
    g = seg_load_graph(scratch, dsn)
    try:
        seg_shapes = load_shapes(g, dsn)
    except Exception:
        seg_shapes = None
    try:
        seg_ways = load_ways(g, dsn)
    except Exception:
        seg_ways = None
    band_segments = []
    for bmz, bMz, length in band_ranges(scfg.bands):
        bcfg = replace(scfg, transition_len_m=length)
        segs, _info = build_segments(g, bcfg, shapes=seg_shapes, ways=seg_ways)
        band_segments.append((bmz, bMz, segs))
    n = emit_segments(band_segments, build_key=scratch, dsn=dsn)
    if verbose:
        print(f"[rebuild] emitted {n} transit_line_segments rows for "
              f"{scratch} in {time.perf_counter()-t0:.1f}s total")
    return scratch


def _measure(site, build_key, dsn, band):
    """Run the Part-2 measurement for `site` against an arbitrary build_key
    and return the verdict dict (no PNG)."""
    from dataclasses import replace as dc_replace
    from segments.segment import LocalProj
    import psycopg
    from tools.sandbox import verify as V

    s = dc_replace(site, build_key=build_key)
    w, sth, e, n = s.bbox
    pad = V.FETCH_PAD_M / 111000.0
    env = (w - pad, sth - pad, e + pad, n + pad)
    proj = LocalProj(s.center[0], s.center[1])
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        rows = V.fetch_segments(cur, build_key, band, env)
        feats = V._build_features(rows, proj)
        track_ll, _src = V.fetch_track(cur, env)
        track_xy = [proj.to_xy(c) for c in track_ll if len(c) >= 2]
    res = V.measure_site(s, feats, s.zoom, track_xy=track_xy)
    res.pop("_rendered", None)
    return res


def _render(site, build_key, dsn, band, out_path):
    """Render one panel for `site` against `build_key` to out_path."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from dataclasses import replace as dc_replace
    from segments.segment import LocalProj
    import psycopg
    from tools.sandbox import verify as V

    s = dc_replace(site, build_key=build_key)
    w, sth, e, n = s.bbox
    pad = V.FETCH_PAD_M / 111000.0
    env = (w - pad, sth - pad, e + pad, n + pad)
    proj = LocalProj(s.center[0], s.center[1])
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        rows = V.fetch_segments(cur, build_key, band, env)
        feats = V._build_features(rows, proj)
        track_ll, _src = V.fetch_track(cur, env)
        track_xy = [proj.to_xy(c) for c in track_ll if len(c) >= 2]
        stations = V.fetch_stations(cur, build_key, env)
        stations_xy = proj.to_xy([(x, y) for (_l, x, y) in stations])
    res = V.measure_site(s, feats, s.zoom, track_xy=track_xy)
    fig, ax = plt.subplots(figsize=(9, 7), dpi=100)
    fig.subplots_adjust(left=0.01, right=0.99, bottom=0.01, top=0.90)
    V.draw_panel(ax, s, res, track_xy, stations_xy, s.zoom)
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=100, facecolor="white")
    plt.close(fig)
    return res


def _short(res):
    for m in res.get("measurements", []):
        if "median_gap_px" in m:
            return (f"gap {m['median_gap_px']}px  coincidence "
                    f"{m.get('centerline_coincidence_m')}m  "
                    f"(exp {m.get('expected_gap_px')}px)  -> {res['verdict']}")
        if "parallel_run_m" in m:
            return (f"co-run {m['parallel_run_m']}m -> {res['verdict']}")
        if "max_stray_from_osm_track_m" in m:
            return (f"stray {m['max_stray_from_osm_track_m']}m -> "
                    f"{res['verdict']}")
    return res["verdict"]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Fast per-site rebuild sandbox")
    ap.add_argument("--site", required=True)
    ap.add_argument("--set", action="append", default=[], metavar="key=value",
                    help="WaygraphConfig/SegmentConfig dial override "
                         "(repeatable), e.g. --set cross_family_gap_m=22")
    ap.add_argument("--buffer-m", type=float, default=DEFAULT_BUFFER_M)
    ap.add_argument("--band", type=int, default=15)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--out-dir", default="data/exam/sandbox/rebuild")
    args = ap.parse_args(argv)

    site = get(args.site)
    if site.build_key not in BUILD_FEED:
        raise SystemExit(f"{site.build_key} has no feed mapping for rebuild")
    overrides = _parse_overrides(args.set)
    out_dir = Path(args.out_dir)

    # BEFORE = the canonical current DB build
    before = _measure(site, site.build_key, args.dsn, args.band)
    _render(site, site.build_key, args.dsn, args.band,
            out_dir / f"{site.key}-before.png")
    print(f"[before] {site.key} (canonical {site.build_key}): {_short(before)}")

    # rebuild truncated subset to sandbox:<key>, then AFTER
    scratch = rebuild_site(site, overrides, args.buffer_m, args.dsn)
    after = _measure(site, scratch, args.dsn, args.band)
    _render(site, scratch, args.dsn, args.band,
            out_dir / f"{site.key}-after.png")
    tag = f" {overrides}" if overrides else " (no dial change — truncation check)"
    print(f"[after ] {site.key} ({scratch}){tag}: {_short(after)}")
    print(f"\nrenders: {out_dir}/{site.key}-before.png  "
          f"{out_dir}/{site.key}-after.png")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
