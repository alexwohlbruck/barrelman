#!/usr/bin/env python3
"""linegraph.build — CLI driver: GTFS patterns -> raster -> skeleton -> graph.

Input = route x direction pattern shapes via shapesnap.match.load_patterns
(representative shape per pattern, deduped by geometry hash — OSM-matched
routes sharing track have IDENTICAL geometry, so shared corridors stamp
once and genuinely-parallel tracks are what the raster fuses).

CLI (repo convention — uv, never system python):
  uv run --with-requirements linegraph/requirements.txt \
      python -m linegraph.build --feed 29 --mode rail --build-key chicago:l-v3 \
      [--zip data/gtfs-processed/29.zip] [--merge-width 18] [--res 2.0] \
      [--postgis-qa [DSN]]

Reads data/gtfs-processed/<feed>.zip by default and falls back to
data/gtfs/<feed>.zip with a loud warning (raw shapes are NOT OSM-matched;
the skeleton will fuse on agency geometry instead of snapped track).

--postgis-qa dumps the edges into the additive linegraph_edges QA table,
idempotently per build_key (delete-and-replace). Phase A writes ONLY this
QA table — transit_graph_* stays untouched.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from shapesnap.match import geometry_hash, load_patterns

from linegraph.model import (
    FORMAT_VERSION,
    LineGraph,
    default_cache_path,
    input_digest,
    load_linegraph,
    save_linegraph,
)
from linegraph.raster import pick_epsg, project_shapes, rasterize
from linegraph.skeleton import skeletonize_grid
from linegraph.vector import vectorize

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)


def resolve_feed_zip(feed_id: str, explicit=None) -> Path:
    """Processed zip first; raw zip only with a loud warning."""
    if explicit is not None:
        p = Path(explicit)
        if not p.exists():
            raise FileNotFoundError(p)
        return p
    processed = REPO_ROOT / "data" / "gtfs-processed" / f"{feed_id}.zip"
    if processed.exists():
        return processed
    raw = REPO_ROOT / "data" / "gtfs" / f"{feed_id}.zip"
    if raw.exists():
        print(
            "=" * 72
            + f"\nWARNING: {processed} missing — falling back to RAW {raw}.\n"
            "Raw shapes are NOT OSM-matched; parallel-track fusing will run on\n"
            "agency geometry and the result is not the v3 pipeline artifact.\n"
            + "=" * 72,
            file=sys.stderr,
        )
        return raw
    raise FileNotFoundError(f"neither {processed} nor {raw} exists")


def collect_shapes(zip_path, mode: str, route_ids=None):
    """Deduped representative shapes (one per pattern geometry).

    Returns (shapes [[(lon, lat), ...]], n_patterns, n_skipped_no_shape).
    load_patterns sorts by descending trip_count, so the dedup keeps a
    deterministic representative and the digest is stable.
    """
    patterns = load_patterns(zip_path, route_ids=route_ids, modes={mode})
    shapes, seen, skipped = [], set(), 0
    for p in patterns:
        if not p.shape or len(p.shape) < 2:
            skipped += 1
            continue
        h = geometry_hash(p.shape)
        if h in seen:
            continue
        seen.add(h)
        shapes.append(list(p.shape))
    return shapes, len(patterns), skipped


def build_linegraph(shapes, merge_width: float, res: float, *,
                    build_key: str = "", feed_id: str = "", mode: str = "",
                    verbose: bool = True) -> LineGraph:
    """shapes (lon/lat) -> LineGraph. The whole raster-skeleton-vector chain."""
    def log(msg):
        if verbose:
            print(f"[linegraph] {msg}", flush=True)

    t0 = time.perf_counter()
    digest = input_digest(shapes, merge_width, res)
    epsg = pick_epsg(shapes)
    shapes_xy = project_shapes(shapes, epsg)

    grid = rasterize(shapes_xy, merge_width, res, epsg=epsg)
    rows, cols = grid.shape
    log(
        f"grid {rows}x{cols} px ({grid.nbytes / 1e6:.0f} MB) epsg={epsg} "
        f"origin=({grid.origin[0]:.0f},{grid.origin[1]:.0f}) "
        f"ink={int(grid.grid.sum())} px"
    )

    t1 = time.perf_counter()
    skel = skeletonize_grid(grid.grid)
    log(f"skeleton {int(skel.sum())} px in {time.perf_counter() - t1:.1f}s")

    t2 = time.perf_counter()
    nodes, edges = vectorize(skel, grid, epsg, merge_width)
    log(f"vectorized {len(nodes)} nodes, {len(edges)} edges "
        f"in {time.perf_counter() - t2:.1f}s")

    lg = LineGraph(
        format_version=FORMAT_VERSION,
        build_key=build_key,
        feed_id=feed_id,
        mode=mode,
        merge_width_m=merge_width,
        resolution_m=res,
        epsg=epsg,
        origin=grid.origin,
        grid_shape=(rows, cols),
        grid_bytes=grid.nbytes,
        input_digest=digest,
        n_input_shapes=len(shapes),
        build_seconds=time.perf_counter() - t0,
        nodes=nodes,
        edges=edges,
    )
    log(
        f"total {lg.total_length_m() / 1000:.1f} km across "
        f"{len(lg.components())} component(s) in {lg.build_seconds:.1f}s"
    )
    return lg


# ── PostGIS QA dump ──────────────────────────────────────────────────────────


def dump_postgis(lg: LineGraph, dsn: str, table: str = "linegraph_edges") -> int:
    """Idempotent per-build_key QA dump (additive DDL only)."""
    import psycopg  # optional dep

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {table} (
                    build_key text NOT NULL,
                    edge_idx int NOT NULL,
                    px_len int NOT NULL,
                    length_m double precision NOT NULL,
                    geom geometry(LineString, 4326),
                    PRIMARY KEY (build_key, edge_idx)
                )"""
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {table}_geom_idx ON {table} USING gist (geom)"
        )
        cur.execute(f"DELETE FROM {table} WHERE build_key = %s", (lg.build_key,))
        with cur.copy(
            f"COPY {table} (build_key, edge_idx, px_len, length_m, geom) FROM STDIN"
        ) as copy:
            for e in lg.edges:
                ewkt = (
                    "SRID=4326;LINESTRING("
                    + ",".join(f"{lon:.7f} {lat:.7f}" for lon, lat in e.coords)
                    + ")"
                )
                copy.write_row(
                    (lg.build_key, e.edge_id, e.px_len, round(e.length_m, 1), ewkt)
                )
        conn.commit()
    return len(lg.edges)


# ── cli ──────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m linegraph.build",
        description="Raster-skeleton-vectorize centerline graph from GTFS patterns.",
    )
    ap.add_argument("--feed", required=True, help="feed id (data/gtfs-processed/<id>.zip)")
    ap.add_argument("--mode", default="rail", choices=("rail", "bus", "ferry"))
    ap.add_argument("--build-key", required=True, help="e.g. chicago:l-v3")
    ap.add_argument("--zip", type=Path, default=None, help="explicit GTFS zip path")
    ap.add_argument("--routes", default=None, help="comma-separated route_ids (debug)")
    ap.add_argument("--merge-width", type=float, default=18.0, metavar="M",
                    help="stroke width in ground meters — the merge criterion")
    ap.add_argument("--res", type=float, default=2.0, metavar="M_PER_PX")
    ap.add_argument("--force", action="store_true", help="ignore the cache")
    ap.add_argument(
        "--postgis-qa", nargs="?", const=DEFAULT_DSN, default=None, metavar="DSN",
        help="dump edges into the linegraph_edges QA table (default DSN from "
             "DATABASE_URL or the barrelman dev DB)",
    )
    args = ap.parse_args(argv)

    zip_path = resolve_feed_zip(args.feed, args.zip)
    print(f"[linegraph] loading patterns from {zip_path} (mode={args.mode})", flush=True)
    t0 = time.perf_counter()
    route_ids = set(args.routes.split(",")) if args.routes else None
    shapes, n_patterns, n_skipped = collect_shapes(zip_path, args.mode, route_ids)
    print(
        f"[linegraph] {n_patterns} patterns -> {len(shapes)} unique shapes "
        f"({n_skipped} without shapes skipped) in {time.perf_counter() - t0:.1f}s"
    )
    if not shapes:
        print("[linegraph] nothing to build", file=sys.stderr)
        return 1

    digest = input_digest(shapes, args.merge_width, args.res)
    cache = default_cache_path(args.feed, args.mode)
    # --routes builds a debug subgraph — never read or replace the canonical
    # full-network cache with it.
    use_cache = args.routes is None
    lg = None
    if use_cache and not args.force:
        try:
            lg = load_linegraph(cache, expect_digest=digest)
            lg.build_key = args.build_key
            print(f"[linegraph] cache hit: {cache}")
        except (FileNotFoundError, ValueError):
            lg = None
    if lg is None:
        lg = build_linegraph(
            shapes, args.merge_width, args.res,
            build_key=args.build_key, feed_id=args.feed, mode=args.mode,
        )
        if use_cache:
            save_linegraph(lg, cache)
            print(f"[linegraph] cache: {cache} ({cache.stat().st_size / 1e6:.1f} MB)")
        else:
            print("[linegraph] --routes debug build — cache untouched")

    comps = lg.components()
    print(
        f"[linegraph] {len(lg.nodes)} nodes, {len(lg.edges)} edges, "
        f"{lg.total_length_m() / 1000:.1f} km, {len(comps)} component(s), "
        f"grid {lg.grid_shape[0]}x{lg.grid_shape[1]} ({lg.grid_bytes / 1e6:.0f} MB), "
        f"built in {lg.build_seconds:.1f}s"
    )

    if args.postgis_qa:
        n = dump_postgis(lg, args.postgis_qa)
        print(f"[linegraph] postgis: {n} rows into linegraph_edges "
              f"(build_key={lg.build_key})")
    return 0


if __name__ == "__main__":
    # canonical module name so pickled dataclasses stay loadable (shapesnap
    # convention: caches must never reference __main__.*)
    from linegraph.build import main as _main

    sys.exit(_main())
