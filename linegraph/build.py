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

--emit runs phase B after the skeleton: shape-evidence geometry refit
(linegraph.refit — default ON, --no-refit to skip), station snapping
(linegraph.stations — splits the refit centerline into station-to-
station segments), route attribution (linegraph.attribute), then the
transit_graph_* contract write (linegraph.emit, delete-and-replace per
build_key).
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
    LGEdge,
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


def dedup_shapes(patterns):
    """Deduped representative shapes (one per pattern geometry).

    Returns (shapes [[(lon, lat), ...]], n_skipped_no_shape).
    load_patterns sorts by descending trip_count, so the dedup keeps a
    deterministic representative and the digest is stable.
    """
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
    return shapes, skipped


def collect_shapes(zip_path, mode: str, route_ids=None):
    """load_patterns + dedup_shapes: (shapes, n_patterns, n_skipped)."""
    patterns = load_patterns(zip_path, route_ids=route_ids, modes={mode})
    shapes, skipped = dedup_shapes(patterns)
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


# ── phase B: stations + attribution + transit_graph emit ────────────────────


def _join_edges(a: LGEdge, b: LGEdge, nid: int) -> LGEdge:
    """One edge from a + b, oriented THROUGH their shared node nid."""
    a_fwd = a.to_node == nid
    ac = a.coords if a_fwd else list(reversed(a.coords))
    ac_xy = a.coords_xy if a_fwd else list(reversed(a.coords_xy))
    u = a.from_node if a_fwd else a.to_node
    b_fwd = b.from_node == nid
    bc = b.coords if b_fwd else list(reversed(b.coords))
    bc_xy = b.coords_xy if b_fwd else list(reversed(b.coords_xy))
    v = b.to_node if b_fwd else b.from_node
    drop_dup = 1 if ac_xy[-1] == bc_xy[0] else 0
    return LGEdge(
        edge_id=-1, from_node=u, to_node=v,
        px_len=a.px_len + b.px_len, length_m=a.length_m + b.length_m,
        coords=ac + bc[drop_dup:], coords_xy=ac_xy + bc_xy[drop_dup:],
    )


def prune_lineless_edges(lg, edge_routes: dict, labels: dict):
    """Drop edges no pattern rides, then re-join the corridors they cut.

    A line-less strand is a skeletonization artifact (crossing rungs a
    shade over the contraction bound), not track. Dropping one leaves
    its endpoints degree-2; when the two surviving edges there carry
    IDENTICAL route sets the node is corridor-interior, so the edges are
    joined into one, oriented head-to-tail — stage 5's raw-slot corridor
    stability relies on corridors being emitted as single aligned edges,
    and two independently-oriented halves meeting head-to-head at an
    artifact node would break it. Station-labeled nodes are never
    joined away; orphaned unlabeled nodes are dropped.

    Mutates lg; returns (edge_routes, n_dropped, n_joined) with
    edge_routes re-keyed to the new edge positions.
    """
    edges = list(lg.edges)
    routes = [dict(edge_routes.get(i, {})) for i in range(len(edges))]
    keep = [i for i in range(len(edges)) if routes[i]]
    n_dropped = len(edges) - len(keep)
    if n_dropped == 0:
        return edge_routes, 0, 0
    edges = [edges[i] for i in keep]
    routes = [routes[i] for i in keep]

    n_joined = 0
    changed = True
    while changed:
        changed = False
        inc: dict = {}
        for i, e in enumerate(edges):
            inc.setdefault(e.from_node, []).append(i)
            inc.setdefault(e.to_node, []).append(i)
        for nid in sorted(inc):
            eids = inc[nid]
            if len(eids) != 2 or eids[0] == eids[1] or nid in labels:
                continue
            i, j = eids
            if set(routes[i]) != set(routes[j]):
                continue
            # join in CHAIN order — the piece flowing INTO nid first —
            # so the joined edge keeps the surrounding corridor's
            # storage direction (stage 5 reads slots in it; an
            # arbitrary-order join between two protected station nodes
            # would mirror every slot on the joined edge)
            if edges[i].to_node != nid and edges[j].to_node == nid:
                i, j = j, i
            joined = _join_edges(edges[i], edges[j], nid)
            if joined.from_node == joined.to_node:
                continue  # would form a self-loop; leave the pair
            edges[i] = joined
            routes[i] = {**routes[j], **routes[i]}
            del edges[j], routes[j]
            n_joined += 1
            changed = True
            break  # incidence is stale; recompute
    for k, e in enumerate(edges):
        e.edge_id = k
    lg.edges = edges
    used = {e.from_node for e in edges} | {e.to_node for e in edges}
    lg.nodes = [n for n in lg.nodes
                if n.node_id in used or n.node_id in labels]
    return {i: r for i, r in enumerate(routes)}, n_dropped, n_joined


def enrich_graph(lg, patterns, zip_path, feed_id: str, *, refit: bool = True,
                 unfuse: bool = True, verbose: bool = True):
    """Refit geometry from shape evidence, then stations, then attribution.

    Phase order: corridor unfuse (linegraph.unfuse — splits raster-fused
    corridors of physically distinct line families, config-gated,
    default ON) -> coarse attribution + geometry refit (linegraph.refit
    — the skeleton is authoritative topology but lossy geometry near
    junctions, config-gated, default ON) -> station snapping, splitting
    on the REFIT centerline -> the final attribution emit consumes ->
    line-less pruning. Returns (lg, snap: StationSnapResult,
    edge_routes, stats). Mutates lg (never the on-disk cache — the
    cache holds the raw skeleton only).
    """
    from linegraph.attribute import attribute_patterns, load_routes_meta
    from linegraph.stations import load_station_complexes, snap_stations

    def log(msg):
        if verbose:
            print(f"[linegraph] {msg}", flush=True)

    from linegraph.unfuse import shape_families

    shapes, _ = dedup_shapes(patterns)
    families = shape_families(patterns, shapes)

    if unfuse:
        from linegraph.unfuse import unfuse_corridors

        us = unfuse_corridors(lg, shapes, families, verbose=verbose)
        log(
            f"unfuse: {us.n_zones} multi-family zones -> {us.n_split} split "
            f"({us.n_edges_removed} fused edges -> {us.n_edges_added} "
            f"corridor edges), {us.n_kept} kept fused, {us.n_skipped} skipped"
        )

    if refit:
        from linegraph.refit import refit_geometry

        rs = refit_geometry(lg, shapes, shape_families=families)
        log(
            f"refit: {rs.n_refit}/{rs.n_edges} edges rebuilt from "
            f"{rs.n_contributions} shape sub-polylines "
            f"({rs.n_no_evidence} no-evidence, {rs.n_capped} capped -> kept "
            f"skeleton), max point move {rs.max_point_move_m:.1f} m, "
            f"max node move {rs.max_node_move_m:.1f} m"
            + (f", {rs.n_node_fallback} node LSQ fallback(s)"
               if rs.n_node_fallback else "")
            + (f", {rs.n_floor_pairs} node-pair floor(s)"
               if rs.n_floor_pairs else "")
        )
        for eid, stray in rs.capped_edges:
            log(f"  capped: edge {eid} refit strayed {stray} m > merge width")
        for eid, before, after in rs.length_outliers:
            log(f"  length outlier: edge {eid} {before} -> {after} m")

    stop_ids = {sid for p in patterns for sid in p.stop_ids}
    complexes = load_station_complexes(zip_path, stop_ids)
    lg, snap = snap_stations(lg, complexes)
    log(
        f"stations: {len(snap.labeled)}/{len(complexes)} complexes labeled "
        f"({snap.n_split_nodes} split nodes, "
        f"{len(complexes) - len(snap.labeled)} unlabeled) -> "
        f"{len(lg.nodes)} nodes, {len(lg.edges)} edges"
    )
    for comp, reason, dist in snap.unlabeled:
        log(f"  unlabeled: {comp.station_id} '{comp.label}' ({reason}"
            f"{'' if dist is None else f', {dist:.0f} m'})")

    routes_meta = load_routes_meta(zip_path)
    edge_routes, stats = attribute_patterns(lg, patterns, feed_id, routes_meta)
    attributed = [s for s in stats if s.n_samples]
    worst = max(attributed, key=lambda s: s.unmatched_fraction, default=None)
    n_excised = sum(s.n_excised for s in attributed)
    log(
        f"attribution: {len(attributed)}/{len(stats)} patterns with shapes, "
        f"{len(edge_routes)}/{len(lg.edges)} edges carry routes, "
        f"{n_excised} deviation-gate excisions"
        + (f", worst unmatched {worst.unmatched_fraction:.2%} ({worst.pattern_key})"
           if worst else "")
    )
    for s in attributed:
        if s.unmatched_fraction >= 0.02:
            log(f"  HIGH unmatched {s.unmatched_fraction:.2%}: {s.pattern_key} "
                f"({s.n_unmatched}/{s.n_samples} samples)")

    edge_routes, n_dropped, n_joined = prune_lineless_edges(
        lg, edge_routes, snap.labels)
    if n_dropped:
        log(f"pruned {n_dropped} line-less artifact edge(s), re-joined "
            f"{n_joined} corridor node(s) -> {len(lg.nodes)} nodes, "
            f"{len(lg.edges)} edges")
    return lg, snap, edge_routes, stats


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
    ap.add_argument(
        "--emit", nargs="?", const=DEFAULT_DSN, default=None, metavar="DSN",
        help="phase B: refit geometry + snap stations + attribute routes, "
             "then delete-and-replace the build_key's transit_graph_* rows "
             "(default DSN as for --postgis-qa)",
    )
    ap.add_argument(
        "--no-refit", action="store_true",
        help="skip the shape-evidence geometry refit (linegraph.refit) and "
             "emit raw skeleton geometry — debugging/comparison only",
    )
    ap.add_argument(
        "--no-unfuse", action="store_true",
        help="skip the corridor unfuse (linegraph.unfuse) that splits "
             "raster-fused corridors of physically distinct line families — "
             "debugging/comparison only",
    )
    args = ap.parse_args(argv)

    zip_path = resolve_feed_zip(args.feed, args.zip)
    print(f"[linegraph] loading patterns from {zip_path} (mode={args.mode})", flush=True)
    t0 = time.perf_counter()
    route_ids = set(args.routes.split(",")) if args.routes else None
    patterns = load_patterns(zip_path, route_ids=route_ids, modes={args.mode})
    shapes, n_skipped = dedup_shapes(patterns)
    n_patterns = len(patterns)
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

    if args.emit:
        from linegraph.emit import emit_build

        if args.routes:
            print(
                "[linegraph] WARNING: --emit with --routes writes a DEBUG "
                "SUBGRAPH under this build_key", file=sys.stderr,
            )
        lg, snap, edge_routes, _stats = enrich_graph(
            lg, patterns, zip_path, args.feed, refit=not args.no_refit,
            unfuse=not args.no_unfuse,
        )
        counts = emit_build(
            lg, edge_routes, snap.labels, build_key=args.build_key,
            feed_id=args.feed, mode=args.mode, dsn=args.emit,
        )
        print(
            f"[linegraph] emitted build_key={args.build_key}: "
            f"{counts['nodes']} nodes ({counts['labeled_nodes']} labeled), "
            f"{counts['edges']} edges "
            f"({counts['edges_dropped_lineless']} line-less dropped), "
            f"{counts['edge_lines']} edge_lines, "
            f"route_type={counts['route_type']}"
        )
    return 0


if __name__ == "__main__":
    # canonical module name so pickled dataclasses stay loadable (shapesnap
    # convention: caches must never reference __main__.*)
    from linegraph.build import main as _main

    sys.exit(_main())
