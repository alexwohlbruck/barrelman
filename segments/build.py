"""segments.build — CLI: ordered graph -> transit_line_segments.

  uv run --with-requirements segments/requirements.txt \\
      python -m segments.build --build-key chicago:l-v3 [--emit] \\
      [--transition-len 60]

Report-only without --emit.

Zoom bands: by default the build runs once per SegmentConfig.bands entry
(z15/60 m, z14/120 m, z13/240 m, z0/480 m — roughly constant transition
SCREEN length across zooms) and --emit writes the complete feature set of
EVERY band (delete-and-replace per build_key covers all bands; row growth
~4x). --transition-len X collapses to a single band covering all zooms —
the pre-band debug knob.
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import replace

from .corridors import DEFAULT_DSN, load_graph
from .segment import SegmentConfig, band_ranges, build_segments


def load_shapes(g, dsn: str) -> dict:
    """matched_shapes geometries for the build's routes (junction pairing
    evidence for ribbons on >= 3 corridor ends)."""
    import json

    import psycopg

    keys = {ln.key for e in g.edges.values() for ln in e.lines}
    feeds = sorted({k[0] for k in keys})
    out: dict = {}
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT feed_id, route_id, ST_AsGeoJSON(geom)
               FROM matched_shapes
               WHERE feed_id = ANY(%s) AND geom IS NOT NULL""", (feeds,))
        for feed, rid, gj in cur.fetchall():
            if (feed, rid) in keys:
                out.setdefault((feed, rid), []).append(
                    json.loads(gj)["coordinates"])
    return out


# GTFS route_type -> the OSM railway tags whose ways are that mode's real
# track (ground truth for off-track corridor reconciliation). Rail modes
# share the rail family (subway trains run on rail/light_rail links at
# portals); bus/ferry have no railway ground truth here and reconcile off.
RAIL_ROUTE_TYPES = {0, 1, 2, 5, 7, 12}  # tram, subway, rail, cable, funi, monorail
RAIL_WAY_TAGS = ("subway", "rail", "light_rail", "tram",
                 "narrow_gauge", "monorail", "funicular")
# non-running service/usage values excluded from the DISPLAY ground truth
# (canonical predicate: shapesnap.graph.is_regular_service_track) — a yard's
# fan of parallel tracks must not pull a reconciliation snap toward track no
# service rides. Matching still sees these (penalized); this is display only.
from shapesnap.graph import (NON_REGULAR_SERVICE_VALUES,  # noqa: E402
                             NON_REGULAR_USAGE_VALUES)


def load_ways(g, dsn: str) -> list:
    """REGULAR-SERVICE OSM track polylines (geo_places railway ways, yard/
    siding/spur/crossover and industrial/military/tourism EXCLUDED) inside
    the build's bounding box, for reconcile_offtrack_corridors. Only loaded
    for rail-mode builds; returns [] otherwise (no reconciliation).
    Each way is [[lon, lat], ...]."""
    import json

    import psycopg

    rtypes = {ln.route_type for e in g.edges.values() for ln in e.lines
              if ln.route_type is not None}
    if not rtypes or not (rtypes & RAIL_ROUTE_TYPES):
        return []
    lons = [n.lon for n in g.nodes.values()]
    lats = [n.lat for n in g.nodes.values()]
    pad = 0.01  # ~1 km — off-run bridges never wander farther than this
    bbox = (min(lons) - pad, min(lats) - pad,
            max(lons) + pad, max(lats) + pad)
    out: list = []
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT ST_AsGeoJSON(geom) FROM geo_places
               WHERE tags->>'railway' = ANY(%s)
                 AND ST_GeometryType(geom) = 'ST_LineString'
                 AND (tags->>'service' IS NULL
                      OR NOT tags->>'service' = ANY(%s))
                 AND (tags->>'usage' IS NULL
                      OR NOT tags->>'usage' = ANY(%s))
                 AND geom && ST_MakeEnvelope(%s, %s, %s, %s, 4326)""",
            (list(RAIL_WAY_TAGS), list(NON_REGULAR_SERVICE_VALUES),
             list(NON_REGULAR_USAGE_VALUES), *bbox))
        for (gj,) in cur.fetchall():
            coords = json.loads(gj)["coordinates"]
            if len(coords) >= 2:
                out.append(coords)
    return out


def summarize(segments, info) -> str:
    by_kind = Counter(s.kind for s in segments)
    km = Counter()
    for s in segments:
        km[s.kind] += s.len_m / 1000.0
    sites = info["sites"]
    lines = [
        f"corridors: {info['corridors']}",
        f"transition sites: {len(sites)} "
        f"({sum(1 for v in sites.values() if v == 'junction')} junctions, "
        f"{sum(1 for v in sites.values() if v == 'composition')} deg-2 "
        f"composition changes)",
        f"segments: {len(segments)} "
        f"(steady {by_kind.get('steady', 0)}, "
        f"transition {by_kind.get('transition', 0)})",
        f"km: steady {km['steady']:.2f}, transition {km['transition']:.2f}",
        f"stubs (terminating ribbons, constant offset): {info['stubs']}",
        f"skipped (unchanged offset + straight): {info['skipped']}",
        f"merged (fully consumed corridors): {info['merged']}",
        f"fillets clamped by short halves: {info['fillet_clamped']}",
    ]
    if info.get("cusp_excised"):
        lines.append(f"reversal cusps excised (collapsed crossing rungs): "
                     f"{info['cusp_excised']}")
    if info.get("corridor_loops_excised"):
        lines.append(f"corridor micro-loops excised (cid: (n, m)): "
                     f"{info['corridor_loops_excised']}")
    if info.get("track_reconciled"):
        lines.append(f"off-track corridors reconciled to real track "
                     f"(cid: (method, before_m, after_m)): "
                     f"{info['track_reconciled']}")
    if info.get("site_len_clamped"):
        lines.append(f"site transition lengths clamped to corridor support "
                     f"(node: m): {info['site_len_clamped']}")
    if info.get("greedy_paired_sites"):
        lines.append(f"greedy-paired sites (no shape evidence): "
                     f"{sorted(set(info['greedy_paired_sites']))}")
    if info.get("two_end_shape_gap_sites"):
        lines.append(f"two-end pairs kept on shared route despite shape gap: "
                     f"{sorted(set(info['two_end_shape_gap_sites']))}")
    if info.get("two_end_unsupported_sites"):
        lines.append(f"two-end pairs demoted to stubs (shape evidence "
                     f"contradicts): {sorted(set(info['two_end_unsupported_sites']))}")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        description="steady/transition segmentation -> transit_line_segments")
    ap.add_argument("--build-key", required=True)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--transition-len", type=float, default=None,
                    help="single-band mode: fixed ground length (m) served "
                         "at every zoom (default: the zoom bands)")
    ap.add_argument("--gap-px", type=float, default=4.4)
    ap.add_argument("--densify", type=float, default=7.5)
    ap.add_argument("--emit", action="store_true",
                    help="write transit_line_segments (delete-and-replace, "
                         "all bands)")
    args = ap.parse_args(argv)

    cfg = SegmentConfig(gap_px=args.gap_px, densify_step_m=args.densify)
    if args.transition_len is not None:
        cfg = replace(cfg, transition_len_m=args.transition_len,
                      bands=((0, args.transition_len),))
    print(f"loading {args.build_key} ...")
    g = load_graph(args.build_key, args.dsn)
    print(f"  {len(g.nodes)} nodes, {len(g.edges)} edges")
    try:
        shapes = load_shapes(g, args.dsn)
        print(f"  {sum(len(v) for v in shapes.values())} matched shapes for "
              f"{len(shapes)} routes")
    except Exception as err:  # shapes are evidence only, never required
        print(f"  matched_shapes unavailable ({err}); greedy pairing")
        shapes = None

    try:
        ways = load_ways(g, args.dsn)
        print(f"  {len(ways)} OSM track ways for off-track reconciliation")
    except Exception as err:  # ways are corrective only, never required
        print(f"  OSM ways unavailable ({err}); no track reconciliation")
        ways = None

    band_segments = []
    for band_minzoom, band_maxzoom, length in band_ranges(cfg.bands):
        bcfg = replace(cfg, transition_len_m=length)
        segments, info = build_segments(g, bcfg, shapes=shapes, ways=ways)
        print(f"--- band z{band_minzoom}..{band_maxzoom} "
              f"(transition {length:.0f} m)")
        print(summarize(segments, info))
        band_segments.append((band_minzoom, band_maxzoom, segments))

    if args.emit:
        from .emit import emit_segments
        n = emit_segments(band_segments, build_key=args.build_key,
                          dsn=args.dsn)
        print(f"emitted {n} rows to transit_line_segments "
              f"(build_key {args.build_key}, {len(band_segments)} band(s))")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
