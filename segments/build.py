"""segments.build — CLI: ordered graph -> transit_line_segments.

  uv run --with-requirements segments/requirements.txt \\
      python -m segments.build --build-key chicago:l-v3 [--emit] \\
      [--transition-len 60]

Report-only without --emit.
"""

from __future__ import annotations

import argparse
from collections import Counter

from .corridors import DEFAULT_DSN, load_graph
from .segment import SegmentConfig, build_segments


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
    ap.add_argument("--transition-len", type=float, default=60.0,
                    help="fixed ground length of transition zones (m)")
    ap.add_argument("--gap-px", type=float, default=4.4)
    ap.add_argument("--densify", type=float, default=7.5)
    ap.add_argument("--emit", action="store_true",
                    help="write transit_line_segments (delete-and-replace)")
    args = ap.parse_args(argv)

    cfg = SegmentConfig(transition_len_m=args.transition_len,
                        gap_px=args.gap_px, densify_step_m=args.densify)
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

    segments, info = build_segments(g, cfg, shapes=shapes)
    print(summarize(segments, info))

    if args.emit:
        from .emit import emit_segments
        n = emit_segments(segments, build_key=args.build_key, dsn=args.dsn)
        print(f"emitted {n} rows to transit_line_segments "
              f"(build_key {args.build_key})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
