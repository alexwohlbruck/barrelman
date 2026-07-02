#!/usr/bin/env python3
"""linegraph.emit — write the transit_graph_* contract (phase B.3).

EXACTLY the schema of src/db.ts ensureTransitGraphSchema() and the
delete-and-replace semantics of import/load-transit-graph.ts, per
build_key, in ONE transaction:

  DELETE nodes + edges for the build_key (edge_lines cascade), COPY the
  new rows, upsert the transit_graph_builds ledger row.

loom_id carries the linegraph node/edge index (stringified), keeping the
column contract without LOOM.

Edges NO pattern rides (empty edge_routes entry) are not emitted: the
skeleton is built from ridden shapes, so a line-less strand is a
skeletonization artifact (crossing rungs slightly over the contraction
bound), not track — every downstream consumer (lineorder, segments,
display SQL) already skips line-less rows, emitting them only inflates
transit_graph_edges. Dropped count is returned in the summary.

edge_lines.slot is a PROVISIONAL deterministic order: routes sorted by
route_id per edge. Stage 5 (crossing-minimizing ordering + slot
stabilization) will overwrite the slots; nothing downstream may treat
this order as final.

The DDL below is a verbatim copy of ensureTransitGraphSchema() — additive
and idempotent, so emit works against a fresh database too.
"""

from __future__ import annotations

from collections import Counter

DDL = """
CREATE TABLE IF NOT EXISTS transit_graph_nodes (
  id SERIAL PRIMARY KEY,
  build_key TEXT NOT NULL,
  loom_id TEXT NOT NULL,
  station_id TEXT,
  station_label TEXT,
  geom geometry(Point, 4326),
  UNIQUE (build_key, loom_id)
);
CREATE INDEX IF NOT EXISTS transit_graph_nodes_geom_idx
  ON transit_graph_nodes USING GIST (geom);

CREATE TABLE IF NOT EXISTS transit_graph_edges (
  id SERIAL PRIMARY KEY,
  build_key TEXT NOT NULL,
  loom_id TEXT NOT NULL,
  line_count INTEGER NOT NULL,
  geom geometry(LineString, 4326),
  UNIQUE (build_key, loom_id)
);
CREATE INDEX IF NOT EXISTS transit_graph_edges_geom_idx
  ON transit_graph_edges USING GIST (geom);

CREATE TABLE IF NOT EXISTS transit_graph_edge_lines (
  edge_id INTEGER NOT NULL REFERENCES transit_graph_edges(id) ON DELETE CASCADE,
  slot INTEGER NOT NULL,
  feed_id TEXT,
  route_id TEXT,
  route_short_name TEXT,
  route_type INTEGER,
  route_color TEXT,
  route_text_color TEXT,
  PRIMARY KEY (edge_id, slot)
);

CREATE TABLE IF NOT EXISTS transit_graph_builds (
  build_key TEXT PRIMARY KEY,
  feed_id TEXT,
  mode TEXT,
  route_type INTEGER,
  built_at TIMESTAMPTZ DEFAULT NOW()
);
"""


def _ewkt_point(lon: float, lat: float) -> str:
    return f"SRID=4326;POINT({lon:.7f} {lat:.7f})"


def _ewkt_line(coords) -> str:
    return (
        "SRID=4326;LINESTRING("
        + ",".join(f"{lon:.7f} {lat:.7f}" for lon, lat in coords)
        + ")"
    )


def emit_build(lg, edge_routes: dict, labels: dict, *, build_key: str,
               feed_id: str, mode: str, dsn: str,
               route_type: int | None = None) -> dict:
    """Delete-and-replace the build_key's graph rows. Returns row counts.

    edge_routes: {edge position: {(feed_id, route_id): RouteInfo}}
    labels:      {node_id: (station_id, station_label)}
    route_type:  ledger value; defaults to the modal route_type across
                 all attributed routes (CTA rail -> 1).
    """
    import psycopg  # optional dep, --emit only

    if route_type is None:
        counted = Counter(
            info.route_type
            for routes in edge_routes.values()
            for info in routes.values()
        )
        route_type = counted.most_common(1)[0][0] if counted else None

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(DDL)
        # delete-and-replace per build_key (edge_lines cascade off edges)
        cur.execute("DELETE FROM transit_graph_nodes WHERE build_key = %s",
                    (build_key,))
        cur.execute("DELETE FROM transit_graph_edges WHERE build_key = %s",
                    (build_key,))

        with cur.copy(
            "COPY transit_graph_nodes"
            " (build_key, loom_id, station_id, station_label, geom) FROM STDIN"
        ) as copy:
            for n in lg.nodes:
                station_id, station_label = labels.get(n.node_id, (None, None))
                copy.write_row((build_key, str(n.node_id), station_id,
                                station_label, _ewkt_point(n.lon, n.lat)))

        n_dropped = 0
        with cur.copy(
            "COPY transit_graph_edges"
            " (build_key, loom_id, line_count, geom) FROM STDIN"
        ) as copy:
            for pos, e in enumerate(lg.edges):
                n_routes = len(edge_routes.get(pos, {}))
                if n_routes == 0:
                    n_dropped += 1  # artifact strand no pattern rides
                    continue
                copy.write_row((build_key, str(e.edge_id), n_routes,
                                _ewkt_line(e.coords)))

        cur.execute(
            "SELECT id, loom_id FROM transit_graph_edges WHERE build_key = %s",
            (build_key,),
        )
        db_id = {loom: eid for eid, loom in cur.fetchall()}

        n_lines = 0
        with cur.copy(
            "COPY transit_graph_edge_lines (edge_id, slot, feed_id, route_id,"
            " route_short_name, route_type, route_color, route_text_color)"
            " FROM STDIN"
        ) as copy:
            for pos, e in enumerate(lg.edges):
                routes = edge_routes.get(pos)
                if not routes:
                    continue
                # PROVISIONAL slot order: sorted by route_id. Stage 5
                # (crossing minimization) overwrites these slots.
                for slot, key in enumerate(sorted(routes, key=lambda k: k[1])):
                    info = routes[key]
                    # v2 §7 lesson: bullets fall back to route_id when the
                    # feed leaves route_short_name blank (CTA rail does).
                    copy.write_row((db_id[str(e.edge_id)], slot, info.feed_id,
                                    info.route_id,
                                    info.route_short_name or info.route_id,
                                    info.route_type, info.route_color,
                                    info.route_text_color))
                    n_lines += 1

        cur.execute(
            """INSERT INTO transit_graph_builds
                 (build_key, feed_id, mode, route_type, built_at)
               VALUES (%s, %s, %s, %s, NOW())
               ON CONFLICT (build_key) DO UPDATE SET
                 feed_id = EXCLUDED.feed_id, mode = EXCLUDED.mode,
                 route_type = EXCLUDED.route_type, built_at = NOW()""",
            (build_key, feed_id, mode, route_type),
        )
        conn.commit()

    return {
        "nodes": len(lg.nodes),
        "edges": len(lg.edges) - n_dropped,
        "edges_dropped_lineless": n_dropped,
        "edge_lines": n_lines,
        "labeled_nodes": len(labels),
        "route_type": route_type,
    }
