-- =============================================================================
-- Queue road markings for rebuilding where a replication diff touched roads
-- =============================================================================
-- Runs inside replicate-extract.sh's swap transaction, which holds every
-- replaced row as it was (osm_replay.old_places) and as it is now
-- (osm_replay.changed). A way's old outline catches a road deleted or moved
-- away; its new one a road added or moved in. scripts/update-road-markings.sh
-- works the queue off later, only where road markings have been built.
-- =============================================================================
CREATE TABLE IF NOT EXISTS road_markings_dirty (
  box geometry(Polygon, 4326) NOT NULL,
  queued_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO road_markings_dirty (box)
SELECT ST_Envelope(ST_Expand(geom, 0.0005))
FROM osm_replay.old_places
WHERE geom_type = 'line'
UNION ALL
SELECT ST_Envelope(ST_Expand(g.geom, 0.0005))
FROM geo_places g
JOIN osm_replay.changed c ON c.id = g.id
WHERE (g.geom_type = 'line' AND (g.tags ? 'highway'))
   OR (g.geom_type = 'point' AND g.tags->>'highway' IN ('crossing', 'traffic_signals', 'stop'));
