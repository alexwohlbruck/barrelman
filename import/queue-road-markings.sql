-- =============================================================================
-- Queue road markings for rebuilding where a replication diff touched roads
-- =============================================================================
-- Runs inside replicate-extract.sh's swap transaction, which holds every
-- replaced row as it was (osm_replay.old_places) and as it is now
-- (osm_replay.changed), and only when ROAD_MARKINGS_INCREMENTAL=1. A way's old
-- outline catches a road deleted or moved away; its new one a road added or
-- moved in. scripts/update-road-markings.sh works the queue off later.
--
-- Only what generate-road-markings.sql reads is queued: the road classes it
-- paves, crossings, and signal and stop nodes. old_places keeps categories, not
-- tags, so an old service road or footway counts only where it was drawn: a
-- service road on a paved surface, a footway under a painted crosswalk.
-- Without those, every alley and sidewalk edit in a built city would queue a
-- rebuild.
--
-- Nothing is queued where road markings were never built. A failure here is
-- reported and skipped rather than raised: it would otherwise roll back the
-- replication cycle it rides in.
-- Needs road_markings_dirty (road-markings-queue-table.sql).
-- =============================================================================
DO $$
DECLARE
  queued bigint;
BEGIN
  IF to_regclass('road_surfaces') IS NULL OR to_regclass('road_markings') IS NULL THEN
    RETURN;
  END IF;

  WITH relevant_new AS (
    SELECT g.id, g.geom
    FROM geo_places g
    JOIN osm_replay.changed c ON c.id = g.id
    WHERE (g.geom_type = 'line'
           AND ((g.tags->>'highway' IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link',
                                        'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'unclassified',
                                        'residential', 'living_street', 'busway')
                 OR (g.tags->>'highway' = 'service' AND g.tags ?| ARRAY['lanes', 'lane_markings', 'width', 'width:carriageway']))
                OR g.tags->>'footway' = 'crossing'))
       OR (g.geom_type = 'point' AND g.tags->>'highway' IN ('crossing', 'traffic_signals', 'stop'))
  ),
  touched AS (
    SELECT o.geom
    FROM osm_replay.old_places o
    WHERE (o.geom_type = 'line'
           AND (o.categories && ARRAY['highway/motorway', 'highway/motorway_link', 'highway/trunk', 'highway/trunk_link',
                                      'highway/primary', 'highway/primary_link', 'highway/secondary', 'highway/secondary_link',
                                      'highway/tertiary', 'highway/tertiary_link', 'highway/unclassified',
                                      'highway/residential', 'highway/living_street', 'highway/busway']
                OR (o.categories && ARRAY['highway/service']
                    AND EXISTS (SELECT 1 FROM road_surfaces s
                                WHERE s.geom && o.geom AND ST_Intersects(s.geom, ST_PointOnSurface(o.geom))))
                OR (o.categories && ARRAY['highway/footway', 'highway/path', 'highway/cycleway']
                    AND EXISTS (SELECT 1 FROM road_markings m WHERE m.geom && o.geom AND m.kind = 'crosswalk'))))
       OR (o.geom_type = 'point' AND o.categories && ARRAY['highway/crossing', 'highway/traffic_signals', 'highway/stop'])
       OR o.id IN (SELECT id FROM relevant_new)
    UNION ALL
    SELECT geom FROM relevant_new
  )
  INSERT INTO road_markings_dirty (box)
  SELECT ST_Envelope(ST_Expand(geom, 0.0005)) FROM touched WHERE geom IS NOT NULL;
  GET DIAGNOSTICS queued = ROW_COUNT;
  RAISE NOTICE 'road markings: queued % outline(s)', queued;
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'road markings: could not queue this diff (%); its roads are not rebuilt until a scoped build covers them', SQLERRM;
END
$$;
