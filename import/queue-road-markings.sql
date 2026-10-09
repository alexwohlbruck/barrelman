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
-- A road's place reads the way before it, and the way after that eases back
-- (generate-road-markings.sql, Where the way runs), so a changed road also
-- queues the ways that carry straight on from either end of it, two deep.
--
-- Nothing is queued where road markings were never built. A failure here is
-- reported and skipped rather than raised: it would otherwise roll back the
-- replication cycle it rides in.
-- Needs detail_dirty (detail-queue-table.sql), and road_is_marked_way from a
-- build with generate-road-markings.sql; without it the diff is skipped with a
-- warning, as below.
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
    WHERE (g.geom_type = 'line' AND (road_is_marked_way(g.tags) OR g.tags->>'footway' = 'crossing'))
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
  ),
  -- A way joined end to end with exactly one other road way: the next one along.
  next1 AS (
    SELECT DISTINCT n.osm_id, n.geom
    FROM touched t, LATERAL (VALUES (ST_StartPoint(t.geom)), (ST_EndPoint(t.geom))) e(pt),
         LATERAL (
           SELECT g.osm_id, g.geom FROM geo_places g
           WHERE g.geom && ST_Expand(e.pt, 1e-7) AND g.geom_type = 'line' AND road_is_marked_way(g.tags)
             AND (ST_DWithin(ST_StartPoint(g.geom), e.pt, 1e-7) OR ST_DWithin(ST_EndPoint(g.geom), e.pt, 1e-7))
         ) n
    WHERE GeometryType(t.geom) = 'LINESTRING' AND NOT ST_Equals(n.geom, t.geom)
      AND (SELECT count(*) FROM geo_places g
           WHERE g.geom && ST_Expand(e.pt, 1e-7) AND g.geom_type = 'line' AND road_is_marked_way(g.tags)
             AND ST_DWithin(g.geom, e.pt, 1e-7)) = 2
  ),
  next2 AS (
    SELECT DISTINCT n.osm_id, n.geom
    FROM next1 t, LATERAL (VALUES (ST_StartPoint(t.geom)), (ST_EndPoint(t.geom))) e(pt),
         LATERAL (
           SELECT g.osm_id, g.geom FROM geo_places g
           WHERE g.geom && ST_Expand(e.pt, 1e-7) AND g.geom_type = 'line' AND road_is_marked_way(g.tags)
             AND (ST_DWithin(ST_StartPoint(g.geom), e.pt, 1e-7) OR ST_DWithin(ST_EndPoint(g.geom), e.pt, 1e-7))
         ) n
    WHERE n.osm_id <> t.osm_id AND NOT EXISTS (SELECT 1 FROM touched x WHERE ST_Equals(x.geom, n.geom))
      AND (SELECT count(*) FROM geo_places g
           WHERE g.geom && ST_Expand(e.pt, 1e-7) AND g.geom_type = 'line' AND road_is_marked_way(g.tags)
             AND ST_DWithin(g.geom, e.pt, 1e-7)) = 2
  )
  INSERT INTO detail_dirty (layer, box)
  SELECT 'road_markings', ST_Envelope(ST_Expand(geom, 0.0005))
  FROM (SELECT geom FROM touched UNION ALL SELECT geom FROM next1 UNION ALL SELECT geom FROM next2) q
  WHERE geom IS NOT NULL;
  GET DIAGNOSTICS queued = ROW_COUNT;
  RAISE NOTICE 'road markings: queued % outline(s)', queued;
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'road markings: could not queue this diff (%); its roads are not rebuilt until a scoped build covers them', SQLERRM;
END
$$;
