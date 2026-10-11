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
-- paves, crossings, and signal, stop and turning circle nodes. old_places
-- keeps categories, not tags, so an old service road or footway counts only
-- where it was drawn: a service road on a paved surface, a footway under a
-- painted crosswalk.
-- Without those, every alley and sidewalk edit in a built city would queue a
-- rebuild.
--
-- A road's place reads the way before it, the way after that eases back, and
-- a fork's branches line up with its stem (generate-road-markings.sql, Where
-- the way runs, Forks), so a changed road also queues the ways ending on it,
-- and round the far ends of those as far as a taper reaches.
--
-- Nothing is queued where road markings were never built. A failure here is
-- reported and skipped rather than raised: it would otherwise roll back the
-- replication cycle it rides in.
-- Needs detail_dirty (detail-queue-table.sql).
-- =============================================================================
-- The same test generate-road-markings.sql builds from, kept here as well so a
-- database whose last build predates it still queues (a test holds the two
-- copies equal).
CREATE OR REPLACE FUNCTION road_is_marked_way(tags jsonb) RETURNS boolean
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
  SELECT tags->>'highway' IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link',
                              'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'unclassified',
                              'residential', 'living_street', 'service', 'busway')
    AND COALESCE(tags->>'tunnel', 'no') = 'no'
    AND COALESCE(tags->>'area', 'no') <> 'yes'
    AND (tags->>'highway' <> 'service' OR tags ?| ARRAY['lanes', 'lane_markings', 'width', 'width:carriageway'])
$$;

DO $$
DECLARE
  queued bigint;
BEGIN
  IF to_regclass('road_surfaces') IS NULL OR to_regclass('road_markings') IS NULL THEN
    RETURN;
  END IF;

  WITH relevant_new AS (
    SELECT g.id, g.osm_id, g.geom
    FROM geo_places g
    JOIN osm_replay.changed c ON c.id = g.id
    WHERE (g.geom_type = 'line' AND g.osm_type = 'W' AND (road_is_marked_way(g.tags) OR g.tags->>'footway' = 'crossing'))
       OR (g.geom_type = 'point' AND g.tags->>'highway' IN ('crossing', 'traffic_signals', 'stop', 'turning_circle', 'turning_loop'))
  ),
  touched AS (
    SELECT o.osm_id, o.geom
    FROM osm_replay.old_places o
    WHERE (o.geom_type = 'line' AND o.osm_type = 'W'
           AND (o.categories && ARRAY['highway/motorway', 'highway/motorway_link', 'highway/trunk', 'highway/trunk_link',
                                      'highway/primary', 'highway/primary_link', 'highway/secondary', 'highway/secondary_link',
                                      'highway/tertiary', 'highway/tertiary_link', 'highway/unclassified',
                                      'highway/residential', 'highway/living_street', 'highway/busway']
                OR (o.categories && ARRAY['highway/service']
                    AND EXISTS (SELECT 1 FROM road_surfaces s
                                WHERE s.geom && o.geom AND ST_Intersects(s.geom, ST_PointOnSurface(o.geom))))
                OR (o.categories && ARRAY['highway/footway', 'highway/path', 'highway/cycleway']
                    AND EXISTS (SELECT 1 FROM road_markings m WHERE m.geom && o.geom AND m.kind = 'crosswalk'))))
       OR (o.geom_type = 'point' AND o.categories && ARRAY['highway/crossing', 'highway/traffic_signals', 'highway/stop',
                                                           'highway/turning_circle', 'highway/turning_loop'])
       OR o.id IN (SELECT id FROM relevant_new)
    UNION ALL
    SELECT osm_id, geom FROM relevant_new
  ),
  -- The ways ending anywhere on a touched road: at its ends they carry on
  -- from it or share a fork with it, and a street running through a fork
  -- decides whether it is one. The touched way's own outline is the old one
  -- where it moved, so it is not looked for in geo_places.
  next1 AS (
    SELECT DISTINCT n.osm_id, n.geom
    FROM touched t, LATERAL (SELECT (ST_DumpPoints(t.geom)).geom) e(pt),
         LATERAL (
           SELECT g.osm_id, g.geom FROM geo_places g
           WHERE g.geom && ST_Expand(e.pt, 1e-7) AND g.geom_type = 'line' AND g.osm_type = 'W' AND road_is_marked_way(g.tags)
             AND g.osm_id <> t.osm_id
             AND (ST_DWithin(ST_StartPoint(g.geom), e.pt, 1e-7) OR ST_DWithin(ST_EndPoint(g.geom), e.pt, 1e-7))
         ) n
    WHERE GeometryType(t.geom) = 'LINESTRING' AND NOT EXISTS (SELECT 1 FROM touched x WHERE x.osm_id = n.osm_id)
  ),
  -- The ways meeting those ends change only as far as a taper reaches from
  -- the shared node (a few hundred metres at most).
  next2 AS (
    SELECT DISTINCT n.osm_id, ST_Expand(e.pt, 0.003) as box
    FROM next1 t, LATERAL (VALUES (ST_StartPoint(t.geom)), (ST_EndPoint(t.geom))) e(pt),
         LATERAL (
           SELECT g.osm_id FROM geo_places g
           WHERE g.geom && ST_Expand(e.pt, 1e-7) AND g.geom_type = 'line' AND g.osm_type = 'W' AND road_is_marked_way(g.tags)
             AND g.osm_id <> t.osm_id
             AND (ST_DWithin(ST_StartPoint(g.geom), e.pt, 1e-7) OR ST_DWithin(ST_EndPoint(g.geom), e.pt, 1e-7))
         ) n
    WHERE NOT EXISTS (SELECT 1 FROM touched x WHERE x.osm_id = n.osm_id)
      AND NOT EXISTS (SELECT 1 FROM next1 x WHERE x.osm_id = n.osm_id)
  )
  INSERT INTO detail_dirty (layer, box)
  SELECT 'road_markings', box
  FROM (SELECT ST_Envelope(ST_Expand(geom, 0.0005)) as box FROM touched WHERE geom IS NOT NULL
        UNION ALL SELECT ST_Envelope(ST_Expand(geom, 0.0005)) FROM next1
        UNION ALL SELECT box FROM next2) q;
  GET DIAGNOSTICS queued = ROW_COUNT;
  RAISE NOTICE 'road markings: queued % outline(s)', queued;
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'road markings: could not queue this diff (%); its roads are not rebuilt until a scoped build covers them', SQLERRM;
END
$$;
