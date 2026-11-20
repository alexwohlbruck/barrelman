-- =============================================================================
-- Queue the bridge decks OSM gives a height with `ele`, for rebuilding
-- =============================================================================
-- Run once after upgrading to a version that reads `ele` differently. From
-- 0.10.55 an `ele` is taken as metres above EGM96, as OSM defines it, and
-- converted to the terrain's datum (src/lib/bridge-decks/datum.ts), which in
-- the conterminous US lowers it by 0.1–1.2 m. A deck already built keeps its
-- old height until something rebuilds it, and replication only queues a deck
-- when its OSM data changes, so this queues each one the conversion moves:
--
--   * a deck a node tagged `ele` lies on, which pins it;
--   * a deck built from a bridge way tagged `ele`;
--   * a deck crossing a man_made=bridge outline tagged `ele`;
--
-- inside the conterminous US box, the only place the conversion changes
-- anything. Update Bridge Decks (or OSM Update after it) then rebuilds them,
-- BRIDGE_DECKS_MAX_CELLS cells a run. Boxes are queued as
-- queue-bridge-decks.sql queues them, and a box already waiting is not queued
-- twice, so running this again is harmless.
-- Needs detail_dirty (detail-queue-table.sql).
-- =============================================================================
DO $$
DECLARE
  queued bigint;
  us geometry := ST_MakeEnvelope(-125, 24, -66, 50, 4326);
BEGIN
  IF to_regclass('bridge_decks') IS NULL OR to_regclass('bridge_deck_cells') IS NULL THEN
    RAISE NOTICE 'bridge decks: none built, nothing to queue';
    RETURN;
  END IF;
  IF to_regclass('detail_dirty') IS NULL THEN
    RAISE EXCEPTION 'detail_dirty does not exist; run import/detail-queue-table.sql, or rebuild the decks with Build Bridge Decks';
  END IF;

  WITH tagged AS (
    -- Nodes on a deck: a point row of its own at the deck's vertex (see nodeHeights in bridge-deck-cells.ts).
    SELECT b.id, b.geom FROM bridge_decks b
    JOIN geo_places g ON g.geom && ST_Expand(b.geom, 0.00002) AND ST_DWithin(g.geom, b.geom, 0.00002)
    WHERE b.geom && us AND g.osm_type = 'N' AND g.geom_type = 'point' AND g.tags ? 'ele'
    UNION
    SELECT b.id, b.geom FROM bridge_decks b
    JOIN geo_places g ON g.osm_type = 'W' AND g.geom_type = 'line' AND g.osm_id = ANY(b.ways)
    WHERE b.geom && us AND g.tags ? 'ele'
    UNION
    SELECT b.id, b.geom FROM bridge_decks b
    JOIN geo_places g ON g.tags @> '{"man_made": "bridge"}' AND g.geom_type = 'area' AND g.geom && b.geom AND ST_Intersects(g.geom, b.geom)
    WHERE b.geom && us AND g.tags ? 'ele'
  ),
  boxes AS (
    SELECT DISTINCT ST_Envelope(ST_Expand(geom, 0.0005)) AS box FROM tagged
  )
  INSERT INTO detail_dirty (layer, box)
  SELECT 'bridge_decks', b.box FROM boxes b
  WHERE EXISTS (SELECT 1 FROM bridge_deck_cells c WHERE c.box && b.box)
    AND NOT EXISTS (SELECT 1 FROM detail_dirty q WHERE q.layer = 'bridge_decks' AND q.box ~= b.box AND q.attempts = 0);
  GET DIAGNOSTICS queued = ROW_COUNT;
  RAISE NOTICE 'bridge decks: queued % box(es) of decks tagged ele; Update Bridge Decks rebuilds them', queued;
END
$$;
