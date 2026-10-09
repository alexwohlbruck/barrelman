-- =============================================================================
-- Queue bridge decks for rebuilding where a replication diff touched them
-- =============================================================================
-- Runs inside replicate-extract.sh's swap transaction, which holds every
-- replaced row as it was (osm_replay.old_places) and as it is now
-- (osm_replay.changed), and only when BRIDGE_DECKS_INCREMENTAL=1.
-- import/update-bridge-decks.ts works the queue off later.
--
-- Queued:
--   * a bridge way or man_made=bridge outline as it is now, which catches a
--     bridge added, moved or retagged into one;
--   * every deck that something it is built from touches, before or after the
--     diff: the bridge ways themselves (deleted, moved, untagged), the roads
--     and railways its ends land on, what it crosses, and the water under it.
--     old_places keeps categories, not tags, so before the diff anything
--     categorised as a road, railway, waterway, water or bridge counts.
-- A deck is queued as its own box, not the box of what touched it, so editing
-- a long river or a county road queues only the decks along it. A row the
-- cycle only rewrote, because a node of it changed while its shape did not
-- (a node gained a tag), is the same as before and queues nothing.
-- osm_replay.edited lists what the diff itself changed.
--
-- Which ways make decks is bridge_deck_class() (create-detail-views.sql), as
-- for the build. Nothing is queued before Build Bridge Decks has recorded a
-- cell, and Update Bridge Decks drops what lies outside them. A
-- failure here is reported and skipped rather than raised: it would otherwise
-- roll back the replication cycle.
-- Needs detail_dirty (detail-queue-table.sql).
-- =============================================================================
DO $$
DECLARE
  queued bigint;
BEGIN
  -- Only where Build Bridge Decks recorded cells, which is all Update Bridge
  -- Decks rebuilds, so nothing piles up that it would drop.
  IF to_regclass('bridge_decks') IS NULL OR to_regclass('bridge_deck_cells') IS NULL THEN
    RETURN;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM bridge_deck_cells) THEN
    RETURN;
  END IF;

  WITH same AS (
    SELECT o.id
    FROM osm_replay.old_places o
    JOIN osm_replay.changed c ON c.id = o.id
    JOIN geo_places g ON g.id = o.id
    WHERE g.geom = o.geom
      AND NOT EXISTS (SELECT 1 FROM osm_replay.edited e WHERE e.osm_type = o.osm_type AND e.osm_id = o.osm_id)
  ),
  now_rows AS (
    SELECT g.id, g.geom, g.geom_type, g.tags
    FROM geo_places g
    JOIN osm_replay.changed c ON c.id = g.id
    WHERE g.geom_type IN ('line', 'area') AND g.id NOT IN (SELECT id FROM same)
  ),
  bridges AS (
    SELECT geom FROM now_rows
    WHERE (geom_type = 'line' AND COALESCE(tags->>'bridge', 'no') NOT IN ('no', 'abandoned')
           AND COALESCE(tags->>'tunnel', 'no') = 'no' AND bridge_deck_class(tags) IS NOT NULL)
       OR (geom_type = 'area' AND tags->>'man_made' = 'bridge')
  ),
  touching AS (
    SELECT geom, geom_type FROM now_rows
    WHERE (geom_type = 'line' AND (tags ? 'highway' OR tags ? 'railway' OR tags ? 'waterway'))
       OR (geom_type = 'area' AND (tags->>'man_made' = 'bridge' OR tags->>'natural' = 'water'
                                   OR tags->>'waterway' = 'riverbank' OR tags->>'landuse' = 'reservoir'))
    UNION ALL
    SELECT o.geom, o.geom_type FROM osm_replay.old_places o
    WHERE o.geom_type IN ('line', 'area') AND o.geom IS NOT NULL AND o.id NOT IN (SELECT id FROM same)
      AND (o.categories && ARRAY['man_made/bridge', 'natural/water', 'waterway/riverbank', 'landuse/reservoir']
           OR EXISTS (SELECT 1 FROM unnest(o.categories) k
                      WHERE k LIKE 'highway/%' OR k LIKE 'railway/%' OR k LIKE 'waterway/%'))
  ),
  -- Only what reaches a recorded cell, by box, before any exact test. An area
  -- (a riverbank, a lake) is tested by intersection, which PostGIS prepares
  -- once per polygon, rather than by distance, which walks it per deck.
  near AS (
    SELECT t.geom, t.geom_type FROM touching t
    WHERE EXISTS (SELECT 1 FROM bridge_deck_cells c WHERE c.box && t.geom)
  ),
  decks AS (
    SELECT DISTINCT b.id, b.geom
    FROM near t
    JOIN bridge_decks b ON b.geom && ST_Expand(t.geom, 0.00002)
     AND CASE WHEN t.geom_type = 'area' THEN ST_Intersects(t.geom, b.geom) ELSE ST_DWithin(b.geom, t.geom, 0.00002) END
  ),
  boxes AS (
    SELECT ST_Envelope(ST_Expand(geom, 0.0005)) AS box FROM bridges
    UNION
    SELECT ST_Envelope(ST_Expand(geom, 0.0005)) FROM decks
  )
  -- A box already waiting, from an earlier cycle, is not queued twice.
  -- Only boxes that reach a recorded cell, which is all an update rebuilds.
  INSERT INTO detail_dirty (layer, box)
  SELECT 'bridge_decks', b.box FROM boxes b
  WHERE EXISTS (SELECT 1 FROM bridge_deck_cells c WHERE c.box && b.box)
    AND NOT EXISTS (SELECT 1 FROM detail_dirty q WHERE q.layer = 'bridge_decks' AND q.box ~= b.box AND q.attempts = 0);
  GET DIAGNOSTICS queued = ROW_COUNT;
  RAISE NOTICE 'bridge decks: queued % box(es)', queued;
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'bridge decks: could not queue this diff (%); its bridges are not rebuilt until Build Bridge Decks covers them', SQLERRM;
END
$$;
