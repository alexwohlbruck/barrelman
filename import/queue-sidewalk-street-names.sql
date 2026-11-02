-- =============================================================================
-- Queue sidewalk street names for rebuilding where a replication diff touched them
-- =============================================================================
-- Runs inside replicate-extract.sh's swap transaction, which holds every
-- replaced row as it was (osm_replay.old_places) and as it is now
-- (osm_replay.changed), and only when SIDEWALK_STREET_NAMES_INCREMENTAL=1.
-- import/update-sidewalk-street-names.ts works the queue off later.
--
-- Queued: every highway way, before and after the diff, grown by a little more
-- than the distance a sidewalk takes a street's name from. That covers a
-- sidewalk added, moved, named or deleted, and a street beside one renamed,
-- moved or gone. A row the cycle only rewrote, because a node of it changed
-- while its shape did not, is the same as before and queues nothing. Nothing is
-- queued before Build Sidewalk Street Names has stored a row. A failure here is
-- reported and skipped rather than raised: it would otherwise roll back the
-- replication cycle. Needs detail_dirty (detail-queue-table.sql).
-- =============================================================================
DO $$
DECLARE
  queued bigint;
BEGIN
  IF to_regclass('sidewalk_street_names') IS NULL OR NOT EXISTS (SELECT 1 FROM sidewalk_street_names) THEN
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
  touched AS (
    SELECT g.geom FROM geo_places g
    JOIN osm_replay.changed c ON c.id = g.id
    WHERE g.geom_type IN ('line', 'area') AND g.tags ? 'highway' AND g.id NOT IN (SELECT id FROM same)
    UNION ALL
    SELECT o.geom FROM osm_replay.old_places o
    WHERE o.geom_type IN ('line', 'area') AND o.geom IS NOT NULL AND o.id NOT IN (SELECT id FROM same)
      AND EXISTS (SELECT 1 FROM unnest(o.categories) k WHERE k LIKE 'highway/%')
  ),
  boxes AS (
    SELECT DISTINCT ST_Envelope(ST_Expand(geom, 0.0004)) AS box FROM touched
  )
  INSERT INTO detail_dirty (layer, box)
  SELECT 'sidewalk_street_names', b.box FROM boxes b
  WHERE NOT EXISTS (SELECT 1 FROM detail_dirty q WHERE q.layer = 'sidewalk_street_names' AND q.box ~= b.box AND q.attempts = 0);
  GET DIAGNOSTICS queued = ROW_COUNT;
  RAISE NOTICE 'sidewalk street names: queued % box(es)', queued;
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'sidewalk street names: could not queue this diff (%); its sidewalks are not renamed until Build Sidewalk Street Names covers them', SQLERRM;
END
$$;
