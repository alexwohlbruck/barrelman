-- =============================================================================
-- Queue tunnel cuts for rebuilding where a replication diff touched them
-- =============================================================================
-- Runs inside replicate-extract.sh's swap transaction, which holds every
-- replaced row as it was (osm_replay.old_places) and as it is now
-- (osm_replay.changed), and only when TUNNEL_CUTS_INCREMENTAL=1.
-- import/update-tunnel-cuts.ts works the queue off later.
--
-- Queued, each as the box its portals' cuts can reach:
--   * a tunnel way as it is now, which catches a tunnel added, moved or
--     retagged into one;
--   * every cut that something it is fitted to touches, before or after the
--     diff: the tunnel itself (deleted, moved, untagged), the roads it opens
--     onto, the surfaces, buildings and walls around its portal. old_places
--     keeps categories, not tags, so before the diff any road, railway,
--     building or barrier counts.
-- A row the cycle only rewrote, because a node of it changed while its shape
-- did not, is the same as before and queues nothing. Nothing is queued before
-- Build Tunnel Cuts has stored a cut. A failure here is reported and skipped
-- rather than raised: it would otherwise roll back the replication cycle.
-- Needs detail_dirty (detail-queue-table.sql).
-- =============================================================================
DO $$
DECLARE
  queued bigint;
BEGIN
  IF to_regclass('tunnel_cuts') IS NULL OR NOT EXISTS (SELECT 1 FROM tunnel_cuts) THEN
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
    SELECT g.geom, g.geom_type, g.tags
    FROM geo_places g
    JOIN osm_replay.changed c ON c.id = g.id
    WHERE g.geom_type IN ('line', 'area') AND g.id NOT IN (SELECT id FROM same)
  ),
  tunnels AS (
    SELECT geom FROM now_rows
    WHERE geom_type = 'line' AND (tags ? 'highway' OR tags ? 'railway')
      AND COALESCE(tags->>'tunnel', 'no') NOT IN ('no', 'false', '0')
  ),
  touching AS (
    SELECT geom FROM now_rows
    WHERE tags ? 'highway' OR tags ? 'railway' OR tags ? 'building' OR tags ? 'building:part' OR tags ? 'barrier'
    UNION ALL
    SELECT o.geom FROM osm_replay.old_places o
    WHERE o.geom_type IN ('line', 'area') AND o.geom IS NOT NULL AND o.id NOT IN (SELECT id FROM same)
      AND EXISTS (SELECT 1 FROM unnest(o.categories) k
                  WHERE k LIKE 'highway/%' OR k LIKE 'railway/%' OR k LIKE 'building%' OR k LIKE 'barrier/%')
  ),
  cuts AS (
    SELECT DISTINCT c.id, c.geom
    FROM touching t
    JOIN tunnel_cuts c ON c.geom && ST_Expand(t.geom, 0.0005) AND ST_DWithin(c.geom, t.geom, 0.0005)
  ),
  -- About the farthest a portal lies from its tunnel's end, and from where its cut opens.
  boxes AS (
    SELECT ST_Envelope(ST_Expand(geom, 0.005)) AS box FROM tunnels
    UNION
    SELECT ST_Envelope(ST_Expand(geom, 0.005)) FROM cuts
  )
  -- A box already waiting, from an earlier cycle, is not queued twice.
  INSERT INTO detail_dirty (layer, box)
  SELECT 'tunnel_cuts', b.box FROM boxes b
  WHERE NOT EXISTS (SELECT 1 FROM detail_dirty q WHERE q.layer = 'tunnel_cuts' AND q.box ~= b.box AND q.attempts = 0);
  GET DIAGNOSTICS queued = ROW_COUNT;
  RAISE NOTICE 'tunnel cuts: queued % box(es)', queued;
EXCEPTION WHEN OTHERS THEN
  RAISE WARNING 'tunnel cuts: could not queue this diff (%); its tunnels are not rebuilt until Build Tunnel Cuts covers them', SQLERRM;
END
$$;
