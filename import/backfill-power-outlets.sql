-- Brings the umbrella category `power/outlet` in line with the import rule
-- without a full reimport: adds it to places that offer a public power outlet
-- and removes it from places that no longer qualify (0.6.0 counted EV connector
-- sockets). Mirrors `offers_power_outlet` in osm2pgsql-flex.lua, which sets it
-- on every import and replication diff. Idempotent: only rows whose category is
-- wrong are written.
--
-- power=outlet itself is left alone — its own tag derives `power/outlet`.

BEGIN;

-- One pass over geo_places (the tags index is jsonb_path_ops, which cannot
-- answer key-exists or key-prefix tests). The API pool's statement timeout
-- would cut that short; LOCAL scopes the lift to this transaction.
SET LOCAL statement_timeout = 0;

-- The rule as a function, so the single UPDATE below can name it in its SET
-- and its WHERE. pg_temp keeps it to this session.
CREATE OR REPLACE FUNCTION pg_temp.offers_power_outlet(tags jsonb)
RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
  SELECT
    coalesce(tags->>'access', '') NOT IN ('private', 'no')
    AND (
      coalesce(tags->>'amenity', '') IN ('device_charging_station', 'power_supply')
      OR (
        -- Vehicle chargers carry socket:* tags for their connectors.
        coalesce(tags->>'amenity', '') <> 'charging_station'
        AND coalesce(tags->>'man_made', '') <> 'charge_point'
        -- Pitch and berth hookups for paying guests, not public outlets.
        AND coalesce(tags->>'tourism', '') NOT IN ('camp_site', 'camp_pitch', 'caravan_site')
        AND coalesce(tags->>'leisure', '') <> 'marina'
        AND (
          coalesce(tags->>'power_supply', 'no') NOT IN ('no', 'wind', 'solar')
          -- Plugs a device takes; see DEVICE_SOCKETS in osm2pgsql-flex.lua.
          OR EXISTS (
            SELECT 1 FROM jsonb_each_text(tags) t
            WHERE t.key LIKE 'socket:%'
              AND lower(substr(t.key, 8)) IN ('schuko', 'typee', 'domestic', 'bs1363',
                                              'nema_5_15', 'nema_5_20', 'as3112', 'usb')
              AND t.value NOT IN ('no', '0')
          )
        )
      )
    )
$$;

-- One UPDATE over the table, with no join back to itself. An earlier version
-- judged the rows in a CTE and joined them back on id. The planner could not
-- tell how few rows that leaves, so it hashed all of geo_places for the join.
-- On a 218M-row table that grew one backend to 7.8 GB, the container's memory
-- limit killed it, and Postgres restarted in recovery.
UPDATE geo_places g
SET categories = CASE WHEN pg_temp.offers_power_outlet(g.tags)
                      THEN array_append(g.categories, 'power/outlet')
                      ELSE array_remove(g.categories, 'power/outlet') END,
    ts = CASE WHEN g.name IS NULL THEN g.ts
         ELSE build_ts(g.osm_type, g.name, g.names, g.name_abbrev,
                       CASE WHEN pg_temp.offers_power_outlet(g.tags)
                            THEN array_append(g.categories, 'power/outlet')
                            ELSE array_remove(g.categories, 'power/outlet') END,
                       g.parent_context)
         END
WHERE coalesce(g.tags->>'power', '') <> 'outlet'
  AND pg_temp.offers_power_outlet(g.tags)
      <> coalesce(g.categories @> ARRAY['power/outlet'], false);

COMMIT;
