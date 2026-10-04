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

WITH judged AS (
  SELECT id, categories, (
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
  ) AS offers
  FROM geo_places
  WHERE coalesce(tags->>'power', '') <> 'outlet'
),
fixed AS (
  SELECT id, CASE WHEN offers THEN array_append(categories, 'power/outlet')
                  ELSE array_remove(categories, 'power/outlet') END AS categories
  FROM judged
  WHERE offers <> coalesce(categories @> ARRAY['power/outlet'], false)
)
UPDATE geo_places g
SET categories = f.categories,
    -- categories feed the tsvector, so "power outlet" finds these by text too.
    -- Unnamed rows carry no ts and stay without one, as fillTsvectors leaves them.
    ts = CASE WHEN g.name IS NULL THEN g.ts
         ELSE build_ts(g.osm_type, g.name, g.names, g.name_abbrev, f.categories, g.parent_context)
         END
FROM fixed f
WHERE g.id = f.id;

COMMIT;
