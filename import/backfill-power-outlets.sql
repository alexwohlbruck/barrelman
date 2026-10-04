-- Adds the umbrella category `power/outlet` to places that offer a public power
-- outlet, without a full reimport. Mirrors `offers_power_outlet` in
-- osm2pgsql-flex.lua, which sets it on every import and replication diff from
-- now on. Idempotent: rows that already carry it are skipped.
--
-- power=outlet itself needs nothing here — its own tag already derives
-- `power/outlet`.

BEGIN;

-- One pass over geo_places (the tags index is jsonb_path_ops, which cannot
-- answer key-exists or key-prefix tests). The API pool's statement timeout
-- would cut that short; LOCAL scopes the lift to this transaction.
SET LOCAL statement_timeout = 0;

UPDATE geo_places
SET categories = array_append(categories, 'power/outlet'),
    -- categories feed the tsvector, so "power outlet" finds these by text too.
    -- Unnamed rows carry no ts and stay without one, as fillTsvectors leaves them.
    ts = CASE WHEN name IS NULL THEN ts
         ELSE build_ts(osm_type, name, names, name_abbrev,
                       array_append(categories, 'power/outlet'), parent_context)
         END
WHERE NOT categories @> ARRAY['power/outlet']
  AND coalesce(tags->>'access', '') NOT IN ('private', 'no')
  AND (
    tags->>'amenity' IN ('device_charging_station', 'power_supply')
    OR (
      -- Vehicle chargers carry socket:* tags for their connectors.
      coalesce(tags->>'amenity', '') <> 'charging_station'
      AND coalesce(tags->>'man_made', '') <> 'charge_point'
      -- Pitch and berth hookups for paying guests, not public outlets.
      AND coalesce(tags->>'tourism', '') NOT IN ('camp_site', 'camp_pitch', 'caravan_site')
      AND coalesce(tags->>'leisure', '') <> 'marina'
      AND (
        coalesce(tags->>'power_supply', 'no') NOT IN ('no', 'wind', 'solar')
        OR EXISTS (
          SELECT 1 FROM jsonb_each_text(tags) t
          WHERE t.key LIKE 'socket:%' AND t.value NOT IN ('no', '0')
        )
      )
    )
  );

COMMIT;
