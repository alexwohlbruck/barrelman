-- Incremental parent context resolution for daily diff updates.
-- Only processes new/changed POIs (parent_context IS NULL) and handles
-- boundary change cascading.
--
-- osm2pgsql-replication deletes and re-inserts changed rows, so changed
-- POIs will have parent_context = NULL after the diff is applied.

-- Pass 1: Resolve parent context for new/changed POIs
UPDATE geo_places p
SET parent_context = trim(
  coalesce(p.address->>'street', '') || ' ' ||
  coalesce(p.address->>'city', '') || ' ' ||
  coalesce(p.address->>'state', '') || ' ' ||
  coalesce(p.address->>'postcode', '') || ' ' ||
  coalesce(sub.boundary_names, '')
)
FROM (
  SELECT
    poi.id,
    string_agg(boundary.name, ' ' ORDER BY boundary.area_m2 ASC) AS boundary_names
  FROM geo_places poi
  JOIN geo_places boundary
    ON boundary.geom_type = 'area'
    AND boundary.name IS NOT NULL
    AND (boundary.admin_level IS NOT NULL
         OR boundary.categories && ARRAY['place/neighbourhood', 'place/suburb', 'place/quarter', 'place/city_block']::text[])
    AND ST_Contains(boundary.geom, poi.centroid)
  WHERE poi.name IS NOT NULL
    AND poi.parent_context IS NULL
  GROUP BY poi.id
) sub
WHERE p.id = sub.id;

-- Handle POIs with address tags but no containing boundaries
UPDATE geo_places
SET parent_context = trim(
  coalesce(address->>'street', '') || ' ' ||
  coalesce(address->>'city', '') || ' ' ||
  coalesce(address->>'state', '') || ' ' ||
  coalesce(address->>'postcode', '')
)
WHERE name IS NOT NULL
  AND parent_context IS NULL
  AND address IS NOT NULL;

-- Cascade: if a boundary itself changed (its parent_context is NULL because it
-- was re-inserted by the diff), invalidate all POIs inside it so they pick up
-- the updated boundary name/geometry on the next resolve pass.
-- Boundary changes are rare (~monthly) so this typically affects zero rows.
UPDATE geo_places poi
SET parent_context = NULL
FROM geo_places boundary
WHERE boundary.geom_type = 'area'
  AND boundary.name IS NOT NULL
  AND (boundary.admin_level IS NOT NULL
       OR boundary.categories && ARRAY['place/neighbourhood', 'place/suburb', 'place/quarter', 'place/city_block']::text[])
  AND boundary.parent_context IS NULL
  AND poi.name IS NOT NULL
  AND poi.parent_context IS NOT NULL
  AND ST_Contains(boundary.geom, poi.centroid);

-- Pass 2: Re-resolve any rows invalidated by the cascade above
UPDATE geo_places p
SET parent_context = trim(
  coalesce(p.address->>'street', '') || ' ' ||
  coalesce(p.address->>'city', '') || ' ' ||
  coalesce(p.address->>'state', '') || ' ' ||
  coalesce(p.address->>'postcode', '') || ' ' ||
  coalesce(sub.boundary_names, '')
)
FROM (
  SELECT
    poi.id,
    string_agg(boundary.name, ' ' ORDER BY boundary.area_m2 ASC) AS boundary_names
  FROM geo_places poi
  JOIN geo_places boundary
    ON boundary.geom_type = 'area'
    AND boundary.name IS NOT NULL
    AND (boundary.admin_level IS NOT NULL
         OR boundary.categories && ARRAY['place/neighbourhood', 'place/suburb', 'place/quarter', 'place/city_block']::text[])
    AND ST_Contains(boundary.geom, poi.centroid)
  WHERE poi.name IS NOT NULL
    AND poi.parent_context IS NULL
  GROUP BY poi.id
) sub
WHERE p.id = sub.id;

-- Handle any remaining POIs with address but no boundaries (from cascade)
UPDATE geo_places
SET parent_context = trim(
  coalesce(address->>'street', '') || ' ' ||
  coalesce(address->>'city', '') || ' ' ||
  coalesce(address->>'state', '') || ' ' ||
  coalesce(address->>'postcode', '')
)
WHERE name IS NOT NULL
  AND parent_context IS NULL
  AND address IS NOT NULL;
