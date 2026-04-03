-- Resolve parent boundary context for all named places.
-- Populates parent_context with admin boundary names + address fields
-- so searches like "starbucks pineville nc" match via tsvector/embeddings.
--
-- Run AFTER post-import.sql (which creates the column and computes area_m2).
-- Joins against admin boundaries and neighbourhood/suburb area polygons.

-- Pass 1: Spatial join — find containing admin boundaries for each named POI
-- Result example: "Providence Road Charlotte NC 28277 Elizabeth Charlotte Mecklenburg County North Carolina United States"
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
  GROUP BY poi.id
) sub
WHERE p.id = sub.id;

-- Pass 2: Handle POIs that have address tags but fall outside any admin boundary
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

-- Stats
DO $$
DECLARE
  ctx_count BIGINT;
BEGIN
  SELECT count(*) INTO ctx_count FROM geo_places WHERE parent_context IS NOT NULL;
  RAISE NOTICE 'Parent context resolved for % places', ctx_count;
END $$;
