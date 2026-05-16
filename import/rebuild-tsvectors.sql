-- Rebuild full-text search tsvectors for geo_places.
-- Intersection names get "&" expanded to multilingual "and" tokens and
-- road suffix abbreviations injected so "hawthorne ln & 8th st" matches.
-- Accepts an optional :scope variable to limit which rows are updated:
--   'all'           — every named row (full import)
--   'intersections'  — only intersection rows (daily update, intersections regenerated)

UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
    CASE WHEN osm_type = 'X'
        THEN replace(replace(replace(replace(replace(replace(replace(
             replace(replace(replace(replace(replace(replace(
               coalesce(name, ''), ' & ', ' and et und y e ')
             , 'Street', 'Street St'), 'Avenue', 'Avenue Ave')
             , 'Boulevard', 'Boulevard Blvd'), 'Drive', 'Drive Dr')
             , 'Lane', 'Lane Ln'), 'Road', 'Road Rd')
             , 'Court', 'Court Ct'), 'Place', 'Place Pl')
             , 'Circle', 'Circle Cir'), 'Parkway', 'Parkway Pkwy')
             , 'Highway', 'Highway Hwy'), 'Trail', 'Trail Trl')
             || ' ' || coalesce(array_to_string(names, ' '), '')
        ELSE coalesce(name, '')
    END || ' ' || coalesce(name_abbrev, '') || ' ' ||
    coalesce(array_to_string(
        ARRAY(SELECT replace(replace(unnest(categories), '/', ' '), '_', ' ')),
    ' '), '') || ' ' ||
    coalesce(parent_context, '')
))
WHERE name IS NOT NULL
  AND (:'scope' = 'all' OR osm_type = 'X' OR ts IS NULL);
