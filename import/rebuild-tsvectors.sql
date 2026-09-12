-- Rebuild full-text search tsvectors for geo_places.
--
-- The document itself is produced by build_ts() (created in post-import.sql)
-- so the normalization lives in exactly one place — see the comment there.
-- Accepts an optional :scope variable to limit which rows are updated:
--   'all'            — every named row (full import)
--   'intersections'  — only intersection rows (daily update, intersections regenerated)
--
-- The IS DISTINCT FROM guard makes re-runs cheap: a row whose document did not
-- change is read, compared and skipped rather than rewritten through the ts
-- GIN index. On a full import after resolve-parent-context.sql (which already
-- writes ts alongside parent_context) this pass verifies instead of
-- re-writing, so running it twice costs a scan, not 30M row versions.
UPDATE geo_places SET ts = build_ts(osm_type, name, names, name_abbrev, categories, parent_context)
WHERE name IS NOT NULL
  AND (:'scope' = 'all' OR osm_type = 'X' OR ts IS NULL)
  AND ts IS DISTINCT FROM build_ts(osm_type, name, names, name_abbrev, categories, parent_context);
