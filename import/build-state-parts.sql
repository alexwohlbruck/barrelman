-- States and provinces cut into small parts, for the states /route lists
--
-- A state polygon runs to tens of thousands of vertices, and testing route
-- pieces against it whole costs milliseconds a piece. Parts of at most 256
-- vertices answer the same questions from a GiST index in microseconds.
--
-- Rebuilt whole from geo_places: seconds on a continental import. The API
-- builds it at startup when it is empty, the OSM import and update rebuild it,
-- and the "Build Route State Parts" task runs this file on demand.

CREATE TABLE IF NOT EXISTS state_parts (
  id text NOT NULL,
  name text,
  code text,
  geom geometry(Polygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS state_parts_geom_idx ON state_parts USING gist (geom);

BEGIN;
SET LOCAL statement_timeout = 0;
-- Readers keep the old parts until COMMIT; a second rebuild waits rather than doubling them.
LOCK TABLE state_parts IN SHARE ROW EXCLUSIVE MODE;
DELETE FROM state_parts;
INSERT INTO state_parts (id, name, code, geom)
SELECT id, name, tags->>'ISO3166-2', ST_Subdivide(ST_CollectionExtract(ST_MakeValid(geom), 3), 256)
FROM geo_places
WHERE geom_type = 'area' AND admin_level IS NOT NULL AND admin_level = 4;
COMMIT;

ANALYZE state_parts;
