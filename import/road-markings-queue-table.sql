-- The road markings rebuild queue: the outline of each road, crossing or
-- signal a replication diff touched (import/queue-road-markings.sql), worked
-- off by scripts/update-road-markings.sh. `attempts` counts runs in which a
-- cell under the entry failed to build, so a box GEOS cannot build moves to the
-- back of the queue, and is dropped after a few tries, instead of holding up
-- the rest.
CREATE TABLE IF NOT EXISTS road_markings_dirty (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  box geometry(Polygon, 4326) NOT NULL,
  queued_at timestamptz NOT NULL DEFAULT now(),
  attempts int NOT NULL DEFAULT 0
);

-- A queue left by a build of this before ids and attempts were added.
ALTER TABLE road_markings_dirty ADD COLUMN IF NOT EXISTS id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY;
ALTER TABLE road_markings_dirty ADD COLUMN IF NOT EXISTS attempts int NOT NULL DEFAULT 0;
