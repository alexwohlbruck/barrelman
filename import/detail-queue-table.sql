-- The rebuild queue for the detail layers built from OSM: the boxes each
-- replication diff touched, as replicate-extract.sh queues them, one row per
-- box and layer. Each layer queues only what it is built from
-- (queue-road-markings.sql, queue-bridge-decks.sql) and its own update script
-- works off its own rows (update-road-markings.sh, update-bridge-decks.ts).
-- `attempts` counts runs in which part of an entry failed to build, so a box
-- that cannot be built moves to the back of the queue, and is dropped after a
-- few tries, instead of holding up the rest.
CREATE TABLE IF NOT EXISTS detail_dirty (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  layer text NOT NULL,
  box geometry(Polygon, 4326) NOT NULL,
  queued_at timestamptz NOT NULL DEFAULT now(),
  attempts int NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS detail_dirty_layer_idx ON detail_dirty (layer, attempts, queued_at);
CREATE INDEX IF NOT EXISTS detail_dirty_box_idx ON detail_dirty USING gist (box);

-- Road markings had a queue of their own before; its failed runs carry over
-- where it counted them.
DO $$
BEGIN
  IF to_regclass('road_markings_dirty') IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'road_markings_dirty' AND column_name = 'attempts'
                 AND table_schema = ANY(current_schemas(false))) THEN
      INSERT INTO detail_dirty (layer, box, queued_at, attempts)
      SELECT 'road_markings', box, queued_at, attempts FROM road_markings_dirty;
    ELSE
      INSERT INTO detail_dirty (layer, box, queued_at) SELECT 'road_markings', box, queued_at FROM road_markings_dirty;
    END IF;
    DROP TABLE road_markings_dirty;
  END IF;
END
$$;
