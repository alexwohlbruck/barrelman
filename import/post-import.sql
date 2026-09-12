-- Post-import SQL: extract structured fields from tags, build search indexes
-- Run after osm2pgsql flex import completes

-- Add columns that osm2pgsql doesn't manage
DO $$
BEGIN
    -- Structured fields extracted from OSM tags
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'address') THEN
        ALTER TABLE geo_places ADD COLUMN address JSONB;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'hours') THEN
        ALTER TABLE geo_places ADD COLUMN hours TEXT;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'phones') THEN
        ALTER TABLE geo_places ADD COLUMN phones TEXT[];
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'websites') THEN
        ALTER TABLE geo_places ADD COLUMN websites TEXT[];
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'name_abbrev') THEN
        ALTER TABLE geo_places ADD COLUMN name_abbrev TEXT;
    END IF;
    -- codes was the one column only the API's ensureSchema() added, which meant
    -- a fresh instance never got it: osm2pgsql creates geo_places long after the
    -- API booted, so the index below and the codes UPDATE in run-import.sh both
    -- failed with "column codes does not exist" on every first-time import.
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'codes') THEN
        ALTER TABLE geo_places ADD COLUMN codes TEXT[];
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'embedding') THEN
        ALTER TABLE geo_places ADD COLUMN embedding vector(512);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'ts') THEN
        ALTER TABLE geo_places ADD COLUMN ts TSVECTOR;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'area_m2') THEN
        ALTER TABLE geo_places ADD COLUMN area_m2 REAL;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'updated_at') THEN
        ALTER TABLE geo_places ADD COLUMN updated_at TIMESTAMPTZ DEFAULT NOW();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'geo_places' AND column_name = 'parent_context') THEN
        ALTER TABLE geo_places ADD COLUMN parent_context TEXT;
    END IF;
END $$;

-- ── Re-derive columns from tags ─────────────────────────────────────────
--
-- These run on every replication update, where a diff touches a few thousand
-- objects out of 27 million. Filtering on "which rows carry the tag" therefore
-- rewrites most of the table to absorb almost nothing: the address statement
-- alone reported UPDATE 7948686 for a diff of 5,411 nodes, and the dead tuples,
-- WAL and index maintenance that came with it are most of why a daily update
-- took ~12 hours. The IS DISTINCT FROM guard writes only the rows that moved.
--
-- The guard repeats the SET expression rather than joining a computed subquery
-- back to the table. geo_places.id is text and carries no unique index, so a
-- self-join on it can match more than one row; joining on ctid avoids that but
-- costs a second full scan plus a 14M-row hash (planner cost 8.4M against 3.3M
-- for the repeat). Repeating a cheap expression is the better trade. area_m2 is
-- the exception below, because its expression is not cheap.

-- Extract address, hours, phones and websites in ONE pass.
--
-- These used to be four statements, which meant four full scans and — worse —
-- up to four row versions for a place carrying all four tag families, each
-- rewrite paying WAL and index maintenance again. One statement writes each
-- affected row exactly once. The IS DISTINCT FROM guards keep the replication
-- path cheap (see the note above); a row is written only if at least one of
-- the four derived values actually moved, and the SET expressions are
-- individually guarded so an UPDATE for one column cannot clobber another
-- with NULL: each column keeps its old value unless its own tags changed it.
UPDATE geo_places SET
    address = CASE WHEN (tags ? 'addr:street' OR tags ? 'addr:housenumber')
        THEN jsonb_build_object(
            'housenumber', tags->>'addr:housenumber',
            'street', tags->>'addr:street',
            'unit', tags->>'addr:unit',
            'city', tags->>'addr:city',
            'state', tags->>'addr:state',
            'postcode', tags->>'addr:postcode',
            'country', tags->>'addr:country')
        ELSE address END,
    hours = CASE WHEN tags ? 'opening_hours'
        THEN tags->>'opening_hours' ELSE hours END,
    phones = CASE WHEN (tags ? 'phone' OR tags ? 'contact:phone' OR tags ? 'contact:mobile')
        THEN ARRAY(SELECT unnest FROM unnest(ARRAY[
            tags->>'phone', tags->>'contact:phone', tags->>'contact:mobile'
        ]) WHERE unnest IS NOT NULL)
        ELSE phones END,
    websites = CASE WHEN (tags ? 'website' OR tags ? 'contact:website' OR tags ? 'url')
        THEN ARRAY(SELECT unnest FROM unnest(ARRAY[
            tags->>'website', tags->>'contact:website', tags->>'url'
        ]) WHERE unnest IS NOT NULL)
        ELSE websites END
WHERE
    ((tags ? 'addr:street' OR tags ? 'addr:housenumber')
      AND address IS DISTINCT FROM jsonb_build_object(
        'housenumber', tags->>'addr:housenumber',
        'street', tags->>'addr:street',
        'unit', tags->>'addr:unit',
        'city', tags->>'addr:city',
        'state', tags->>'addr:state',
        'postcode', tags->>'addr:postcode',
        'country', tags->>'addr:country'))
    OR (tags ? 'opening_hours'
      AND hours IS DISTINCT FROM tags->>'opening_hours')
    OR ((tags ? 'phone' OR tags ? 'contact:phone' OR tags ? 'contact:mobile')
      AND phones IS DISTINCT FROM ARRAY(SELECT unnest FROM unnest(ARRAY[
        tags->>'phone', tags->>'contact:phone', tags->>'contact:mobile'
      ]) WHERE unnest IS NOT NULL))
    OR ((tags ? 'website' OR tags ? 'contact:website' OR tags ? 'url')
      AND websites IS DISTINCT FROM ARRAY(SELECT unnest FROM unnest(ARRAY[
        tags->>'website', tags->>'contact:website', tags->>'url'
      ]) WHERE unnest IS NOT NULL));

-- Backfill area for polygons that don't have one.
--
-- On a fresh import area_m2 is a GENERATED column (see osm2pgsql-flex.lua):
-- Postgres computes it during osm2pgsql's COPY, so there is nothing to do
-- here — and updating a generated column is an error, hence the guard. This
-- backfill exists for databases imported before that change, where the DO
-- block above added area_m2 as a plain column: measured at US scale, deriving
-- it here for every area row was ~30 minutes of ST_Area math followed by
-- ELEVEN HOURS of rewriting half the table through live indexes. Only NULLs
-- are filled, so it runs once per legacy database and never again.
--
-- The ctid join (not id) is deliberate: id carries no unique index.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'geo_places' AND column_name = 'area_m2'
          AND is_generated = 'NEVER'
    ) THEN
        WITH computed AS MATERIALIZED (
            SELECT ctid AS row_id, ST_Area(geom::geography)::real AS area_m2
            FROM geo_places
            WHERE geom_type = 'area' AND area_m2 IS NULL
        )
        UPDATE geo_places p SET area_m2 = computed.area_m2
        FROM computed
        WHERE p.ctid = computed.row_id;
    END IF;
END $$;

-- NOTE: tsvector (ts column) is NOT built here — it depends on name_abbrev,
-- codes, and parent_context which are populated in later pipeline steps.
-- The tsvector is built as the final step of run-import.sh / update-osm.sh.

-- ── The one shared tsvector builder ─────────────────────────────────────
--
-- The FTS document used to be written out twice — once in rebuild-tsvectors.sql
-- and once implied by fillTsvectors() in src/lib/search-enrichment.ts — with a
-- comment begging them to stay in sync. Now the SQL side has exactly one copy,
-- and every statement that writes ts calls it. Keep THIS in sync with
-- fillTsvectors() / TS_NORMALIZATION_VERSION in src/lib/search-enrichment.ts.
--
-- Intersection names ('X' rows) get "&" expanded to multilingual "and" tokens
-- and road-suffix abbreviations injected so "hawthorne ln & 8th st" matches.
CREATE OR REPLACE FUNCTION build_ts(
    p_osm_type TEXT, p_name TEXT, p_names TEXT[],
    p_name_abbrev TEXT, p_categories TEXT[], p_parent_context TEXT
) RETURNS tsvector
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
SELECT to_tsvector('simple', unaccent(replace(
    (CASE WHEN p_osm_type = 'X'
        THEN replace(replace(replace(replace(replace(replace(replace(
             replace(replace(replace(replace(replace(replace(
               coalesce(p_name, ''), ' & ', ' and et und y e ')
             , 'Street', 'Street St'), 'Avenue', 'Avenue Ave')
             , 'Boulevard', 'Boulevard Blvd'), 'Drive', 'Drive Dr')
             , 'Lane', 'Lane Ln'), 'Road', 'Road Rd')
             , 'Court', 'Court Ct'), 'Place', 'Place Pl')
             , 'Circle', 'Circle Cir'), 'Parkway', 'Parkway Pkwy')
             , 'Highway', 'Highway Hwy'), 'Trail', 'Trail Trl')
             || ' ' || coalesce(array_to_string(p_names, ' '), '')
        ELSE replace(coalesce(p_name, ''), ' & ', ' and et und y e ')
    END) || ' ' || coalesce(p_name_abbrev, '') || ' ' ||
    coalesce(array_to_string(
        ARRAY(SELECT replace(replace(unnest(p_categories), '/', ' '), '_', ' ')),
    ' '), '') || ' ' ||
    coalesce(p_parent_context, '')
, chr(39), '')))
$fn$;

-- ── Indexes the pipeline itself needs ───────────────────────────────────
--
-- Only these four are created here. The rest — every GIN, the trigram GIST,
-- the search-layer btrees — moved to finalize-indexes.sql, run AFTER the
-- enrichment steps. The reason is write amplification: codes, abbreviations,
-- parent context and the tsvector each rewrite most named rows, every rewrite
-- is non-HOT once indexed columns change, and each one had to maintain all
-- fifteen indexes per row. Worst was the ts GIN index: created here on an
-- all-NULL column, then populated through ~30M incremental inserts by the
-- final UPDATE — the slowest possible way to build a GIN index. Deferring
-- them turns all of that into one bulk build per index over settled data.
--
-- What stays, and why:
--   id          — every enrichment UPDATE joins its computed rows back on id
--   geom GIST   — buildings_3d containment (documented below as load-bearing:
--                 without it the parts join planned 36 trillion pairs),
--                 intersections' road self-join, transit views
--   centroid GIST — parent-context probes boundaries per-POI centroid
--   admin_geom  — the boundary side of that same join
CREATE INDEX IF NOT EXISTS geo_places_id_idx ON geo_places(id);
CREATE INDEX IF NOT EXISTS geo_places_geom_idx ON geo_places USING GIST(geom);
CREATE INDEX IF NOT EXISTS geo_places_centroid_idx ON geo_places USING GIST(centroid);
CREATE INDEX IF NOT EXISTS geo_places_admin_geom_idx ON geo_places USING GIST(geom) WHERE geom_type = 'area' AND (admin_level IS NOT NULL OR categories && ARRAY['place/neighbourhood', 'place/suburb', 'place/quarter', 'place/city_block']::text[]);

-- Analyze tables for query planner
ANALYZE geo_places;
ANALYZE bicycle_ways;
ANALYZE bicycle_routes;

-- Print stats
DO $$
DECLARE
    total_count BIGINT;
    named_count BIGINT;
    point_count BIGINT;
    line_count BIGINT;
    area_count BIGINT;
    bike_ways_count BIGINT;
    bike_routes_count BIGINT;
BEGIN
    SELECT count(*) INTO total_count FROM geo_places;
    SELECT count(*) INTO named_count FROM geo_places WHERE name IS NOT NULL;
    SELECT count(*) INTO point_count FROM geo_places WHERE geom_type = 'point';
    SELECT count(*) INTO line_count FROM geo_places WHERE geom_type = 'line';
    SELECT count(*) INTO area_count FROM geo_places WHERE geom_type = 'area';
    SELECT count(*) INTO bike_ways_count FROM bicycle_ways;
    SELECT count(*) INTO bike_routes_count FROM bicycle_routes;

    RAISE NOTICE 'Import complete:';
    RAISE NOTICE '  Total objects: %', total_count;
    RAISE NOTICE '  Named objects: %', named_count;
    RAISE NOTICE '  Points: %', point_count;
    RAISE NOTICE '  Lines: %', line_count;
    RAISE NOTICE '  Areas: %', area_count;
    RAISE NOTICE '  Bicycle ways: %', bike_ways_count;
    RAISE NOTICE '  Bicycle routes: %', bike_routes_count;
END $$;
