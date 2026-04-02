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
END $$;

-- Extract address from addr:* tags
UPDATE geo_places SET address = jsonb_build_object(
    'housenumber', tags->>'addr:housenumber',
    'street', tags->>'addr:street',
    'unit', tags->>'addr:unit',
    'city', tags->>'addr:city',
    'state', tags->>'addr:state',
    'postcode', tags->>'addr:postcode',
    'country', tags->>'addr:country'
)
WHERE tags ? 'addr:street' OR tags ? 'addr:housenumber';

-- Extract opening hours
UPDATE geo_places SET hours = tags->>'opening_hours'
WHERE tags ? 'opening_hours';

-- Extract phone numbers
UPDATE geo_places SET phones = ARRAY(
    SELECT unnest FROM unnest(ARRAY[
        tags->>'phone',
        tags->>'contact:phone',
        tags->>'contact:mobile'
    ]) WHERE unnest IS NOT NULL
)
WHERE tags ? 'phone' OR tags ? 'contact:phone' OR tags ? 'contact:mobile';

-- Extract websites
UPDATE geo_places SET websites = ARRAY(
    SELECT unnest FROM unnest(ARRAY[
        tags->>'website',
        tags->>'contact:website',
        tags->>'url'
    ]) WHERE unnest IS NOT NULL
)
WHERE tags ? 'website' OR tags ? 'contact:website' OR tags ? 'url';

-- Compute area for polygons
UPDATE geo_places SET area_m2 = ST_Area(geom::geography)
WHERE geom_type = 'area';

-- Build full-text search tsvector (only for named objects)
-- Includes name, abbreviation, and category labels so users can search by type
-- e.g. "winnifred apartments" finds "The Winnifred" (building/apartments)
UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
    coalesce(name, '') || ' ' || coalesce(name_abbrev, '') || ' ' ||
    coalesce(array_to_string(
        ARRAY(SELECT replace(replace(unnest(categories), '/', ' '), '_', ' ')),
    ' '), '')
))
WHERE name IS NOT NULL;

-- Create indexes
-- Primary key index (osm2pgsql doesn't create this automatically)
CREATE INDEX IF NOT EXISTS geo_places_id_idx ON geo_places(id);

-- Universal indexes (all rows)
CREATE INDEX IF NOT EXISTS geo_places_centroid_idx ON geo_places USING GIST(centroid);
CREATE INDEX IF NOT EXISTS geo_places_geom_idx ON geo_places USING GIST(geom);
CREATE INDEX IF NOT EXISTS geo_places_tags_idx ON geo_places USING GIN(tags jsonb_path_ops);
CREATE INDEX IF NOT EXISTS geo_places_geom_type_idx ON geo_places(geom_type);

-- Partial indexes (only relevant rows)
CREATE INDEX IF NOT EXISTS geo_places_name_trgm_idx ON geo_places USING GIN(name gin_trgm_ops) WHERE name IS NOT NULL;
CREATE INDEX IF NOT EXISTS geo_places_categories_idx ON geo_places USING GIN(categories) WHERE categories != '{}';
CREATE INDEX IF NOT EXISTS geo_places_ts_idx ON geo_places USING GIN(ts) WHERE ts IS NOT NULL;
CREATE INDEX IF NOT EXISTS geo_places_admin_level_idx ON geo_places(admin_level) WHERE admin_level IS NOT NULL;

-- Analyze table for query planner
ANALYZE geo_places;

-- Print stats
DO $$
DECLARE
    total_count BIGINT;
    named_count BIGINT;
    point_count BIGINT;
    line_count BIGINT;
    area_count BIGINT;
BEGIN
    SELECT count(*) INTO total_count FROM geo_places;
    SELECT count(*) INTO named_count FROM geo_places WHERE name IS NOT NULL;
    SELECT count(*) INTO point_count FROM geo_places WHERE geom_type = 'point';
    SELECT count(*) INTO line_count FROM geo_places WHERE geom_type = 'line';
    SELECT count(*) INTO area_count FROM geo_places WHERE geom_type = 'area';

    RAISE NOTICE 'Import complete:';
    RAISE NOTICE '  Total objects: %', total_count;
    RAISE NOTICE '  Named objects: %', named_count;
    RAISE NOTICE '  Points: %', point_count;
    RAISE NOTICE '  Lines: %', line_count;
    RAISE NOTICE '  Areas: %', area_count;
END $$;
