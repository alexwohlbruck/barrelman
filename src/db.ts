import postgres from 'postgres'
import { drizzle } from 'drizzle-orm/postgres-js'
import { sql } from 'drizzle-orm'

export const dbUrl = process.env.DATABASE_URL || 'postgresql://barrelman:barrelman@localhost:5434/barrelman'

export const connection = postgres(dbUrl)
export const db = drizzle(connection)

/**
 * Ensure post-import columns exist on the geo_places table.
 *
 * osm2pgsql creates the base columns (id, osm_type, osm_id, name, names,
 * tags, categories, centroid, geom, geom_type, admin_level).  The remaining
 * columns are normally added by `import/post-import.sql`, but if that script
 * hasn't been run yet (e.g. fresh import, dev setup), SELECT queries that
 * reference the missing columns will fail.
 *
 * This function adds the columns idempotently (ADD COLUMN IF NOT EXISTS) so
 * the API can start cleanly even before the full post-import pipeline runs.
 */
export async function ensureSchema() {
  await db.execute(sql.raw(`
    ALTER TABLE geo_places
      ADD COLUMN IF NOT EXISTS name_abbrev TEXT,
      ADD COLUMN IF NOT EXISTS codes TEXT[],
      ADD COLUMN IF NOT EXISTS address JSONB,
      ADD COLUMN IF NOT EXISTS hours TEXT,
      ADD COLUMN IF NOT EXISTS phones TEXT[],
      ADD COLUMN IF NOT EXISTS websites TEXT[],
      ADD COLUMN IF NOT EXISTS area_m2 REAL,
      ADD COLUMN IF NOT EXISTS parent_context TEXT,
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

    -- Indexes for codes and name_abbrev search layers.
    -- Without these, the codes (@> array) and name_abbrev (= text) queries
    -- fall back to sequential scans on the full table (~7M rows).
    CREATE INDEX IF NOT EXISTS geo_places_codes_idx
      ON geo_places USING GIN (codes) WHERE codes IS NOT NULL;
    CREATE INDEX IF NOT EXISTS geo_places_name_abbrev_idx
      ON geo_places (name_abbrev) WHERE name_abbrev IS NOT NULL;

    -- GiST trigram index for the Layer-2 fuzzy search (search.service.ts).
    -- That layer uses the KNN distance operator (name <-> query) with
    -- ORDER BY ... <-> ... LIMIT, which ONLY a GiST trigram index can serve.
    -- The GIN trigram index (gin_trgm_ops) supports % / ILIKE but NOT <->,
    -- so without this GiST index every fuzzy query degrades to a parallel
    -- sequential scan over the full table (~45s on 21M rows) — which silently
    -- blows past the API search timeout and returns no place results.
    --
    -- siglen=128: the default 12-byte signatures are too lossy at ~2M named
    -- rows — the KNN scan visits far too many pages and recomputes distances
    -- (measured 335ms-1.3s per query). 128-byte signatures cut that to
    -- tens of ms at the cost of a larger index. (Replaces the old
    -- default-siglen geo_places_name_gist_trgm_idx.)
    CREATE INDEX IF NOT EXISTS geo_places_name_gist_trgm_sig128_idx
      ON geo_places USING gist (name gist_trgm_ops(siglen=128)) WHERE name IS NOT NULL;
  `))

  await ensureBrandCatalogSchema()
}

/**
 * Create the brand catalog materialized view (geo_brands) and its indexes.
 *
 * One row per distinct brand — keyed by the brand:wikidata QID when present,
 * otherwise the normalized `brand` name ("name:<lower>"). It powers brand
 * autocomplete ("McDonald's" → a brand suggestion) and the "see all locations
 * of this brand" browse.
 *
 * Created empty here (WITH NO DATA) so startup stays cheap; population and
 * subsequent refreshes happen in the background via ensureSearchEnrichment().
 * The unique index on brand_key is required for REFRESH ... CONCURRENTLY.
 */
export async function ensureBrandCatalogSchema() {
  await db.execute(sql.raw(`
    CREATE MATERIALIZED VIEW IF NOT EXISTS geo_brands AS
    WITH branded AS (
      SELECT
        COALESCE(
          NULLIF(tags->>'brand:wikidata', ''),
          'name:' || lower(regexp_replace(trim(tags->>'brand'), '\\s+', ' ', 'g'))
        )                                        AS brand_key,
        NULLIF(tags->>'brand:wikidata', '')      AS wikidata,
        tags->>'brand'                           AS brand_name,
        categories[1]                            AS category,
        centroid
      FROM geo_places
      WHERE tags ? 'brand' AND name IS NOT NULL
    )
    SELECT
      brand_key,
      mode() WITHIN GROUP (ORDER BY brand_name)  AS name,
      max(wikidata)                              AS wikidata,
      count(*)::int                              AS location_count,
      mode() WITHIN GROUP (ORDER BY category)    AS category,
      ST_Y(ST_Centroid(ST_Collect(centroid)))    AS rep_lat,
      ST_X(ST_Centroid(ST_Collect(centroid)))    AS rep_lng
    FROM branded
    GROUP BY brand_key
    HAVING count(*) >= 2
    WITH NO DATA;

    -- Unique key REQUIRED for REFRESH MATERIALIZED VIEW CONCURRENTLY.
    CREATE UNIQUE INDEX IF NOT EXISTS geo_brands_key_idx
      ON geo_brands (brand_key);
    -- Trigram index over the display name for prefix + fuzzy autocomplete.
    CREATE INDEX IF NOT EXISTS geo_brands_name_trgm_idx
      ON geo_brands USING gin (name gin_trgm_ops);
    -- Popular-first ordering / tie-break.
    CREATE INDEX IF NOT EXISTS geo_brands_count_idx
      ON geo_brands (location_count DESC);

    -- Brand logos + descriptions resolved from Wikidata (P154 + descriptions),
    -- keyed by the brand:wikidata QID. A plain TABLE (not part of the matview)
    -- so it persists across REFRESH MATERIALIZED VIEW; populated in the
    -- background by lib/brand-logos.ts and LEFT JOINed by the /brands queries.
    CREATE TABLE IF NOT EXISTS brand_logos (
      wikidata TEXT PRIMARY KEY,
      logo_url TEXT,
      description TEXT,
      fetched_at TIMESTAMPTZ DEFAULT NOW()
    );
  `))
}

/**
 * Ensure GTFS transit tables exist.
 *
 * Creates the tables for GTFS stop/route data used by the transit routing
 * endpoints. Idempotent — safe to call on every startup.
 */
export async function ensureGtfsSchema() {
  await db.execute(sql.raw(`
    CREATE TABLE IF NOT EXISTS gtfs_feeds (
      id SERIAL PRIMARY KEY,
      feed_id TEXT NOT NULL UNIQUE,
      onestop_id TEXT,
      name TEXT,
      url TEXT,
      region TEXT,
      stop_count INTEGER DEFAULT 0,
      route_count INTEGER DEFAULT 0,
      rt_urls JSONB,
      imported_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Add rt_urls column if it doesn't exist (for existing installs)
    ALTER TABLE gtfs_feeds ADD COLUMN IF NOT EXISTS rt_urls JSONB;

    CREATE TABLE IF NOT EXISTS gtfs_stops (
      id SERIAL PRIMARY KEY,
      stop_id TEXT NOT NULL,
      feed_id TEXT NOT NULL,
      stop_name TEXT,
      stop_code TEXT,
      stop_lat DOUBLE PRECISION NOT NULL,
      stop_lon DOUBLE PRECISION NOT NULL,
      location_type INTEGER DEFAULT 0,
      parent_station TEXT,
      wheelchair_boarding INTEGER DEFAULT 0,
      platform_code TEXT,
      geom GEOMETRY(Point, 4326)
    );

    CREATE UNIQUE INDEX IF NOT EXISTS gtfs_stops_feed_stop_idx
      ON gtfs_stops (feed_id, stop_id);
    CREATE INDEX IF NOT EXISTS gtfs_stops_geom_idx
      ON gtfs_stops USING GIST (geom);
    -- The walking-transfer join uses ST_DWithin(geom::geography, …); without a
    -- GiST index on the geography cast the planner falls back to a full
    -- cartesian nested loop (77k² pairs → hangs for tens of minutes). This
    -- functional index makes that join an index nested loop (~seconds).
    CREATE INDEX IF NOT EXISTS gtfs_stops_geog_idx
      ON gtfs_stops USING GIST ((geom::geography))
      WHERE (location_type = 0 OR location_type IS NULL);
    CREATE INDEX IF NOT EXISTS gtfs_stops_feed_id_idx
      ON gtfs_stops (feed_id);
    CREATE INDEX IF NOT EXISTS gtfs_stops_parent_idx
      ON gtfs_stops (parent_station);
    CREATE INDEX IF NOT EXISTS gtfs_stops_name_idx
      ON gtfs_stops (stop_name);

    CREATE TABLE IF NOT EXISTS gtfs_routes (
      id SERIAL PRIMARY KEY,
      route_id TEXT NOT NULL,
      feed_id TEXT NOT NULL,
      agency_id TEXT,
      agency_name TEXT,
      route_short_name TEXT,
      route_long_name TEXT,
      route_type INTEGER NOT NULL,
      route_color TEXT,
      route_text_color TEXT,
      route_url TEXT
    );

    CREATE UNIQUE INDEX IF NOT EXISTS gtfs_routes_feed_route_idx
      ON gtfs_routes (feed_id, route_id);
    CREATE INDEX IF NOT EXISTS gtfs_routes_feed_id_idx
      ON gtfs_routes (feed_id);

    CREATE TABLE IF NOT EXISTS gtfs_stop_routes (
      id SERIAL PRIMARY KEY,
      feed_id TEXT NOT NULL,
      stop_id TEXT NOT NULL,
      route_id TEXT NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS gtfs_stop_routes_uniq_idx
      ON gtfs_stop_routes (feed_id, stop_id, route_id);
    CREATE INDEX IF NOT EXISTS gtfs_stop_routes_stop_idx
      ON gtfs_stop_routes (feed_id, stop_id);
    CREATE INDEX IF NOT EXISTS gtfs_stop_routes_route_idx
      ON gtfs_stop_routes (feed_id, route_id);

    -- Distinct trip patterns: the ordered station sequence each route runs,
    -- one row per (route, direction, sequence). Stop ids are normalised to the
    -- parent station (COALESCE(parent_station, stop_id)) so platform-vs-station
    -- granularity doesn't matter, and stored as a comma-delimited, comma-bounded
    -- string so a leg's board→alight run is a plain substring (strpos) match —
    -- which is inherently direction-correct (the opposite direction is the
    -- reversed sequence). Powers "every line that runs this segment directly",
    -- independent of which ones MOTIS happened to surface.
    CREATE TABLE IF NOT EXISTS gtfs_trip_patterns (
      id SERIAL PRIMARY KEY,
      feed_id TEXT NOT NULL,
      route_id TEXT NOT NULL,
      direction_id INTEGER,
      stop_seq TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS gtfs_trip_patterns_feed_idx
      ON gtfs_trip_patterns (feed_id);

    -- Agency-declared transfers (transfers.txt): the authoritative
    -- definition of which stations form one complex (e.g. Times Sq
    -- 1/2/3 <-> N/Q/R/W) and the minimum connection times. Used to
    -- aggregate the lines serving a station across its whole complex.
    CREATE TABLE IF NOT EXISTS gtfs_transfers (
      id SERIAL PRIMARY KEY,
      feed_id TEXT NOT NULL,
      from_stop_id TEXT NOT NULL,
      to_stop_id TEXT NOT NULL,
      transfer_type INTEGER DEFAULT 0,
      min_transfer_time INTEGER
    );

    CREATE UNIQUE INDEX IF NOT EXISTS gtfs_transfers_uniq_idx
      ON gtfs_transfers (feed_id, from_stop_id, to_stop_id);
    CREATE INDEX IF NOT EXISTS gtfs_transfers_from_idx
      ON gtfs_transfers (feed_id, from_stop_id);
    CREATE INDEX IF NOT EXISTS gtfs_transfers_to_idx
      ON gtfs_transfers (feed_id, to_stop_id);

    -- Route shapes: stores GTFS shapes as ordered coordinate arrays.
    -- shape_id from shapes.txt; coordinates stored as JSONB [[lng,lat], ...].
    CREATE TABLE IF NOT EXISTS gtfs_shapes (
      id SERIAL PRIMARY KEY,
      feed_id TEXT NOT NULL,
      shape_id TEXT NOT NULL,
      coordinates JSONB NOT NULL
    );

    CREATE UNIQUE INDEX IF NOT EXISTS gtfs_shapes_feed_shape_idx
      ON gtfs_shapes (feed_id, shape_id);
    CREATE INDEX IF NOT EXISTS gtfs_shapes_feed_id_idx
      ON gtfs_shapes (feed_id);

    -- Add shape_id column to routes (most common shape for each route,
    -- derived from trips.txt during import).
    ALTER TABLE gtfs_routes ADD COLUMN IF NOT EXISTS shape_id TEXT;

    -- bikes_allowed: 0=unknown, 1=at least one bike-allowed trip,
    -- 2=all trips allow bikes. Derived from trips.txt bikes_allowed field.
    ALTER TABLE gtfs_routes ADD COLUMN IF NOT EXISTS bikes_allowed INTEGER DEFAULT 0;
  `))
}

/**
 * Create GBFS shared-mobility tables for bikeshare/scootershare.
 */
export async function ensureGbfsSchema() {
  await db.execute(sql.raw(`
    -- ── GBFS system catalog ───────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS gbfs_systems (
      id SERIAL PRIMARY KEY,
      system_id TEXT NOT NULL UNIQUE,
      name TEXT,
      operator TEXT,
      url TEXT NOT NULL,
      country_code TEXT,
      lat DOUBLE PRECISION,
      lon DOUBLE PRECISION,
      vehicle_types JSONB DEFAULT '[]'::jsonb,
      has_stations BOOLEAN DEFAULT TRUE,
      has_free_floating BOOLEAN DEFAULT FALSE,
      feed_urls JSONB DEFAULT '{}'::jsonb,
      ttl INTEGER DEFAULT 300,
      last_polled_at TIMESTAMPTZ,
      imported_at TIMESTAMPTZ DEFAULT NOW(),
      enabled BOOLEAN DEFAULT TRUE
    );

    CREATE INDEX IF NOT EXISTS gbfs_systems_country_idx
      ON gbfs_systems (country_code);

    -- ── GBFS stations ─────────────────────────────────────────────────
    CREATE TABLE IF NOT EXISTS gbfs_stations (
      id SERIAL PRIMARY KEY,
      system_id TEXT NOT NULL,
      station_id TEXT NOT NULL,
      name TEXT,
      lat DOUBLE PRECISION NOT NULL,
      lon DOUBLE PRECISION NOT NULL,
      capacity INTEGER,
      num_bikes_available INTEGER DEFAULT 0,
      num_ebikes_available INTEGER DEFAULT 0,
      num_scooters_available INTEGER DEFAULT 0,
      num_docks_available INTEGER DEFAULT 0,
      is_renting BOOLEAN DEFAULT TRUE,
      is_returning BOOLEAN DEFAULT TRUE,
      last_reported TIMESTAMPTZ,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    CREATE UNIQUE INDEX IF NOT EXISTS gbfs_stations_system_station_idx
      ON gbfs_stations (system_id, station_id);
    CREATE INDEX IF NOT EXISTS gbfs_stations_system_idx
      ON gbfs_stations (system_id);
    CREATE INDEX IF NOT EXISTS gbfs_stations_lat_lon_idx
      ON gbfs_stations (lat, lon);
  `))
}
