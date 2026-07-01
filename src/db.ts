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
    CREATE INDEX IF NOT EXISTS geo_places_name_gist_trgm_idx
      ON geo_places USING gist (name gist_trgm_ops) WHERE name IS NOT NULL;
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

    -- Materialized LineString geometry derived from the coordinates JSONB
    -- (populated in importShapes and the backfill-shape-geom script). Lets the
    -- transit display layer run PostGIS spatial ops + ST_AsMVT tile generation
    -- without parsing JSONB per query. Degenerate shapes (<2 points) stay NULL.
    ALTER TABLE gtfs_shapes ADD COLUMN IF NOT EXISTS geom geometry(LineString, 4326);
    CREATE INDEX IF NOT EXISTS gtfs_shapes_geom_idx
      ON gtfs_shapes USING GIST (geom);

    -- Add shape_id column to routes (most common shape for each route,
    -- derived from trips.txt during import).
    ALTER TABLE gtfs_routes ADD COLUMN IF NOT EXISTS shape_id TEXT;

    -- bikes_allowed: 0=unknown, 1=at least one bike-allowed trip,
    -- 2=all trips allow bikes. Derived from trips.txt bikes_allowed field.
    ALTER TABLE gtfs_routes ADD COLUMN IF NOT EXISTS bikes_allowed INTEGER DEFAULT 0;

    -- osm_name: the name of the nearest matching OSM transit stop, used to
    -- override ugly/abbreviated GTFS stop_name in the display layer. Populated
    -- by import/backfill-osm-stop-names.sql (spatial join over geo_places).
    ALTER TABLE gtfs_stops ADD COLUMN IF NOT EXISTS osm_name TEXT;
  `))
}

/**
 * Ensure the transit line-graph schema (LOOM output) exists.
 *
 * LOOM (gtfs2graph | topo | loom) reduces a set of routes to a topological
 * line graph: nodes (merged stations) and edges (shared centreline segments),
 * where each edge carries an ORDERED list of the lines running on it. That
 * order is LOOM's crossing-minimised line ordering — the offset slot order.
 * The transit_lines_offset materialized view (import/create-transit-lines-
 * offset.sql) turns each (edge x line, slot) into a parallel offset line via
 * ST_OffsetCurve, which the display layer serves as bundled, non-overlapping
 * ribbons. `build_key` scopes a graph (e.g. 'nyc:subway') so it can be rebuilt
 * independently.
 */
export async function ensureTransitGraphSchema() {
  await db.execute(sql.raw(`
    CREATE TABLE IF NOT EXISTS transit_graph_nodes (
      id SERIAL PRIMARY KEY,
      build_key TEXT NOT NULL,
      loom_id TEXT NOT NULL,
      station_id TEXT,
      station_label TEXT,
      geom geometry(Point, 4326),
      UNIQUE (build_key, loom_id)
    );
    CREATE INDEX IF NOT EXISTS transit_graph_nodes_geom_idx
      ON transit_graph_nodes USING GIST (geom);

    CREATE TABLE IF NOT EXISTS transit_graph_edges (
      id SERIAL PRIMARY KEY,
      build_key TEXT NOT NULL,
      loom_id TEXT NOT NULL,
      line_count INTEGER NOT NULL,
      geom geometry(LineString, 4326),
      UNIQUE (build_key, loom_id)
    );
    CREATE INDEX IF NOT EXISTS transit_graph_edges_geom_idx
      ON transit_graph_edges USING GIST (geom);

    -- One row per (edge x line). slot = position in LOOM's ordered line list.
    CREATE TABLE IF NOT EXISTS transit_graph_edge_lines (
      edge_id INTEGER NOT NULL REFERENCES transit_graph_edges(id) ON DELETE CASCADE,
      slot INTEGER NOT NULL,
      feed_id TEXT,
      route_id TEXT,
      route_short_name TEXT,
      route_type INTEGER,
      route_color TEXT,
      route_text_color TEXT,
      PRIMARY KEY (edge_id, slot)
    );

    -- Ledger of which (build_key) graphs are populated, so the plain
    -- transit_routes view can exclude feeds/modes that have a bundled graph.
    CREATE TABLE IF NOT EXISTS transit_graph_builds (
      build_key TEXT PRIMARY KEY,
      feed_id TEXT,
      mode TEXT,
      route_type INTEGER,
      built_at TIMESTAMPTZ DEFAULT NOW()
    );
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
