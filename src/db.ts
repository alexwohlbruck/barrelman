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
      imported_at TIMESTAMPTZ DEFAULT NOW()
    );

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
  `))
}
