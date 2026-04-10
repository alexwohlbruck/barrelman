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
