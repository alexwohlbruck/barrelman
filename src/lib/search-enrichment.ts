import postgres from 'postgres'
import { dbUrl } from '../db'

/**
 * Self-healing search enrichment.
 *
 * The search code depends on four derived columns on geo_places that osm2pgsql
 * does NOT produce — they are filled by post-import steps (scripts/import-osm.sh
 * steps 4,5,7,8):
 *
 *   - codes          IATA/ICAO/ref/short_name/… → exact-code lookups ("jfk", "lga")
 *   - name_abbrev    initials of multi-word names → acronym lookups ("nyu")
 *   - parent_context admin/neighbourhood names   → "<poi> <neighbourhood>" matches
 *   - ts             full-text tsvector          → ALL word/prefix search
 *
 * If a database was imported before those steps existed (or with a partial /
 * raw osm2pgsql import), these columns are NULL and search silently degrades:
 * short queries and code/acronym lookups return nothing because every word-based
 * layer is empty. ensureSchema() only *creates* the columns — nothing fills them.
 *
 * This routine fills any missing values idempotently on startup so the database
 * self-heals without a manual re-import. It is:
 *   - gated by a marker row (skips instantly once done, unless the row count
 *     changed materially — e.g. a fresh import — in which case it re-runs)
 *   - guarded by a session advisory lock so only one instance runs the backfill
 *   - incremental (each step only touches rows still missing the column)
 *   - run in the background (never blocks server startup)
 *
 * All work runs on a dedicated single connection (max: 1) — the session advisory
 * lock requires connection affinity, and it keeps the heavy UPDATEs off the main
 * query pool. Keep the SQL here in sync with scripts/import-osm.sh.
 */

// Arbitrary, stable key for pg_try_advisory_lock — must not collide with other locks.
const ENRICHMENT_LOCK_KEY = 0x5ea2c4
// Re-run enrichment if the live row count drifts from the marker by more than this
// fraction (catches a fresh/raw re-import that left columns unfilled).
const REIMPORT_DRIFT = 0.1

type Sql = ReturnType<typeof postgres>

async function rowEstimate(sql: Sql): Promise<number> {
  const rows = await sql<{ n: number }[]>`
    SELECT reltuples::bigint AS n FROM pg_class WHERE relname = 'geo_places'
  `
  return Number(rows[0]?.n ?? 0)
}

/** Cheap gate: has enrichment already completed for the current dataset? */
async function alreadyEnriched(sql: Sql, estimate: number): Promise<boolean> {
  await sql`
    CREATE TABLE IF NOT EXISTS search_enrichment_state (
      id integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
      completed_at timestamptz,
      row_estimate bigint
    )
  `
  const rows = await sql<{ completed_at: string | null; row_estimate: number | null }[]>`
    SELECT completed_at, row_estimate FROM search_enrichment_state WHERE id = 1
  `
  const marker = rows[0]
  if (!marker?.completed_at || !marker.row_estimate) return false
  const drift = Math.abs(estimate - Number(marker.row_estimate)) / Number(marker.row_estimate)
  return drift <= REIMPORT_DRIFT
}

async function markEnriched(sql: Sql, estimate: number): Promise<void> {
  await sql`
    INSERT INTO search_enrichment_state (id, completed_at, row_estimate)
    VALUES (1, NOW(), ${estimate})
    ON CONFLICT (id) DO UPDATE SET completed_at = NOW(), row_estimate = ${estimate}
  `
}

// ── Enrichment steps (incremental — only fill rows still missing the value) ──

async function fillCodes(sql: Sql): Promise<void> {
  await sql`
    UPDATE geo_places
    SET codes = sub.codes
    FROM (
      SELECT id,
        array_agg(DISTINCT lower(trim(code))) FILTER (WHERE trim(code) <> '') AS codes
      FROM geo_places,
      LATERAL unnest(
        string_to_array(coalesce(tags->>'iata', ''), ';') ||
        string_to_array(coalesce(tags->>'icao', ''), ';') ||
        string_to_array(coalesce(tags->>'ref', ''), ';') ||
        string_to_array(coalesce(tags->>'short_name', ''), ';') ||
        string_to_array(coalesce(tags->>'abbreviation', ''), ';') ||
        string_to_array(coalesce(tags->>'alt_name', ''), ';')
      ) AS code
      WHERE tags IS NOT NULL
        AND (tags->>'iata' IS NOT NULL OR tags->>'icao' IS NOT NULL OR tags->>'ref' IS NOT NULL
             OR tags->>'short_name' IS NOT NULL OR tags->>'abbreviation' IS NOT NULL OR tags->>'alt_name' IS NOT NULL)
      GROUP BY id
    ) sub
    WHERE geo_places.id = sub.id
      AND geo_places.codes IS NULL
  `
}

async function fillNameAbbrev(sql: Sql): Promise<void> {
  await sql`
    UPDATE geo_places
    SET name_abbrev = sub.abbrev
    FROM (
      SELECT id, lower(string_agg(left(word, 1), '' ORDER BY ord)) AS abbrev
      FROM (
        SELECT id, word, ord
        FROM geo_places,
        LATERAL unnest(regexp_split_to_array(name, '\\s+')) WITH ORDINALITY AS t(word, ord)
        WHERE name IS NOT NULL AND name ~ '^[\\w\\s\\d\\-''\\.&]+$'
      ) words
      WHERE lower(word) NOT IN (
        'of','the','and','at','in','for','a','an',
        'de','la','le','les','du','des','et','au',
        'der','die','das','von','und','im','am',
        'del','los','las','el','dos','e',
        'di','della','dei','degli'
      ) AND length(word) > 0
      GROUP BY id
      HAVING count(*) >= 2
    ) sub
    WHERE geo_places.id = sub.id
      AND geo_places.name_abbrev IS NULL
  `
}

async function fillParentContext(sql: Sql): Promise<void> {
  // Pass 1: spatial join against containing admin / neighbourhood boundaries.
  await sql`
    UPDATE geo_places p
    SET parent_context = trim(
      coalesce(p.address->>'street', '') || ' ' ||
      coalesce(p.address->>'city', '') || ' ' ||
      coalesce(p.address->>'state', '') || ' ' ||
      coalesce(p.address->>'postcode', '') || ' ' ||
      coalesce(sub.boundary_names, '')
    )
    FROM (
      SELECT poi.id,
        string_agg(boundary.name, ' ' ORDER BY boundary.area_m2 ASC) AS boundary_names
      FROM geo_places poi
      JOIN geo_places boundary
        ON boundary.geom_type = 'area'
        AND boundary.name IS NOT NULL
        AND (boundary.admin_level IS NOT NULL
             OR boundary.categories && ARRAY['place/neighbourhood', 'place/suburb', 'place/quarter', 'place/city_block']::text[])
        AND ST_Contains(boundary.geom, poi.centroid)
      WHERE poi.name IS NOT NULL AND poi.parent_context IS NULL
      GROUP BY poi.id
    ) sub
    WHERE p.id = sub.id AND p.parent_context IS NULL
  `
  // Pass 2: POIs with address tags but outside any boundary.
  await sql`
    UPDATE geo_places
    SET parent_context = trim(
      coalesce(address->>'street', '') || ' ' ||
      coalesce(address->>'city', '') || ' ' ||
      coalesce(address->>'state', '') || ' ' ||
      coalesce(address->>'postcode', '')
    )
    WHERE name IS NOT NULL AND parent_context IS NULL AND address IS NOT NULL
  `
}

async function fillTsvectors(sql: Sql): Promise<void> {
  // Build the full-text tsvector from name (+ intersection suffix expansion),
  // name_abbrev, categories and parent_context. Only fills rows missing it.
  await sql`
    UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
        CASE WHEN osm_type = 'X'
            THEN replace(replace(replace(replace(replace(replace(replace(
                 replace(replace(replace(replace(replace(replace(
                   coalesce(name, ''), ' & ', ' and et und y e ')
                 , 'Street', 'Street St'), 'Avenue', 'Avenue Ave')
                 , 'Boulevard', 'Boulevard Blvd'), 'Drive', 'Drive Dr')
                 , 'Lane', 'Lane Ln'), 'Road', 'Road Rd')
                 , 'Court', 'Court Ct'), 'Place', 'Place Pl')
                 , 'Circle', 'Circle Cir'), 'Parkway', 'Parkway Pkwy')
                 , 'Highway', 'Highway Hwy'), 'Trail', 'Trail Trl')
                 || ' ' || coalesce(array_to_string(names, ' '), '')
            ELSE coalesce(name, '')
        END || ' ' || coalesce(name_abbrev, '') || ' ' ||
        coalesce(array_to_string(
            ARRAY(SELECT replace(replace(unnest(categories), '/', ' '), '_', ' ')),
        ' '), '') || ' ' ||
        coalesce(parent_context, '')
    ))
    WHERE name IS NOT NULL AND ts IS NULL
  `
}

/**
 * Fill any missing search-enrichment columns. Safe to call on every startup —
 * skips instantly when already done. Intended to be fired without awaiting so it
 * never delays server boot.
 */
export async function ensureSearchEnrichment(): Promise<void> {
  // Dedicated single connection: the advisory lock is session-scoped (needs
  // connection affinity) and this keeps the heavy UPDATEs off the main pool.
  const sql = postgres(dbUrl, { max: 1 })
  try {
    const estimate = await rowEstimate(sql)
    if (estimate === 0) return // table empty / not imported yet
    if (await alreadyEnriched(sql, estimate)) return

    const [{ locked }] = await sql<{ locked: boolean }[]>`
      SELECT pg_try_advisory_lock(${ENRICHMENT_LOCK_KEY}) AS locked
    `
    if (!locked) return

    try {
      // Re-check under the lock in case another instance just finished.
      if (await alreadyEnriched(sql, estimate)) return

      console.log('[search-enrichment] Backfilling derived search columns (one-time)…')
      const t0 = Date.now()

      console.log('[search-enrichment] codes…')
      await fillCodes(sql)
      console.log('[search-enrichment] name_abbrev…')
      await fillNameAbbrev(sql)
      console.log('[search-enrichment] parent_context…')
      await fillParentContext(sql)
      console.log('[search-enrichment] tsvectors…')
      await fillTsvectors(sql)

      await sql`ANALYZE geo_places`
      await markEnriched(sql, estimate)
      console.log(`[search-enrichment] Done in ${Math.round((Date.now() - t0) / 1000)}s.`)
    } finally {
      await sql`SELECT pg_advisory_unlock(${ENRICHMENT_LOCK_KEY})`
    }
  } catch (err) {
    // Never let enrichment crash the server — log and move on.
    console.error('[search-enrichment] Failed:', err)
  } finally {
    await sql.end({ timeout: 5 })
  }
}
