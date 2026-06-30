#!/usr/bin/env bun
/**
 * Backfill the gtfs_shapes.geom LineString column from the `coordinates` JSONB
 * for already-imported feeds, without a full re-import. Idempotent — only fills
 * rows where geom IS NULL. Degenerate shapes (<2 points) are left NULL.
 *
 * Usage:
 *   bun run import/backfill-shape-geom.ts
 */
import { sql } from 'drizzle-orm'
import { db, ensureGtfsSchema } from '../src/db'
import { populateShapeGeom } from '../src/services/gtfs.service'

async function main() {
  console.log('Ensuring GTFS schema (adds gtfs_shapes.geom if missing)...')
  await ensureGtfsSchema()

  const before = await db.execute(
    sql.raw(
      `SELECT count(*)::int AS missing FROM gtfs_shapes
       WHERE geom IS NULL AND jsonb_array_length(coordinates) >= 2`,
    ),
  )
  const missing = (before as any)[0]?.missing ?? 0
  console.log(`Shapes needing geom: ${missing}`)

  console.log('Populating geom from coordinates...')
  await populateShapeGeom()

  const after = await db.execute(
    sql.raw(
      `SELECT
         count(*) FILTER (WHERE geom IS NOT NULL)::int AS with_geom,
         count(*) FILTER (WHERE geom IS NULL)::int     AS without_geom
       FROM gtfs_shapes`,
    ),
  )
  const row = (after as any)[0]
  console.log(`Done. geom set: ${row?.with_geom}, still null (degenerate): ${row?.without_geom}`)

  process.exit(0)
}

main().catch((err) => {
  console.error('Fatal error:', err)
  process.exit(1)
})
