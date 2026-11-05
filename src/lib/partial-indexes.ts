import { maintenanceConnection } from '../db'

/**
 * Build partial indexes on geo_places in the background, in order.
 *
 * CONCURRENTLY, because each build still scans the whole table (minutes on a
 * national import) and a plain build would hold off replication writes for
 * that long. That rules out the startup DDL batch — CONCURRENTLY cannot run in
 * a transaction — and leaves a failed build behind as an INVALID index that
 * IF NOT EXISTS would then skip forever, so an invalid one is dropped and
 * rebuilt. Throws on failure; the caller decides what that costs.
 */
export async function buildPartialIndexes(indexes: [name: string, definition: string][]): Promise<void> {
  const client = maintenanceConnection()
  try {
    for (const [name, definition] of indexes) {
      const [existing] = await client<{ valid: boolean }[]>`
        SELECT i.indisvalid AS valid FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = ${name}
      `
      if (existing?.valid) continue
      if (existing) await client.unsafe(`DROP INDEX CONCURRENTLY IF EXISTS ${name}`)
      console.log(`[schema] building ${name}`)
      await client.unsafe(`CREATE INDEX CONCURRENTLY IF NOT EXISTS ${name} ON geo_places ${definition}`)
    }
  } finally {
    await client.end({ timeout: 5 })
  }
}
