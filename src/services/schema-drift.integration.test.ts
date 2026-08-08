/**
 * Guards the gap between the two places that describe the same tables.
 *
 * `ensureGtfsSchema()` / `ensureGbfsSchema()` in `src/db.ts` run the DDL that
 * actually creates them; `src/schema/gtfs.ts` describes them for drizzle. They
 * are independent, and nothing forces them to agree.
 *
 * Drift in one direction is a live bug rather than untidiness: drizzle names
 * every declared column in an INSERT, so a definition claiming a column the DDL
 * does not create makes every insert against that table fail — on fresh
 * installs only, which is the worst place to find out. That is exactly what
 * happened when `gtfs_shapes.geom` was added here from a long-lived database,
 * where an old ad-hoc migration had introduced it; `importShapes` then failed
 * on any instance that had never had it.
 *
 * Drift the other way (a column in the DDL that drizzle omits) is merely
 * unused, so it is reported rather than failed.
 *
 * The check has to run against a *fresh* database — asserting against a
 * long-lived one would have passed for exactly the case above, since the column
 * was there. It therefore creates a throwaway database, and does the work in a
 * subprocess: `db.ts` reads DATABASE_URL once at module scope, so pointing it
 * at the scratch database in-process would hand every other integration test in
 * the same run an empty database to query.
 *
 * Run: bun run test:integration
 */

import { describe, test, expect, beforeAll, afterAll } from 'bun:test'
import postgres from 'postgres'

const ENABLED = Boolean(process.env.BARRELMAN_INTEGRATION_TESTS && process.env.DATABASE_URL)

/** A scratch database, so nothing here can touch real data. */
const SCRATCH = 'barrelman_schema_drift_test'

let scratchUrl = ''
let admin: ReturnType<typeof postgres> | null = null

beforeAll(async () => {
  if (!ENABLED) return
  const adminUrl = new URL(process.env.DATABASE_URL!)
  adminUrl.pathname = '/postgres'
  admin = postgres(adminUrl.toString(), { max: 1, onnotice: () => {} })

  await admin.unsafe(`DROP DATABASE IF EXISTS ${SCRATCH} WITH (FORCE)`)
  await admin.unsafe(`CREATE DATABASE ${SCRATCH}`)

  const scratch = new URL(process.env.DATABASE_URL!)
  scratch.pathname = `/${SCRATCH}`
  scratchUrl = scratch.toString()

  const seed = postgres(scratchUrl, { max: 1, onnotice: () => {} })
  await seed.unsafe('CREATE EXTENSION IF NOT EXISTS postgis')
  await seed.end({ timeout: 5 })
})

afterAll(async () => {
  if (!ENABLED || !admin) return
  await admin.unsafe(`DROP DATABASE IF EXISTS ${SCRATCH} WITH (FORCE)`)
  await admin.end({ timeout: 5 })
})

/**
 * Applies the startup DDL to the scratch database and reports, per table, the
 * columns drizzle declares and the columns that actually exist.
 */
const PROBE = `
  const { db, ensureGtfsSchema, ensureGbfsSchema } = await import('./src/db.ts')
  const { sql, getTableName, getTableColumns } = await import('drizzle-orm')
  const schema = await import('./src/schema/gtfs.ts')

  await ensureGtfsSchema()
  await ensureGbfsSchema()

  const out = []
  for (const t of Object.values(schema)) {
    if (!t || typeof t !== 'object' || !t[Symbol.for('drizzle:Name')]) continue
    const table = getTableName(t)
    const rows = await db.execute(sql\`
      SELECT column_name FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = \${table}\`)
    out.push({
      table,
      declared: Object.values(getTableColumns(t)).map((c) => c.name),
      actual: rows.map((r) => r.column_name),
    })
  }
  console.log('__DRIFT__' + JSON.stringify(out))
  process.exit(0)
`

describe.if(ENABLED)('drizzle definitions match the DDL a fresh install runs', () => {
  test('every declared column exists after ensure*Schema()', async () => {
    const proc = Bun.spawn(['bun', '-e', PROBE], {
      cwd: new URL('../..', import.meta.url).pathname,
      env: { ...process.env, DATABASE_URL: scratchUrl },
      stdout: 'pipe',
      stderr: 'pipe',
    })
    const stdout = await new Response(proc.stdout).text()
    const stderr = await new Response(proc.stderr).text()
    expect(await proc.exited, `probe failed:\n${stderr}`).toBe(0)

    const line = stdout.split('\n').find((l) => l.startsWith('__DRIFT__'))
    expect(line, `no drift report in output:\n${stdout}\n${stderr}`).toBeTruthy()

    const tables = JSON.parse(line!.slice('__DRIFT__'.length)) as Array<{
      table: string
      declared: string[]
      actual: string[]
    }>
    expect(tables.length).toBeGreaterThan(0)

    const undeclared: string[] = []
    for (const { table, declared, actual } of tables) {
      // Relations built by the import scripts rather than by ensure*Schema()
      // (the station-link matviews, stop_area_members) are absent here by
      // design — station.service probes for them at runtime for that reason.
      if (actual.length === 0) continue

      const missing = declared.filter((c) => !actual.includes(c))
      expect({ table, missing }).toEqual({ table, missing: [] })

      for (const column of actual) {
        if (!declared.includes(column)) undeclared.push(`${table}.${column}`)
      }
    }

    // Not a failure — just visible, so a column added to the DDL and forgotten
    // here does not stay invisible.
    if (undeclared.length) console.warn(`[schema] in the DDL but not declared: ${undeclared.join(', ')}`)
  }, 60_000)
})
