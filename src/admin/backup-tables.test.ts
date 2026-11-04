import { describe, expect, it } from 'bun:test'
import { readdirSync, readFileSync, statSync } from 'node:fs'
import { join, resolve } from 'node:path'
import { getTableConfig, type PgTable } from 'drizzle-orm/pg-core'
import { BACKUP_TABLES } from './backup-tables'
import * as accountsSchema from '../schema/accounts'

const ROOT = resolve(import.meta.dir, '../..')

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name)
    if (statSync(path).isDirectory()) return sourceFiles(path)
    return /\.(ts|sh)$/.test(name) && !/\.test\.ts$/.test(name) ? [path] : []
  })
}

/** Every table name a `CREATE TABLE IF NOT EXISTS` in src/ or scripts/ creates. */
const createdTables = new Set(
  [...sourceFiles(join(ROOT, 'src')), ...sourceFiles(join(ROOT, 'scripts'))].flatMap((file) =>
    [...readFileSync(file, 'utf8').matchAll(/CREATE TABLE IF NOT EXISTS\s+([a-z_][a-z0-9_]*)/g)].map((m) => m[1]),
  ),
)

/**
 * A table missing from the backup is only discovered the day it is needed. The
 * prefixes below are the ones that hold operator and customer state, so a new
 * table under any of them has to be added to BACKUP_TABLES or excluded here
 * with a reason.
 */
describe('BACKUP_TABLES', () => {
  it('covers every accounts_, ops_ and import_ table the code creates', () => {
    const stateful = [...createdTables].filter((t) => /^(accounts|ops|import)_/.test(t))
    expect(stateful.length).toBeGreaterThan(10)
    expect(stateful.filter((t) => !(BACKUP_TABLES as readonly string[]).includes(t))).toEqual([])
  })

  it('covers every table in the drizzle accounts schema', () => {
    const names = Object.values(accountsSchema)
      .filter((v) => v && typeof v === 'object' && Symbol.for('drizzle:IsDrizzleTable') in v)
      .map((t) => getTableConfig(t as PgTable).name)
    expect(names.length).toBeGreaterThan(5)
    expect(names.filter((n) => !(BACKUP_TABLES as readonly string[]).includes(n))).toEqual([])
  })

  it('names only tables that exist', () => {
    expect(BACKUP_TABLES.filter((t) => !createdTables.has(t))).toEqual([])
  })

  it('leaves out the imported data', () => {
    const imported = BACKUP_TABLES.filter((t) => /^(geo_|planet_osm_|gtfs_|gbfs_|road_|bridge_)/.test(t))
    expect(imported).toEqual([])
  })
})
