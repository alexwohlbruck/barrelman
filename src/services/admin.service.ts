import { db } from '../db'
import { sql } from 'drizzle-orm'
import { readFileSync } from 'fs'
import { join } from 'path'

const IMPORT_DIR = join(import.meta.dir, '../../import')

/** Run a named SQL file from the import/ directory and return timing + notices. */
async function runSqlFile(filename: string): Promise<{ file: string; durationMs: number; notices: string[] }> {
  const path = join(IMPORT_DIR, filename)
  const content = readFileSync(path, 'utf-8')
  const notices: string[] = []

  const start = performance.now()
  await db.execute(sql.raw(content))
  const durationMs = Math.round(performance.now() - start)

  return { file: filename, durationMs, notices }
}

export interface AdminTaskResult {
  task: string
  steps: { file: string; durationMs: number; notices: string[] }[]
  totalMs: number
}

/** Run post-import schema setup (columns, indexes, tsvector, etc.) */
export async function runPostImport(): Promise<AdminTaskResult> {
  const start = performance.now()
  const steps = [await runSqlFile('post-import.sql')]
  return { task: 'post-import', steps, totalMs: Math.round(performance.now() - start) }
}

/** Resolve parent context for all named places via spatial join. */
export async function runResolveParentContext(): Promise<AdminTaskResult> {
  const start = performance.now()
  const steps = [await runSqlFile('resolve-parent-context.sql')]
  return { task: 'resolve-parent-context', steps, totalMs: Math.round(performance.now() - start) }
}

/** Incremental parent context resolve (new/changed POIs + cascade). */
export async function runResolveParentContextIncremental(): Promise<AdminTaskResult> {
  const start = performance.now()
  const steps = [await runSqlFile('resolve-parent-context-incremental.sql')]
  return { task: 'resolve-parent-context-incremental', steps, totalMs: Math.round(performance.now() - start) }
}

/** Rebuild tsvectors for all named places. */
export async function runRebuildTsvectors(): Promise<AdminTaskResult> {
  const start = performance.now()
  await db.execute(sql`
    UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
        coalesce(name, '') || ' ' || coalesce(name_abbrev, '') || ' ' ||
        coalesce(array_to_string(
            ARRAY(SELECT replace(replace(unnest(categories), '/', ' '), '_', ' ')),
        ' '), '') || ' ' ||
        coalesce(parent_context, '')
    ))
    WHERE name IS NOT NULL
  `)
  const durationMs = Math.round(performance.now() - start)
  return { task: 'rebuild-tsvectors', steps: [{ file: 'inline', durationMs, notices: [] }], totalMs: durationMs }
}

/** Full migration: post-import → resolve parent context → rebuild tsvectors. */
export async function runFullMigration(): Promise<AdminTaskResult> {
  const start = performance.now()
  const steps = []

  steps.push(await runSqlFile('post-import.sql'))
  steps.push(await runSqlFile('resolve-parent-context.sql'))

  // Rebuild tsvectors (now that parent_context is populated)
  const tsStart = performance.now()
  await db.execute(sql`
    UPDATE geo_places SET ts = to_tsvector('simple', unaccent(
        coalesce(name, '') || ' ' || coalesce(name_abbrev, '') || ' ' ||
        coalesce(array_to_string(
            ARRAY(SELECT replace(replace(unnest(categories), '/', ' '), '_', ' ')),
        ' '), '') || ' ' ||
        coalesce(parent_context, '')
    ))
    WHERE name IS NOT NULL
  `)
  steps.push({ file: 'rebuild-tsvectors', durationMs: Math.round(performance.now() - tsStart), notices: [] })

  return { task: 'full-migration', steps, totalMs: Math.round(performance.now() - start) }
}

/** Get migration status — check if parent_context column exists and is populated. */
export async function getMigrationStatus() {
  const colExists = await db.execute(sql`
    SELECT EXISTS (
      SELECT 1 FROM information_schema.columns
      WHERE table_name = 'geo_places' AND column_name = 'parent_context'
    ) AS exists
  `)

  if (!(colExists as any[])[0]?.exists) {
    return { parent_context_column: false, populated: 0, total_named: 0, coverage: 0 }
  }

  const stats = await db.execute(sql`
    SELECT
      count(*) FILTER (WHERE parent_context IS NOT NULL) AS populated,
      count(*) AS total_named
    FROM geo_places
    WHERE name IS NOT NULL
  `)

  const row = (stats as any[])[0]
  const populated = Number(row.populated)
  const totalNamed = Number(row.total_named)

  return {
    parent_context_column: true,
    populated,
    total_named: totalNamed,
    coverage: totalNamed > 0 ? Math.round((populated / totalNamed) * 10000) / 100 : 0,
  }
}
