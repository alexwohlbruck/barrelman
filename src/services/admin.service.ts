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

/** Generate codes from OSM tags (IATA, ICAO, ref, short_name, abbreviation, alt_name). */
export async function runGenerateCodes(): Promise<AdminTaskResult> {
  const start = performance.now()
  await db.execute(sql.raw(`
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
        AND (
          tags->>'iata' IS NOT NULL OR
          tags->>'icao' IS NOT NULL OR
          tags->>'ref' IS NOT NULL OR
          tags->>'short_name' IS NOT NULL OR
          tags->>'abbreviation' IS NOT NULL OR
          tags->>'alt_name' IS NOT NULL
        )
      GROUP BY id
    ) sub
    WHERE geo_places.id = sub.id
      AND (geo_places.codes IS NULL OR geo_places.codes <> sub.codes);
  `))
  const durationMs = Math.round(performance.now() - start)
  return { task: 'generate-codes', steps: [{ file: 'inline', durationMs, notices: [] }], totalMs: durationMs }
}

/** Generate abbreviations for multi-word Latin-script names. */
export async function runGenerateAbbreviations(): Promise<AdminTaskResult> {
  const start = performance.now()
  await db.execute(sql.raw(`
    UPDATE geo_places
    SET name_abbrev = sub.abbrev
    FROM (
      SELECT id,
        lower(string_agg(left(word, 1), '' ORDER BY ord)) AS abbrev
      FROM (
        SELECT id, word, ord
        FROM geo_places,
        LATERAL unnest(regexp_split_to_array(name, '\\s+')) WITH ORDINALITY AS t(word, ord)
        WHERE name IS NOT NULL
          AND name_abbrev IS NULL
          AND name ~ '^[\\w\\s\\d\\-''\\.\&]+$'
      ) words
      WHERE lower(word) NOT IN (
        'of','the','and','at','in','for','a','an',
        'de','la','le','les','du','des','et','au',
        'der','die','das','von','und','im','am',
        'del','los','las','el','dos','e',
        'di','della','dei','degli'
      )
      AND length(word) > 0
      GROUP BY id
      HAVING count(*) >= 2
    ) sub
    WHERE geo_places.id = sub.id;
  `))
  const durationMs = Math.round(performance.now() - start)
  return { task: 'generate-abbreviations', steps: [{ file: 'inline', durationMs, notices: [] }], totalMs: durationMs }
}

/** Full migration: post-import → codes → abbreviations → parent context → tsvectors. */
export async function runFullMigration(): Promise<AdminTaskResult> {
  const start = performance.now()
  const steps = []

  steps.push(await runSqlFile('post-import.sql'))

  // Generate codes and abbreviations before tsvectors (tsvectors include abbreviations)
  const codesResult = await runGenerateCodes()
  steps.push(codesResult.steps[0])

  const abbrevResult = await runGenerateAbbreviations()
  steps.push(abbrevResult.steps[0])

  steps.push(await runSqlFile('resolve-parent-context.sql'))

  // Rebuild tsvectors (now that codes, abbreviations, and parent_context are populated)
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
