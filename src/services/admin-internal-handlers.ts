/**
 * In-process handlers for manifest scripts whose `exec.kind === 'internal'`.
 *
 * These run inside the API process using the existing Drizzle/postgres client
 * rather than spawning `psql`, so they work regardless of whether psql is on
 * the host PATH. Each handler receives a `log` callback to stream progress into
 * the job's log stream, and either resolves (success) or throws (failure).
 */
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { sql } from 'drizzle-orm'
import { db } from '../db'
import {
  runPostImport,
  runGenerateCodes,
  runGenerateAbbreviations,
  runResolveParentContext,
  runResolveParentContextIncremental,
  runRebuildTsvectors,
  runFullMigration,
  type AdminTaskResult,
} from './admin.service'

const IMPORT_DIR = join(import.meta.dir, '../../import')

export type LogFn = (text: string) => void

/** Report the structured result of an admin.service task into the log stream. */
function reportTask(log: LogFn, result: AdminTaskResult) {
  for (const step of result.steps) {
    log(`✓ ${step.file} — ${step.durationMs} ms`)
    for (const notice of step.notices) log(`  ${notice}`)
  }
  log(`Done: ${result.task} in ${result.totalMs} ms`)
}

/** Run a raw SQL file from import/ in-process, streaming basic timing. */
async function runSqlFile(log: LogFn, filename: string) {
  const path = join(IMPORT_DIR, filename)
  log(`Reading ${filename} …`)
  const content = readFileSync(path, 'utf-8')
  log(`Executing ${filename} (${content.split('\n').length} lines) …`)
  const start = performance.now()
  await db.execute(sql.raw(content))
  const ms = Math.round(performance.now() - start)
  log(`✓ ${filename} completed in ${ms} ms`)
}

export const INTERNAL_HANDLERS: Record<string, (log: LogFn) => Promise<void>> = {
  // admin.service-backed migration tasks
  'admin:full-migration': async (log) => {
    log('Running full migration: post-import → codes → abbreviations → parent context → tsvectors')
    reportTask(log, await runFullMigration(log))
  },
  'admin:post-import': async (log) => reportTask(log, await runPostImport()),
  'admin:generate-codes': async (log) => reportTask(log, await runGenerateCodes()),
  'admin:generate-abbreviations': async (log) => reportTask(log, await runGenerateAbbreviations()),
  'admin:resolve-parent-context': async (log) => {
    log('Running full spatial join for parent_context …')
    reportTask(log, await runResolveParentContext())
  },
  'admin:resolve-parent-context-incremental': async (log) =>
    reportTask(log, await runResolveParentContextIncremental()),
  'admin:rebuild-tsvectors': async (log) => reportTask(log, await runRebuildTsvectors()),

  // raw SQL-file tasks
  'sql:create-station-links.sql': (log) => runSqlFile(log, 'create-station-links.sql'),
  'sql:create-transit-views.sql': (log) => runSqlFile(log, 'create-transit-views.sql'),
  'sql:generate-intersections.sql': (log) => runSqlFile(log, 'generate-intersections.sql'),
}
