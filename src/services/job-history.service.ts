/**
 * Job history + runtime estimation.
 *
 * Persists one row per completed console job to `script_runs` and keeps an
 * in-memory cache of the median successful duration per script. The job runner
 * uses that median as an ETA baseline for the progress bar, and parses the
 * `[N/M]` / `x/total` / `NN%` markers that several scripts already print for a
 * true (coarse) progress fraction when available.
 *
 * Everything here is best-effort: a DB hiccup must never break job execution.
 */
import { connection, ensureScriptRunsSchema } from '../db'

/** median successful duration (ms) per scriptId */
const medians = new Map<string, number>()

export async function initJobHistory(): Promise<void> {
  await ensureScriptRunsSchema()
  await refreshAllMedians()
}

async function refreshAllMedians(): Promise<void> {
  try {
    const rows = await connection<{ script_id: string; median: number | null }[]>`
      SELECT script_id,
             percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS median
      FROM script_runs
      WHERE status = 'succeeded' AND duration_ms IS NOT NULL
      GROUP BY script_id`
    medians.clear()
    for (const r of rows) {
      if (r.median != null) medians.set(r.script_id, Math.round(Number(r.median)))
    }
  } catch {
    /* history is optional metadata; ignore */
  }
}

async function refreshMedian(scriptId: string): Promise<void> {
  try {
    const rows = await connection<{ median: number | null }[]>`
      SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS median
      FROM script_runs
      WHERE script_id = ${scriptId} AND status = 'succeeded' AND duration_ms IS NOT NULL`
    const m = rows[0]?.median
    if (m != null) medians.set(scriptId, Math.round(Number(m)))
  } catch {
    /* ignore */
  }
}

/** Median successful runtime (ms) for a script, if we've seen enough history. */
export function getEtaMs(scriptId: string): number | undefined {
  return medians.get(scriptId)
}

export interface RecordedRun {
  id: string
  scriptId: string
  status: string
  startedAt: number
  endedAt?: number
  durationMs?: number
  exitCode?: number | null
}

/** Persist a finished run and refresh the ETA baseline for that script. */
export async function recordRun(run: RecordedRun): Promise<void> {
  try {
    await connection`
      INSERT INTO script_runs (id, script_id, status, started_at, ended_at, duration_ms, exit_code)
      VALUES (${run.id}, ${run.scriptId}, ${run.status},
              ${new Date(run.startedAt)},
              ${run.endedAt ? new Date(run.endedAt) : null},
              ${run.durationMs ?? null}, ${run.exitCode ?? null})
      ON CONFLICT (id) DO NOTHING`
    if (run.status === 'succeeded' && run.durationMs != null) {
      await refreshMedian(run.scriptId)
    }
  } catch {
    /* best-effort */
  }
}

/**
 * Parse a `[N/M] Stage name` marker — the house convention that scripts print
 * at each stage boundary (e.g. `[3/8] Running post-import SQL`). A leading
 * `[HH:MM:SS]` timestamp is ignored because it has colons, not a slash.
 * Returns the 1-based stage index, total, and (trimmed) name, or null.
 */
export function parseStage(text: string): { index: number; total: number; name: string } | null {
  const m = text.match(/\[(\d+)\s*\/\s*(\d+)\]\s*(.*)$/)
  if (!m) return null
  const index = Number(m[1])
  const total = Number(m[2])
  if (!(total > 0 && index >= 1 && index <= total)) return null
  const name = m[3].trim().replace(/[.…:]+$/, '').trim()
  return { index, total, name }
}

/**
 * Parse a within-stage progress fraction from a running count (`1000 / 5000`)
 * or a percentage (`42%`), ignoring bracketed `[N/M]` stage markers. Returns a
 * fraction in [0, 1] or null.
 */
export function parseCount(text: string): number | null {
  const pct = text.match(/(\d+(?:\.\d+)?)\s*%/)
  if (pct) {
    const v = Number(pct[1])
    if (v >= 0 && v <= 100) return clamp01(v / 100)
  }
  const count = text.match(/(?<!\[)\b(\d{1,3}(?:[,\d]*))\s*\/\s*(\d{1,3}(?:[,\d]*))\b(?!\s*\])/)
  if (count) {
    const n = Number(count[1].replace(/,/g, ''))
    const m = Number(count[2].replace(/,/g, ''))
    if (m > 0 && n >= 0 && n <= m) return clamp01(n / m)
  }
  return null
}

function clamp01(x: number): number {
  return x < 0 ? 0 : x > 1 ? 1 : x
}
