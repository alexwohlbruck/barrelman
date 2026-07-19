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
 * Parse a log line for a progress signal. Returns a fraction in (0, 1] plus a
 * short label, or null. Recognizes, in priority order:
 *   - bracketed step markers `[3/8]` (import-osm.sh, update-osm.sh, gtfs, …)
 *   - explicit percentages `42%` / `42.5 %`
 *   - running counts `1000 / 5000` (embed, gtfs pairs, backfill, …)
 * Callers should treat progress as monotonic (only ever advance it) so that a
 * small sub-step count never drags a later stage backwards.
 */
export function parseProgress(text: string): { fraction: number; label: string } | null {
  const bracket = text.match(/\[(\d+)\s*\/\s*(\d+)\]/)
  if (bracket) {
    const n = Number(bracket[1])
    const m = Number(bracket[2])
    if (m > 0 && n >= 0 && n <= m) return { fraction: clamp01(n / m), label: `${n}/${m}` }
  }

  const pct = text.match(/(\d+(?:\.\d+)?)\s*%/)
  if (pct) {
    const v = Number(pct[1])
    if (v >= 0 && v <= 100) return { fraction: clamp01(v / 100), label: `${pct[1]}%` }
  }

  const count = text.match(/(\d{1,3}(?:[,\d]*))\s*\/\s*(\d{1,3}(?:[,\d]*))(?!\s*\])/)
  if (count) {
    const n = Number(count[1].replace(/,/g, ''))
    const m = Number(count[2].replace(/,/g, ''))
    if (m > 0 && n >= 0 && n <= m) return { fraction: clamp01(n / m), label: `${n}/${m}` }
  }

  return null
}

function clamp01(x: number): number {
  return x < 0 ? 0 : x > 1 ? 1 : x
}
