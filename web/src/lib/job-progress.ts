/**
 * Derives a display progress model for a running job.
 *
 * Priority:
 *   1. `job.progress` — true fraction parsed from the script's own log markers
 *   2. estimate from `job.etaMs` (median of past successful runs) vs. elapsed
 *   3. indeterminate — no history and no markers yet
 *
 * A shared 1s ticker keeps the estimate advancing between server updates.
 */
import { computed, onScopeDispose, ref, type Ref } from 'vue'
import type { Job } from './types'
import { formatDuration } from './utils'

let tickers = 0
let timer: ReturnType<typeof setInterval> | undefined
const now = ref(Date.now())

function useTick() {
  tickers++
  if (!timer) timer = setInterval(() => { now.value = Date.now() }, 1000)
  onScopeDispose(() => {
    tickers = Math.max(0, tickers - 1)
    if (tickers === 0 && timer) { clearInterval(timer); timer = undefined }
  })
}

export type ProgressSource = 'marker' | 'estimate' | 'indeterminate'

export interface JobProgress {
  indeterminate: boolean
  fraction: number // 0–1
  percent: number // 0–100, rounded
  label: string // marker label, e.g. "3/8" (empty otherwise)
  etaLabel: string // e.g. "~2m 3s left" (empty when unknown)
  source: ProgressSource
}

const clamp01 = (x: number) => (x < 0 ? 0 : x > 1 ? 1 : x)

/** Reactive progress model for a running job, or null when not running. */
export function useJobProgress(job: Ref<Job | null | undefined>) {
  useTick()
  return computed<JobProgress | null>(() => {
    const j = job.value
    if (!j || j.status !== 'running') return null
    const elapsed = Math.max(0, now.value - j.startedAt)

    // 1) True progress from the script's own markers.
    if (typeof j.progress === 'number' && j.progress > 0) {
      const fraction = clamp01(j.progress)
      return {
        indeterminate: false,
        fraction,
        percent: Math.round(fraction * 100),
        label: j.progressLabel ?? '',
        etaLabel: etaFromPace(fraction, elapsed),
        source: 'marker',
      }
    }

    // 2) Estimate from historical median runtime.
    if (j.etaMs && j.etaMs > 0) {
      const raw = elapsed / j.etaMs
      const fraction = Math.min(raw, 0.95) // never claim 100% before it's actually done
      const remaining = Math.max(0, j.etaMs - elapsed)
      return {
        indeterminate: false,
        fraction,
        percent: Math.round(fraction * 100),
        label: '',
        etaLabel: raw >= 1 ? 'wrapping up…' : `~${formatDuration(remaining)} left`,
        source: 'estimate',
      }
    }

    // 3) No signal yet.
    return { indeterminate: true, fraction: 0, percent: 0, label: '', etaLabel: '', source: 'indeterminate' }
  })
}

/** Project remaining time from observed pace (marker fraction over elapsed). */
function etaFromPace(fraction: number, elapsed: number): string {
  if (fraction <= 0.02 || fraction >= 1) return ''
  const total = elapsed / fraction
  return `~${formatDuration(Math.max(0, total - elapsed))} left`
}
