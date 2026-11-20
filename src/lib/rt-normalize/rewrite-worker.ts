import { createRewriter, owned, type Rewriter } from './rewrite'

/** Milliseconds one message may take to rewrite; a first match builds the timetable index, which is the slow part. */
const JOB_TIMEOUT_MS = 120_000

/**
 * The relay's CPU work off the request thread. MOTIS polls every relayed feed
 * at once each cycle, and decoding, matching and encoding them here held the
 * event loop for seconds at a time, stalling every tile and search behind it.
 * Falls back to working in-thread where workers are unavailable.
 */
export function workerRewriter(): Rewriter {
  let worker: Worker | null = null
  let here: Rewriter | null = null
  let next = 0
  const jobs = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: ReturnType<typeof setTimeout> }>()
  const settle = (id: number) => {
    const job = jobs.get(id)
    if (job) clearTimeout(job.timer)
    jobs.delete(id)
    return job
  }
  // A worker that dies fails what it holds; the next request starts another, which rebuilds its indexes.
  const lost = (w: Worker, why: string) => {
    if (worker !== w) return
    worker = null
    for (const id of [...jobs.keys()]) settle(id)?.reject(new Error(`Realtime worker: ${why}`))
  }
  const start = () => {
    const w = new Worker(new URL('./rewrite.worker.ts', import.meta.url))
    w.onmessage = ({ data }: MessageEvent<{ id: number; result?: unknown; error?: string }>) => {
      const job = settle(data.id)
      if (data.error !== undefined) job?.reject(new Error(data.error))
      else job?.resolve(data.result)
    }
    w.onerror = event => lost(w, event.message)
    w.addEventListener('close', () => lost(w, 'closed'))
    ;(w as Worker & { unref?: () => void }).unref?.()
    return w
  }
  const run = <T>(message: object, transfer: Transferable[], inThread: (r: Rewriter) => Promise<T>): Promise<T> => {
    try {
      worker ??= start()
    } catch {
      here ??= createRewriter()
      return inThread(here)
    }
    const id = next++
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => settle(id)?.reject(new Error('Realtime worker timed out')), JOB_TIMEOUT_MS)
      jobs.set(id, { resolve, reject, timer })
      try {
        worker!.postMessage({ id, ...message }, transfer)
      } catch (error) {
        settle(id)?.reject(error as Error)
      }
    })
  }
  return {
    rewrite: job => {
      const raw = owned(job.raw)
      return run({ op: 'rewrite', job: { ...job, raw } }, [raw.buffer], r => r.rewrite(job))
    },
    // Copied, not transferred: the caller keeps serving this body to MOTIS.
    calls: body => run({ op: 'calls', body }, [], r => r.calls(body)),
  }
}
