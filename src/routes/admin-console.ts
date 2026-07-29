import Elysia, { t } from 'elysia'
import { adminAuthHandler } from '../middleware/auth'
import {
  SCRIPTS,
  CATEGORY_LABELS,
  CATEGORY_ORDER,
  getScript,
} from '../admin/scripts-manifest'
import {
  startJob,
  listJobs,
  getJob,
  cancelJob,
  readLogsSince,
  jobStats,
  JobConflictError,
} from '../services/job-runner.service'
import { getDataMetrics, getServiceStatuses } from '../services/admin-metrics.service'
import {
  listRegions,
  getRegion,
  createRegion,
  updateRegion,
  deleteRegion,
  type RegionInput,
} from '../services/region-store.service'
import { GLOBAL_KEY } from '../config/regions'

const selfPort = Number(process.env.PORT) || 5001
const SELF_BASE = `http://127.0.0.1:${selfPort}`

// Validation for the region CRUD body. bbox is [west, south, east, north].
const peliasSchema = t.Object({
  openaddresses: t.Optional(t.Array(t.String())),
  wofIds: t.Optional(t.Array(t.String())),
  tigerStates: t.Optional(t.Array(t.Number())),
  countryCode: t.Optional(t.String()),
})
const regionFields = {
  label: t.String({ minLength: 1 }),
  osmExtracts: t.Optional(t.Array(t.String())),
  osmReplication: t.Optional(t.Array(t.String())),
  bbox: t.Tuple([t.Number(), t.Number(), t.Number(), t.Number()]),
  gtfsRegion: t.Optional(t.String()),
  pelias: t.Optional(peliasSchema),
  enabled: t.Optional(t.Boolean()),
}
const createRegionBody = t.Object({ key: t.String({ minLength: 1 }), ...regionFields })
const updateRegionBody = t.Object(regionFields)
const KEY_RE = /^[a-z0-9][a-z0-9-]*$/

/**
 * Public (unauthenticated) endpoint so the console's login screen can discover
 * whether an admin key is required before prompting for one.
 */
export const adminConsoleConfigRoutes = new Elysia({ prefix: '/admin' }).get(
  '/config',
  () => ({
    authRequired: Boolean(process.env.BARRELMAN_ADMIN_KEY || process.env.BARRELMAN_API_KEY),
    usingDedicatedAdminKey: Boolean(process.env.BARRELMAN_ADMIN_KEY),
    apiName: 'Barrelman',
    version: '0.3.0',
  }),
  { detail: { summary: 'Admin console config', tags: ['Admin'] } },
)

/**
 * Authenticated admin console API.
 *
 * The guard is attached directly with `.onBeforeHandle(adminAuthHandler)` rather
 * than via a `.use(plugin)` — Elysia scopes a plugin's lifecycle hooks to that
 * plugin instance, so a `.use()`d auth plugin does NOT protect sibling routes on
 * this instance. Attaching the handler here guarantees every route below is
 * gated. (See the note in middleware/auth.ts.)
 */
export const adminConsoleRoutes = new Elysia({ prefix: '/admin' })
  .onBeforeHandle(adminAuthHandler)

  // Lightweight probe used by the login screen to validate a supplied key.
  .get('/verify', () => ({ ok: true }), { detail: { summary: 'Verify admin key', tags: ['Admin'] } })

  // ── Import regions ──────────────────────────────────────────────────
  // The DB-backed region store (seeded from config/regions.json) that drives
  // which geographies the OSM/GTFS/GBFS/Pelias importers fetch. Editing here
  // changes what a subsequent import (run with REGIONS=<key>) pulls in.
  .get('/regions', async () => ({ regions: await listRegions() }), {
    detail: { summary: 'List import regions', tags: ['Admin'] },
  })
  .get(
    '/regions/:key',
    async ({ params, set }) => {
      const region = await getRegion(params.key)
      if (!region) {
        set.status = 404
        return { error: 'Region not found' }
      }
      return { region }
    },
    { detail: { summary: 'Get an import region', tags: ['Admin'] } },
  )
  .post(
    '/regions',
    async ({ body, set }) => {
      const b = body as RegionInput
      if (!KEY_RE.test(b.key)) {
        set.status = 400
        return { error: 'Key must be lowercase letters, numbers and dashes (e.g. "north-carolina")' }
      }
      if (b.key === GLOBAL_KEY) {
        set.status = 400
        return { error: `"${GLOBAL_KEY}" is a reserved region key` }
      }
      if (await getRegion(b.key)) {
        set.status = 409
        return { error: `Region "${b.key}" already exists` }
      }
      const region = await createRegion(b)
      set.status = 201
      return { region }
    },
    { body: createRegionBody, detail: { summary: 'Create an import region', tags: ['Admin'] } },
  )
  .put(
    '/regions/:key',
    async ({ params, body, set }) => {
      const region = await updateRegion(params.key, { key: params.key, ...(body as Omit<RegionInput, 'key'>) })
      if (!region) {
        set.status = 404
        return { error: 'Region not found' }
      }
      return { region }
    },
    { body: updateRegionBody, detail: { summary: 'Update an import region', tags: ['Admin'] } },
  )
  .delete(
    '/regions/:key',
    async ({ params, set }) => {
      if (params.key === GLOBAL_KEY) {
        set.status = 400
        return { error: 'The global (planet) region cannot be deleted' }
      }
      const ok = await deleteRegion(params.key)
      if (!ok) {
        set.status = 404
        return { error: 'Region not found' }
      }
      return { ok: true }
    },
    { detail: { summary: 'Delete an import region', tags: ['Admin'] } },
  )

  // ── Scripts manifest ────────────────────────────────────────────────
  .get(
    '/scripts',
    () => ({
      categories: CATEGORY_ORDER.map((key) => ({
        key,
        label: CATEGORY_LABELS[key],
        scripts: SCRIPTS.filter((s) => s.category === key),
      })).filter((c) => c.scripts.length > 0),
      scripts: SCRIPTS,
    }),
    { detail: { summary: 'List runnable scripts', tags: ['Admin'] } },
  )

  // ── Run a script ────────────────────────────────────────────────────
  .post(
    '/scripts/:id/run',
    async ({ params, body, set }) => {
      const script = getScript(params.id)
      if (!script) {
        set.status = 404
        return { error: `Unknown script: ${params.id}` }
      }
      try {
        const job = await startJob(params.id, (body as any)?.params ?? {})
        set.status = 201
        return { job }
      } catch (err) {
        if (err instanceof JobConflictError) {
          set.status = 409
          return { error: err.message }
        }
        set.status = 500
        return { error: err instanceof Error ? err.message : 'Failed to start job' }
      }
    },
    {
      body: t.Optional(t.Object({ params: t.Optional(t.Record(t.String(), t.Any())) })),
      detail: { summary: 'Run a script', tags: ['Admin'] },
    },
  )

  // ── Jobs ────────────────────────────────────────────────────────────
  .get('/jobs', async () => ({ jobs: await listJobs(), stats: await jobStats() }), {
    detail: { summary: 'List jobs', tags: ['Admin'] },
  })

  .get(
    '/jobs/:id',
    async ({ params, set }) => {
      const found = await getJob(params.id)
      if (!found) {
        set.status = 404
        return { error: 'Job not found' }
      }
      return found
    },
    { detail: { summary: 'Get job detail + logs', tags: ['Admin'] } },
  )

  .post(
    '/jobs/:id/cancel',
    async ({ params, set }) => {
      const result = await cancelJob(params.id)
      if (!result.ok) set.status = 409
      return result
    },
    { detail: { summary: 'Cancel a running job', tags: ['Admin'] } },
  )

  // ── Live log stream (SSE) ───────────────────────────────────────────
  // Jobs may run in a different process (the ops worker), so we can't use an
  // in-memory event emitter — poll the DB job store for new log rows + status.
  .get(
    '/jobs/:id/stream',
    async ({ params, set }) => {
      const existing = await getJob(params.id)
      if (!existing) {
        set.status = 404
        return { error: 'Job not found' }
      }

      const id = params.id
      const encoder = new TextEncoder()
      let closed = false

      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          const send = (event: string, data: unknown) => {
            try {
              controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`))
            } catch {
              closed = true
            }
          }
          const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
          const terminal = (s: string) => s !== 'running' && s !== 'queued'

          ;(async () => {
            let nextSeq = 0
            for (const line of existing.logs) {
              send('log', line)
              nextSeq = line.seq + 1
            }
            let lastStatus = existing.job.status
            send('status', existing.job)
            if (terminal(existing.job.status)) {
              controller.close()
              return
            }
            while (!closed) {
              await sleep(1000)
              const newLogs = await readLogsSince(id, nextSeq)
              for (const line of newLogs) {
                send('log', line)
                nextSeq = line.seq + 1
              }
              const cur = await getJob(id)
              if (!cur) break
              if (cur.job.status !== lastStatus) {
                lastStatus = cur.job.status
                send('status', cur.job)
              }
              if (terminal(cur.job.status)) break
            }
            try {
              controller.close()
            } catch {
              /* already closed */
            }
          })()
        },
        cancel() {
          closed = true
        },
      })

      return new Response(stream, {
        headers: {
          'content-type': 'text/event-stream',
          'cache-control': 'no-cache',
          connection: 'keep-alive',
        },
      })
    },
    { detail: { summary: 'Stream job logs (SSE)', tags: ['Admin'] } },
  )

  // ── Metrics & service health ────────────────────────────────────────
  .get('/metrics', () => getDataMetrics(), { detail: { summary: 'Data metrics', tags: ['Admin'] } })
  .get('/services', async () => ({ services: await getServiceStatuses() }), {
    detail: { summary: 'Downstream service health', tags: ['Admin'] },
  })

  // ── Endpoint tester (server-side proxy to the running API) ──────────
  .post(
    '/test-endpoint',
    async ({ body, set }) => {
      const b = body as { method?: string; path?: string; query?: string; body?: string; auth?: 'api' | 'admin' | 'none' }
      const method = (b.method || 'GET').toUpperCase()
      let path = b.path || '/'
      if (!path.startsWith('/')) path = `/${path}`
      if (b.query) path += (path.includes('?') ? '&' : '?') + b.query.replace(/^\?/, '')

      const headers: Record<string, string> = {}
      if (b.auth === 'api' && process.env.BARRELMAN_API_KEY) {
        headers['authorization'] = `Bearer ${process.env.BARRELMAN_API_KEY}`
      } else if (b.auth === 'admin' && (process.env.BARRELMAN_ADMIN_KEY || process.env.BARRELMAN_API_KEY)) {
        headers['authorization'] = `Bearer ${process.env.BARRELMAN_ADMIN_KEY || process.env.BARRELMAN_API_KEY}`
      }

      const init: RequestInit = { method, headers, signal: AbortSignal.timeout(30000) }
      if (method !== 'GET' && method !== 'HEAD' && b.body) {
        headers['content-type'] = 'application/json'
        init.body = b.body
      }

      const start = performance.now()
      try {
        const res = await fetch(`${SELF_BASE}${path}`, init)
        const durationMs = Math.round(performance.now() - start)
        const text = await res.text()
        const contentType = res.headers.get('content-type') || ''
        let parsed: unknown = text
        if (contentType.includes('application/json')) {
          try {
            parsed = JSON.parse(text)
          } catch {
            /* leave as text */
          }
        }
        return {
          ok: res.ok,
          status: res.status,
          statusText: res.statusText,
          durationMs,
          contentType,
          bytes: text.length,
          body: parsed,
        }
      } catch (err) {
        set.status = 200 // report the failure in the payload, not as an HTTP error
        return {
          ok: false,
          status: 0,
          statusText: 'Request failed',
          durationMs: Math.round(performance.now() - start),
          error: err instanceof Error ? err.message : 'Request failed',
        }
      }
    },
    {
      body: t.Object({
        method: t.Optional(t.String()),
        path: t.String(),
        query: t.Optional(t.String()),
        body: t.Optional(t.String()),
        auth: t.Optional(t.Union([t.Literal('api'), t.Literal('admin'), t.Literal('none')])),
      }),
      detail: { summary: 'Proxy a request to the running API for testing', tags: ['Admin'] },
    },
  )
