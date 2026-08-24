import Elysia, { t } from 'elysia'
import { adminAuthHandler } from '../middleware/auth'
import {
  SCRIPTS,
  CATEGORY_LABELS,
  CATEGORY_ORDER,
  getScript,
} from '../admin/scripts-manifest'
import { isExclusive } from '../services/job-invocation'
import {
  startJob,
  listJobs,
  getJob,
  cancelJob,
  readLogsSince,
  jobStats,
  JobConflictError,
} from '../services/job-runner.service'
import {
  createSchedule,
  defaultTimeZone,
  deleteSchedule,
  getSchedule,
  listSchedules,
  setScheduleEnabled,
  updateSchedule,
  ScheduleValidationError,
} from '../services/schedules.service'
import { describeCron, nextRun } from '../lib/cron'
import { getDataMetrics, getServiceStatuses } from '../services/admin-metrics.service'
import {
  listRegions,
  getRegion,
  createRegion,
  updateRegion,
  deleteRegion,
  type RegionInput,
} from '../services/region-store.service'
import {
  searchBoundaries,
  getBoundary,
  refreshBoundaryCatalog,
  deriveRegion,
  countBoundaries,
  catalogFetchedAt,
} from '../services/boundary-catalog.service'
import { GLOBAL_KEY } from '../config/regions'
import { accountsEnabled } from '../config/accounts.config'

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

// Cron/timezone/script are validated in the service (against the manifest and
// the cron grammar), so the shape check here is deliberately loose.
const scheduleBody = t.Object({
  scriptId: t.String({ minLength: 1 }),
  cron: t.String({ minLength: 1 }),
  timezone: t.Optional(t.String()),
  params: t.Optional(t.Record(t.String(), t.Any())),
  enabled: t.Optional(t.Boolean()),
})
type ScheduleBody = {
  scriptId: string
  cron: string
  timezone?: string
  params?: Record<string, unknown>
  enabled?: boolean
}

/**
 * Public (unauthenticated) endpoint so the console's login screen can render
 * before it has any credential — it reports the instance name and whether
 * accounts are on at all.
 */
export const adminConsoleConfigRoutes = new Elysia({ prefix: '/admin' }).get(
  '/config',
  () => ({
    // Admin access is always account-based now — an admin-role session, or an
    // account API key carrying the `admin` scope. There is no shared secret to
    // advertise, so the console always asks for a sign-in.
    authRequired: true,
    accountsEnabled,
    apiName: 'Barrelman',
    version: '0.4.0',
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

  // Lightweight probe: a script can check that its admin credential still works
  // without running anything. Reaching the handler at all means it does.
  .get('/verify', () => ({ ok: true }), {
    detail: { summary: 'Verify admin credentials', tags: ['Admin'] },
  })

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

  // ── Boundary catalog ────────────────────────────────────────────────
  // Search Geofabrik's published extracts by name and turn one into a fully
  // populated region definition, so defining a region is "type Colorado"
  // instead of hand-assembling six URLs and a bounding box.
  .get(
    '/boundaries',
    async ({ query }) => {
      const q = (query as Record<string, string>).q ?? ''
      const limit = Math.min(Number((query as Record<string, string>).limit) || 20, 100)
      const [count, fetchedAt] = await Promise.all([countBoundaries(), catalogFetchedAt()])
      return {
        catalog: { count, fetchedAt },
        boundaries: q ? await searchBoundaries(q, limit) : [],
      }
    },
    {
      query: t.Object({ q: t.Optional(t.String()), limit: t.Optional(t.String()) }),
      detail: { summary: 'Search the boundary catalog', tags: ['Admin'] },
    },
  )
  .post(
    '/boundaries/refresh',
    async ({ set }) => {
      try {
        return await refreshBoundaryCatalog()
      } catch (err) {
        set.status = 502
        return { error: err instanceof Error ? err.message : 'Boundary catalog refresh failed' }
      }
    },
    { detail: { summary: 'Refresh the boundary catalog from Geofabrik', tags: ['Admin'] } },
  )
  .post(
    '/boundaries/resolve',
    async ({ body, set }) => {
      // A preview, not a write: the console shows what was auto-filled (and any
      // warnings) and the operator saves it through the normal region endpoint.
      const { id } = body as { id: string }
      const boundary = await getBoundary(id)
      if (!boundary) {
        set.status = 404
        return {
          error:
            `Unknown boundary "${id}". If the catalog is empty, refresh it first ` +
            `(POST /admin/boundaries/refresh or bun run scripts/fetch-boundaries.ts).`,
        }
      }
      return await deriveRegion(boundary)
    },
    {
      body: t.Object({ id: t.String({ minLength: 1 }) }),
      detail: { summary: 'Derive a region definition from a boundary', tags: ['Admin'] },
    },
  )

  // ── Scripts manifest ────────────────────────────────────────────────
  .get(
    '/scripts',
    () => {
      // `exclusive` is implied by `longRunning` when not set explicitly, so send
      // the resolved value — the console has to show the same rule the enqueue
      // guard applies, not the manifest's shorthand for it.
      const scripts = SCRIPTS.map((s) => ({ ...s, exclusive: isExclusive(s) }))
      return {
        categories: CATEGORY_ORDER.map((key) => ({
          key,
          label: CATEGORY_LABELS[key],
          scripts: scripts.filter((s) => s.category === key),
        })).filter((c) => c.scripts.length > 0),
        scripts,
      }
    },
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
          return { error: err.message, activeJobId: err.activeJobId, activeStatus: err.activeStatus }
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

  // ── Schedules ───────────────────────────────────────────────────────
  // Cron entries that enqueue manifest scripts. Firing one produces an ordinary
  // job (trigger:'schedule'), so scheduled imports appear in the list below
  // alongside manual runs instead of vanishing into a host logfile.
  .get(
    '/schedules',
    async () => ({
      schedules: (await listSchedules()).map((s) => ({ ...s, description: describeCron(s.cron, s.timezone) })),
      defaultTimezone: defaultTimeZone(),
    }),
    { detail: { summary: 'List job schedules', tags: ['Admin'] } },
  )

  .post(
    '/schedules',
    async ({ body, set }) => {
      try {
        set.status = 201
        return { schedule: await createSchedule(body as ScheduleBody) }
      } catch (err) {
        set.status = err instanceof ScheduleValidationError ? 400 : 500
        return { error: err instanceof Error ? err.message : 'Failed to create schedule' }
      }
    },
    { body: scheduleBody, detail: { summary: 'Create a job schedule', tags: ['Admin'] } },
  )

  .put(
    '/schedules/:id',
    async ({ params, body, set }) => {
      try {
        const schedule = await updateSchedule(params.id, body as ScheduleBody)
        if (!schedule) {
          set.status = 404
          return { error: 'Schedule not found' }
        }
        return { schedule }
      } catch (err) {
        set.status = err instanceof ScheduleValidationError ? 400 : 500
        return { error: err instanceof Error ? err.message : 'Failed to update schedule' }
      }
    },
    { body: scheduleBody, detail: { summary: 'Update a job schedule', tags: ['Admin'] } },
  )

  // Separate from PUT so the console's on/off switch doesn't have to round-trip
  // the whole expression (and can't corrupt it by racing an open edit form).
  .post(
    '/schedules/:id/enabled',
    async ({ params, body, set }) => {
      const schedule = await setScheduleEnabled(params.id, (body as { enabled: boolean }).enabled)
      if (!schedule) {
        set.status = 404
        return { error: 'Schedule not found' }
      }
      return { schedule }
    },
    {
      body: t.Object({ enabled: t.Boolean() }),
      detail: { summary: 'Enable or disable a schedule', tags: ['Admin'] },
    },
  )

  .delete(
    '/schedules/:id',
    async ({ params, set }) => {
      if (!(await deleteSchedule(params.id))) {
        set.status = 404
        return { error: 'Schedule not found' }
      }
      return { ok: true }
    },
    { detail: { summary: 'Delete a job schedule', tags: ['Admin'] } },
  )

  // Run a schedule now, out of band. The job is still tagged to the schedule so
  // the console can show it as that schedule's last run; the cron timing is
  // untouched, so this doesn't skip or shift the next occurrence.
  .post(
    '/schedules/:id/run',
    async ({ params, set }) => {
      const schedule = await getSchedule(params.id)
      if (!schedule) {
        set.status = 404
        return { error: 'Schedule not found' }
      }
      try {
        const job = await startJob(schedule.scriptId, schedule.params, {
          trigger: 'schedule',
          scheduleId: schedule.id,
          scheduleName: schedule.scriptName,
        })
        set.status = 201
        return { job }
      } catch (err) {
        if (err instanceof JobConflictError) {
          set.status = 409
          return { error: err.message, activeJobId: err.activeJobId, activeStatus: err.activeStatus }
        }
        set.status = 500
        return { error: err instanceof Error ? err.message : 'Failed to start job' }
      }
    },
    { detail: { summary: 'Run a schedule immediately', tags: ['Admin'] } },
  )

  // Preview upcoming fire times so an operator can sanity-check an expression
  // (especially its timezone) before saving it.
  .post(
    '/schedules/preview',
    async ({ body, set }) => {
      const { cron, timezone } = body as { cron: string; timezone?: string }
      const zone = timezone || defaultTimeZone()
      const upcoming: string[] = []
      let cursor = new Date()
      for (let i = 0; i < 5; i++) {
        const next = nextRun(cron, cursor, zone)
        if (!next) break
        upcoming.push(next.toISOString())
        cursor = next
      }
      if (!upcoming.length) {
        set.status = 400
        return { error: `"${cron}" is not a valid cron expression, or never matches` }
      }
      return { description: describeCron(cron, zone), timezone: zone, upcoming }
    },
    {
      body: t.Object({ cron: t.String(), timezone: t.Optional(t.String()) }),
      detail: { summary: 'Preview a cron expression', tags: ['Admin'] },
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
      // Only the read-API service credential is injectable. Admin requests are
      // not: they need an account credential, and this replay runs server-side
      // with no session. Point the tester at public endpoints, or drive
      // /admin/* with your own admin-scoped key from a shell.
      if (b.auth === 'api' && process.env.BARRELMAN_API_KEY) {
        headers['authorization'] = `Bearer ${process.env.BARRELMAN_API_KEY}`
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
