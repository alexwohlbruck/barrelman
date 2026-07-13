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
  subscribeJob,
  jobStats,
  JobConflictError,
} from '../services/job-runner.service'
import { getDataMetrics, getServiceStatuses } from '../services/admin-metrics.service'

const selfPort = Number(process.env.PORT) || 5001
const SELF_BASE = `http://127.0.0.1:${selfPort}`

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
    ({ params, body, set }) => {
      const script = getScript(params.id)
      if (!script) {
        set.status = 404
        return { error: `Unknown script: ${params.id}` }
      }
      try {
        const job = startJob(params.id, (body as any)?.params ?? {})
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
  .get('/jobs', () => ({ jobs: listJobs(), stats: jobStats() }), {
    detail: { summary: 'List jobs', tags: ['Admin'] },
  })

  .get(
    '/jobs/:id',
    ({ params, set }) => {
      const found = getJob(params.id)
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
    ({ params, set }) => {
      const result = cancelJob(params.id)
      if (!result.ok) set.status = 409
      return result
    },
    { detail: { summary: 'Cancel a running job', tags: ['Admin'] } },
  )

  // ── Live log stream (SSE over fetch; consumed with a streaming reader) ──
  .get(
    '/jobs/:id/stream',
    ({ params, set }) => {
      const existing = getJob(params.id)
      if (!existing) {
        set.status = 404
        return { error: 'Job not found' }
      }

      let unsub: (() => void) | undefined
      const encoder = new TextEncoder()

      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          const send = (event: string, data: unknown) => {
            try {
              controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`))
            } catch {
              /* controller closed */
            }
          }

          // Replay backlog, then stream live.
          for (const line of existing.logs) send('log', line)
          send('status', existing.job)

          if (existing.job.status !== 'running') {
            controller.close()
            return
          }

          unsub = subscribeJob(params.id, (evt) => {
            if (evt.type === 'log') {
              send('log', evt.line)
            } else {
              send('status', evt.job)
              if (evt.job.status !== 'running') {
                unsub?.()
                try {
                  controller.close()
                } catch {
                  /* already closed */
                }
              }
            }
          })
        },
        cancel() {
          unsub?.()
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
