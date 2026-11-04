/**
 * Optional error tracking with Sentry.
 *
 * Off unless SENTRY_DSN is set, and then `@sentry/bun` is loaded with a dynamic
 * import, so an instance that never configures it never evaluates the SDK.
 *
 * What gets reported: errors that reach the process-level handlers in the API
 * and the ops worker, and handler errors that answer 5xx (see
 * middleware/error-tracking.ts). Nothing is captured automatically beyond that.
 * The SDK's default integrations are replaced by an allowlist, because several
 * of them collect exactly what must not leave the box:
 *
 *   - `Console` turns log lines into breadcrumbs, and the API logs sign-in
 *     codes alongside the address they were sent to when SMTP is unset.
 *   - `Http`/`Fetch` record outgoing URLs, which carry upstream API keys in
 *     query strings, and inject trace headers into calls to our own services.
 *   - `RequestData` and the Bun server integrations attach the incoming
 *     request: headers, cookies, query string.
 *   - `OnUncaughtException`/`OnUnhandledRejection` would capture a second copy
 *     of what our own process handlers already report.
 *
 * `beforeSend` scrubs whatever is left, as a second line in case a future SDK
 * version starts attaching something new.
 */
import pkg from '../../package.json' with { type: 'json' }
import { envRaw, envString } from '../config/env'

type SentryModule = typeof import('@sentry/bun')
type SentryEvent = import('@sentry/bun').ErrorEvent
type Breadcrumb = import('@sentry/bun').Breadcrumb

/** Integrations that read only the error itself and the runtime, never request or log data. */
const KEPT_INTEGRATIONS = new Set([
  'EventFilters',
  'FunctionToString',
  'LinkedErrors',
  'Dedupe',
  'NodeSystemError',
  'ContextLines',
  'Context',
])

const SENTRY_KEY = Symbol.for('barrelman.sentry')

const EMAIL = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi
// Our own key format plus anything presented as a bearer token.
const SECRET = /\b(brm_[A-Za-z0-9_]+|Bearer\s+[A-Za-z0-9._~+/-]+=*)/g

/** Strip emails and credentials from free text such as an error message. */
export function scrubText(text: string): string {
  return text.replace(EMAIL, '[email]').replace(SECRET, '[redacted]')
}

/** Drop query strings: they carry API keys for us and for upstream services. */
function scrubUrl(url: string): string {
  const q = url.indexOf('?')
  return q === -1 ? url : `${url.slice(0, q)}?[redacted]`
}

/**
 * Remove request bodies, headers, cookies, query strings, user identity and
 * anything credential- or email-shaped in messages. Exported for tests.
 */
export function scrubEvent<E extends SentryEvent>(event: E): E {
  if (event.request) {
    event.request = {
      method: event.request.method,
      url: event.request.url ? scrubUrl(event.request.url) : undefined,
    }
  }
  delete event.user
  if (event.message) event.message = scrubText(event.message)
  for (const value of event.exception?.values ?? []) {
    if (value.value) value.value = scrubText(value.value)
  }
  if (event.breadcrumbs) event.breadcrumbs = event.breadcrumbs.map(scrubBreadcrumb)
  return event
}

function scrubBreadcrumb(crumb: Breadcrumb): Breadcrumb {
  const { data: _data, ...rest } = crumb
  return { ...rest, message: crumb.message ? scrubText(crumb.message) : undefined }
}

function loaded(): SentryModule | undefined {
  return (globalThis as Record<symbol, unknown>)[SENTRY_KEY] as SentryModule | undefined
}

/**
 * Initialise once per process; a no-op without SENTRY_DSN. Held on globalThis
 * so `bun --hot` does not initialise a second client on every edit.
 */
export async function initErrorTracking(component: 'api' | 'ops'): Promise<void> {
  const dsn = envRaw('SENTRY_DSN')
  if (!dsn || loaded()) return

  try {
    const Sentry = await import('@sentry/bun')
    Sentry.init({
      dsn,
      release: `barrelman@${pkg.version}`,
      environment: envString('SENTRY_ENVIRONMENT', 'production'),
      integrations: (defaults) => defaults.filter((i) => KEPT_INTEGRATIONS.has(i.name)),
      beforeSend: (event) => scrubEvent(event),
      beforeBreadcrumb: (crumb) => scrubBreadcrumb(crumb),
      initialScope: { tags: { component } },
    })
    ;(globalThis as Record<symbol, unknown>)[SENTRY_KEY] = Sentry
    console.log(`[sentry] error tracking enabled (${component})`)
  } catch (err) {
    // A broken DSN or SDK is no reason to refuse to serve.
    console.error('[sentry] failed to initialise:', err)
  }
}

/** Report an error. A no-op when error tracking is off. */
export function captureError(error: unknown, context?: { tags?: Record<string, string> }): void {
  const Sentry = loaded()
  if (!Sentry) return
  Sentry.captureException(error, context?.tags ? { tags: context.tags } : undefined)
}

/** Give queued events a moment to send before the process exits. */
export async function flushErrorTracking(timeoutMs = 2000): Promise<void> {
  await loaded()?.flush(timeoutMs)
}
