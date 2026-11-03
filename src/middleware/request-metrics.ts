/**
 * Times every metered request and records how it was answered.
 *
 * One global plugin rather than a hook beside each `apiAuth` guard, because
 * `onAfterResponse` is the only hook that also fires for a thrown handler and
 * for a refusal returned from `onBeforeHandle` — the two outcomes these metrics
 * most need to see. The guard attributes the request to a caller (and, when it
 * refuses, says why) through `attribute()`; this reads that back once the
 * response is out.
 */
import Elysia from 'elysia'
import { groupForPath } from '../billing/plans'
import { recordRequest, UNIDENTIFIED_SUBJECT, type Refusal } from '../services/request-metrics.service'

export interface Attribution {
  subject: string
  apiKeyId?: string | null
  endpoint: string
  refusal?: Refusal
}

const STARTED = Symbol.for('barrelman.metrics.started')
const ATTRIBUTION = Symbol.for('barrelman.metrics.attribution')

type Tagged = Record<symbol, unknown>

/** Called by the guard: who made this request, and why it was refused if it was. */
export function attribute(request: Request, attribution: Partial<Attribution>): void {
  const tagged = request as unknown as Tagged
  tagged[ATTRIBUTION] = { ...(tagged[ATTRIBUTION] as Attribution | undefined), ...attribution }
}

export function attributionFor(request: Request): Attribution | undefined {
  return (request as unknown as Tagged)[ATTRIBUTION] as Attribution | undefined
}

/** The 429 layers map one to one; the rest are named by status. */
export function refusalFor(status: number, layer?: string): Refusal {
  if (status === 429) {
    if (layer === 'ip' || layer === 'key' || layer === 'account' || layer === 'concurrency' || layer === 'penalty') {
      return layer
    }
    return 'account'
  }
  if (status === 402) return 'quota'
  return 'denied'
}

export const requestMetrics = new Elysia({ name: 'request-metrics' })
  .onRequest(({ request }) => {
    ;(request as unknown as Tagged)[STARTED] = performance.now()
  })
  .onAfterResponse({ as: 'global' }, ({ request, response, set }) => {
    if (request.method === 'OPTIONS') return
    const started = (request as unknown as Tagged)[STARTED] as number | undefined
    if (started === undefined) return

    const attribution = attributionFor(request)
    const endpoint = attribution?.endpoint ?? groupForPath(new URL(request.url).pathname)
    // Console, auth and health traffic is not metered and not interesting here.
    if (!endpoint) return

    const status = response instanceof Response ? response.status : Number(set.status ?? 200)

    recordRequest({
      subject: attribution?.subject ?? UNIDENTIFIED_SUBJECT,
      apiKeyId: attribution?.apiKeyId,
      endpoint,
      status,
      durationMs: performance.now() - started,
      refusal: attribution?.refusal,
    })
  })
