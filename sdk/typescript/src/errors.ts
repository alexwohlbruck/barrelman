/**
 * The refusal codes documented in Stability and errors
 * (https://docs.barrelman.dev/usage/stability). New codes may be added, so
 * `code` stays open to any string; treat one you do not recognise by `status`.
 */
export type BarrelmanErrorCode =
  | 'missing_key'
  | 'invalid_key'
  | 'insufficient_credits'
  | 'overage_limit_reached'
  | 'account_suspended'
  | 'scope_denied'
  | 'origin_denied'
  | 'rate_limited'
  | 'too_many_failures'
  | (string & {})

/** A non-2xx response from the API. */
export class BarrelmanError extends Error {
  override readonly name = 'BarrelmanError'
  /** HTTP status of the response. */
  readonly status: number
  /** Stable refusal code, present on auth, credit and rate-limit refusals. */
  readonly code: BarrelmanErrorCode | undefined
  /** The `error` message from the body. Wording may change; branch on `code`. */
  readonly error: string | undefined
  /** `X-Request-Id` of the response. Include it when reporting a failure. */
  readonly requestId: string | undefined
  /** Seconds to wait before retrying, from `Retry-After`. Sent with every 429. */
  readonly retryAfter: number | undefined
  /** The parsed JSON body, or the raw text when it was not JSON. */
  readonly body: unknown

  constructor(init: {
    status: number
    code?: string
    error?: string
    requestId?: string
    retryAfter?: number
    body?: unknown
    statusText?: string
  }) {
    const reason = init.error ?? init.statusText ?? 'Request failed'
    super(`Barrelman API ${init.status}${init.code ? ` ${init.code}` : ''}: ${reason}`)
    this.status = init.status
    this.code = init.code
    this.error = init.error
    this.requestId = init.requestId
    this.retryAfter = init.retryAfter
    this.body = init.body
  }

  /** Build an error from a failed response, reading its body once. */
  static async fromResponse(response: Response): Promise<BarrelmanError> {
    const text = await response.text().catch(() => '')
    let body: unknown = text
    try {
      body = text ? JSON.parse(text) : undefined
    } catch {
      // A proxy in front of the API can answer with HTML or plain text.
    }
    const fields = body && typeof body === 'object' ? (body as Record<string, unknown>) : {}
    return new BarrelmanError({
      status: response.status,
      statusText: response.statusText || undefined,
      code: typeof fields.code === 'string' ? fields.code : undefined,
      error: typeof fields.error === 'string' ? fields.error : typeof body === 'string' && body ? body : undefined,
      requestId: response.headers.get('x-request-id') ?? undefined,
      retryAfter: parseRetryAfter(response.headers.get('retry-after')),
      body,
    })
  }
}

/** `Retry-After` is either delta-seconds or an HTTP date. Returns seconds. */
export function parseRetryAfter(value: string | null, now: number = Date.now()): number | undefined {
  if (!value) return undefined
  const trimmed = value.trim()
  if (/^\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed)
  const at = Date.parse(trimmed)
  if (Number.isNaN(at)) return undefined
  return Math.max(0, Math.ceil((at - now) / 1000))
}
