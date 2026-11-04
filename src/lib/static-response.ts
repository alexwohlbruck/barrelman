import { createHash } from 'node:crypto'
import { clientIp, createRateLimiter, type RateLimiter } from './rate-limit'

/**
 * Shared pieces for the public, unmetered asset routes: glyphs, sprites and
 * brand logos. Browsers fetch these from `<img>` tags and MapLibre, neither of
 * which can send a Bearer header, so they take no key. The per-address limit
 * keeps them from being a free bulk-download channel.
 */

/** A month. The files change only with a release (glyphs, sprites) or a new file name (logos). */
export const LONG_CACHE = 'public, max-age=2592000, stale-while-revalidate=86400'

type Set = { status?: number | string; headers: Record<string, string | number> }

/** A quoted strong ETag for some bytes. */
export function etagOf(bytes: Uint8Array): string {
  return `"${createHash('sha1').update(bytes).digest('hex').slice(0, 16)}"`
}

/**
 * Answer with bytes, or 304 when the caller already holds this version.
 * `headers` is the request's, read for If-None-Match.
 */
export function sendBytes(
  set: Set,
  requestHeaders: Record<string, string | undefined>,
  body: { bytes: Uint8Array; contentType: string; etag: string; cacheControl?: string },
): Response {
  const headers: Record<string, string> = {
    ...(Object.fromEntries(Object.entries(set.headers).map(([k, v]) => [k, String(v)])) as Record<string, string>),
    'content-type': body.contentType,
    'cache-control': body.cacheControl ?? LONG_CACHE,
    etag: body.etag,
    'access-control-allow-origin': '*',
  }
  const inm = requestHeaders['if-none-match']
  if (inm && inm.split(',').some((t) => t.trim() === body.etag)) {
    return new Response(null, { status: 304, headers })
  }
  return new Response(body.bytes as unknown as BodyInit, { status: 200, headers })
}

/**
 * An onBeforeHandle guard that answers 429 once an address has spent its
 * budget for the window.
 */
export function perIpLimit(limiter: RateLimiter) {
  return ({ request, set }: { request: Request; set: Set }) => {
    const ip = clientIp(request)
    if (limiter.check(ip)) return
    set.status = 429
    set.headers['retry-after'] = String(limiter.retryAfter(ip))
    return { error: 'Too many requests for static assets from this address. Slow down and retry.' }
  }
}

export { createRateLimiter }
