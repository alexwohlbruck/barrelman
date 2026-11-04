/**
 * Response headers that announce a deprecated endpoint, per the policy in
 * docs/content/docs/usage/stability.mdx.
 *
 *  - `Deprecation` (RFC 9745): when the endpoint was deprecated, as a
 *    structured-field date, `@<unix seconds>`.
 *  - `Sunset` (RFC 8594): when it stops working, as an HTTP-date.
 *  - `Link`: where the replacement or the changelog entry is described, with
 *    `rel="deprecation"` (and `rel="sunset"` when a sunset is set).
 *
 * Attach to a route group as a guard. It never refuses, it only adds headers:
 *
 *     new Elysia()
 *       .onBeforeHandle(deprecated({ since: '2027-01-01', sunset: '2027-04-01', link: '…' }))
 *       .get('/old', …)
 *
 * Headers set before the handler runs survive into the response, refusals
 * included, so a client sees the notice even on a 4xx.
 */

export interface DeprecationOptions {
  /** When the endpoint was deprecated. */
  since: Date | string
  /** When it is removed. Policy requires at least 90 days after `since`. */
  sunset?: Date | string
  /** A page describing the change and the replacement. */
  link?: string
}

function toDate(value: Date | string, field: string): Date {
  const date = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(date.getTime())) throw new Error(`deprecated(): invalid ${field} date: ${String(value)}`)
  return date
}

/** The headers for one deprecation, computed once. Throws on an invalid date. */
export function deprecationHeaders(options: DeprecationOptions): Record<string, string> {
  const since = toDate(options.since, 'since')
  const headers: Record<string, string> = {
    deprecation: `@${Math.floor(since.getTime() / 1000)}`,
  }

  if (options.sunset !== undefined) {
    const sunset = toDate(options.sunset, 'sunset')
    if (sunset.getTime() < since.getTime()) throw new Error('deprecated(): sunset is before the deprecation date')
    headers.sunset = sunset.toUTCString()
  }

  if (options.link) {
    const rels = options.sunset !== undefined ? 'deprecation sunset' : 'deprecation'
    headers.link = `<${options.link}>; rel="${rels}"`
  }

  return headers
}

/**
 * An `onBeforeHandle` guard that stamps the deprecation headers on every
 * response. Validates its dates at construction, so a typo fails at startup.
 */
export function deprecated(options: DeprecationOptions) {
  const headers = deprecationHeaders(options)

  return function deprecationHandler({ set }: { set: { headers: Record<string, string | number> } }) {
    for (const [name, value] of Object.entries(headers)) {
      const existing = name === 'link' ? set.headers.link : undefined
      // Keep any Link the route already set; the header is a list.
      set.headers[name] = existing ? `${existing}, ${value}` : value
    }
  }
}
