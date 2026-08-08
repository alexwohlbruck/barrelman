/**
 * Origin restrictions for API keys.
 *
 * A key embedded in a web page is public — MapLibre cannot send an
 * `Authorization` header, so a tile key travels in the query string and anyone
 * viewing source can lift it. Scopes bound what a stolen key can *reach*; this
 * bounds where it can be used *from*.
 *
 * What this is worth, precisely: `Origin` and `Referer` are set by the browser
 * and cannot be overridden by page script, so a copied key pasted into someone
 * else's site stops working. They are trivially forged by curl, so this is not
 * a defence against a determined scraper — the same guarantee Mapbox and Google
 * Maps give for their URL restrictions, and the same reason both still meter.
 * It is the difference between casual theft and deliberate theft, which is the
 * difference that actually shows up in traffic.
 *
 * The unrestricted case stays unrestricted: an empty list means "any origin",
 * so every key that existed before this shipped behaves exactly as it did.
 */

/**
 * Labels separated by dots, each starting and ending alphanumeric. Covers
 * `localhost`, `barrelman.dev`, `deploy--site.netlify.app` and bare IPv4.
 * Bracketed IPv6 is deliberately unsupported: a browser key restricted to a
 * literal v6 address is not a real case, and the parsing is a liability.
 */
const HOST_OK = /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*$/

const DEFAULT_PORT: Record<string, string> = { http: '80', https: '443' }

/**
 * Parse one entry into a canonical origin, or null if it cannot be one.
 *
 * Deliberately forgiving about input, because this is typed into a form by
 * someone copying from their address bar: a bare host gets `https://`, and a
 * pasted URL keeps its authority and loses the path. Deliberately strict about
 * output, because the result is compared by string equality.
 *
 * A `*.` prefix marks a subdomain wildcard, which is what makes deploy previews
 * usable — `https://*.netlify.app` covers every hashed preview URL without
 * listing them.
 */
export function normalizeOrigin(raw: string): string | null {
  let value = raw.trim().toLowerCase()
  if (!value) return null

  const scheme = /^(https?):\/\//.exec(value)?.[1] ?? 'https'
  value = value.replace(/^https?:\/\//, '')

  // Keep the authority, drop path, query and fragment.
  value = value.split(/[/?#]/)[0] ?? ''
  if (!value) return null

  // Credentials in an origin mean nothing and are always a paste accident.
  if (value.includes('@')) return null

  const wildcard = value.startsWith('*.')
  const hostPort = wildcard ? value.slice(2) : value
  // A second `*` anywhere, or one not in the leading position, is not a form
  // this supports and must not be silently accepted as a literal host.
  if (hostPort.includes('*')) return null

  const portMatch = /:(\d{1,5})$/.exec(hostPort)
  const host = portMatch ? hostPort.slice(0, -portMatch[0].length) : hostPort
  let port = portMatch?.[1] ?? null

  if (!HOST_OK.test(host)) return null

  // `*.dev` would hand out a whole TLD, which is never what anyone means.
  if (wildcard && !host.includes('.')) return null

  if (port !== null) {
    const n = Number(port)
    if (n < 1 || n > 65535) return null
    // A browser omits the default port, so keeping it would make an entry that
    // looks right never match.
    if (port === DEFAULT_PORT[scheme]) port = null
  }

  const authority = wildcard ? `*.${host}` : host
  return port ? `${scheme}://${authority}:${port}` : `${scheme}://${authority}`
}

/**
 * Normalize a whole list, dropping entries that cannot be parsed and any
 * duplicates that normalization collapses together.
 */
export function normalizeOrigins(origins: string[] | undefined | null): string[] {
  if (!origins) return []
  const seen = new Set<string>()
  for (const entry of origins) {
    const normalized = normalizeOrigin(entry)
    if (normalized) seen.add(normalized)
  }
  return [...seen]
}

/** The entries that could not be parsed, so the API can name them in a 400. */
export function invalidOrigins(origins: string[]): string[] {
  return origins.filter((entry) => normalizeOrigin(entry) === null)
}

function matchesWildcard(pattern: string, origin: string): boolean {
  const marker = pattern.indexOf('://*.')
  if (marker === -1) return false

  const prefix = `${pattern.slice(0, marker)}://`
  if (!origin.startsWith(prefix)) return false

  // `rest` carries the port when the pattern has one, so a wildcard scoped to
  // `:8443` cannot be satisfied on another port.
  const rest = pattern.slice(marker + '://*.'.length)
  const authority = origin.slice(prefix.length)

  // `endsWith('.' + rest)` and nothing else: `evil-netlify.app` ends with
  // `-netlify.app`, not `.netlify.app`, and must not match `*.netlify.app`.
  // The length check requires at least one real label in front, so the apex
  // does not match its own wildcard — list it separately if you want it.
  return authority.endsWith(`.${rest}`) && authority.length > rest.length + 1
}

/**
 * Whether a request presenting `presented` may use a key restricted to
 * `allowed`.
 *
 * Two decisions worth stating outright:
 *
 *  - An empty `allowed` permits everything. That is what keeps this backward
 *    compatible and what makes a server-side key ordinary.
 *  - A restricted key with *no* origin at all is refused. curl sends neither
 *    header, so accepting the absent case would leave the restriction as
 *    decoration — anyone who stripped the header would walk straight through.
 *    The cost is real and intended: an origin-restricted key cannot be used
 *    from a server.
 */
export function originAllowed(allowed: string[], presented: string | null | undefined): boolean {
  if (allowed.length === 0) return true
  if (!presented) return false

  // A caller-supplied value must never be read as a pattern; otherwise sending
  // `Origin: https://*.example.com` matches a wildcard entry by equality.
  if (presented.includes('*')) return false

  const origin = normalizeOrigin(presented)
  if (!origin) return false

  return allowed.some((pattern) => pattern === origin || matchesWildcard(pattern, origin))
}

/**
 * The origin a request claims, from the headers a browser actually sets.
 *
 * `Origin` is preferred and is present on CORS requests, which is what a tile
 * fetch is. `Referer` is the fallback for same-origin and navigation requests,
 * where `Origin` may be omitted; its path is stripped by `normalizeOrigin`.
 *
 * The literal string `null` is what a sandboxed iframe or an opaque origin
 * sends. It is an origin in the spec's sense and a non-answer in ours.
 */
export function presentedOrigin(headers: Record<string, string | undefined>): string | null {
  const origin = headers.origin
  if (origin && origin !== 'null') return origin
  return headers.referer || null
}
