/**
 * Cookie serialisation for the short-lived cookies the auth ceremonies need
 * (WebAuthn challenges, OAuth state). Session cookies are Lucia's job.
 *
 * These are all `httpOnly` and `sameSite=lax`: lax rather than strict because
 * the OAuth callback is a top-level cross-site navigation back from the
 * provider, and a strict cookie would not be sent with it.
 */
import { isProduction } from '../config/accounts.config'

export interface CookieOptions {
  maxAgeSeconds?: number
  path?: string
  sameSite?: 'strict' | 'lax' | 'none'
  httpOnly?: boolean
}

export function serializeCookie(name: string, value: string, options: CookieOptions = {}): string {
  const { maxAgeSeconds = 600, path = '/', sameSite = 'lax', httpOnly = true } = options

  const parts = [`${name}=${encodeURIComponent(value)}`, `Path=${path}`, `Max-Age=${maxAgeSeconds}`, `SameSite=${sameSite[0]!.toUpperCase()}${sameSite.slice(1)}`]
  if (httpOnly) parts.push('HttpOnly')
  if (isProduction) parts.push('Secure')

  return parts.join('; ')
}

/** A cookie with the same attributes but no value, expiring immediately. */
export function clearCookie(name: string, options: CookieOptions = {}): string {
  return serializeCookie(name, '', { ...options, maxAgeSeconds: 0 })
}

export function readCookie(request: Request, name: string): string | null {
  const header = request.headers.get('cookie')
  if (!header) return null

  for (const pair of header.split(';')) {
    const index = pair.indexOf('=')
    if (index < 0) continue
    if (pair.slice(0, index).trim() !== name) continue
    return decodeURIComponent(pair.slice(index + 1).trim())
  }
  return null
}
