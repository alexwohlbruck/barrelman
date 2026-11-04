/**
 * Tests for cookie serialisation and parsing.
 *
 * These carry WebAuthn challenges and OAuth state, so the attributes are
 * security-relevant: httpOnly by default, and SameSite=Lax rather than Strict
 * so the cross-site navigation back from an OAuth provider still presents them.
 */
import { describe, test, expect } from 'bun:test'
import { clearCookie, readCookie, serializeCookie } from './cookies'
import { cookiesSecure } from '../config/accounts.config'

describe('serializeCookie', () => {
  test('sets the defensive attributes by default', () => {
    const cookie = serializeCookie('barrelman_oauth', 'abc123')

    expect(cookie).toContain('barrelman_oauth=abc123')
    expect(cookie).toContain('HttpOnly')
    expect(cookie).toContain('Path=/')
    // Strict would break the OAuth callback, which is a top-level cross-site
    // navigation back from the provider.
    expect(cookie).toContain('SameSite=Lax')
  })

  test('percent-encodes values that would break the header', () => {
    const cookie = serializeCookie('state', '{"a":"b c";x}')

    expect(cookie).not.toContain('; x}')
    expect(cookie.split(';')[0]).toBe('state=%7B%22a%22%3A%22b%20c%22%3Bx%7D')
  })

  test('honours an explicit max age and sameSite', () => {
    const cookie = serializeCookie('x', 'y', { maxAgeSeconds: 42, sameSite: 'strict' })

    expect(cookie).toContain('Max-Age=42')
    expect(cookie).toContain('SameSite=Strict')
  })

  test('can opt out of httpOnly', () => {
    expect(serializeCookie('x', 'y', { httpOnly: false })).not.toContain('HttpOnly')
  })
})

describe('clearCookie', () => {
  test('expires immediately with an empty value', () => {
    const cookie = clearCookie('barrelman_webauthn')

    expect(cookie).toContain('barrelman_webauthn=')
    expect(cookie).toContain('Max-Age=0')
  })
})

describe('readCookie', () => {
  const withCookies = (header: string) => new Request('http://localhost/x', { headers: { cookie: header } })

  test('finds a cookie among several', () => {
    const request = withCookies('a=1; barrelman_oauth=wanted; z=3')

    expect(readCookie(request, 'barrelman_oauth')).toBe('wanted')
  })

  test('round-trips an encoded value', () => {
    const value = '{"state":"s","codeVerifier":"v"}'
    const serialized = serializeCookie('barrelman_oauth', value)
    const request = withCookies(serialized.split(';')[0]!)

    expect(readCookie(request, 'barrelman_oauth')).toBe(value)
  })

  test('returns null when absent or when no cookie header exists', () => {
    expect(readCookie(withCookies('a=1'), 'missing')).toBeNull()
    expect(readCookie(new Request('http://localhost/x'), 'anything')).toBeNull()
  })

  test('does not match on a name that is merely a prefix', () => {
    // `barrelman_session` must not be returned when asked for `barrelman_s`.
    const request = withCookies('barrelman_session=real')

    expect(readCookie(request, 'barrelman_s')).toBeNull()
    expect(readCookie(request, 'barrelman_session')).toBe('real')
  })

  test('tolerates whitespace and a trailing semicolon', () => {
    expect(readCookie(withCookies('  a=1 ;  b=2 ;'), 'b')).toBe('2')
  })
})

describe('cookiesSecure', () => {
  test('is on in production whatever the origin', () => {
    expect(cookiesSecure({ production: true, origins: ['http://localhost:5001'] })).toBe(true)
  })

  test('is on for an https origin without NODE_ENV=production', () => {
    // Compose sets no NODE_ENV, so a TLS deployment relied on the origin alone.
    expect(cookiesSecure({ production: false, origins: ['https://api.example.com'] })).toBe(true)
    expect(cookiesSecure({ production: false, origins: ['http://localhost:5001', 'HTTPS://console.example.com'] })).toBe(true)
  })

  test('stays off for plain-http local development', () => {
    expect(cookiesSecure({ production: false, origins: ['http://localhost:5001', 'http://localhost:5199'] })).toBe(false)
  })
})
