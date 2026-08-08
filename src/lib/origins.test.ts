/**
 * Tests for API key origin restrictions.
 *
 * The matching rules are the whole feature, so the cases that matter are the
 * near-misses: a suffix that looks like a subdomain but is not, a caller
 * sending a pattern instead of an origin, and the absent-header case that
 * decides whether the restriction means anything at all.
 */
import { describe, test, expect } from 'bun:test'
import { invalidOrigins, normalizeOrigin, normalizeOrigins, originAllowed, presentedOrigin } from './origins'

describe('normalizeOrigin', () => {
  test('assumes https for a bare host', () => {
    expect(normalizeOrigin('barrelman.dev')).toBe('https://barrelman.dev')
  })

  test('keeps an explicit scheme and port', () => {
    expect(normalizeOrigin('http://localhost:5200')).toBe('http://localhost:5200')
  })

  test('drops path, query and fragment from a pasted URL', () => {
    expect(normalizeOrigin('https://barrelman.dev/docs?a=1#top')).toBe('https://barrelman.dev')
  })

  test('drops the default port, since a browser never sends it', () => {
    expect(normalizeOrigin('https://barrelman.dev:443')).toBe('https://barrelman.dev')
    expect(normalizeOrigin('http://barrelman.dev:80')).toBe('http://barrelman.dev')
  })

  test('keeps a non-default port', () => {
    expect(normalizeOrigin('https://barrelman.dev:8443')).toBe('https://barrelman.dev:8443')
  })

  test('lowercases and trims', () => {
    expect(normalizeOrigin('  HTTPS://Barrelman.DEV  ')).toBe('https://barrelman.dev')
  })

  test('accepts a subdomain wildcard', () => {
    expect(normalizeOrigin('*.netlify.app')).toBe('https://*.netlify.app')
    expect(normalizeOrigin('https://*.netlify.app')).toBe('https://*.netlify.app')
  })

  test('refuses a wildcard over a bare TLD', () => {
    expect(normalizeOrigin('*.dev')).toBeNull()
  })

  test('refuses a star anywhere but the leading label', () => {
    expect(normalizeOrigin('https://foo.*.example.com')).toBeNull()
    expect(normalizeOrigin('https://*.*.example.com')).toBeNull()
    expect(normalizeOrigin('https://ex*mple.com')).toBeNull()
  })

  test('refuses junk, credentials and empty input', () => {
    expect(normalizeOrigin('')).toBeNull()
    expect(normalizeOrigin('   ')).toBeNull()
    expect(normalizeOrigin('not a host')).toBeNull()
    expect(normalizeOrigin('https://user:pass@example.com')).toBeNull()
    expect(normalizeOrigin('https://-leading.example.com')).toBeNull()
    expect(normalizeOrigin('https://example..com')).toBeNull()
    expect(normalizeOrigin('https://example.com:99999')).toBeNull()
  })

  test('refuses a non-http scheme rather than reading it as a host', () => {
    // `ftp://x` has no `https?://` prefix, so the scheme would otherwise be
    // mistaken for a label and the whole thing accepted as a hostname.
    expect(normalizeOrigin('ftp://example.com')).toBeNull()
    expect(normalizeOrigin('javascript:alert(1)')).toBeNull()
  })
})

describe('normalizeOrigins', () => {
  test('drops unparseable entries and collapses duplicates', () => {
    expect(normalizeOrigins(['barrelman.dev', 'https://barrelman.dev/', 'nope nope', ''])).toEqual([
      'https://barrelman.dev',
    ])
  })

  test('is empty for null or undefined', () => {
    expect(normalizeOrigins(null)).toEqual([])
    expect(normalizeOrigins(undefined)).toEqual([])
  })

  test('names the entries it could not parse', () => {
    expect(invalidOrigins(['barrelman.dev', 'not a host', '*.dev'])).toEqual(['not a host', '*.dev'])
  })
})

describe('originAllowed', () => {
  const allowed = ['https://barrelman.dev', 'https://*.netlify.app', 'http://localhost:5200']

  test('permits everything when the list is empty', () => {
    expect(originAllowed([], null)).toBe(true)
    expect(originAllowed([], 'https://anywhere.example')).toBe(true)
  })

  test('matches an exact origin', () => {
    expect(originAllowed(allowed, 'https://barrelman.dev')).toBe(true)
  })

  test('matches through a Referer with a path', () => {
    expect(originAllowed(allowed, 'https://barrelman.dev/pricing')).toBe(true)
  })

  test('matches a subdomain against a wildcard', () => {
    expect(originAllowed(allowed, 'https://deploy--site.netlify.app')).toBe(true)
    expect(originAllowed(allowed, 'https://a.b.netlify.app')).toBe(true)
  })

  test('does not let a wildcard match its own apex', () => {
    expect(originAllowed(allowed, 'https://netlify.app')).toBe(false)
  })

  test('does not let a lookalike suffix match a wildcard', () => {
    // The attack this rule exists for: registering a domain that ends with the
    // allowed one as a plain string.
    expect(originAllowed(allowed, 'https://evil-netlify.app')).toBe(false)
    expect(originAllowed(allowed, 'https://netlify.app.evil.com')).toBe(false)
  })

  test('distinguishes scheme and port', () => {
    expect(originAllowed(allowed, 'http://barrelman.dev')).toBe(false)
    expect(originAllowed(allowed, 'http://localhost:5199')).toBe(false)
    expect(originAllowed(allowed, 'http://localhost:5200')).toBe(true)
  })

  test('refuses a restricted key presenting no origin at all', () => {
    // The case that decides whether this is a real restriction: curl sends
    // neither header, and accepting that would make the feature decoration.
    expect(originAllowed(allowed, null)).toBe(false)
    expect(originAllowed(allowed, undefined)).toBe(false)
    expect(originAllowed(allowed, '')).toBe(false)
  })

  test('refuses a caller that sends a pattern instead of an origin', () => {
    expect(originAllowed(allowed, 'https://*.netlify.app')).toBe(false)
    expect(originAllowed(allowed, '*.netlify.app')).toBe(false)
  })

  test('refuses an opaque origin', () => {
    expect(originAllowed(allowed, 'null')).toBe(false)
  })
})

describe('presentedOrigin', () => {
  test('prefers Origin', () => {
    expect(presentedOrigin({ origin: 'https://a.example', referer: 'https://b.example/x' })).toBe('https://a.example')
  })

  test('falls back to Referer when Origin is absent', () => {
    expect(presentedOrigin({ referer: 'https://b.example/x' })).toBe('https://b.example/x')
  })

  test('treats an opaque Origin as absent and falls through', () => {
    expect(presentedOrigin({ origin: 'null', referer: 'https://b.example/x' })).toBe('https://b.example/x')
  })

  test('is null when neither header is present', () => {
    expect(presentedOrigin({})).toBeNull()
  })
})
