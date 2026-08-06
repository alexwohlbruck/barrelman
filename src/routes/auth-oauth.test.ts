/**
 * HTTP-layer tests for /auth/oauth.
 *
 * The provider round trip is injected, so what is exercised here is the part
 * that protects the flow: state must round-trip through the httpOnly cookie,
 * the post-sign-in destination must stay on this origin, provider errors must
 * surface to the console rather than 500, and linking must not rotate an
 * existing session.
 */
import { describe, test, expect, mock } from 'bun:test'
import { createOAuthRoutes, type OAuthDeps } from './auth-oauth'
import { consoleOrigin } from '../config/accounts.config'

const BASE = 'http://localhost'
const STATE = 'state-token'
const VERIFIER = 'pkce-verifier'

function makeUser(overrides: Record<string, unknown> = {}) {
  return {
    id: 'user-1',
    email: 'user@example.com',
    name: null,
    picture: null,
    role: 'user',
    plan: 'free',
    suspendedAt: null,
    createdAt: new Date('2026-01-01T00:00:00Z'),
    ...overrides,
  } as never
}

const profile = {
  providerAccountId: '12345',
  email: 'user@example.com',
  emailVerified: true,
  name: 'A User',
  picture: null,
}

function deps(overrides: Partial<OAuthDeps> = {}): Partial<OAuthDeps> {
  return {
    createAuthorization: mock(() => ({
      url: 'https://accounts.google.com/o/oauth2/v2/auth?client_id=x',
      state: STATE,
      codeVerifier: VERIFIER,
    })),
    fetchProfile: mock(async () => profile),
    resolveOAuthSignIn: mock(async () => ({ ok: true as const, user: makeUser(), created: false })),
    listLinkedProviders: mock(async () => []),
    unlinkProvider: mock(async () => true),
    startSession: mock(async () => ({
      session: { id: 'session-1' } as never,
      cookie: 'barrelman_session=abc; Path=/',
    })),
    resolveSession: mock(async () => ({ user: null, session: null })),
    ...overrides,
  }
}

const signedIn = () => ({ resolveSession: mock(async () => ({ user: makeUser(), session: { id: 'session-1' } as never })) })

function pendingCookie(overrides: Record<string, unknown> = {}) {
  const value = JSON.stringify({ state: STATE, codeVerifier: VERIFIER, next: '/console/', ...overrides })
  return `barrelman_oauth=${encodeURIComponent(value)}`
}

function get(path: string, headers: Record<string, string> = {}) {
  return new Request(`${BASE}${path}`, { headers })
}

function cookies(res: Response): string[] {
  const all = res.headers.getSetCookie?.() ?? []
  if (all.length) return all
  const single = res.headers.get('set-cookie')
  return single ? [single] : []
}

/** The `error` query parameter the console shows on a failed sign-in. */
function redirectError(res: Response): string | null {
  const location = res.headers.get('location')
  return location ? new URL(location).searchParams.get('error') : null
}

describe('GET /auth/oauth/:provider', () => {
  test('404s for a provider that is not configured', async () => {
    const app = createOAuthRoutes(deps({ createAuthorization: mock(() => null) }))

    const res = await app.handle(get('/auth/oauth/nonsense'))

    expect(res.status).toBe(404)
  })
})

describe('GET /auth/oauth/:provider/callback', () => {
  test('signs in and redirects to the console with a session cookie', async () => {
    const d = deps()
    const app = createOAuthRoutes(d)

    const res = await app.handle(
      get(`/auth/oauth/google/callback?code=abc&state=${STATE}`, { cookie: pendingCookie() }),
    )

    expect(res.status).toBe(302)
    expect(res.headers.get('location')).toContain('/console/')
    expect(cookies(res).some((c) => c.startsWith('barrelman_session='))).toBe(true)
    expect(d.fetchProfile).toHaveBeenCalledWith('google', 'abc', VERIFIER)
  })

  test('clears the pending-authorization cookie once spent', async () => {
    const app = createOAuthRoutes(deps())

    const res = await app.handle(
      get(`/auth/oauth/google/callback?code=abc&state=${STATE}`, { cookie: pendingCookie() }),
    )

    expect(cookies(res).some((c) => c.startsWith('barrelman_oauth=') && c.includes('Max-Age=0'))).toBe(true)
  })

  test('refuses a state that does not match the cookie', async () => {
    const d = deps()
    const app = createOAuthRoutes(d)

    const res = await app.handle(
      get('/auth/oauth/google/callback?code=abc&state=forged', { cookie: pendingCookie() }),
    )

    // A mismatched state is the CSRF signal — the code must never be exchanged.
    expect(res.status).toBe(302)
    expect(redirectError(res)).toContain('could not be verified')
    expect(d.fetchProfile).not.toHaveBeenCalled()
  })

  test('refuses a callback with no pending authorization at all', async () => {
    const d = deps()
    const app = createOAuthRoutes(d)

    const res = await app.handle(get(`/auth/oauth/google/callback?code=abc&state=${STATE}`))

    expect(redirectError(res)).toContain('expired')
    expect(d.fetchProfile).not.toHaveBeenCalled()
  })

  test('refuses a callback that carries no code', async () => {
    const d = deps()
    const app = createOAuthRoutes(d)

    const res = await app.handle(get(`/auth/oauth/google/callback?state=${STATE}`, { cookie: pendingCookie() }))

    expect(redirectError(res)).toBeTruthy()
    expect(d.fetchProfile).not.toHaveBeenCalled()
  })

  test('surfaces a refusal from the provider', async () => {
    const app = createOAuthRoutes(deps())

    const res = await app.handle(
      get('/auth/oauth/google/callback?error=access_denied&error_description=User+said+no', {
        cookie: pendingCookie(),
      }),
    )

    expect(res.status).toBe(302)
    expect(redirectError(res)).toBe('User said no')
  })

  test('surfaces an unreachable provider rather than throwing', async () => {
    const app = createOAuthRoutes(
      deps({
        fetchProfile: mock(async () => {
          throw new Error('ECONNREFUSED')
        }),
      }),
    )

    const res = await app.handle(
      get(`/auth/oauth/google/callback?code=abc&state=${STATE}`, { cookie: pendingCookie() }),
    )

    expect(res.status).toBe(302)
    expect(redirectError(res)).toContain('Could not reach')
  })

  test('surfaces an unverified provider address without signing in', async () => {
    const d = deps({
      resolveOAuthSignIn: mock(async () => ({
        ok: false as const,
        error: 'Your github email address is not verified',
      })),
    })
    const app = createOAuthRoutes(d)

    const res = await app.handle(
      get(`/auth/oauth/github/callback?code=abc&state=${STATE}`, { cookie: pendingCookie() }),
    )

    expect(redirectError(res)).toContain('not verified')
    expect(d.startSession).not.toHaveBeenCalled()
  })

  test.each([
    ['//evil.example/phish', 'protocol-relative URL to another origin'],
    ['https://evil.example', 'absolute URL'],
    ['javascript:alert(1)', 'script URL'],
  ])('refuses to redirect to %s (%s)', async (next) => {
    const app = createOAuthRoutes(deps())

    const res = await app.handle(
      get(`/auth/oauth/google/callback?code=abc&state=${STATE}`, { cookie: pendingCookie({ next }) }),
    )

    const location = new URL(res.headers.get('location')!)
    expect(location.origin).toBe(consoleOrigin)
    expect(location.pathname).toBe('/console/')
  })

  test('honours a same-site destination', async () => {
    const app = createOAuthRoutes(deps())

    const res = await app.handle(
      get(`/auth/oauth/google/callback?code=abc&state=${STATE}`, {
        cookie: pendingCookie({ next: '/console/keys' }),
      }),
    )

    const location = new URL(res.headers.get('location')!)
    expect(location.origin).toBe(consoleOrigin)
    expect(location.pathname).toBe('/console/keys')
  })

  test('linking an extra provider leaves the existing session alone', async () => {
    const d = deps(signedIn())
    const app = createOAuthRoutes(d)

    const res = await app.handle(
      get(`/auth/oauth/github/callback?code=abc&state=${STATE}`, {
        cookie: `${pendingCookie({ linkUserId: 'user-1', next: '/console/account' })}`,
      }),
    )

    expect(res.status).toBe(302)
    expect(res.headers.get('location')).toContain('linked=github')
    // Rotating the session here would sign the user out of nothing useful.
    expect(d.startSession).not.toHaveBeenCalled()
    expect(d.resolveOAuthSignIn).toHaveBeenCalledWith(
      'github',
      expect.anything(),
      expect.objectContaining({ signedInUserId: 'user-1' }),
    )
  })

  test('recovers from a corrupted pending cookie', async () => {
    const app = createOAuthRoutes(deps())

    const res = await app.handle(
      get(`/auth/oauth/google/callback?code=abc&state=${STATE}`, { cookie: 'barrelman_oauth=not-json' }),
    )

    expect(res.status).toBe(302)
    expect(redirectError(res)).toContain('malformed')
  })
})

describe('linked provider management', () => {
  test.each([
    ['GET', '/auth/oauth/'],
    ['DELETE', '/auth/oauth/github'],
  ])('%s %s requires a session', async (method, path) => {
    const app = createOAuthRoutes(deps())

    expect((await app.handle(new Request(`${BASE}${path}`, { method }))).status).toBe(401)
  })

  test('lists linked providers', async () => {
    const app = createOAuthRoutes(
      deps({
        ...signedIn(),
        listLinkedProviders: mock(async () => [{ provider: 'github' as const, createdAt: new Date() }]),
      }),
    )

    const res = await app.handle(get('/auth/oauth/'))

    expect(res.status).toBe(200)
    expect((await res.json())[0].provider).toBe('github')
  })

  test('unlinks scoped to the calling account', async () => {
    const d = deps(signedIn())
    const app = createOAuthRoutes(d)

    const res = await app.handle(new Request(`${BASE}/auth/oauth/github`, { method: 'DELETE' }))

    expect(res.status).toBe(204)
    expect(d.unlinkProvider).toHaveBeenCalledWith('user-1', 'github')
  })

  test('404s when the provider was never linked', async () => {
    const app = createOAuthRoutes(deps({ ...signedIn(), unlinkProvider: mock(async () => false) }))

    const res = await app.handle(new Request(`${BASE}/auth/oauth/gitlab`, { method: 'DELETE' }))

    expect(res.status).toBe(404)
  })
})
