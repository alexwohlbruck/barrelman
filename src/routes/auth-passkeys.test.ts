/**
 * HTTP-layer tests for /auth/passkeys.
 *
 * The WebAuthn crypto itself is @simplewebauthn's job and needs a real
 * authenticator, so it is injected. What is tested here is the surrounding
 * protocol: that the challenge is taken from the httpOnly cookie and never
 * from the client, that a spent challenge is cleared, that the management
 * routes are guarded, and that a verification failure yields no session.
 */
import { describe, test, expect, mock } from 'bun:test'
import { createPasskeyRoutes, type PasskeyDeps } from './auth-passkeys'

const BASE = 'http://localhost'
const CHALLENGE = 'challenge-from-the-server'

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

function deps(overrides: Partial<PasskeyDeps> = {}): Partial<PasskeyDeps> {
  return {
    beginRegistration: mock(async () => ({ challenge: CHALLENGE, rp: { id: 'localhost' } }) as never),
    completeRegistration: mock(async () => ({
      ok: true,
      passkey: { id: 'cred-1', name: 'iCloud Keychain' } as never,
    })),
    beginAuthentication: mock(async () => ({ challenge: CHALLENGE }) as never),
    completeAuthentication: mock(async () => ({ ok: true, user: makeUser() })),
    listPasskeys: mock(async () => []),
    renamePasskey: mock(async () => true),
    deletePasskey: mock(async () => true),
    startSession: mock(async () => ({
      session: { id: 'session-1' } as never,
      cookie: 'barrelman_session=abc; Path=/',
    })),
    resolveSession: mock(async () => ({ user: null, session: null })),
    ...overrides,
  }
}

const signedIn = () => ({ resolveSession: mock(async () => ({ user: makeUser(), session: { id: 'session-1' } as never })) })

function post(path: string, body: unknown = {}, headers: Record<string, string> = {}) {
  return new Request(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  })
}

const assertionBody = {
  id: 'cred-1',
  rawId: 'cred-1',
  type: 'public-key',
  response: { clientDataJSON: 'x', authenticatorData: 'y', signature: 'z' },
}

const attestationBody = {
  id: 'cred-1',
  rawId: 'cred-1',
  type: 'public-key',
  response: { clientDataJSON: 'x', attestationObject: 'y', transports: ['internal'] },
}

/** Every Set-Cookie value on a response, since several routes emit two. */
function cookies(res: Response): string[] {
  const all = res.headers.getSetCookie?.() ?? []
  if (all.length) return all
  const single = res.headers.get('set-cookie')
  return single ? [single] : []
}

describe('POST /auth/passkeys/authenticate/options', () => {
  test('returns options and stashes the challenge in an httpOnly cookie', async () => {
    const app = createPasskeyRoutes(deps())

    const res = await app.handle(post('/auth/passkeys/authenticate/options'))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.challenge).toBe(CHALLENGE)
    const cookie = cookies(res).find((c) => c.startsWith('barrelman_webauthn='))
    expect(cookie).toContain('HttpOnly')
    expect(cookie).toContain(CHALLENGE)
  })

  test('is available without a session', async () => {
    const app = createPasskeyRoutes(deps())

    expect((await app.handle(post('/auth/passkeys/authenticate/options'))).status).toBe(200)
  })
})

describe('POST /auth/passkeys/authenticate/verify', () => {
  test('signs in and issues a session when verification succeeds', async () => {
    const d = deps()
    const app = createPasskeyRoutes(d)

    const res = await app.handle(
      post('/auth/passkeys/authenticate/verify', assertionBody, { cookie: `barrelman_webauthn=${CHALLENGE}` }),
    )
    const body = await res.json()

    expect(res.status).toBe(201)
    expect(body.token).toBe('session-1')
    expect(body.user.id).toBe('user-1')
    // The challenge the service verified must be the one from the cookie.
    expect(d.completeAuthentication).toHaveBeenCalledWith(expect.anything(), CHALLENGE)
  })

  test('sets the session cookie and clears the spent challenge', async () => {
    const app = createPasskeyRoutes(deps())

    const res = await app.handle(
      post('/auth/passkeys/authenticate/verify', assertionBody, { cookie: `barrelman_webauthn=${CHALLENGE}` }),
    )

    const all = cookies(res)
    expect(all.some((c) => c.startsWith('barrelman_session='))).toBe(true)
    expect(all.some((c) => c.startsWith('barrelman_webauthn=') && c.includes('Max-Age=0'))).toBe(true)
  })

  test('refuses when no challenge cookie is present', async () => {
    const d = deps()
    const app = createPasskeyRoutes(d)

    const res = await app.handle(post('/auth/passkeys/authenticate/verify', assertionBody))

    expect(res.status).toBe(400)
    expect(d.completeAuthentication).not.toHaveBeenCalled()
  })

  test('ignores a challenge supplied in the request body', async () => {
    const d = deps()
    const app = createPasskeyRoutes(d)

    await app.handle(
      post(
        '/auth/passkeys/authenticate/verify',
        { ...assertionBody, challenge: 'attacker-chosen' },
        { cookie: `barrelman_webauthn=${CHALLENGE}` },
      ),
    )

    // Taking the challenge from the body would let a caller replay a captured
    // assertion against a challenge of their choosing.
    expect(d.completeAuthentication).toHaveBeenCalledWith(expect.anything(), CHALLENGE)
  })

  test('issues no session when verification fails', async () => {
    const d = deps({ completeAuthentication: mock(async () => ({ ok: false, error: 'Passkey not recognised' })) })
    const app = createPasskeyRoutes(d)

    const res = await app.handle(
      post('/auth/passkeys/authenticate/verify', assertionBody, { cookie: `barrelman_webauthn=${CHALLENGE}` }),
    )

    expect(res.status).toBe(401)
    expect(d.startSession).not.toHaveBeenCalled()
  })

  test('surfaces a cloned-credential rejection', async () => {
    const app = createPasskeyRoutes(
      deps({
        completeAuthentication: mock(async () => ({
          ok: false,
          error: 'Passkey signature counter did not advance — possible cloned credential',
        })),
      }),
    )

    const res = await app.handle(
      post('/auth/passkeys/authenticate/verify', assertionBody, { cookie: `barrelman_webauthn=${CHALLENGE}` }),
    )

    expect(res.status).toBe(401)
    expect((await res.json()).error).toContain('cloned')
  })

  test('refuses a suspended account', async () => {
    const app = createPasskeyRoutes(
      deps({ completeAuthentication: mock(async () => ({ ok: false, error: 'This account has been suspended' })) }),
    )

    const res = await app.handle(
      post('/auth/passkeys/authenticate/verify', assertionBody, { cookie: `barrelman_webauthn=${CHALLENGE}` }),
    )

    expect(res.status).toBe(401)
  })
})

describe('passkey registration', () => {
  test.each([
    ['/auth/passkeys/register/options'],
    ['/auth/passkeys/register/verify'],
  ])('%s requires a session', async (path) => {
    const app = createPasskeyRoutes(deps())

    expect((await app.handle(post(path, attestationBody))).status).toBe(401)
  })

  test('registration options are bound to the signed-in account', async () => {
    const d = deps(signedIn())
    const app = createPasskeyRoutes(d)

    const res = await app.handle(post('/auth/passkeys/register/options'))

    expect(res.status).toBe(200)
    expect(d.beginRegistration).toHaveBeenCalledWith('user-1', 'user@example.com')
  })

  test('verification uses the cookie challenge and returns the stored passkey', async () => {
    const d = deps(signedIn())
    const app = createPasskeyRoutes(d)

    const res = await app.handle(
      post('/auth/passkeys/register/verify', attestationBody, { cookie: `barrelman_webauthn=${CHALLENGE}` }),
    )
    const body = await res.json()

    expect(res.status).toBe(201)
    expect(body.name).toBe('iCloud Keychain')
    // Fourth argument is the user agent, absent on this synthetic request.
    expect(d.completeRegistration).toHaveBeenCalledWith('user-1', expect.anything(), CHALLENGE, null)
  })

  test('clears the challenge even when registration fails', async () => {
    const app = createPasskeyRoutes(
      deps({ ...signedIn(), completeRegistration: mock(async () => ({ ok: false, error: 'Already registered' })) }),
    )

    const res = await app.handle(
      post('/auth/passkeys/register/verify', attestationBody, { cookie: `barrelman_webauthn=${CHALLENGE}` }),
    )

    expect(res.status).toBe(400)
    expect(cookies(res).some((c) => c.startsWith('barrelman_webauthn=') && c.includes('Max-Age=0'))).toBe(true)
  })

  test('refuses verification with no challenge in progress', async () => {
    const app = createPasskeyRoutes(deps(signedIn()))

    expect((await app.handle(post('/auth/passkeys/register/verify', attestationBody))).status).toBe(400)
  })
})

describe('passkey management', () => {
  test.each([
    ['GET', '/auth/passkeys/'],
    ['PATCH', '/auth/passkeys/cred-1'],
    ['DELETE', '/auth/passkeys/cred-1'],
  ])('%s %s requires a session', async (method, path) => {
    const app = createPasskeyRoutes(deps())

    const res = await app.handle(
      new Request(`${BASE}${path}`, {
        method,
        headers: { 'content-type': 'application/json' },
        body: method === 'PATCH' ? JSON.stringify({ name: 'x' }) : undefined,
      }),
    )

    expect(res.status).toBe(401)
  })

  test('lists this account passkeys', async () => {
    const app = createPasskeyRoutes(
      deps({
        ...signedIn(),
        listPasskeys: mock(async () => [
          {
            id: 'cred-1',
            name: 'iCloud Keychain',
            deviceType: 'multiDevice',
            backedUp: true,
            lastUsedAt: null,
            createdAt: new Date(),
          },
        ]),
      }),
    )

    const res = await app.handle(new Request(`${BASE}/auth/passkeys/`))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body[0].name).toBe('iCloud Keychain')
  })

  test('rename and delete are scoped to the calling account', async () => {
    const d = deps(signedIn())
    const app = createPasskeyRoutes(d)

    await app.handle(
      new Request(`${BASE}/auth/passkeys/cred-1`, {
        method: 'PATCH',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ name: 'Work laptop' }),
      }),
    )
    await app.handle(new Request(`${BASE}/auth/passkeys/cred-1`, { method: 'DELETE' }))

    // Passing the user id is what stops one account touching another's key.
    expect(d.renamePasskey).toHaveBeenCalledWith('user-1', 'cred-1', 'Work laptop')
    expect(d.deletePasskey).toHaveBeenCalledWith('user-1', 'cred-1')
  })

  test('404s when the passkey belongs to someone else', async () => {
    const app = createPasskeyRoutes(deps({ ...signedIn(), deletePasskey: mock(async () => false) }))

    const res = await app.handle(new Request(`${BASE}/auth/passkeys/someone-elses`, { method: 'DELETE' }))

    expect(res.status).toBe(404)
  })
})
