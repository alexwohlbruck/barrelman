/**
 * HTTP-layer tests for /auth.
 *
 * Every dependency is injected through createAuthRoutes(), so none of this
 * touches Postgres, Lucia or SMTP. Covers:
 *   - the code-request flow, including that it never discloses account existence
 *   - sign-up gating (disposable addresses, invite-only, per-IP budget)
 *   - code verification and its distinct failure reasons
 *   - session read/sign-out, and that the guarded routes actually 401
 *
 * The rate limiters live at module scope and are shared by every test in the
 * file, so each test uses a distinct email and source IP to stay independent.
 */
import { describe, test, expect, mock } from 'bun:test'
import { createAuthRoutes, type AuthDeps } from './auth'
import { SignupError } from '../services/accounts.service'

const BASE = 'http://localhost'

let counter = 0
/** Unique email + IP per call, so module-scoped limiters never bleed between tests. */
function identity() {
  counter += 1
  return { email: `user${counter}@example.com`, ip: `203.0.113.${counter % 250}` }
}

function makeUser(overrides: Record<string, unknown> = {}) {
  return {
    id: 'user-1',
    email: 'user@example.com',
    emailNormalized: 'user@example.com',
    name: null,
    picture: null,
    role: 'user',
    plan: 'free',
    polarCustomerId: null,
    signupIpHash: null,
    suspendedAt: null,
    createdAt: new Date('2026-01-01T00:00:00Z'),
    updatedAt: new Date('2026-01-01T00:00:00Z'),
    ...overrides,
  } as never
}

function makeSession(overrides: Record<string, unknown> = {}) {
  return { id: 'session-1', userId: 'user-1', fresh: false, expiresAt: new Date(), ...overrides } as never
}

/** Deps that succeed by default; each test overrides only what it cares about. */
function deps(overrides: Partial<AuthDeps> = {}): Partial<AuthDeps> {
  return {
    findUserByEmail: mock(async () => null),
    findOrCreateUser: mock(async () => ({ user: makeUser(), created: true })),
    consumeSignupAllowance: mock(async () => true),
    createOtp: mock(async () => '12345678'),
    verifyOtp: mock(async () => 'valid' as const),
    sendVerificationCode: mock(async () => true),
    startSession: mock(async () => ({ session: makeSession(), cookie: 'barrelman_session=abc; Path=/' })),
    endSession: mock(async () => 'barrelman_session=; Max-Age=0'),
    endAllSessions: mock(async () => undefined),
    endOtherSessions: mock(async () => undefined),
    listSessions: mock(async () => []),
    deleteSession: mock(async () => true),
    resolveSession: mock(async () => ({ user: null, session: null })),
    ...overrides,
  }
}

function post(path: string, body: unknown, ip = '203.0.113.1') {
  return new Request(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-forwarded-for': ip },
    body: JSON.stringify(body),
  })
}

describe('GET /auth/config', () => {
  test('advertises the available sign-in methods without auth', async () => {
    const app = createAuthRoutes(deps())

    const res = await app.handle(new Request(`${BASE}/auth/config`))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.methods.email).toBe(true)
    expect(body.methods.passkey).toBe(true)
    expect(Array.isArray(body.methods.oauth)).toBe(true)
    expect(body.registrationMode).toBeDefined()
  })
})

describe('POST /auth/request-code', () => {
  test('creates the account and emails a code on first sign-in', async () => {
    const { email, ip } = identity()
    const d = deps()
    const app = createAuthRoutes(d)

    const res = await app.handle(post('/auth/request-code', { email }, ip))

    expect(res.status).toBe(201)
    expect(await res.json()).toEqual({ sent: true })
    expect(d.findOrCreateUser).toHaveBeenCalled()
    expect(d.createOtp).toHaveBeenCalled()
    expect(d.sendVerificationCode).toHaveBeenCalledWith(email, '12345678')
  })

  test('reports the same success for an existing account', async () => {
    const { email, ip } = identity()
    const d = deps({
      findUserByEmail: mock(async () => makeUser()),
      findOrCreateUser: mock(async () => ({ user: makeUser(), created: false })),
    })
    const app = createAuthRoutes(d)

    const res = await app.handle(post('/auth/request-code', { email }, ip))

    expect(res.status).toBe(201)
    expect(await res.json()).toEqual({ sent: true })
  })

  test('does not spend the sign-up budget when the account already exists', async () => {
    const { email, ip } = identity()
    const d = deps({ findUserByEmail: mock(async () => makeUser()) })
    const app = createAuthRoutes(d)

    await app.handle(post('/auth/request-code', { email }, ip))

    // An office behind one NAT address must be able to sign in indefinitely.
    expect(d.consumeSignupAllowance).not.toHaveBeenCalled()
  })

  test('still reports success when delivery fails', async () => {
    const { email, ip } = identity()
    const app = createAuthRoutes(deps({ sendVerificationCode: mock(async () => false) }))

    const res = await app.handle(post('/auth/request-code', { email }, ip))

    // Whether an address is deliverable is not something an anonymous caller
    // gets to probe for.
    expect(res.status).toBe(201)
  })

  test('rejects a malformed address before touching the store', async () => {
    const d = deps()
    const app = createAuthRoutes(d)

    const res = await app.handle(post('/auth/request-code', { email: 'not-an-email' }, '203.0.113.200'))

    expect(res.status).toBe(400)
    expect(d.findOrCreateUser).not.toHaveBeenCalled()
  })

  test('refuses a disposable address with a reason', async () => {
    const { email, ip } = identity()
    const app = createAuthRoutes(
      deps({
        findOrCreateUser: mock(async () => {
          throw new SignupError('disposable-email', 'Addresses at mailinator.com are not accepted')
        }),
      }),
    )

    const res = await app.handle(post('/auth/request-code', { email }, ip))
    const body = await res.json()

    expect(res.status).toBe(403)
    expect(body.reason).toBe('disposable-email')
  })

  test('refuses new accounts on an invite-only instance', async () => {
    const { email, ip } = identity()
    const app = createAuthRoutes(
      deps({
        findOrCreateUser: mock(async () => {
          throw new SignupError('invite-only', 'This instance is invite-only')
        }),
      }),
    )

    const res = await app.handle(post('/auth/request-code', { email }, ip))

    expect(res.status).toBe(403)
    expect((await res.json()).reason).toBe('invite-only')
  })

  test('refuses once the per-address sign-up budget is spent', async () => {
    const { email, ip } = identity()
    const d = deps({ consumeSignupAllowance: mock(async () => false) })
    const app = createAuthRoutes(d)

    const res = await app.handle(post('/auth/request-code', { email }, ip))

    expect(res.status).toBe(429)
    expect(d.createOtp).not.toHaveBeenCalled()
  })

  test('rate-limits repeated requests for one address', async () => {
    const { email, ip } = identity()
    const app = createAuthRoutes(deps())

    // Limit is 5 per 15 minutes; the sixth must be refused.
    const statuses: number[] = []
    for (let i = 0; i < 6; i += 1) {
      statuses.push((await app.handle(post('/auth/request-code', { email }, ip))).status)
    }

    expect(statuses.slice(0, 5)).toEqual([201, 201, 201, 201, 201])
    expect(statuses[5]).toBe(429)
  })
})

describe('POST /auth/verify-code', () => {
  test('issues a session and sets a cookie on a valid code', async () => {
    const { email, ip } = identity()
    const app = createAuthRoutes(deps({ findUserByEmail: mock(async () => makeUser({ email })) }))

    const res = await app.handle(post('/auth/verify-code', { email, code: '12345678' }, ip))
    const body = await res.json()

    expect(res.status).toBe(201)
    expect(body.token).toBe('session-1')
    expect(body.user.email).toBe(email)
    expect(res.headers.get('set-cookie')).toContain('barrelman_session=')
  })

  test('never returns internal account columns', async () => {
    const { email, ip } = identity()
    const app = createAuthRoutes(
      deps({ findUserByEmail: mock(async () => makeUser({ email, signupIpHash: 'deadbeef' })) }),
    )

    const res = await app.handle(post('/auth/verify-code', { email, code: '12345678' }, ip))
    const body = await res.json()

    expect(body.user.signupIpHash).toBeUndefined()
    expect(body.user.emailNormalized).toBeUndefined()
    expect(Object.keys(body.user).sort()).toEqual(['createdAt', 'email', 'id', 'name', 'picture', 'plan', 'role'])
  })

  test('answers 401 identically for an unknown account and a wrong code', async () => {
    const a = identity()
    const b = identity()

    const unknown = createAuthRoutes(deps({ findUserByEmail: mock(async () => null) }))
    const wrong = createAuthRoutes(
      deps({
        findUserByEmail: mock(async () => makeUser()),
        verifyOtp: mock(async () => 'invalid' as const),
      }),
    )

    const resUnknown = await unknown.handle(post('/auth/verify-code', { email: a.email, code: '11111111' }, a.ip))
    const resWrong = await wrong.handle(post('/auth/verify-code', { email: b.email, code: '11111111' }, b.ip))

    expect(resUnknown.status).toBe(401)
    expect(resWrong.status).toBe(401)
    // Same message too — a different one would be an existence oracle.
    expect((await resUnknown.json()).error).toBe((await resWrong.json()).error)
  })

  test('distinguishes an expired code so the UI can offer a new one', async () => {
    const { email, ip } = identity()
    const app = createAuthRoutes(
      deps({
        findUserByEmail: mock(async () => makeUser()),
        verifyOtp: mock(async () => 'expired' as const),
      }),
    )

    const res = await app.handle(post('/auth/verify-code', { email, code: '12345678' }, ip))
    const body = await res.json()

    expect(res.status).toBe(401)
    expect(body.reason).toBe('expired')
    expect(body.error).toContain('expired')
  })

  test('reports a burned code after too many wrong guesses', async () => {
    const { email, ip } = identity()
    const app = createAuthRoutes(
      deps({
        findUserByEmail: mock(async () => makeUser()),
        verifyOtp: mock(async () => 'too-many-attempts' as const),
      }),
    )

    const res = await app.handle(post('/auth/verify-code', { email, code: '12345678' }, ip))

    expect((await res.json()).reason).toBe('too-many-attempts')
  })

  test('refuses a suspended account before checking the code', async () => {
    const { email, ip } = identity()
    const d = deps({ findUserByEmail: mock(async () => makeUser({ suspendedAt: new Date() })) })
    const app = createAuthRoutes(d)

    const res = await app.handle(post('/auth/verify-code', { email, code: '12345678' }, ip))

    expect(res.status).toBe(403)
    expect(d.verifyOtp).not.toHaveBeenCalled()
  })

  test('does not issue a session when the code is rejected', async () => {
    const { email, ip } = identity()
    const d = deps({
      findUserByEmail: mock(async () => makeUser()),
      verifyOtp: mock(async () => 'invalid' as const),
    })
    const app = createAuthRoutes(d)

    await app.handle(post('/auth/verify-code', { email, code: '00000000' }, ip))

    expect(d.startSession).not.toHaveBeenCalled()
  })
})

describe('GET /auth/session', () => {
  test('returns 204 when not signed in', async () => {
    const app = createAuthRoutes(deps())

    const res = await app.handle(new Request(`${BASE}/auth/session`))

    // The console polls this to decide whether to show the sign-in screen, so
    // "not signed in" must be a success, not a 401.
    expect(res.status).toBe(204)
  })

  test('returns the account when signed in', async () => {
    const app = createAuthRoutes(
      deps({ resolveSession: mock(async () => ({ user: makeUser(), session: makeSession() })) }),
    )

    const res = await app.handle(new Request(`${BASE}/auth/session`))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.user.id).toBe('user-1')
    expect(body.token).toBe('session-1')
  })
})

describe('DELETE /auth/session', () => {
  test('clears the cookie and ends the session', async () => {
    const d = deps({ resolveSession: mock(async () => ({ user: makeUser(), session: makeSession() })) })
    const app = createAuthRoutes(d)

    const res = await app.handle(new Request(`${BASE}/auth/session`, { method: 'DELETE' }))

    expect(res.status).toBe(204)
    expect(d.endSession).toHaveBeenCalledWith('session-1')
    expect(res.headers.get('set-cookie')).toContain('Max-Age=0')
  })

  test('is a no-op for an anonymous caller', async () => {
    const d = deps()
    const app = createAuthRoutes(d)

    const res = await app.handle(new Request(`${BASE}/auth/session`, { method: 'DELETE' }))

    expect(res.status).toBe(204)
    expect(d.endSession).not.toHaveBeenCalled()
  })
})

describe('session management routes', () => {
  const signedIn = () => ({ resolveSession: mock(async () => ({ user: makeUser(), session: makeSession() })) })

  test.each([
    ['GET', '/auth/sessions'],
    ['DELETE', '/auth/sessions'],
    ['DELETE', '/auth/sessions/other-session'],
  ])('%s %s requires a session', async (method, path) => {
    const app = createAuthRoutes(deps())

    const res = await app.handle(new Request(`${BASE}${path}`, { method }))

    expect(res.status).toBe(401)
  })

  test('lists sessions and marks the current one', async () => {
    const app = createAuthRoutes(
      deps({
        ...signedIn(),
        listSessions: mock(async () => [
          { id: 'session-1', userAgent: 'Firefox', createdAt: new Date(), expiresAt: new Date(), current: true },
          { id: 'session-2', userAgent: 'curl', createdAt: new Date(), expiresAt: new Date(), current: false },
        ]),
      }),
    )

    const res = await app.handle(new Request(`${BASE}/auth/sessions`))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body).toHaveLength(2)
    expect(body[0].current).toBe(true)
  })

  test('scope=others keeps the calling session alive', async () => {
    const d = deps(signedIn())
    const app = createAuthRoutes(d)

    const res = await app.handle(new Request(`${BASE}/auth/sessions?scope=others`, { method: 'DELETE' }))

    expect(res.status).toBe(204)
    expect(d.endOtherSessions).toHaveBeenCalledWith('user-1', 'session-1')
    expect(d.endAllSessions).not.toHaveBeenCalled()
  })

  test('the default scope ends every session including this one', async () => {
    const d = deps(signedIn())
    const app = createAuthRoutes(d)

    const res = await app.handle(new Request(`${BASE}/auth/sessions`, { method: 'DELETE' }))

    expect(res.status).toBe(204)
    expect(d.endAllSessions).toHaveBeenCalledWith('user-1')
    expect(res.headers.get('set-cookie')).toContain('Max-Age=0')
  })

  test('revoking one session scopes the delete to the caller', async () => {
    const d = deps(signedIn())
    const app = createAuthRoutes(d)

    const res = await app.handle(new Request(`${BASE}/auth/sessions/session-2`, { method: 'DELETE' }))

    expect(res.status).toBe(204)
    // Passing the user id is what stops one account revoking another's session.
    expect(d.deleteSession).toHaveBeenCalledWith('user-1', 'session-2')
  })

  test('404s when the session does not belong to the caller', async () => {
    const app = createAuthRoutes(deps({ ...signedIn(), deleteSession: mock(async () => false) }))

    const res = await app.handle(new Request(`${BASE}/auth/sessions/someone-elses`, { method: 'DELETE' }))

    expect(res.status).toBe(404)
  })
})
