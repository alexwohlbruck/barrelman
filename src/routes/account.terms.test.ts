/**
 * Tests for terms-of-service gating on /account.
 *
 * The rule is deliberately narrow: accepting the terms is required to mint a
 * NEW key, and nothing else. A version bump must never break a running
 * integration, so existing keys keep working — which is what most of these
 * assert.
 */
import { describe, test, expect, mock, beforeEach, afterEach } from 'bun:test'
import { createAccountRoutes, type AccountDeps } from './account'
import { terms as termsConfig } from '../config/accounts.config'

const tosVersion = termsConfig.version

const BASE = 'http://localhost'

function makeUser(overrides: Record<string, unknown> = {}) {
  return {
    id: 'user-1',
    email: 'user@example.com',
    name: null,
    picture: null,
    role: 'user',
    plan: 'free',
    suspendedAt: null,
    suspendedReason: null,
    suspendedKind: null,
    suspendedUntil: null,
    tosVersion: null,
    tosAcceptedAt: null,
    createdAt: new Date('2026-01-01T00:00:00Z'),
    ...overrides,
  } as never
}

function deps(overrides: Partial<AccountDeps> = {}): Partial<AccountDeps> {
  return {
    createApiKey: mock(async () => ({
      key: 'brm_live_x',
      record: { id: 'key-1', name: 'k' } as never,
    })),
    listApiKeys: mock(async () => []),
    renameApiKey: mock(async () => true),
    revokeApiKey: mock(async () => true),
    updateApiKeyScopes: mock(async () => true),
    getBalance: mock(async () => ({}) as never),
    listLedger: mock(async () => []),
    usageByDay: mock(async () => []),
    usageByKey: mock(async () => []),
    updateUser: mock(async () => makeUser()),
    acceptTerms: mock(async (_id: string, version: string) =>
      makeUser({ tosVersion: version, tosAcceptedAt: new Date() }),
    ),
    findUserById: mock(async () => makeUser()),
    resolveSession: mock(async () => ({ user: makeUser(), session: { id: 'session-1' } as never })),
    ...overrides,
  }
}

function post(path: string, body: unknown) {
  return new Request(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
}

/**
 * Terms are only enforced once an operator has terms to point at, so the tests
 * that exercise the gate configure a URL for their duration.
 */
const savedUrl = termsConfig.url

function withTerms(url: string) {
  termsConfig.url = url
}

beforeEach(() => {
  termsConfig.url = savedUrl
})

afterEach(() => {
  termsConfig.url = savedUrl
})

describe('GET /account', () => {
  test('reports terms and suspension state alongside the profile', async () => {
    const app = createAccountRoutes(deps())

    const res = await app.handle(new Request(`${BASE}/account`))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.user.email).toBe('user@example.com')
    expect(body.terms).toBeDefined()
    expect(body.terms.version).toBe(tosVersion)
    expect(body.suspension.suspended).toBe(false)
  })

  test('reads the account through rather than trusting the session snapshot', async () => {
    // A suspension applied after sign-in has to show up here, and the session
    // object is a snapshot from whenever the cookie was last validated.
    const d = deps({
      findUserById: mock(async () =>
        makeUser({ suspendedAt: new Date(), suspendedReason: 'Abuse', suspendedKind: 'abuse' }),
      ),
    })
    const app = createAccountRoutes(d)

    const body = await (await app.handle(new Request(`${BASE}/account`))).json()

    expect(d.findUserById).toHaveBeenCalledWith('user-1')
    expect(body.suspension.suspended).toBe(true)
    expect(body.suspension.reason).toBe('Abuse')
  })

  test('marks a conduct suspension appealable and a billing one not', async () => {
    const conduct = createAccountRoutes(
      deps({
        findUserById: mock(async () =>
          makeUser({ suspendedAt: new Date(), suspendedKind: 'tos-violation', suspendedReason: 'x' }),
        ),
      }),
    )
    const billing = createAccountRoutes(
      deps({
        findUserById: mock(async () =>
          makeUser({ suspendedAt: new Date(), suspendedKind: 'billing', suspendedReason: 'y' }),
        ),
      }),
    )

    expect((await (await conduct.handle(new Request(`${BASE}/account`))).json()).suspension.appealable).toBe(true)
    // A billing hold clears itself once payment succeeds — there is nothing to appeal.
    expect((await (await billing.handle(new Request(`${BASE}/account`))).json()).suspension.appealable).toBe(false)
  })
})

describe('POST /account/accept-terms', () => {
  test('records acceptance of the current version', async () => {
    const d = deps()
    const app = createAccountRoutes(d)

    const res = await app.handle(post('/account/accept-terms', { version: tosVersion }))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(d.acceptTerms).toHaveBeenCalledWith('user-1', tosVersion)
    expect(body.terms.acceptedVersion).toBe(tosVersion)
  })

  test('refuses to accept a stale version', async () => {
    const d = deps()
    const app = createAccountRoutes(d)

    const res = await app.handle(post('/account/accept-terms', { version: 'ancient' }))

    // Marking someone up to date against text they never saw is not consent.
    expect(res.status).toBe(409)
    expect(d.acceptTerms).not.toHaveBeenCalled()
  })

  test('requires a session', async () => {
    const app = createAccountRoutes(deps({ resolveSession: mock(async () => ({ user: null, session: null })) }))

    expect((await app.handle(post('/account/accept-terms', { version: tosVersion }))).status).toBe(401)
  })
})

describe('key creation and outstanding terms', () => {
  test('creates a key when terms are accepted or not required', async () => {
    const d = deps({ findUserById: mock(async () => makeUser({ tosVersion, tosAcceptedAt: new Date() })) })
    const app = createAccountRoutes(d)

    const res = await app.handle(post('/account/keys', { name: 'my key' }))

    expect(res.status).toBe(201)
    expect(d.createApiKey).toHaveBeenCalled()
  })

  test('refuses a new key while terms are outstanding', async () => {
    withTerms('https://example.com/terms')
    const d = deps({ findUserById: mock(async () => makeUser({ tosVersion: null })) })
    const app = createAccountRoutes(d)

    const res = await app.handle(post('/account/keys', { name: 'my key' }))
    const body = await res.json()

    expect(res.status).toBe(451)
    expect(body.terms.outstanding).toBe(true)
    expect(d.createApiKey).not.toHaveBeenCalled()
  })

  test('refuses a new key after a version bump the user has not accepted', async () => {
    withTerms('https://example.com/terms')
    const d = deps({ findUserById: mock(async () => makeUser({ tosVersion: 'previous-version' })) })
    const app = createAccountRoutes(d)

    expect((await app.handle(post('/account/keys', { name: 'my key' }))).status).toBe(451)
  })

  test('allows a new key once the current version is accepted', async () => {
    withTerms('https://example.com/terms')
    const d = deps({ findUserById: mock(async () => makeUser({ tosVersion, tosAcceptedAt: new Date() })) })
    const app = createAccountRoutes(d)

    expect((await app.handle(post('/account/keys', { name: 'my key' }))).status).toBe(201)
  })

  test('never blocks listing or revoking existing keys on terms', async () => {
    // A version bump must not break a running integration — only the creation
    // of new keys is gated.
    withTerms('https://example.com/terms')
    const d = deps({ findUserById: mock(async () => makeUser({ tosVersion: 'old-version' })) })
    const app = createAccountRoutes(d)

    expect((await app.handle(new Request(`${BASE}/account/keys`))).status).toBe(200)
    expect(
      (await app.handle(new Request(`${BASE}/account/keys/key-1`, { method: 'DELETE' }))).status,
    ).toBe(204)
  })
})
