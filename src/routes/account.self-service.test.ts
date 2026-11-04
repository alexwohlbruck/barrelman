/**
 * Tests for the GDPR self-service routes: GET /account/export and the
 * deletion flow (POST /account/delete/request, /delete/passkey-options and
 * DELETE /account).
 *
 * What matters here is who can reach them and what they refuse: both need a
 * session, deletion needs a fresh confirmation on top of it, and an export
 * never carries a credential.
 */
import { describe, test, expect, mock } from 'bun:test'
import { createAccountRoutes, type AccountDeps } from './account'
import { buildAccountExport, type ExportSources } from '../services/account-export.service'
import { LastAdminError } from '../services/accounts.service'
import { BillingTeardownError } from '../services/billing.service'

const BASE = 'http://localhost'

function makeUser(overrides: Record<string, unknown> = {}) {
  return {
    id: 'user-1',
    email: 'User@Example.com',
    name: 'Ada',
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

const signedIn = () => mock(async () => ({ user: makeUser(), session: { id: 'session-1' } as never }))
const signedOut = () => mock(async () => ({ user: null, session: null }))

/** Full rows, secrets included, the way loadExportSources() reads them. */
function rawSources(): ExportSources {
  const created = new Date('2026-02-01T00:00:00Z')
  return {
    user: {
      ...(makeUser() as object),
      emailNormalized: 'user@example.com',
      polarCustomerId: 'polar-cust-1',
      subscriptionId: null,
      subscriptionStatus: null,
      subscriptionCancelAtPeriodEnd: false,
      subscriptionPeriodEnd: null,
      subscriptionModifiedAt: null,
      signupIpHash: 'iphash',
      suspendedBy: null,
      updatedAt: created,
    } as never,
    oauth: [{ provider: 'github', providerAccountId: '12345', userId: 'user-1', createdAt: created }],
    passkeys: [
      {
        id: 'credential-id-secretish',
        userId: 'user-1',
        name: 'iCloud Keychain',
        publicKey: 'PUBLIC-KEY-MATERIAL',
        counter: 7,
        deviceType: 'multiDevice',
        backedUp: true,
        transports: 'internal',
        lastUsedAt: created,
        createdAt: created,
      },
    ],
    apiKeys: [
      {
        id: 'key-1',
        userId: 'user-1',
        name: 'Production',
        hash: 'KEY-HASH-SHOULD-NOT-LEAVE',
        prefix: 'brm_live_ab12',
        last4: 'wxyz',
        scopes: ['*'],
        allowedOrigins: [],
        rateLimitExempt: false,
        lastUsedAt: created,
        revokedAt: null,
        expiresAt: null,
        createdAt: created,
      },
    ],
    sessions: [
      {
        id: 'SESSION-ID-IS-A-CREDENTIAL',
        userId: 'user-1',
        expiresAt: created,
        ipHash: 'session-ip-hash',
        userAgent: 'Mozilla/5.0',
        createdAt: created,
      },
    ],
    usage: [{ day: '2026-02-01', endpoint: 'search', requests: 10, credits: 10, rejected: 0 }],
    requestMetrics: {
      from: created.toISOString(),
      retentionDays: 90,
      totals: {} as never,
      byEndpoint: [],
    },
    ledger: [
      {
        id: 'ledger-1',
        userId: 'user-1',
        amount: 1000,
        kind: 'purchase',
        description: 'Credit pack',
        externalId: 'polar-order-1',
        createdAt: created,
      },
    ],
    moderation: [
      {
        id: 'mod-1',
        userId: 'user-1',
        action: 'warn',
        kind: null,
        reason: 'Please slow down',
        actorId: 'admin-user-id',
        metadata: null,
        createdAt: created,
      },
    ],
    abuseSignals: [],
    balance: null,
  }
}

/**
 * An in-memory stand-in for the server-side challenge store, with the same
 * contract as storeChallenge/consumeChallenge: one challenge per (user,
 * purpose), replaced on issue and deleted on any attempt.
 */
function challengeStore() {
  const stored = new Map<string, string>()
  return {
    storeChallenge: mock(async (userId: string, type: string, challenge: string) => {
      stored.set(`${userId}:${type}`, challenge)
    }),
    consumeChallenge: mock(async (userId: string, type: string, challenge: string) => {
      const key = `${userId}:${type}`
      const expected = stored.get(key)
      stored.delete(key)
      return expected === challenge
    }),
  }
}

function deps(overrides: Partial<AccountDeps> = {}): Partial<AccountDeps> {
  return {
    ...(challengeStore() as unknown as Pick<AccountDeps, 'storeChallenge' | 'consumeChallenge'>),
    findUserById: mock(async () => makeUser()),
    resolveSession: signedIn(),
    exportAccount: mock(async () => buildAccountExport(rawSources())),
    createOtp: mock(async () => '12345678'),
    verifyOtp: mock(async () => 'valid' as const),
    sendAccountDeletionCode: mock(async () => true),
    beginConfirmation: mock(async () => ({ challenge: 'chal', allowCredentials: [{ id: 'cred-1' }] }) as never),
    confirmPasskey: mock(async () => ({ ok: true })),
    deleteAccount: mock(async () => makeUser()),
    blankSessionCookie: () => 'barrelman_session=; Max-Age=0; Path=/',
    ...overrides,
  }
}

function app(overrides: Partial<AccountDeps> = {}) {
  const d = deps(overrides)
  // One router per test, so its rate-limit budget persists across requests.
  const routes = createAccountRoutes(d)
  return { d, handle: (req: Request) => routes.handle(req) }
}

function del(body: unknown, headers: Record<string, string> = {}) {
  return new Request(`${BASE}/account`, {
    method: 'DELETE',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  })
}

const post = (path: string) => new Request(`${BASE}${path}`, { method: 'POST' })

// ── Export ────────────────────────────────────────────────────────────

describe('GET /account/export', () => {
  test('requires a session', async () => {
    const { d, handle } = app({ resolveSession: signedOut() })
    const res = await handle(new Request(`${BASE}/account/export`))
    expect(res.status).toBe(401)
    expect(d.exportAccount).not.toHaveBeenCalled()
  })

  test('downloads as a JSON attachment', async () => {
    const { handle } = app()
    const res = await handle(new Request(`${BASE}/account/export`))
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toContain('application/json')
    expect(res.headers.get('content-disposition')).toMatch(/^attachment; filename="barrelman-account-.+\.json"$/)
    const body = await res.json()
    expect(body.profile.email).toBe('User@Example.com')
    expect(body.apiKeys[0].prefix).toBe('brm_live_ab12')
    expect(body.oauthIdentities[0]).toEqual({ provider: 'github', subject: '12345', linkedAt: expect.any(String) })
  })

  test('carries no credential: key hash, passkey key, session id', async () => {
    const { handle } = app()
    const text = await (await handle(new Request(`${BASE}/account/export`))).text()

    for (const secret of ['KEY-HASH-SHOULD-NOT-LEAVE', 'PUBLIC-KEY-MATERIAL', 'SESSION-ID-IS-A-CREDENTIAL', 'credential-id-secretish']) {
      expect(text).not.toContain(secret)
    }
    const body = JSON.parse(text)
    expect(body.apiKeys[0]).not.toHaveProperty('hash')
    expect(body.passkeys[0]).not.toHaveProperty('publicKey')
    expect(body.passkeys[0]).not.toHaveProperty('id')
    expect(body.sessions[0]).not.toHaveProperty('id')
    // No field anywhere is named like a secret.
    expect(text).not.toMatch(/"(hash|publicKey|token|accessToken|refreshToken|secret)"\s*:/)
  })

  test('names the moderating actor only as a role', async () => {
    const { handle } = app()
    const text = await (await handle(new Request(`${BASE}/account/export`))).text()
    expect(text).not.toContain('admin-user-id')
    expect(JSON.parse(text).moderationLog[0].actor).toBe('administrator')
  })

  test('is limited to once a minute per account', async () => {
    const { d, handle } = app()
    expect((await handle(new Request(`${BASE}/account/export`))).status).toBe(200)
    const second = await handle(new Request(`${BASE}/account/export`))
    expect(second.status).toBe(429)
    expect(Number(second.headers.get('retry-after'))).toBeGreaterThan(0)
    expect(d.exportAccount).toHaveBeenCalledTimes(1)
  })
})

// ── Deletion ──────────────────────────────────────────────────────────

describe('POST /account/delete/request', () => {
  test('requires a session', async () => {
    const { d, handle } = app({ resolveSession: signedOut() })
    expect((await handle(post('/account/delete/request'))).status).toBe(401)
    expect(d.createOtp).not.toHaveBeenCalled()
  })

  test('issues a deletion-only code to the account address', async () => {
    const { d, handle } = app()
    const res = await handle(post('/account/delete/request'))
    expect(res.status).toBe(201)
    expect(d.createOtp).toHaveBeenCalledWith('user-1', 'User@Example.com', 'delete-account')
    expect(d.sendAccountDeletionCode).toHaveBeenCalledWith('User@Example.com', '12345678')
  })

  test('is rate limited', async () => {
    const { handle } = app()
    for (let i = 0; i < 3; i++) expect((await handle(post('/account/delete/request'))).status).toBe(201)
    expect((await handle(post('/account/delete/request'))).status).toBe(429)
  })
})

describe('POST /account/delete/passkey-options', () => {
  test('stores the challenge server-side, bound to the account and purpose', async () => {
    const { d, handle } = app()
    const res = await handle(post('/account/delete/passkey-options'))
    expect(res.status).toBe(200)
    expect(d.storeChallenge).toHaveBeenCalledWith('user-1', 'delete-account-passkey', 'chal')
    // Nothing for the client to hold on to or swap.
    expect(res.headers.get('set-cookie')).toBeNull()
  })

  test('is rate limited per session', async () => {
    const { handle } = app()
    for (let i = 0; i < 10; i++) expect((await handle(post('/account/delete/passkey-options'))).status).toBe(200)
    expect((await handle(post('/account/delete/passkey-options'))).status).toBe(429)
  })

  test('refuses an account with no passkeys', async () => {
    const { handle } = app({ beginConfirmation: mock(async () => ({ challenge: 'c', allowCredentials: [] }) as never) })
    expect((await handle(post('/account/delete/passkey-options'))).status).toBe(409)
  })
})

describe('DELETE /account', () => {
  test('requires a session', async () => {
    const { d, handle } = app({ resolveSession: signedOut() })
    const res = await handle(del({ email: 'user@example.com', code: '12345678' }))
    expect(res.status).toBe(401)
    expect(d.deleteAccount).not.toHaveBeenCalled()
  })

  test('refuses an API key presented as a bearer', async () => {
    // With the real session resolver: a brm_ bearer is never treated as a
    // session, so the route sees no user at all.
    const d = deps()
    delete d.resolveSession
    const res = await createAccountRoutes(d).handle(
      del({ email: 'user@example.com', code: '12345678' }, { authorization: 'Bearer brm_live_abc' }),
    )
    expect(res.status).toBe(401)
    expect(d.deleteAccount).not.toHaveBeenCalled()
  })

  test('needs the email typed back', async () => {
    const { d, handle } = app()
    const res = await handle(del({ email: 'someone@else.com', code: '12345678' }))
    expect(res.status).toBe(400)
    expect(d.verifyOtp).not.toHaveBeenCalled()
    expect(d.deleteAccount).not.toHaveBeenCalled()
  })

  test('needs a code or a passkey, not just the session', async () => {
    const { d, handle } = app()
    const res = await handle(del({ email: 'user@example.com' }))
    expect(res.status).toBe(400)
    expect((await res.json()).reason).toBe('confirmation-required')
    expect(d.deleteAccount).not.toHaveBeenCalled()
  })

  test('refuses a wrong code', async () => {
    const { d, handle } = app({ verifyOtp: mock(async () => 'invalid' as const) })
    const res = await handle(del({ email: 'user@example.com', code: '00000000' }))
    expect(res.status).toBe(401)
    expect(d.deleteAccount).not.toHaveBeenCalled()
  })

  test('checks the code against deletion codes, never sign-in codes', async () => {
    const { d, handle } = app()
    await handle(del({ email: 'user@example.com', code: '12345678' }))
    expect(d.verifyOtp).toHaveBeenCalledWith('user-1', '12345678', 'delete-account')
  })

  test('deletes with a valid code and signs the user out', async () => {
    const { d, handle } = app()
    const res = await handle(del({ email: ' USER@example.com ', code: '12345678' }))
    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ deleted: true })
    expect(d.deleteAccount).toHaveBeenCalledWith('user-1', 'user-1')
    expect(res.headers.get('set-cookie')).toContain('barrelman_session=;')
  })

  /** An assertion whose clientDataJSON names `challenge`, as a browser encodes it. */
  const assertion = (challenge: string) => ({
    id: 'cred-1',
    rawId: 'cred-1',
    type: 'public-key',
    response: {
      clientDataJSON: Buffer.from(
        JSON.stringify({ type: 'webauthn.get', challenge, origin: 'http://localhost' }),
      ).toString('base64url'),
      authenticatorData: 'y',
      signature: 'z',
    },
  })

  test('a passkey needs a challenge issued for deletion', async () => {
    const { d, handle } = app()
    const res = await handle(del({ email: 'user@example.com', passkey: assertion('chal') }))
    expect(res.status).toBe(400)
    expect(d.confirmPasskey).not.toHaveBeenCalled()
    expect(d.deleteAccount).not.toHaveBeenCalled()
  })

  test('refuses a replayed sign-in assertion, even with its challenge in a cookie', async () => {
    // A session holder who captured a sign-in assertion: its signature is
    // genuine (confirmPasskey would accept it), but its challenge came from
    // /auth/passkeys/authenticate/options, not from the deletion flow.
    const { d, handle } = app()
    await handle(post('/account/delete/passkey-options'))
    const res = await handle(
      del(
        { email: 'user@example.com', passkey: assertion('sign-in-challenge') },
        { cookie: 'barrelman_webauthn=sign-in-challenge; barrelman_webauthn_confirm=sign-in-challenge' },
      ),
    )
    expect(res.status).toBe(400)
    expect(d.confirmPasskey).not.toHaveBeenCalled()
    expect(d.deleteAccount).not.toHaveBeenCalled()
  })

  test('deletes with a passkey answering the stored challenge', async () => {
    const { d, handle } = app()
    await handle(post('/account/delete/passkey-options'))
    const res = await handle(del({ email: 'user@example.com', passkey: assertion('chal') }))
    expect(res.status).toBe(200)
    expect(d.consumeChallenge).toHaveBeenCalledWith('user-1', 'delete-account-passkey', 'chal')
    expect(d.confirmPasskey).toHaveBeenCalledWith('user-1', expect.anything(), 'chal')
    expect(d.deleteAccount).toHaveBeenCalled()
  })

  test('a challenge answers one attempt only', async () => {
    const { d, handle } = app({ confirmPasskey: mock(async () => ({ ok: false, error: 'nope' })) })
    await handle(post('/account/delete/passkey-options'))
    const first = await handle(del({ email: 'user@example.com', passkey: assertion('chal') }))
    expect(first.status).toBe(401)
    const second = await handle(del({ email: 'user@example.com', passkey: assertion('chal') }))
    expect(second.status).toBe(400)
    expect(d.confirmPasskey).toHaveBeenCalledTimes(1)
    expect(d.deleteAccount).not.toHaveBeenCalled()
  })

  test('keeps the last administrator', async () => {
    const { handle } = app({
      deleteAccount: mock(async () => {
        throw new LastAdminError('This is the only administrator.')
      }),
    })
    const res = await handle(del({ email: 'user@example.com', code: '12345678' }))
    expect(res.status).toBe(409)
    expect(res.headers.get('set-cookie')).toBeNull()
  })

  test('reports a Polar failure as 502 and keeps the session', async () => {
    const { handle } = app({
      deleteAccount: mock(async () => {
        throw new BillingTeardownError('Polar is down')
      }),
    })
    const res = await handle(del({ email: 'user@example.com', code: '12345678' }))
    expect(res.status).toBe(502)
    expect(res.headers.get('set-cookie')).toBeNull()
  })
})
