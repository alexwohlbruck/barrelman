/**
 * HTTP-layer tests for the moderation API.
 *
 * Suspending an account is the most destructive thing an administrator can do
 * through the console — it ends every session and disables every key at once —
 * and it had no tests at all. What matters here is that the guard actually
 * guards, that the acting administrator is recorded truthfully, and that the
 * self-suspension footgun stays closed.
 */
import { describe, test, expect, mock } from 'bun:test'
import { createAdminUserRoutes, type AdminUserDeps } from './admin-users'

const BASE = 'http://localhost'
const ADMIN_ID = 'admin-1'
const TARGET_ID = 'user-2'

function suspendedUser(overrides: Record<string, unknown> = {}) {
  return {
    id: TARGET_ID,
    suspendedAt: new Date('2026-08-04T00:00:00Z'),
    suspendedReason: 'Terms of service violation',
    suspendedKind: 'tos-violation',
    suspendedUntil: null,
    ...overrides,
  } as never
}

function adminSession() {
  return mock(async () => ({ user: { id: ADMIN_ID, role: 'admin' } as never, session: null }))
}

function deps(overrides: Partial<AdminUserDeps> = {}): Partial<AdminUserDeps> {
  return {
    // NOTE: guards are plain functions, never `mock()`. Elysia compiles its
    // lifecycle chain by inspecting the hook function, and a bun mock defeats
    // that — a mocked guard returns its refusal as the response but the handler
    // still runs, so a test using one would report a passing guard while the
    // handler executed anyway. Verified against the real adminAuthHandler,
    // which short-circuits correctly. Call counts are tracked by hand below.
    guard: (async () => undefined) as never,
    suspendUser: mock(async () => suspendedUser()),
    unsuspendUser: mock(async () => ({ ...suspendedUser(), suspendedAt: null, suspendedReason: null }) as never),
    warnUser: mock(async () => undefined),
    addNote: mock(async () => undefined),
    resolveAbuseSignal: mock(async () => true),
    listAbuseSignals: mock(async () => []),
    countOpenSignals: mock(async () => 0),
    resolveSession: adminSession(),
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

const validSuspend = { reason: 'Automated bulk scraping', kind: 'tos-violation' }

describe('the guard', () => {
  test('every moderation route runs the admin guard', async () => {
    let calls = 0
    const guard = async () => {
      calls += 1
    }
    const app = createAdminUserRoutes(deps({ guard: guard as never }))

    await app.handle(new Request(`${BASE}/admin/users`))
    await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, validSuspend))
    await app.handle(new Request(`${BASE}/admin/abuse`))

    expect(calls).toBe(3)
  })

  test('a refusing guard blocks the handler entirely', async () => {
    const guard = async ({ set }: { set: { status?: number } }) => {
      set.status = 403
      return { error: 'Administrator access required' }
    }
    const d = deps({ guard: guard as never })
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, validSuspend))

    // The point of the guard: a refusal must stop the side effect, not merely
    // change the status code on a suspension that already happened.
    expect(res.status).toBe(403)
    expect(d.suspendUser).not.toHaveBeenCalled()
  })
})

describe('POST /admin/users/:id/suspend', () => {
  test('suspends and returns the resulting state', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, validSuspend))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.suspension.suspended).toBe(true)
    expect(body.suspension.reason).toBe('Terms of service violation')
  })

  test('records the acting administrator, not a placeholder', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, validSuspend))

    // The audit trail is the whole point of the moderation log; an action
    // attributed to nobody is not an audit trail.
    expect(d.suspendUser).toHaveBeenCalledWith(expect.objectContaining({ actorId: ADMIN_ID }))
  })

  test('records the shared key honestly when there is no session', async () => {
    const d = deps({ resolveSession: mock(async () => ({ user: null, session: null })) })
    const app = createAdminUserRoutes(d)

    await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, validSuspend))

    // We know the credential, not who held it — saying so beats inventing an id.
    expect(d.suspendUser).toHaveBeenCalledWith(expect.objectContaining({ actorId: 'admin-key' }))
  })

  test('refuses to let an administrator suspend themselves', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${ADMIN_ID}/suspend`, validSuspend))

    // Almost always a misclick on the wrong row, and it would lock the console's
    // own operator out mid-action.
    expect(res.status).toBe(400)
    expect(d.suspendUser).not.toHaveBeenCalled()
  })

  test('converts an hours value into an absolute expiry', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    const before = Date.now()
    await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, { ...validSuspend, hours: 6 }))

    const call = (d.suspendUser as ReturnType<typeof mock>).mock.calls[0]![0] as { until: Date | null }
    expect(call.until).toBeInstanceOf(Date)
    const delta = call.until!.getTime() - before
    expect(delta).toBeGreaterThan(5.9 * 60 * 60_000)
    expect(delta).toBeLessThan(6.1 * 60 * 60_000)
  })

  test('omitting hours means indefinite', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, validSuspend))

    expect(d.suspendUser).toHaveBeenCalledWith(expect.objectContaining({ until: null }))
  })

  test('404s for an account that does not exist', async () => {
    const app = createAdminUserRoutes(deps({ suspendUser: mock(async () => null) }))

    const res = await app.handle(post(`/admin/users/ghost/suspend`, validSuspend))

    expect(res.status).toBe(404)
  })

  test('rejects an empty or missing reason', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    const blank = await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, { ...validSuspend, reason: '' }))
    const missing = await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, { kind: 'abuse' }))

    // The reason is shown to the user verbatim; suspending with nothing to show
    // them is the outcome the whole flow exists to prevent.
    expect(blank.status).toBe(422)
    expect(missing.status).toBe(422)
    expect(d.suspendUser).not.toHaveBeenCalled()
  })

  test('rejects a category outside the known set', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    const res = await app.handle(
      post(`/admin/users/${TARGET_ID}/suspend`, { reason: 'because', kind: 'made-up-category' }),
    )

    expect(res.status).toBe(422)
    expect(d.suspendUser).not.toHaveBeenCalled()
  })
})

describe('POST /admin/users/:id/unsuspend', () => {
  test('reinstates and reports the cleared state', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${TARGET_ID}/unsuspend`, { reason: 'Appeal upheld' }))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.suspension.suspended).toBe(false)
    expect(d.unsuspendUser).toHaveBeenCalledWith(TARGET_ID, ADMIN_ID, 'Appeal upheld')
  })

  test('an administrator may reinstate themselves', async () => {
    // The self-suspension guard is about not locking yourself out; reinstating
    // is the recovery path and must never be blocked.
    const d = deps()
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${ADMIN_ID}/unsuspend`, {}))

    expect(res.status).toBe(200)
    expect(d.unsuspendUser).toHaveBeenCalled()
  })

  test('404s for an account that does not exist', async () => {
    const app = createAdminUserRoutes(deps({ unsuspendUser: mock(async () => null) }))

    expect((await app.handle(post('/admin/users/ghost/unsuspend', {}))).status).toBe(404)
  })
})

describe('warnings and notes', () => {
  test('a warning is logged against the account without restricting it', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${TARGET_ID}/warn`, { reason: 'Excessive retries' }))

    expect(res.status).toBe(200)
    expect(d.warnUser).toHaveBeenCalledWith(TARGET_ID, 'Excessive retries', ADMIN_ID)
    expect(d.suspendUser).not.toHaveBeenCalled()
  })

  test('a note is attributed to the acting administrator', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    await app.handle(post(`/admin/users/${TARGET_ID}/note`, { note: 'Spoke to them by email' }))

    expect(d.addNote).toHaveBeenCalledWith(TARGET_ID, 'Spoke to them by email', ADMIN_ID)
  })
})

describe('the abuse queue', () => {
  test('lists open signals with the throttle snapshot', async () => {
    const app = createAdminUserRoutes(
      deps({
        listAbuseSignals: mock(async () => [{ id: 'sig-1', kind: 'burn-rate', severity: 'high' } as never]),
        countOpenSignals: mock(async () => 1),
      }),
    )

    const res = await app.handle(new Request(`${BASE}/admin/abuse`))
    const body = await res.json()

    expect(res.status).toBe(200)
    expect(body.open).toBe(1)
    expect(body.signals[0].kind).toBe('burn-rate')
    expect(body.throttle).toBeDefined()
  })

  test('dismissing records who dismissed it', async () => {
    const d = deps()
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post('/admin/abuse/sig-1/resolve', {}))

    expect(res.status).toBe(200)
    expect(d.resolveAbuseSignal).toHaveBeenCalledWith('sig-1', ADMIN_ID)
  })

  test('404s on a signal already resolved', async () => {
    const app = createAdminUserRoutes(deps({ resolveAbuseSignal: mock(async () => false) }))

    expect((await app.handle(post('/admin/abuse/sig-1/resolve', {}))).status).toBe(404)
  })
})
