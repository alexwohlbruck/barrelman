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
import { LastAdminError } from '../services/accounts.service'

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
    setPlan: mock(async () => undefined),
    invalidateUserKeys: mock(() => undefined),
    findUserById: mock(async () => ({ id: TARGET_ID, plan: 'free' }) as never),
    setUserRole: mock(async () => ({ id: TARGET_ID, role: 'admin' }) as never),
    countAdmins: mock(async () => 2),
    resolveApiKey: mock(async () => null),
    resolveSession: adminSession(),
    ...overrides,
  }
}

function post(path: string, body: unknown, token?: string) {
  return new Request(`${BASE}${path}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
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

  test('attributes a key-authenticated caller to the key owner', async () => {
    // No session, but an admin-scoped API key resolves to a person — which is
    // the reason the anonymous shared admin secret was retired.
    const d = deps({
      resolveSession: mock(async () => ({ user: null, session: null })),
      resolveApiKey: mock(async () => ({ keyId: 'k1', userId: 'key-owner-9', scopes: ['admin'], role: 'admin' })),
    })
    const app = createAdminUserRoutes(d)

    await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, validSuspend, 'brm_live_admin'))

    expect(d.suspendUser).toHaveBeenCalledWith(expect.objectContaining({ actorId: 'key-owner-9' }))
  })

  test('falls back to a non-null placeholder when nothing resolves', async () => {
    const d = deps({
      resolveSession: mock(async () => ({ user: null, session: null })),
      resolveApiKey: mock(async () => null),
    })
    const app = createAdminUserRoutes(d)

    await app.handle(post(`/admin/users/${TARGET_ID}/suspend`, validSuspend))

    // The guard should make this unreachable; the audit row must not be null.
    expect(d.suspendUser).toHaveBeenCalledWith(expect.objectContaining({ actorId: 'unknown' }))
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

/**
 * Moving an account onto `demo` grants unmetered API access, so this endpoint
 * is a privileged action and is tested as one: who can reach it, what it
 * refuses, and whether it leaves a trail.
 */
describe('POST /admin/users/:id/plan', () => {
  const onPlan = (plan: string) =>
    deps({ findUserById: mock(async () => ({ id: TARGET_ID, plan }) as never) })

  test('assigns the plan and drops the cached key state', async () => {
    const d = onPlan('free')
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${TARGET_ID}/plan`, { plan: 'demo' }))

    expect(res.status).toBe(200)
    expect((await res.json()).plan.id).toBe('demo')
    expect(d.setPlan).toHaveBeenCalledWith(TARGET_ID, 'demo')
    // Keys cache the plan, so without this the account keeps billing until the
    // cache expires — and an operator watching the console sees nothing happen.
    expect(d.invalidateUserKeys).toHaveBeenCalledWith(TARGET_ID)
  })

  test('records the change against the acting administrator', async () => {
    const d = onPlan('free')
    const app = createAdminUserRoutes(d)

    await app.handle(post(`/admin/users/${TARGET_ID}/plan`, { plan: 'demo', reason: 'landing page hero' }))

    expect(d.addNote).toHaveBeenCalledWith(
      TARGET_ID,
      'Plan changed from free to demo: landing page hero',
      ADMIN_ID,
    )
  })

  test('refuses an unknown plan without touching the account', async () => {
    const d = onPlan('free')
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${TARGET_ID}/plan`, { plan: 'platinum' }))

    expect(res.status).toBe(400)
    expect(d.setPlan).not.toHaveBeenCalled()
  })

  test('refuses an inherited property masquerading as a plan', async () => {
    // `PLANS` is a plain object, so `'constructor' in PLANS` is true. Reaching
    // setPlan with that would write a plan id nothing can resolve, and the
    // account would silently fall back to free on every lookup.
    const d = onPlan('free')
    const app = createAdminUserRoutes(d)

    expect((await app.handle(post(`/admin/users/${TARGET_ID}/plan`, { plan: 'constructor' }))).status).toBe(400)
    expect(d.setPlan).not.toHaveBeenCalled()
  })

  test('404s for an account that does not exist', async () => {
    const d = deps({ findUserById: mock(async () => null) })
    const app = createAdminUserRoutes(d)

    expect((await app.handle(post(`/admin/users/${TARGET_ID}/plan`, { plan: 'demo' }))).status).toBe(404)
    expect(d.setPlan).not.toHaveBeenCalled()
  })

  test('a refusing guard blocks the plan change', async () => {
    const guard = async ({ set }: { set: { status?: number } }) => {
      set.status = 403
      return { error: 'Administrator access required' }
    }
    const d = deps({ guard: guard as never, findUserById: mock(async () => ({ id: TARGET_ID, plan: 'free' }) as never) })
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${TARGET_ID}/plan`, { plan: 'demo' }))

    expect(res.status).toBe(403)
    expect(d.setPlan).not.toHaveBeenCalled()
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

describe('POST /admin/users/:id/role', () => {
  test('promotes an account and records it', async () => {
    const d = deps({ setUserRole: mock(async () => ({ id: TARGET_ID, role: 'admin' })) })
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${TARGET_ID}/role`, { role: 'admin' }))
    expect(res.status).toBe(200)
    expect(d.setUserRole).toHaveBeenCalledWith(TARGET_ID, 'admin')
    // Granting admin reaches every destructive operation, so it is audited.
    expect(d.addNote).toHaveBeenCalled()
  })

  // With no shared admin secret, zero admins is only recoverable from psql.
  test('refuses to demote the last administrator with 409', async () => {
    const d = deps({
      setUserRole: mock(async () => {
        throw new LastAdminError('This is the only administrator.')
      }),
    })
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post(`/admin/users/${TARGET_ID}/role`, { role: 'user' }))
    expect(res.status).toBe(409)
    expect((await res.json()).error).toContain('only administrator')
  })

  test('404s for an unknown account', async () => {
    const d = deps({ findUserById: mock(async () => null) })
    const app = createAdminUserRoutes(d)

    const res = await app.handle(post('/admin/users/nope/role', { role: 'admin' }))
    expect(res.status).toBe(404)
  })

  test('rejects a role outside the enum', async () => {
    const app = createAdminUserRoutes(deps())
    const res = await app.handle(post(`/admin/users/${TARGET_ID}/role`, { role: 'superuser' }))
    expect(res.status).toBe(422)
  })

  test('is a no-op when the role already matches', async () => {
    const d = deps({ findUserById: mock(async () => ({ id: TARGET_ID, plan: 'free', role: 'user' }) as never) })
    const app = createAdminUserRoutes(d)
    const res = await app.handle(post(`/admin/users/${TARGET_ID}/role`, { role: 'user' }))
    expect(res.status).toBe(200)
    expect(d.setUserRole).not.toHaveBeenCalled()
  })
})
