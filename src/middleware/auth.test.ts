import { describe, test, expect } from 'bun:test'
import { createAdminAuthHandler } from './auth'
import { ADMIN_SCOPE } from '../billing/plans'

/**
 * These cover who may reach `/admin/*` — full re-imports, DROP/TRUNCATE,
 * moderation. Plain functions rather than mock(): see the note in CLAUDE.md
 * about bun mocks defeating Elysia's hook compilation.
 */

const noSession = async () => ({ user: null }) as never
const adminSession = async () => ({ user: { id: 'u1', role: 'admin' } }) as never
const userSession = async () => ({ user: { id: 'u2', role: 'user' } }) as never
const noKey = async () => null

function ctx(token?: string) {
  const set: { status?: number | string } = {}
  return {
    set,
    headers: token ? { authorization: `Bearer ${token}` } : {},
    request: new Request('http://localhost/admin/verify'),
  }
}

function key(over: Record<string, unknown> = {}) {
  return {
    keyId: 'k1',
    userId: 'u1',
    scopes: [ADMIN_SCOPE],
    allowedOrigins: [],
    plan: 'free',
    role: 'admin',
    suspended: false,
    suspensionReason: null,
    ...over,
  }
}

describe('adminAuthHandler', () => {
  test('allows an admin-role session', async () => {
    const h = createAdminAuthHandler({ resolveSession: adminSession, resolveApiKey: noKey })
    const c = ctx()
    expect(await h(c)).toBeUndefined()
    expect(c.set.status).toBeUndefined()
  })

  test('refuses a signed-in non-admin with 403', async () => {
    const h = createAdminAuthHandler({ resolveSession: userSession, resolveApiKey: noKey })
    const c = ctx()
    expect(await h(c)).toMatchObject({ error: expect.stringContaining('Administrator') })
    expect(c.set.status).toBe(403)
  })

  test('refuses an anonymous caller with 401', async () => {
    const h = createAdminAuthHandler({ resolveSession: noSession, resolveApiKey: noKey })
    const c = ctx()
    expect(c.set.status).toBeUndefined()
    await h(c)
    expect(c.set.status).toBe(401)
  })

  test('allows an admin-scoped key owned by an admin', async () => {
    const h = createAdminAuthHandler({ resolveSession: noSession, resolveApiKey: async () => key() as never })
    const c = ctx('brm_live_x')
    expect(await h(c)).toBeUndefined()
    expect(c.set.status).toBeUndefined()
  })

  // The escalation that retiring BARRELMAN_ADMIN_KEY was meant to end: a data
  // key — including the default wildcard — must never reach /admin/*.
  test('refuses a wildcard key', async () => {
    const h = createAdminAuthHandler({
      resolveSession: noSession,
      resolveApiKey: async () => key({ scopes: ['*'] }) as never,
    })
    const c = ctx('brm_live_x')
    await h(c)
    expect(c.set.status).toBe(403)
  })

  test('refuses an admin-scoped key whose owner is not an admin', async () => {
    const h = createAdminAuthHandler({
      resolveSession: noSession,
      resolveApiKey: async () => key({ role: 'user' }) as never,
    })
    const c = ctx('brm_live_x')
    await h(c)
    expect(c.set.status).toBe(403)
  })

  test('refuses an unknown key', async () => {
    const h = createAdminAuthHandler({ resolveSession: noSession, resolveApiKey: noKey })
    const c = ctx('brm_live_bogus')
    await h(c)
    expect(c.set.status).toBe(403)
  })

  test('refuses a valid admin key on a suspended account', async () => {
    const h = createAdminAuthHandler({
      resolveSession: noSession,
      resolveApiKey: async () => key({ suspended: true, suspensionReason: 'abuse' }) as never,
    })
    const c = ctx('brm_live_x')
    expect(await h(c)).toMatchObject({ error: 'abuse' })
    expect(c.set.status).toBe(403)
  })

  test('fails closed with 503 when accounts are disabled', async () => {
    const h = createAdminAuthHandler({
      resolveSession: adminSession,
      resolveApiKey: async () => key() as never,
      accountsEnabled: false,
    })
    const c = ctx('brm_live_x')
    expect(await h(c)).toMatchObject({ error: expect.stringContaining('accounts') })
    expect(c.set.status).toBe(503)
  })
})
