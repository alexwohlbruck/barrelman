import { describe, test, expect } from 'bun:test'
import Elysia from 'elysia'
import { createAdminAuthHandler, createAdminIpGuard } from './auth'
import { parseCidr } from '../lib/cidr'
import { ADMIN_SCOPE } from '../billing/plans'
import type { AdminAccessDeps } from '../services/admin-access.service'

/**
 * BARRELMAN_ADMIN_REQUIRE_PASSKEY and BARRELMAN_ADMIN_ALLOWED_IPS, as the admin
 * guard applies them. Guards are plain functions, never mock(): see CLAUDE.md.
 */

const enforced = { enforced: async () => true, startupCheck: async () => {} }
const notEnforced = { enforced: async () => false, startupCheck: async () => {} }
const openList = { configured: false, ranges: [] }

function access(over: Partial<AdminAccessDeps> = {}): AdminAccessDeps {
  return { allowlist: openList, passkeyPolicy: notEnforced, clientIp: () => '203.0.113.7', ...over }
}

const noSession = async () => ({ user: null, session: null }) as never
const noKey = async () => null
const sessionVia = (authMethod: string | null) => async () =>
  ({ user: { id: 'u1', role: 'admin' }, session: { id: 's1', authMethod } }) as never

function adminKey() {
  return {
    keyId: 'k1',
    userId: 'u1',
    scopes: [ADMIN_SCOPE],
    allowedOrigins: [],
    plan: 'free',
    role: 'admin',
    suspended: false,
    suspensionReason: null,
  } as never
}

function ctx(token?: string) {
  const set: { status?: number | string } = {}
  return {
    set,
    headers: token ? { authorization: `Bearer ${token}` } : {},
    request: new Request('http://localhost/admin/verify'),
  }
}

describe('passkey requirement', () => {
  test('refuses an email-code admin session once enforced, naming the fix', async () => {
    const h = createAdminAuthHandler({
      resolveSession: sessionVia('email'),
      resolveApiKey: noKey,
      access: access({ passkeyPolicy: enforced }),
    })
    const c = ctx()
    expect(await h(c)).toMatchObject({ code: 'passkey_required', error: expect.stringContaining('account page') })
    expect(c.set.status).toBe(403)
  })

  test('refuses a session from before the upgrade, which has no method recorded', async () => {
    const h = createAdminAuthHandler({
      resolveSession: sessionVia(null),
      resolveApiKey: noKey,
      access: access({ passkeyPolicy: enforced }),
    })
    const c = ctx()
    await h(c)
    expect(c.set.status).toBe(403)
  })

  test('allows a passkey admin session when enforced', async () => {
    const h = createAdminAuthHandler({
      resolveSession: sessionVia('passkey'),
      resolveApiKey: noKey,
      access: access({ passkeyPolicy: enforced }),
    })
    expect(await h(ctx())).toBeUndefined()
  })

  test('allows an email-code admin session while not enforced', async () => {
    const h = createAdminAuthHandler({ resolveSession: sessionVia('email'), resolveApiKey: noKey, access: access() })
    expect(await h(ctx())).toBeUndefined()
  })

  test('leaves admin-scoped API keys alone', async () => {
    const h = createAdminAuthHandler({
      resolveSession: noSession,
      resolveApiKey: async () => adminKey(),
      access: access({ passkeyPolicy: enforced }),
    })
    expect(await h(ctx('brm_live_x'))).toBeUndefined()
  })
})

describe('IP allowlist', () => {
  const allowlist = { configured: true, ranges: [parseCidr('10.0.0.0/8')!, parseCidr('2001:db8::/32')!] }

  test('allows an admin inside the list, IPv4 and IPv6', async () => {
    for (const ip of ['10.1.2.3', '2001:db8::1', '::ffff:10.9.9.9']) {
      const h = createAdminAuthHandler({
        resolveSession: sessionVia('email'),
        resolveApiKey: noKey,
        access: access({ allowlist, clientIp: () => ip }),
      })
      expect(await h(ctx())).toBeUndefined()
    }
  })

  test('refuses a valid admin key from outside it, before resolving the key', async () => {
    let keysLookedUp = 0
    const h = createAdminAuthHandler({
      resolveSession: noSession,
      resolveApiKey: async () => {
        keysLookedUp++
        return adminKey()
      },
      access: access({ allowlist, clientIp: () => '198.51.100.9' }),
    })
    const c = ctx('brm_live_x')
    expect(await h(c)).toMatchObject({ code: 'ip_not_allowed' })
    expect(c.set.status).toBe(403)
    expect(keysLookedUp).toBe(0)
  })

  test('refuses an admin session from outside it, and an unknown address', async () => {
    for (const ip of ['198.51.100.9', '2001:db9::1', 'unknown']) {
      const h = createAdminAuthHandler({
        resolveSession: sessionVia('passkey'),
        resolveApiKey: noKey,
        access: access({ allowlist, clientIp: () => ip }),
      })
      const c = ctx()
      await h(c)
      expect(c.set.status).toBe(403)
    }
  })

  test('the config route guard applies the same list', () => {
    const outside = createAdminIpGuard(access({ allowlist, clientIp: () => '198.51.100.9' }))
    const set: { status?: number | string } = {}
    expect(outside({ request: new Request('http://localhost/admin/config'), set })).toMatchObject({
      code: 'ip_not_allowed',
    })
    expect(set.status).toBe(403)

    const inside = createAdminIpGuard(access({ allowlist, clientIp: () => '10.0.0.1' }))
    expect(inside({ request: new Request('http://localhost/admin/config'), set: {} })).toBeUndefined()
  })
})

describe('mounted on a route', () => {
  async function hit(guard: ReturnType<typeof createAdminAuthHandler>) {
    let ran = 0
    const app = new Elysia().onBeforeHandle(guard).get('/admin/verify', () => {
      ran++
      return { ok: true }
    })
    const res = await app.handle(new Request('http://localhost/admin/verify'))
    return { status: res.status, ran }
  }

  test('a failed passkey check never reaches the handler', async () => {
    const r = await hit(
      createAdminAuthHandler({
        resolveSession: sessionVia('email'),
        resolveApiKey: noKey,
        access: access({ passkeyPolicy: enforced }),
      }),
    )
    expect(r).toEqual({ status: 403, ran: 0 })
  })

  test('a refused address never reaches the handler', async () => {
    const r = await hit(
      createAdminAuthHandler({
        resolveSession: sessionVia('passkey'),
        resolveApiKey: noKey,
        access: access({
          allowlist: { configured: true, ranges: [parseCidr('10.0.0.0/8')!] },
          clientIp: () => '198.51.100.9',
        }),
      }),
    )
    expect(r).toEqual({ status: 403, ran: 0 })
  })
})
