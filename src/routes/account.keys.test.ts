/**
 * Permission boundaries on /account/keys.
 *
 * These routes are the self-service half of key management; the admin half is
 * /admin/users/:id/keys. What matters is that the self-service half can only
 * ever reach the caller's own keys, cannot grant `admin` to a non-admin, and
 * cannot be used to lift a rate limit at all.
 */
import { describe, test, expect, mock } from 'bun:test'
import { createAccountRoutes, type AccountDeps } from './account'

const BASE = 'http://localhost'

function user(role: 'user' | 'admin' = 'user') {
  return { id: 'user-1', email: 'u@example.com', role, plan: 'free', suspendedAt: null } as never
}

function deps(role: 'user' | 'admin' = 'user', overrides: Partial<AccountDeps> = {}): Partial<AccountDeps> {
  return {
    renameApiKey: mock(async () => true),
    updateApiKeyScopes: mock(async () => true),
    updateApiKeyOrigins: mock(async () => true),
    revokeApiKey: mock(async () => true),
    findUserById: mock(async () => user(role)),
    resolveSession: mock(async () => ({ user: user(role), session: { id: 's' } as never })),
    ...overrides,
  }
}

function patch(body: unknown) {
  return new Request(`${BASE}/account/keys/key-9`, {
    method: 'PATCH',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  })
}

describe('PATCH /account/keys/:id', () => {
  test('refuses a rate-limit exemption, even from an administrator', async () => {
    for (const role of ['user', 'admin'] as const) {
      const d = deps(role)
      const res = await createAccountRoutes(d).handle(patch({ name: 'x', rateLimitExempt: true }))

      expect(res.status).toBe(403)
      // Refused before anything is applied, not after the rename went through.
      expect(d.renameApiKey).not.toHaveBeenCalled()
    }
  })

  test('only reaches the caller\'s own keys', async () => {
    const d = deps()
    await createAccountRoutes(d).handle(patch({ name: 'renamed', scopes: ['tiles'], allowedOrigins: [] }))

    // Every write is keyed on the session's user, so a key id belonging to
    // someone else matches no row — the service filters on both.
    expect(d.renameApiKey).toHaveBeenCalledWith('user-1', 'key-9', 'renamed')
    expect(d.updateApiKeyScopes).toHaveBeenCalledWith('user-1', 'key-9', ['tiles'])
    expect(d.updateApiKeyOrigins).toHaveBeenCalledWith('user-1', 'key-9', [])
  })

  test('a key that is not the caller\'s is a 404', async () => {
    const d = deps('user', { renameApiKey: mock(async () => false) })
    expect((await createAccountRoutes(d).handle(patch({ name: 'x' }))).status).toBe(404)
  })

  test('will not widen a non-admin\'s key to `admin`', async () => {
    const d = deps('user')
    const res = await createAccountRoutes(d).handle(patch({ scopes: ['*', 'admin'] }))

    expect(res.status).toBe(403)
    expect(d.updateApiKeyScopes).not.toHaveBeenCalled()
  })

  test('lets an administrator give their own key `admin`', async () => {
    const d = deps('admin')
    expect((await createAccountRoutes(d).handle(patch({ scopes: ['*', 'admin'] }))).status).toBe(200)
  })

  test('requires a session', async () => {
    const d = deps('user', { resolveSession: mock(async () => ({ user: null, session: null })) })
    expect((await createAccountRoutes(d).handle(patch({ name: 'x' }))).status).toBe(401)
    expect(d.renameApiKey).not.toHaveBeenCalled()
  })
})

describe('DELETE /account/keys/:id', () => {
  test('revokes only within the caller\'s account', async () => {
    const d = deps()
    const res = await createAccountRoutes(d).handle(new Request(`${BASE}/account/keys/key-9`, { method: 'DELETE' }))

    expect(res.status).toBe(204)
    expect(d.revokeApiKey).toHaveBeenCalledWith('user-1', 'key-9')
  })
})
