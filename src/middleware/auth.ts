import Elysia from 'elysia'
import { resolveSession } from './session'
import { safeEqual } from '../lib/crypto'
import { resolveApiKey } from '../services/api-keys.service'
import { scopeAllowsAdmin, ADMIN_SCOPE } from '../billing/plans'
import { accountsEnabled } from '../config/accounts.config'

/**
 * Operator-console auth.
 *
 * The public read API is authenticated elsewhere — see `middleware/api-auth.ts`,
 * which resolves customer API keys and meters them. This module gates only the
 * `/admin/*` surface, which is far more powerful than the read API: full
 * re-imports, DROP/TRUNCATE, graph rebuilds.
 *
 * Two credentials are accepted, both tied to a real account:
 *
 *   - A signed-in session whose account holds the `admin` role. This is how a
 *     human uses the console, and it is what the browser sends.
 *   - An account API key carrying the `admin` scope, whose owner holds the
 *     `admin` role. This is for scripts and CI.
 *
 * `BARRELMAN_ADMIN_KEY` used to be accepted here and is gone. It was a single
 * static shared secret: unattributed in the moderation log, unrevocable without
 * recreating the container, unscoped, and — because it fell back to
 * `BARRELMAN_API_KEY` when unset, which is the compose default — it quietly
 * turned every holder of the *data* key into an administrator. An account key
 * has an owner, a scope, an expiry and immediate revocation.
 *
 * There is deliberately no "open when nothing is configured" path. A fresh
 * instance is reached by signing up: the first account created becomes an
 * admin, so the console is usable immediately without any secret existing.
 */

interface AuthContext {
  headers: Record<string, string | undefined>
  request: Request
  set: { status?: number | string }
}

function presentedToken(headers: Record<string, string | undefined>): string | null {
  const authorization = headers['authorization']
  if (!authorization) return null
  return authorization.replace('Bearer ', '').trim() || null
}

/**
 * Attach with `.onBeforeHandle(adminAuthHandler)` directly on the instance
 * declaring the routes — NOT via `.use()` of a plugin. Elysia scopes a plugin's
 * lifecycle hooks to the plugin instance, so a `.use()`d guard silently leaves
 * the parent's routes public. See the note in `middleware/api-auth.ts` for what
 * that cost us on the read API.
 */
export interface AdminAuthDeps {
  resolveSession: typeof resolveSession
  resolveApiKey: typeof resolveApiKey
  accountsEnabled: boolean
}

/**
 * Factory so the guard can be exercised without a database — this decides who
 * can run a full re-import, and it was previously only reachable through one.
 * Same `deps` shape the route modules use.
 */
export function createAdminAuthHandler(deps: Partial<AdminAuthDeps> = {}) {
  const d: AdminAuthDeps = {
    resolveSession,
    resolveApiKey,
    accountsEnabled,
    ...deps,
  }
  return async function adminAuthHandler({ headers, request, set }: AuthContext) {
    return handle(d, { headers, request, set })
  }
}

export async function adminAuthHandler(ctx: AuthContext) {
  return handle({ resolveSession, resolveApiKey, accountsEnabled }, ctx)
}

async function handle(d: AdminAuthDeps, { headers, request, set }: AuthContext) {
  // Every admin credential is an account credential now, so with accounts off
  // there is nothing that could authorise this. Fail closed and say why rather
  // than 401 at someone who has no way to satisfy it.
  if (!d.accountsEnabled) {
    set.status = 503
    return {
      error:
        'Admin routes require accounts, which are disabled (BARRELMAN_ACCOUNTS_ENABLED=false). ' +
        'Enable accounts and sign in — the first account created becomes an administrator.',
    }
  }

  // A session with the admin role.
  const { user } = await d.resolveSession(request)
  if (user) {
    if (user.role === 'admin') return
    set.status = 403
    return { error: 'Administrator access required' }
  }

  // An account API key scoped to `admin`, owned by an admin.
  const token = presentedToken(headers)
  if (token) {
    const key = await d.resolveApiKey(token)
    if (key && key.role === 'admin' && scopeAllowsAdmin(key.scopes)) {
      if (key.suspended) {
        set.status = 403
        return { error: key.suspensionReason ?? 'Account suspended' }
      }
      return
    }
    set.status = 403
    return {
      error: `Not an administrator key. It must be created by an admin account with the "${ADMIN_SCOPE}" scope.`,
    }
  }

  set.status = 401
  return { error: 'Sign in to the console, or present an admin-scoped API key' }
}

export const adminAuthMiddleware = new Elysia({ name: 'admin-auth' }).onBeforeHandle(adminAuthHandler)

/**
 * Legacy shared-secret check for the READ API.
 *
 * Unrelated to the admin surface above: this is `BARRELMAN_API_KEY`, the
 * unmetered service credential Parchment authenticates with, and it stays.
 * Superseded for customer traffic by `apiAuth()` in `middleware/api-auth.ts`,
 * which additionally resolves account keys, enforces scopes and meters usage.
 * Retained for callers that genuinely want "the service key and nothing else".
 */
export function authHandler({
  headers,
  set,
}: {
  headers: Record<string, string | undefined>
  set: { status?: number | string }
}) {
  const apiKey = process.env.BARRELMAN_API_KEY
  if (!apiKey) return

  const token = presentedToken(headers)
  if (!token) {
    set.status = 401
    return { error: 'Missing Authorization header' }
  }
  if (!safeEqual(token, apiKey)) {
    set.status = 401
    return { error: 'Invalid API key' }
  }
}

export const authMiddleware = new Elysia({ name: 'auth' }).onBeforeHandle(authHandler)
