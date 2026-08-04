import Elysia from 'elysia'
import { resolveSession } from './session'
import { safeEqual } from '../lib/crypto'

/**
 * Operator-console auth.
 *
 * The public read API is authenticated elsewhere — see `middleware/api-auth.ts`,
 * which resolves customer API keys and meters them. This module gates only the
 * `/admin/*` surface, which is far more powerful than the read API: full
 * re-imports, DROP/TRUNCATE, graph rebuilds.
 *
 * Two credentials are accepted, because the console has two kinds of caller:
 *
 *   - A signed-in account holding the `admin` role. This is how a human uses
 *     the console, and it is what the browser sends.
 *   - The shared `BARRELMAN_ADMIN_KEY` (falling back to `BARRELMAN_API_KEY`).
 *     This is for scripts and CI, and it is how every existing deployment
 *     already drives these routes.
 *
 * When no key is configured AND nobody is signed in, access is open — this is
 * the local-development case, matching the previous behaviour.
 */

interface AuthContext {
  headers: Record<string, string | undefined>
  request: Request
  set: { status?: number | string }
}

function configuredAdminKey(): string | undefined {
  return process.env.BARRELMAN_ADMIN_KEY || process.env.BARRELMAN_API_KEY
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
export async function adminAuthHandler({ headers, request, set }: AuthContext) {
  const adminKey = configuredAdminKey()
  const token = presentedToken(headers)

  // Constant-time — see the note in api-auth.ts on timing leaks.
  if (adminKey && token && safeEqual(token, adminKey)) return

  // A session with the admin role is equally sufficient.
  const { user } = await resolveSession(request)
  if (user) {
    if (user.role === 'admin') return
    set.status = 403
    return { error: 'Administrator access required' }
  }

  // Nothing presented and nothing configured to check against: open, so a
  // fresh local instance still has a usable console.
  if (!adminKey) return

  set.status = 401
  return { error: token ? 'Invalid admin key' : 'Sign in to the console, or present an admin key' }
}

export const adminAuthMiddleware = new Elysia({ name: 'admin-auth' }).onBeforeHandle(adminAuthHandler)

/**
 * Legacy shared-secret check for the read API.
 *
 * Superseded by `apiAuth()` in `middleware/api-auth.ts`, which additionally
 * resolves customer keys, enforces scopes and meters usage. Retained only for
 * callers that genuinely want "the service key and nothing else".
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
