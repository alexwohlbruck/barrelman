import Elysia from 'elysia'

/**
 * Simple Bearer token auth middleware.
 * Validates against BARRELMAN_API_KEY env var.
 * Will be replaced with Unkey validation later.
 */

/**
 * The raw auth handler function. Exported separately so it can be attached
 * directly with `.onBeforeHandle(authHandler)` in tests, bypassing Elysia's
 * named-plugin deduplication which only allows a plugin instance to be
 * applied to a single app instance.
 */
export function authHandler({ headers, set }: { headers: Record<string, string | undefined>, set: { status: number | string } }) {
  const apiKey = process.env.BARRELMAN_API_KEY
  if (!apiKey) {
    // No key configured = open access (dev mode)
    return
  }

  const authorization = headers['authorization']
  if (!authorization) {
    set.status = 401
    return { error: 'Missing Authorization header' }
  }

  const token = authorization.replace('Bearer ', '')
  if (token !== apiKey) {
    set.status = 401
    return { error: 'Invalid API key' }
  }
}

export const authMiddleware = new Elysia({ name: 'auth' }).onBeforeHandle(authHandler)

/**
 * Admin console auth. Gates the operator console and all script/job routes,
 * which are far more powerful than the read API (full re-imports, DROP/TRUNCATE,
 * graph rebuilds). Validates against BARRELMAN_ADMIN_KEY, falling back to
 * BARRELMAN_API_KEY when the dedicated admin key isn't configured.
 *
 * When neither key is set the console is open — dev mode only.
 */
export function adminAuthHandler({ headers, set }: { headers: Record<string, string | undefined>, set: { status: number | string } }) {
  const adminKey = process.env.BARRELMAN_ADMIN_KEY || process.env.BARRELMAN_API_KEY
  if (!adminKey) {
    // No key configured = open access (dev mode)
    return
  }

  const authorization = headers['authorization']
  if (!authorization) {
    set.status = 401
    return { error: 'Missing Authorization header' }
  }

  const token = authorization.replace('Bearer ', '')
  if (token !== adminKey) {
    set.status = 401
    return { error: 'Invalid admin key' }
  }
}

export const adminAuthMiddleware = new Elysia({ name: 'admin-auth' }).onBeforeHandle(adminAuthHandler)
