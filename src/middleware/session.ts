/**
 * Browser/console session resolution for account routes.
 *
 * These are plain functions rather than an Elysia plugin, for the reason spelt
 * out in `middleware/auth.ts` and CLAUDE.md: Elysia scopes a plugin's lifecycle
 * hooks to the plugin instance, so a `.use()`d guard silently leaves sibling
 * routes on the parent open. Attach them directly:
 *
 *     new Elysia().derive(deriveSession).onBeforeHandle(requireUser)
 *
 * Two credential shapes are accepted. Browsers send the session cookie; scripts
 * and the CLI send `Authorization: Bearer <session id>`. Only the cookie form
 * is a CSRF risk — it travels automatically — so only that form is origin-checked.
 */
import type { Session, User } from 'lucia'
import { lucia } from '../lib/lucia'
import { allowedOrigins, sessionCookieName } from '../config/accounts.config'

export interface SessionContext {
  user: User | null
  session: Session | null
  /** Set when a session slid forward and the client should store a fresh cookie. */
  refreshedCookie?: string
}

/** Pull a session id out of either credential form. */
export function readSessionId(request: Request): { id: string | null; fromCookie: boolean } {
  const authorization = request.headers.get('authorization')
  if (authorization?.startsWith('Bearer ')) {
    const token = authorization.slice(7).trim()
    // API keys share this header but are a different credential entirely —
    // don't hand them to Lucia, which would just miss and cost a query.
    if (token && !token.startsWith('brm_')) return { id: token, fromCookie: false }
  }

  const cookieHeader = request.headers.get('cookie')
  if (cookieHeader) {
    const id = lucia.readSessionCookie(cookieHeader)
    if (id) return { id, fromCookie: true }
  }

  return { id: null, fromCookie: false }
}

function originAllowed(request: Request): boolean {
  const origin = request.headers.get('origin')
  if (origin) return allowedOrigins.includes(origin.replace(/\/+$/, ''))

  // No Origin header. Sec-Fetch-Site is the fallback, and it is sent by every
  // browser that has shipped since 2020.
  //
  // Fails CLOSED when neither header is present. An earlier version treated a
  // missing Sec-Fetch-Site as same-origin, which meant any client sending
  // neither header — a stripping proxy, an old embedded webview — satisfied
  // the CSRF check on a cookie-authenticated mutation. A caller that genuinely
  // cannot send either header can still authenticate with a bearer token,
  // which carries no CSRF risk because it is never attached automatically.
  const site = request.headers.get('sec-fetch-site')
  return site === 'same-origin' || site === 'none'
}

/**
 * Resolve the caller's session. Never throws and never 401s — routes decide
 * what an anonymous caller means. Returns nulls for a bad, expired or
 * cross-origin credential.
 */
export async function resolveSession(request: Request): Promise<SessionContext> {
  const { id, fromCookie } = readSessionId(request)
  if (!id) return { user: null, session: null }

  // A cookie is presented by the browser on any request the attacker's page
  // can provoke, so a state-changing one must prove it came from our own UI.
  if (fromCookie && request.method !== 'GET' && request.method !== 'HEAD' && !originAllowed(request)) {
    return { user: null, session: null }
  }

  const { session, user } = await lucia.validateSession(id)
  if (!session || !user) return { user: null, session: null }

  // A suspended account keeps its rows but loses access immediately.
  if (user.suspendedAt) return { user: null, session: null }

  return {
    user,
    session,
    refreshedCookie: session.fresh && fromCookie ? lucia.createSessionCookie(session.id).serialize() : undefined,
  }
}

/**
 * Elysia `.derive()` handler: puts `user` and `session` in context.
 *
 * The context parameters are typed loosely (`set.headers` as `string | number`,
 * `set.status` optional) to stay assignable to Elysia's own context type —
 * narrowing them makes the handler fail the framework's contravariant check.
 */
export async function deriveSession({
  request,
  set,
}: {
  request: Request
  set: { headers: Record<string, string | number> }
}) {
  const { user, session, refreshedCookie } = await resolveSession(request)
  if (refreshedCookie) set.headers['set-cookie'] = refreshedCookie
  return { user, session }
}

interface GuardContext {
  user: User | null
  set: { status?: number | string }
}

/** Elysia `.onBeforeHandle()` guard: 401 unless a session resolved. */
export function requireUser({ user, set }: GuardContext) {
  if (!user) {
    set.status = 401
    return { error: 'Authentication required' }
  }
}

/** Elysia `.onBeforeHandle()` guard: 401 anonymous, 403 non-admin. */
export function requireAdmin({ user, set }: GuardContext) {
  if (!user) {
    set.status = 401
    return { error: 'Authentication required' }
  }
  if (user.role !== 'admin') {
    set.status = 403
    return { error: 'Administrator access required' }
  }
}
