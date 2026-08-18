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
 *
 * Both are *candidates*, not alternatives: a request may carry a bearer AND a
 * cookie, and the bearer may not be a session at all. Each is tried in turn.
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

export interface SessionCandidate {
  id: string
  fromCookie: boolean
}

/**
 * Every session-id candidate on the request, bearer first.
 *
 * Returns a list rather than the first hit. Returning only the first let an
 * unrecognised bearer *shadow* the cookie: the bearer was handed to Lucia, it
 * missed, and the perfectly good session cookie on the same request was never
 * read. A console holding a stale token in `localStorage` therefore looked
 * signed out to every admin route, which reported "not an administrator key" at
 * an operator who was, in fact, a signed-in administrator.
 */
export function readSessionIds(request: Request): SessionCandidate[] {
  const candidates: SessionCandidate[] = []

  const authorization = request.headers.get('authorization')
  if (authorization?.startsWith('Bearer ')) {
    const token = authorization.slice(7).trim()
    // API keys share this header but are a different credential entirely —
    // don't hand them to Lucia, which would just miss and cost a query.
    if (token && !token.startsWith('brm_')) candidates.push({ id: token, fromCookie: false })
  }

  const cookieHeader = request.headers.get('cookie')
  if (cookieHeader) {
    const id = lucia.readSessionCookie(cookieHeader)
    if (id) candidates.push({ id, fromCookie: true })
  }

  return candidates
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
  const stateChanging = request.method !== 'GET' && request.method !== 'HEAD'

  for (const { id, fromCookie } of readSessionIds(request)) {
    // A cookie is presented by the browser on any request the attacker's page
    // can provoke, so a state-changing one must prove it came from our own UI.
    if (fromCookie && stateChanging && !originAllowed(request)) continue

    const { session, user } = await lucia.validateSession(id)
    if (!session || !user) continue

    // A suspended account keeps its rows but loses access immediately. This is
    // a hard stop, not a miss — don't fall through to another credential for
    // the same person.
    if (user.suspendedAt) return { user: null, session: null }

    return {
      user,
      session,
      refreshedCookie: session.fresh && fromCookie ? lucia.createSessionCookie(session.id).serialize() : undefined,
    }
  }

  return { user: null, session: null }
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
