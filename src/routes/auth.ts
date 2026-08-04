/**
 * Sign-in and session routes for the public API console.
 *
 * Built through a factory so tests can inject fakes for the store, the mailer
 * and the session layer, and run the real HTTP surface without Postgres.
 *
 * The guard is attached with `.onBeforeHandle(requireUser)` on a dedicated
 * instance rather than `.use()`d — see the note in `middleware/session.ts`.
 */
import Elysia, { t } from 'elysia'
import {
  findOrCreateUser as _findOrCreateUser,
  findUserByEmail as _findUserByEmail,
  consumeSignupAllowance as _consumeSignupAllowance,
  hashIp,
  toPublicUser,
  SignupError,
} from '../services/accounts.service'
import {
  createOtp as _createOtp,
  verifyOtp as _verifyOtp,
  startSession as _startSession,
  endSession as _endSession,
  endAllSessions as _endAllSessions,
  endOtherSessions as _endOtherSessions,
  listSessions as _listSessions,
  deleteSession as _deleteSession,
} from '../services/auth.service'
import { sendVerificationCode as _sendVerificationCode } from '../services/mailer.service'
import { resolveSession as _resolveSession, requireUser } from '../middleware/session'
import { describeSuspension } from '../services/moderation.service'
import { isValidEmail } from '../lib/email'
import { clientIp, createRateLimiter } from '../lib/rate-limit'
import { registrationMode, terms } from '../config/accounts.config'
import { enabledOAuthProviders } from '../config/oauth.config'

/**
 * Code requests are limited per address AND per source IP: the first stops one
 * inbox being mail-bombed, the second stops one host enumerating many.
 * Verification is limited per address so a stolen address can't be brute-forced
 * from a rotating pool of IPs.
 */
const codeRequestsPerEmail = createRateLimiter(5, 15 * 60_000)
const codeRequestsPerIp = createRateLimiter(20, 15 * 60_000)
const verifyAttemptsPerEmail = createRateLimiter(10, 15 * 60_000)

export interface AuthDeps {
  findUserByEmail: typeof _findUserByEmail
  findOrCreateUser: typeof _findOrCreateUser
  consumeSignupAllowance: typeof _consumeSignupAllowance
  createOtp: typeof _createOtp
  verifyOtp: typeof _verifyOtp
  sendVerificationCode: typeof _sendVerificationCode
  startSession: typeof _startSession
  endSession: typeof _endSession
  endAllSessions: typeof _endAllSessions
  endOtherSessions: typeof _endOtherSessions
  listSessions: typeof _listSessions
  deleteSession: typeof _deleteSession
  /** Injected so route tests can exercise the guarded paths without Postgres. */
  resolveSession: typeof _resolveSession
}

const defaultDeps: AuthDeps = {
  findUserByEmail: _findUserByEmail,
  findOrCreateUser: _findOrCreateUser,
  consumeSignupAllowance: _consumeSignupAllowance,
  createOtp: _createOtp,
  verifyOtp: _verifyOtp,
  sendVerificationCode: _sendVerificationCode,
  startSession: _startSession,
  endSession: _endSession,
  endAllSessions: _endAllSessions,
  endOtherSessions: _endOtherSessions,
  listSessions: _listSessions,
  deleteSession: _deleteSession,
  resolveSession: _resolveSession,
}

export function createAuthRoutes(overrides: Partial<AuthDeps> = {}) {
  const deps = { ...defaultDeps, ...overrides }

  // Local `.derive()` handler bound to the injected session resolver.
  const derive = async ({
    request,
    set,
  }: {
    request: Request
    set: { headers: Record<string, string | number> }
  }) => {
    const { user, session, refreshedCookie } = await deps.resolveSession(request)
    if (refreshedCookie) set.headers['set-cookie'] = refreshedCookie
    return { user, session }
  }

  // ── Public: what sign-in methods this instance offers ────────────────
  const publicRoutes = new Elysia({ prefix: '/auth' })
    .get(
      '/config',
      () => ({
        registrationMode,
        terms: { required: terms.required, version: terms.version, url: terms.url, privacyUrl: terms.privacyUrl },
        methods: {
          email: true,
          passkey: true,
          oauth: enabledOAuthProviders(),
        },
      }),
      { detail: { summary: 'Sign-in methods available', tags: ['Auth'] } },
    )

    .post(
      '/request-code',
      async ({ body, request, set }) => {
        const email = body.email.trim()
        if (!isValidEmail(email)) {
          set.status = 400
          return { error: 'Enter a valid email address' }
        }

        const ip = clientIp(request)
        const emailKey = email.toLowerCase()
        if (!codeRequestsPerEmail.check(emailKey) || !codeRequestsPerIp.check(ip)) {
          set.status = 429
          set.headers['retry-after'] = String(
            Math.max(codeRequestsPerEmail.retryAfter(emailKey), codeRequestsPerIp.retryAfter(ip)),
          )
          return { error: 'Too many code requests. Try again in a few minutes.' }
        }

        const existing = await deps.findUserByEmail(email)

        // Only a genuinely new account spends the per-IP sign-up budget —
        // an office behind one address can still sign in to existing accounts
        // all day.
        if (!existing && !(await deps.consumeSignupAllowance(hashIp(ip)))) {
          set.status = 429
          return { error: 'Too many accounts created from this network today.' }
        }

        let userId: string
        try {
          const { user } = await deps.findOrCreateUser({ email, ipHash: hashIp(ip) })
          userId = user.id
        } catch (err) {
          if (err instanceof SignupError) {
            // Invite-only is a property of the instance, not of the address, so
            // saying so leaks nothing. A rejected disposable domain is likewise
            // self-evident to whoever typed it.
            set.status = 403
            return { error: err.message, reason: err.reason }
          }
          throw err
        }

        const code = await deps.createOtp(userId, email)
        await deps.sendVerificationCode(email, code)

        // Always 201, whether or not delivery succeeded and whether or not the
        // account existed — an unauthenticated caller learns nothing about who
        // has an account here.
        set.status = 201
        return { sent: true }
      },
      {
        body: t.Object({ email: t.String({ maxLength: 254 }) }),
        detail: {
          summary: 'Request a sign-in code',
          description:
            'Emails a one-time code, creating the account on first use when registration is open. ' +
            'Always reports success so account existence is not disclosed.',
          tags: ['Auth'],
        },
      },
    )

    .post(
      '/verify-code',
      async ({ body, request, set }) => {
        const email = body.email.trim()
        const emailKey = email.toLowerCase()

        if (!verifyAttemptsPerEmail.check(emailKey)) {
          set.status = 429
          set.headers['retry-after'] = String(verifyAttemptsPerEmail.retryAfter(emailKey))
          return { error: 'Too many attempts. Request a new code.' }
        }

        const user = await deps.findUserByEmail(email)
        // Same 401 for "no such account" and "wrong code" — the request-code
        // endpoint already refuses to disclose existence, so this must too.
        if (!user) {
          set.status = 401
          return { error: 'That code is not valid' }
        }
        if (user.suspendedAt) {
          set.status = 403
          // Someone locked out is owed the reason and whether it lifts on its
          // own; "suspended, goodbye" turns into a support thread that starts
          // from nothing.
          return {
            error: user.suspendedReason
              ? `This account is suspended: ${user.suspendedReason}`
              : 'This account has been suspended',
            suspension: describeSuspension(user),
          }
        }

        const result = await deps.verifyOtp(user.id, body.code)
        if (result !== 'valid') {
          set.status = 401
          return {
            error:
              result === 'expired'
                ? 'That code has expired — request a new one'
                : result === 'too-many-attempts'
                  ? 'Too many incorrect attempts — request a new code'
                  : 'That code is not valid',
            reason: result,
          }
        }

        verifyAttemptsPerEmail.reset(emailKey)
        const { session, cookie } = await deps.startSession(user.id, {
          ip: clientIp(request),
          userAgent: request.headers.get('user-agent'),
        })

        set.headers['set-cookie'] = cookie
        set.status = 201
        return { token: session.id, user: toPublicUser(user) }
      },
      {
        body: t.Object({
          email: t.String({ maxLength: 254 }),
          code: t.String({ minLength: 4, maxLength: 16 }),
        }),
        detail: {
          summary: 'Exchange a sign-in code for a session',
          tags: ['Auth'],
        },
      },
    )

  // ── Session-aware: answers for signed-in and anonymous callers alike ──
  const sessionRoutes = new Elysia({ prefix: '/auth' })
    .derive(derive)

    .get(
      '/session',
      ({ user, session, set }) => {
        if (!user || !session) {
          set.status = 204
          return null
        }
        return { token: session.id, user: toPublicUser(user) }
      },
      {
        detail: {
          summary: 'Current session',
          description: 'Returns the signed-in account, or 204 when not signed in.',
          tags: ['Auth'],
        },
      },
    )

    .delete(
      '/session',
      async ({ session, set }) => {
        if (session) set.headers['set-cookie'] = await deps.endSession(session.id)
        set.status = 204
        return null
      },
      { detail: { summary: 'Sign out', tags: ['Auth'] } },
    )

  // ── Guarded: managing this account's other sessions ──────────────────
  const guardedRoutes = new Elysia({ prefix: '/auth' })
    .derive(derive)
    .onBeforeHandle(requireUser)

    .get('/sessions', ({ user, session }) => deps.listSessions(user!.id, session?.id), {
      detail: { summary: 'List active sessions', tags: ['Auth'] },
    })

    .delete(
      '/sessions',
      async ({ user, session, query, set }) => {
        if (query.scope === 'others' && session) {
          await deps.endOtherSessions(user!.id, session.id)
        } else {
          await deps.endAllSessions(user!.id)
          set.headers['set-cookie'] = await deps.endSession(session!.id)
        }
        set.status = 204
        return null
      },
      {
        query: t.Object({ scope: t.Optional(t.Union([t.Literal('all'), t.Literal('others')])) }),
        detail: {
          summary: 'Sign out everywhere',
          description: 'Ends every session for this account, or every session but the current one with ?scope=others.',
          tags: ['Auth'],
        },
      },
    )

    .delete(
      '/sessions/:id',
      async ({ user, params, set }) => {
        const removed = await deps.deleteSession(user!.id, params.id)
        if (!removed) {
          set.status = 404
          return { error: 'Session not found' }
        }
        set.status = 204
        return null
      },
      { detail: { summary: 'Revoke one session', tags: ['Auth'] } },
    )

  return new Elysia().use(publicRoutes).use(sessionRoutes).use(guardedRoutes)
}

export const authRoutes = createAuthRoutes()
