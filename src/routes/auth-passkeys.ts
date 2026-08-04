/**
 * Passkey routes: register a credential against a signed-in account, and sign
 * in with one.
 *
 * The WebAuthn challenge issued by an `/options` call is returned to the client
 * AND stashed in an httpOnly cookie; the matching `/verify` call reads it back
 * from the cookie. The client never gets to tell us what challenge it was
 * supposed to answer, which is the entire point.
 */
import Elysia, { t } from 'elysia'
import {
  beginAuthentication as _beginAuthentication,
  beginRegistration as _beginRegistration,
  challengeCookieName,
  completeAuthentication as _completeAuthentication,
  completeRegistration as _completeRegistration,
  deletePasskey as _deletePasskey,
  listPasskeys as _listPasskeys,
  renamePasskey as _renamePasskey,
} from '../services/passkey.service'
import { startSession as _startSession } from '../services/auth.service'
import { resolveSession as _resolveSession, requireUser } from '../middleware/session'
import { toPublicUser } from '../services/accounts.service'
import { clearCookie, readCookie, serializeCookie } from '../lib/cookies'
import { clientIp, createRateLimiter } from '../lib/rate-limit'

/** A ceremony is a few seconds of user interaction; ten minutes is generous. */
const CHALLENGE_TTL_SECONDS = 600

/**
 * Handing out challenges is cheap for us and cheap for an attacker, so cap it.
 * Sign-in attempts are capped harder than registrations, which already require
 * a session.
 */
const authOptionsPerIp = createRateLimiter(30, 60_000)
const authVerifyPerIp = createRateLimiter(20, 60_000)

export interface PasskeyDeps {
  beginRegistration: typeof _beginRegistration
  completeRegistration: typeof _completeRegistration
  beginAuthentication: typeof _beginAuthentication
  completeAuthentication: typeof _completeAuthentication
  listPasskeys: typeof _listPasskeys
  renamePasskey: typeof _renamePasskey
  deletePasskey: typeof _deletePasskey
  startSession: typeof _startSession
  resolveSession: typeof _resolveSession
}

const defaultDeps: PasskeyDeps = {
  beginRegistration: _beginRegistration,
  completeRegistration: _completeRegistration,
  beginAuthentication: _beginAuthentication,
  completeAuthentication: _completeAuthentication,
  listPasskeys: _listPasskeys,
  renamePasskey: _renamePasskey,
  deletePasskey: _deletePasskey,
  startSession: _startSession,
  resolveSession: _resolveSession,
}

const challengeCookie = (challenge: string) =>
  serializeCookie(challengeCookieName, challenge, { maxAgeSeconds: CHALLENGE_TTL_SECONDS })

export function createPasskeyRoutes(overrides: Partial<PasskeyDeps> = {}) {
  const deps = { ...defaultDeps, ...overrides }

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

  // ── Sign-in with a passkey (no session required) ─────────────────────
  const publicRoutes = new Elysia({ prefix: '/auth/passkeys' })
    .post(
      '/authenticate/options',
      async ({ request, set }) => {
        if (!authOptionsPerIp.check(clientIp(request))) {
          set.status = 429
          return { error: 'Too many attempts. Try again shortly.' }
        }
        const options = await deps.beginAuthentication()
        set.headers['set-cookie'] = challengeCookie(options.challenge)
        return options
      },
      {
        detail: {
          summary: 'Begin passkey sign-in',
          description: 'Returns WebAuthn assertion options. No email needed — passkeys here are discoverable.',
          tags: ['Auth'],
        },
      },
    )

    .post(
      '/authenticate/verify',
      async ({ body, request, set }) => {
        if (!authVerifyPerIp.check(clientIp(request))) {
          set.status = 429
          return { error: 'Too many attempts. Try again shortly.' }
        }

        const challenge = readCookie(request, challengeCookieName)
        if (!challenge) {
          set.status = 400
          return { error: 'No passkey challenge in progress — start again' }
        }

        const result = await deps.completeAuthentication(body as never, challenge)
        if (!result.ok || !result.user) {
          set.status = 401
          set.headers['set-cookie'] = clearCookie(challengeCookieName)
          return { error: result.error ?? 'Passkey sign-in failed' }
        }

        const { session, cookie } = await deps.startSession(result.user.id, {
          ip: clientIp(request),
          userAgent: request.headers.get('user-agent'),
        })

        // Two cookies at once: the new session, and the spent challenge cleared.
        set.headers['set-cookie'] = [cookie, clearCookie(challengeCookieName)]
        set.status = 201
        return { token: session.id, user: toPublicUser(result.user) }
      },
      {
        body: t.Object(
          {
            id: t.String(),
            rawId: t.String(),
            type: t.String(),
            response: t.Object({
              clientDataJSON: t.String(),
              authenticatorData: t.String(),
              signature: t.String(),
              userHandle: t.Optional(t.String()),
            }),
            clientExtensionResults: t.Optional(t.Any()),
            authenticatorAttachment: t.Optional(t.String()),
          },
          { additionalProperties: true },
        ),
        detail: { summary: 'Complete passkey sign-in', tags: ['Auth'] },
      },
    )

  // ── Managing this account's passkeys ─────────────────────────────────
  const guardedRoutes = new Elysia({ prefix: '/auth/passkeys' })
    .derive(derive)
    .onBeforeHandle(requireUser)

    .get('/', ({ user }) => deps.listPasskeys(user!.id), {
      detail: { summary: 'List passkeys', tags: ['Auth'] },
    })

    .post(
      '/register/options',
      async ({ user, set }) => {
        const options = await deps.beginRegistration(user!.id, user!.email)
        set.headers['set-cookie'] = challengeCookie(options.challenge)
        return options
      },
      { detail: { summary: 'Begin passkey registration', tags: ['Auth'] } },
    )

    .post(
      '/register/verify',
      async ({ user, body, request, set }) => {
        const challenge = readCookie(request, challengeCookieName)
        if (!challenge) {
          set.status = 400
          return { error: 'No passkey challenge in progress — start again' }
        }

        const result = await deps.completeRegistration(
          user!.id,
          body as never,
          challenge,
          request.headers.get('user-agent'),
        )

        set.headers['set-cookie'] = clearCookie(challengeCookieName)
        if (!result.ok) {
          set.status = 400
          return { error: result.error ?? 'Passkey registration failed' }
        }

        set.status = 201
        return result.passkey
      },
      {
        body: t.Object(
          {
            id: t.String(),
            rawId: t.String(),
            type: t.String(),
            name: t.Optional(t.String({ maxLength: 80 })),
            response: t.Object({
              clientDataJSON: t.String(),
              attestationObject: t.String(),
              transports: t.Optional(t.Any()),
            }),
            clientExtensionResults: t.Optional(t.Any()),
            authenticatorAttachment: t.Optional(t.String()),
          },
          { additionalProperties: true },
        ),
        detail: { summary: 'Complete passkey registration', tags: ['Auth'] },
      },
    )

    .patch(
      '/:id',
      async ({ user, params, body, set }) => {
        const renamed = await deps.renamePasskey(user!.id, params.id, body.name.trim())
        if (!renamed) {
          set.status = 404
          return { error: 'Passkey not found' }
        }
        return { ok: true }
      },
      {
        body: t.Object({ name: t.String({ minLength: 1, maxLength: 80 }) }),
        detail: { summary: 'Rename a passkey', tags: ['Auth'] },
      },
    )

    .delete(
      '/:id',
      async ({ user, params, set }) => {
        const deleted = await deps.deletePasskey(user!.id, params.id)
        if (!deleted) {
          set.status = 404
          return { error: 'Passkey not found' }
        }
        set.status = 204
        return null
      },
      {
        detail: {
          summary: 'Delete a passkey',
          description:
            'Email sign-in always remains available, so removing the last passkey cannot lock an account out.',
          tags: ['Auth'],
        },
      },
    )

  return new Elysia().use(publicRoutes).use(guardedRoutes)
}

export const passkeyRoutes = createPasskeyRoutes()
