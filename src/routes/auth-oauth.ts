/**
 * OAuth sign-in routes: `/auth/oauth/:provider` starts the dance and
 * `/auth/oauth/:provider/callback` finishes it.
 *
 * Both ends are browser navigations, not XHR, so the callback answers with a
 * redirect back into the console rather than JSON — errors travel as a query
 * parameter for the sign-in screen to display.
 *
 * The CSRF defence is the state parameter: it is generated here, stored in an
 * httpOnly cookie alongside the PKCE verifier, and compared on the way back. A
 * callback that arrives without a matching cookie is someone else's, and is
 * refused before we ever exchange the code.
 */
import Elysia, { t } from 'elysia'
import {
  createAuthorization as _createAuthorization,
  fetchProfile as _fetchProfile,
  listLinkedProviders as _listLinkedProviders,
  resolveOAuthSignIn as _resolveOAuthSignIn,
  unlinkProvider as _unlinkProvider,
} from '../services/oauth.service'
import { startSession as _startSession } from '../services/auth.service'
import { hashIp } from '../services/accounts.service'
import { resolveSession as _resolveSession, requireUser } from '../middleware/session'
import { getOAuthProvider } from '../config/oauth.config'
import { consoleOrigin } from '../config/accounts.config'
import { clearCookie, readCookie, serializeCookie } from '../lib/cookies'
import { clientIp, createRateLimiter } from '../lib/rate-limit'
import type { OAuthProvider } from '../schema/accounts'

const stateCookieName = 'barrelman_oauth'
const STATE_TTL_SECONDS = 600

const startsPerIp = createRateLimiter(20, 60_000)

/** Where the console picks up after the round trip. */
const CONSOLE_PATH = '/console/'

interface PendingAuthorization {
  state: string
  codeVerifier: string
  /** Set when a signed-in user is adding a provider rather than signing in. */
  linkUserId?: string
  /** Console path to land on afterwards. */
  next: string
}

/**
 * Only same-site paths are accepted as a post-sign-in destination. A protocol-
 * relative value like `//evil.example` is a URL to another origin despite
 * starting with a slash, and `javascript:` is not a location at all, so
 * anything that is not a plain absolute path is discarded.
 *
 * Applied both when storing the value and again when acting on it. Validating
 * only on the way in would mean the redirect target is trusted purely because
 * it came back from a cookie, and a cookie is not a trustworthy channel — any
 * sibling subdomain can write one for this host.
 */
function safeNext(value: string | undefined): string {
  if (!value || !value.startsWith('/') || value.startsWith('//')) return CONSOLE_PATH
  return value
}

function consoleRedirect(path: string, params: Record<string, string> = {}): Response {
  const url = new URL(path, consoleOrigin)
  for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value)
  return new Response(null, { status: 302, headers: { location: url.toString() } })
}

export interface OAuthDeps {
  createAuthorization: typeof _createAuthorization
  fetchProfile: typeof _fetchProfile
  resolveOAuthSignIn: typeof _resolveOAuthSignIn
  listLinkedProviders: typeof _listLinkedProviders
  unlinkProvider: typeof _unlinkProvider
  startSession: typeof _startSession
  resolveSession: typeof _resolveSession
}

const defaultDeps: OAuthDeps = {
  createAuthorization: _createAuthorization,
  fetchProfile: _fetchProfile,
  resolveOAuthSignIn: _resolveOAuthSignIn,
  listLinkedProviders: _listLinkedProviders,
  unlinkProvider: _unlinkProvider,
  startSession: _startSession,
  resolveSession: _resolveSession,
}

export function createOAuthRoutes(overrides: Partial<OAuthDeps> = {}) {
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

  const routes = new Elysia({ prefix: '/auth/oauth' })
    .derive(derive)

    .get(
      '/:provider',
      async ({ params, query, request, user, set }) => {
        if (!startsPerIp.check(clientIp(request))) {
          set.status = 429
          return { error: 'Too many sign-in attempts. Try again shortly.' }
        }

        if (!getOAuthProvider(params.provider)) {
          set.status = 404
          return { error: `Unknown or unconfigured provider "${params.provider}"` }
        }

        const authorization = deps.createAuthorization(params.provider)
        if (!authorization) {
          set.status = 404
          return { error: `Unknown or unconfigured provider "${params.provider}"` }
        }

        const pending: PendingAuthorization = {
          state: authorization.state,
          codeVerifier: authorization.codeVerifier,
          // `link` only means anything for a caller who already has a session;
          // otherwise this is an ordinary sign-in.
          linkUserId: query.link === '1' && user ? user.id : undefined,
          next: safeNext(query.next),
        }

        set.headers['set-cookie'] = serializeCookie(stateCookieName, JSON.stringify(pending), {
          maxAgeSeconds: STATE_TTL_SECONDS,
        })

        return new Response(null, { status: 302, headers: { location: authorization.url } })
      },
      {
        params: t.Object({ provider: t.String() }),
        query: t.Object({ link: t.Optional(t.String()), next: t.Optional(t.String()) }),
        detail: {
          summary: 'Start OAuth sign-in',
          description: 'Redirects to the provider. Pass ?link=1 while signed in to attach it to the current account.',
          tags: ['Auth'],
        },
      },
    )

    .get(
      '/:provider/callback',
      async ({ params, query, request }) => {
        const raw = readCookie(request, stateCookieName)
        const clear = clearCookie(stateCookieName)

        const fail = (message: string) => {
          const response = consoleRedirect(CONSOLE_PATH, { error: message })
          response.headers.append('set-cookie', clear)
          return response
        }

        // The provider reports user-side refusals here rather than as an error
        // status, so a denied consent screen lands in this branch.
        if (query.error) {
          return fail(query.error_description || query.error)
        }
        if (!raw) return fail('Sign-in session expired — please try again')

        let pending: PendingAuthorization
        try {
          pending = JSON.parse(raw) as PendingAuthorization
        } catch {
          return fail('Sign-in session was malformed — please try again')
        }

        if (!query.state || !query.code || query.state !== pending.state) {
          return fail('Sign-in could not be verified — please try again')
        }

        let profile
        try {
          profile = await deps.fetchProfile(params.provider, query.code, pending.codeVerifier)
        } catch (err) {
          console.error('[oauth] profile fetch failed', err)
          return fail('Could not reach the identity provider — please try again')
        }
        if (!profile) return fail(`Unknown or unconfigured provider "${params.provider}"`)

        const result = await deps.resolveOAuthSignIn(params.provider as OAuthProvider, profile, {
          ipHash: hashIp(clientIp(request)),
          signedInUserId: pending.linkUserId,
        })

        if (!result.ok) return fail(result.error)

        // Linking from settings: the caller already has a session, so leave it
        // alone rather than rotating it for no reason.
        if (pending.linkUserId) {
          const response = consoleRedirect(safeNext(pending.next), { linked: params.provider })
          response.headers.append('set-cookie', clear)
          return response
        }

        const { cookie } = await deps.startSession(result.user.id, {
          ip: clientIp(request),
          userAgent: request.headers.get('user-agent'),
        })

        const response = consoleRedirect(safeNext(pending.next))
        response.headers.append('set-cookie', cookie)
        response.headers.append('set-cookie', clear)
        return response
      },
      {
        params: t.Object({ provider: t.String() }),
        query: t.Object({
          code: t.Optional(t.String()),
          state: t.Optional(t.String()),
          error: t.Optional(t.String()),
          error_description: t.Optional(t.String()),
        }),
        detail: { summary: 'OAuth callback', tags: ['Auth'] },
      },
    )

  const guardedRoutes = new Elysia({ prefix: '/auth/oauth' })
    .derive(derive)
    .onBeforeHandle(requireUser)

    .get('/', ({ user }) => deps.listLinkedProviders(user!.id), {
      detail: { summary: 'List linked providers', tags: ['Auth'] },
    })

    .delete(
      '/:provider',
      async ({ user, params, set }) => {
        const removed = await deps.unlinkProvider(user!.id, params.provider as OAuthProvider)
        if (!removed) {
          set.status = 404
          return { error: 'That provider is not linked' }
        }
        set.status = 204
        return null
      },
      {
        params: t.Object({ provider: t.String() }),
        detail: {
          summary: 'Unlink a provider',
          description: 'Safe to remove the last one — email sign-in is always available as a fallback.',
          tags: ['Auth'],
        },
      },
    )

  // The guarded list/unlink routes are registered first so `/auth/oauth/` and
  // `DELETE /auth/oauth/:provider` are not shadowed by the `GET /:provider`
  // redirect above them.
  return new Elysia().use(guardedRoutes).use(routes)
}

export const oauthRoutes = createOAuthRoutes()
