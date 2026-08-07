/**
 * Account self-service: profile, API keys, usage and credit balance.
 *
 * Every route here is session-authenticated (the console), never API-key
 * authenticated — a leaked API key must not be able to mint more keys or read
 * the billing history of the account it belongs to.
 */
import Elysia, { t } from 'elysia'
import {
  createApiKey as _createApiKey,
  listApiKeys as _listApiKeys,
  renameApiKey as _renameApiKey,
  revokeApiKey as _revokeApiKey,
  updateApiKeyOrigins as _updateApiKeyOrigins,
  updateApiKeyScopes as _updateApiKeyScopes,
} from '../services/api-keys.service'
import { invalidOrigins } from '../lib/origins'
import {
  getBalance as _getBalance,
  listLedger as _listLedger,
} from '../services/credits.service'
import {
  usageByDay as _usageByDay,
  usageByKey as _usageByKey,
  currentCycleStart,
  utcDay,
} from '../services/usage.service'
import {
  acceptTerms as _acceptTerms,
  describeTerms,
  findUserById as _findUserById,
  toPublicUser,
  updateUser as _updateUser,
} from '../services/accounts.service'
import { describeSuspension } from '../services/moderation.service'
import { terms as termsConfig } from '../config/accounts.config'
import { resolveSession as _resolveSession, requireUser } from '../middleware/session'
import {
  ALL_SCOPES,
  CREDIT_COSTS,
  includedPricePerThousand,
  isValidScope,
  listPlans,
  overagePerThousand,
} from '../billing/plans'

/** Keys per account. High enough never to bind in practice, low enough to bound abuse. */
const MAX_KEYS_PER_ACCOUNT = 50

/** Origins per key. Generous for deploy previews, bounded so the array stays sane. */
const MAX_ORIGINS_PER_KEY = 20

/** Shown alongside a rejected origin, since the accepted forms are not obvious. */
const ORIGIN_EXAMPLE = 'https://example.com, http://localhost:5200, https://*.netlify.app'

export interface AccountDeps {
  createApiKey: typeof _createApiKey
  listApiKeys: typeof _listApiKeys
  renameApiKey: typeof _renameApiKey
  revokeApiKey: typeof _revokeApiKey
  updateApiKeyScopes: typeof _updateApiKeyScopes
  updateApiKeyOrigins: typeof _updateApiKeyOrigins
  getBalance: typeof _getBalance
  listLedger: typeof _listLedger
  usageByDay: typeof _usageByDay
  usageByKey: typeof _usageByKey
  updateUser: typeof _updateUser
  acceptTerms: typeof _acceptTerms
  findUserById: typeof _findUserById
  resolveSession: typeof _resolveSession
}

const defaultDeps: AccountDeps = {
  createApiKey: _createApiKey,
  listApiKeys: _listApiKeys,
  renameApiKey: _renameApiKey,
  revokeApiKey: _revokeApiKey,
  updateApiKeyScopes: _updateApiKeyScopes,
  updateApiKeyOrigins: _updateApiKeyOrigins,
  getBalance: _getBalance,
  listLedger: _listLedger,
  usageByDay: _usageByDay,
  usageByKey: _usageByKey,
  updateUser: _updateUser,
  acceptTerms: _acceptTerms,
  findUserById: _findUserById,
  resolveSession: _resolveSession,
}

export function createAccountRoutes(overrides: Partial<AccountDeps> = {}) {
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

  // Public: the pricing table, so the console can render plans before sign-in.
  const publicRoutes = new Elysia({ prefix: '/account' }).get(
    '/plans',
    () => ({
      plans: listPlans().map((plan) => ({
        ...plan,
        // Derived here so the pricing page does not have to reproduce the
        // micro-dollar arithmetic in three different clients.
        overagePerThousand: overagePerThousand(plan),
        includedPricePerThousand: includedPricePerThousand(plan),
      })),
      creditCosts: CREDIT_COSTS,
      scopes: ALL_SCOPES,
    }),
    {
      detail: {
        summary: 'Plans and credit pricing',
        description: 'What each plan includes and what each endpoint group costs in credits.',
        tags: ['Account'],
      },
    },
  )

  const routes = new Elysia({ prefix: '/account' })
    .derive(derive)
    .onBeforeHandle(requireUser)

    // ── Profile ───────────────────────────────────────────────────────
    .get(
      '/',
      async ({ user }) => {
        // Read through rather than trusting the session snapshot: terms and
        // suspension both change out from under a live session.
        const account = (await deps.findUserById(user!.id)) ?? null
        return {
          user: toPublicUser(account ?? user!),
          terms: describeTerms(account ?? { tosVersion: null, tosAcceptedAt: null }),
          suspension: account
            ? describeSuspension(account)
            : { suspended: false, reason: null, kind: null, until: null, appealable: false },
        }
      },
      { detail: { summary: 'Current account, terms state and any suspension', tags: ['Account'] } },
    )

    .post(
      '/accept-terms',
      async ({ user, body, set }) => {
        if (body.version !== termsConfig.version) {
          // Refuse a stale version: accepting terms the user was not shown is
          // not consent, and it would mark them up to date against text that
          // has since changed.
          set.status = 409
          return {
            error: 'Those terms are out of date — reload and review the current version',
            version: termsConfig.version,
          }
        }
        const updated = await deps.acceptTerms(user!.id, termsConfig.version)
        return {
          terms: describeTerms(updated ?? { tosVersion: termsConfig.version, tosAcceptedAt: new Date() }),
        }
      },
      {
        body: t.Object({ version: t.String({ maxLength: 40 }) }),
        detail: { summary: 'Accept the current terms of service', tags: ['Account'] },
      },
    )

    .patch(
      '/',
      async ({ user, body }) => {
        const updated = await deps.updateUser(user!.id, { name: body.name?.trim() || null })
        return { user: updated ? toPublicUser(updated) : toPublicUser(user!) }
      },
      {
        body: t.Object({ name: t.Optional(t.String({ maxLength: 120 })) }),
        detail: { summary: 'Update profile', tags: ['Account'] },
      },
    )

    // ── API keys ──────────────────────────────────────────────────────
    .get(
      '/keys',
      async ({ user, query }) => ({
        keys: await deps.listApiKeys(user!.id, query.includeRevoked === 'true'),
      }),
      {
        query: t.Object({ includeRevoked: t.Optional(t.String()) }),
        detail: { summary: 'List API keys', tags: ['Account'] },
      },
    )

    .post(
      '/keys',
      async ({ user, body, set }) => {
        // Creating a key is the moment someone starts actually using the API,
        // which makes it the right place to require agreement. Existing keys
        // keep working across a terms change so a version bump never breaks a
        // running integration.
        const account = await deps.findUserById(user!.id)
        const terms = describeTerms(account ?? { tosVersion: null, tosAcceptedAt: null })
        if (terms.outstanding) {
          set.status = 451
          return {
            error: 'Accept the terms of service before creating an API key',
            terms,
          }
        }

        const existing = await deps.listApiKeys(user!.id)
        if (existing.length >= MAX_KEYS_PER_ACCOUNT) {
          set.status = 409
          return { error: `You can have at most ${MAX_KEYS_PER_ACCOUNT} active keys. Revoke one first.` }
        }

        const invalid = (body.scopes ?? []).filter((s) => !isValidScope(s))
        if (invalid.length > 0) {
          set.status = 400
          return { error: `Unknown scope(s): ${invalid.join(', ')}`, validScopes: ALL_SCOPES }
        }

        const badOrigins = invalidOrigins(body.allowedOrigins ?? [])
        if (badOrigins.length > 0) {
          set.status = 400
          return { error: `Not valid origin(s): ${badOrigins.join(', ')}`, example: ORIGIN_EXAMPLE }
        }

        const created = await deps.createApiKey({
          userId: user!.id,
          name: body.name,
          scopes: body.scopes,
          allowedOrigins: body.allowedOrigins,
        })

        set.status = 201
        // The plaintext appears here and nowhere else, ever.
        return {
          key: created.key,
          record: created.record,
          warning: 'Copy this key now — it cannot be shown again.',
        }
      },
      {
        body: t.Object({
          name: t.String({ minLength: 1, maxLength: 80 }),
          scopes: t.Optional(t.Array(t.String())),
          allowedOrigins: t.Optional(t.Array(t.String(), { maxItems: MAX_ORIGINS_PER_KEY })),
        }),
        detail: {
          summary: 'Create an API key',
          description:
            'Returns the only copy of the key that will ever exist. `allowedOrigins` restricts the key to ' +
            'requests carrying a matching `Origin` or `Referer` — use it for keys that ship in a web page. ' +
            'A restricted key cannot be used from a server, which sends neither header.',
          tags: ['Account'],
        },
      },
    )

    .patch(
      '/keys/:id',
      async ({ user, params, body, set }) => {
        let changed = false

        if (body.name !== undefined) {
          changed = (await deps.renameApiKey(user!.id, params.id, body.name)) || changed
        }
        if (body.scopes !== undefined) {
          const invalid = body.scopes.filter((s) => !isValidScope(s))
          if (invalid.length > 0) {
            set.status = 400
            return { error: `Unknown scope(s): ${invalid.join(', ')}`, validScopes: ALL_SCOPES }
          }
          changed = (await deps.updateApiKeyScopes(user!.id, params.id, body.scopes)) || changed
        }
        if (body.allowedOrigins !== undefined) {
          const badOrigins = invalidOrigins(body.allowedOrigins)
          if (badOrigins.length > 0) {
            set.status = 400
            return { error: `Not valid origin(s): ${badOrigins.join(', ')}`, example: ORIGIN_EXAMPLE }
          }
          changed = (await deps.updateApiKeyOrigins(user!.id, params.id, body.allowedOrigins)) || changed
        }

        if (!changed) {
          set.status = 404
          return { error: 'API key not found' }
        }
        return { ok: true }
      },
      {
        body: t.Object({
          name: t.Optional(t.String({ minLength: 1, maxLength: 80 })),
          scopes: t.Optional(t.Array(t.String())),
          allowedOrigins: t.Optional(t.Array(t.String(), { maxItems: MAX_ORIGINS_PER_KEY })),
        }),
        detail: {
          summary: 'Rename an API key or change its scopes and origins',
          description: 'Passing an empty `allowedOrigins` lifts the origin restriction entirely.',
          tags: ['Account'],
        },
      },
    )

    .delete(
      '/keys/:id',
      async ({ user, params, set }) => {
        const revoked = await deps.revokeApiKey(user!.id, params.id)
        if (!revoked) {
          set.status = 404
          return { error: 'API key not found or already revoked' }
        }
        set.status = 204
        return null
      },
      {
        detail: {
          summary: 'Revoke an API key',
          description: 'Takes effect immediately. The row is kept so its usage history stays attributable.',
          tags: ['Account'],
        },
      },
    )

    // ── Usage and credits ─────────────────────────────────────────────
    .get('/credits', ({ user }) => deps.getBalance(user!.id), {
      detail: { summary: 'Credit balance for the current cycle', tags: ['Account'] },
    })

    .get('/credits/ledger', async ({ user }) => ({ entries: await deps.listLedger(user!.id) }), {
      detail: { summary: 'Credit purchases and adjustments', tags: ['Account'] },
    })

    .get(
      '/usage',
      async ({ user, query }) => {
        const from = query.from ?? currentCycleStart()
        const to = query.to ?? utcDay()
        const [daily, byKey] = await Promise.all([
          deps.usageByDay(user!.id, from, to),
          deps.usageByKey(user!.id, from, to),
        ])
        return { from, to, daily, byKey }
      },
      {
        query: t.Object({ from: t.Optional(t.String()), to: t.Optional(t.String()) }),
        detail: {
          summary: 'Usage over a date range',
          description: 'Defaults to the current billing cycle. Dates are UTC (YYYY-MM-DD).',
          tags: ['Account'],
        },
      },
    )

  return new Elysia().use(publicRoutes).use(routes)
}

export const accountRoutes = createAccountRoutes()
