/**
 * Account self-service: profile, API keys, usage, credit balance, data export
 * and deletion.
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
  keyPatchError,
} from '../services/api-keys.service'
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
import { deleteAccount as _deleteAccount, describeSuspension } from '../services/moderation.service'
import {
  blankSessionCookie as _blankSessionCookie,
  createOtp as _createOtp,
  verifyOtp as _verifyOtp,
} from '../services/auth.service'
import { sendAccountDeletionCode as _sendAccountDeletionCode } from '../services/mailer.service'
import {
  beginConfirmation as _beginConfirmation,
  confirmPasskey as _confirmPasskey,
} from '../services/passkey.service'
import { exportAccount as _exportAccount } from '../services/account-export.service'
import { LastAdminError } from '../services/accounts.service'
import { BillingTeardownError } from '../services/billing.service'
import { clearCookie, readCookie, serializeCookie } from '../lib/cookies'
import { createRateLimiter } from '../lib/rate-limit'
import { terms as termsConfig } from '../config/accounts.config'
import { resolveSession as _resolveSession, requireUser } from '../middleware/session'
import { ALL_SCOPES, CREDIT_COSTS, listPlans, publicPlan } from '../billing/plans'

/** Keys per account. High enough never to bind in practice, low enough to bound abuse. */
const MAX_KEYS_PER_ACCOUNT = 50

/** Origins per key. Generous for deploy previews, bounded so the array stays sane. */
const MAX_ORIGINS_PER_KEY = 20

/**
 * The WebAuthn challenge for confirming a deletion. A cookie of its own rather
 * than the sign-in one, so a challenge issued for signing in is never the one
 * a deletion is checked against.
 */
const CONFIRM_CHALLENGE_COOKIE = 'barrelman_webauthn_confirm'
const CONFIRM_CHALLENGE_TTL_SECONDS = 600


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
  exportAccount: typeof _exportAccount
  createOtp: typeof _createOtp
  verifyOtp: typeof _verifyOtp
  sendAccountDeletionCode: typeof _sendAccountDeletionCode
  beginConfirmation: typeof _beginConfirmation
  confirmPasskey: typeof _confirmPasskey
  deleteAccount: typeof _deleteAccount
  blankSessionCookie: typeof _blankSessionCookie
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
  exportAccount: _exportAccount,
  createOtp: _createOtp,
  verifyOtp: _verifyOtp,
  sendAccountDeletionCode: _sendAccountDeletionCode,
  beginConfirmation: _beginConfirmation,
  confirmPasskey: _confirmPasskey,
  deleteAccount: _deleteAccount,
  blankSessionCookie: _blankSessionCookie,
}

export function createAccountRoutes(overrides: Partial<AccountDeps> = {}) {
  const deps = { ...defaultDeps, ...overrides }

  // Per router rather than per module, so each test builds a fresh budget.
  // An export reads every account table; once a minute is plenty for a person
  // and bounds what a stolen session can make the database do.
  const exportsPerUser = createRateLimiter(1, 60_000)
  // Each deletion code is an email to the account's own inbox.
  const deletionCodesPerUser = createRateLimiter(3, 15 * 60_000)

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
      plans: listPlans().map(publicPlan),
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

        // `admin` reaches /admin/* — full re-imports, DROP/TRUNCATE, moderation.
        // Minting it has to be gated on the requester already being an admin,
        // or self-service key creation becomes a privilege-escalation route.
        const invalid = keyPatchError(body, user!.role === 'admin')
        if (invalid) {
          set.status = invalid.status
          return invalid.body
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
        // Refused for everyone here, administrators included: the exemption is
        // only ever set through /admin/users/:id/keys, behind the admin guard
        // and into the account's moderation log. Accepting it on this route —
        // even gated on role — would be a second door to keep in step.
        if (body.rateLimitExempt !== undefined) {
          set.status = 403
          return { error: 'Rate-limit exemptions are set by an administrator from the admin console.' }
        }

        const invalid = keyPatchError(body, user!.role === 'admin')
        if (invalid) {
          set.status = invalid.status
          return invalid.body
        }

        let changed = false
        if (body.name !== undefined) {
          changed = (await deps.renameApiKey(user!.id, params.id, body.name)) || changed
        }
        if (body.scopes !== undefined) {
          changed = (await deps.updateApiKeyScopes(user!.id, params.id, body.scopes)) || changed
        }
        if (body.allowedOrigins !== undefined) {
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
          // Declared so it is refused with a reason rather than silently dropped.
          rateLimitExempt: t.Optional(t.Boolean()),
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

    // ── Data export ───────────────────────────────────────────────────
    .get(
      '/export',
      async ({ user, set }) => {
        if (!exportsPerUser.check(user!.id)) {
          set.status = 429
          set.headers['retry-after'] = String(exportsPerUser.retryAfter(user!.id))
          return { error: 'You can export your data once a minute. Try again shortly.' }
        }

        const data = await deps.exportAccount(user!.id)
        if (!data) {
          set.status = 404
          return { error: 'Account not found' }
        }

        set.headers['content-type'] = 'application/json; charset=utf-8'
        set.headers['content-disposition'] =
          `attachment; filename="barrelman-account-${data.exportedAt.slice(0, 10)}.json"`
        set.headers['cache-control'] = 'no-store'
        return JSON.stringify(data, null, 2)
      },
      {
        detail: {
          summary: 'Export your account data',
          description:
            'A JSON download of everything held about the signed-in account: profile, linked identities, ' +
            'passkeys, API keys, sessions, usage, request metrics, credit ledger, billing state, terms ' +
            'acceptance and moderation history. Credentials (key hashes, passkey public keys, session ids) ' +
            'are never included. Limited to once a minute.',
          tags: ['Account'],
        },
      },
    )

    // ── Deletion ──────────────────────────────────────────────────────
    .post(
      '/delete/request',
      async ({ user, set }) => {
        if (!deletionCodesPerUser.check(user!.id)) {
          set.status = 429
          set.headers['retry-after'] = String(deletionCodesPerUser.retryAfter(user!.id))
          return { error: 'Too many deletion codes requested. Try again in a few minutes.' }
        }
        const code = await deps.createOtp(user!.id, user!.email, 'delete-account')
        await deps.sendAccountDeletionCode(user!.email, code)
        set.status = 201
        return { sent: true }
      },
      {
        detail: {
          summary: 'Email a code that confirms account deletion',
          description: 'The code is valid for deletion only, for 15 minutes, and replaces any earlier one.',
          tags: ['Account'],
        },
      },
    )

    .post(
      '/delete/passkey-options',
      async ({ user, set }) => {
        const options = await deps.beginConfirmation(user!.id)
        if (!options.allowCredentials?.length) {
          set.status = 409
          return { error: 'This account has no passkeys. Confirm with an emailed code instead.' }
        }
        set.headers['set-cookie'] = serializeCookie(CONFIRM_CHALLENGE_COOKIE, options.challenge, {
          maxAgeSeconds: CONFIRM_CHALLENGE_TTL_SECONDS,
        })
        return options
      },
      {
        detail: {
          summary: 'Begin confirming account deletion with a passkey',
          description: "WebAuthn assertion options limited to this account's own passkeys.",
          tags: ['Account'],
        },
      },
    )

    .delete(
      '/',
      async ({ user, body, request, set }) => {
        // Typed confirmation, checked here as well as in the console, so a
        // script holding a session cannot delete by accident either.
        if (body.email.trim().toLowerCase() !== user!.email.trim().toLowerCase()) {
          set.status = 400
          return { error: 'The email you typed does not match this account' }
        }

        // A fresh proof of presence. A session alone is not enough: it can be
        // a laptop left open, and this cannot be undone.
        if (body.passkey) {
          const challenge = readCookie(request, CONFIRM_CHALLENGE_COOKIE)
          if (!challenge) {
            set.status = 400
            return { error: 'No passkey confirmation in progress. Start again.' }
          }
          const result = await deps.confirmPasskey(user!.id, body.passkey as never, challenge)
          if (!result.ok) {
            set.status = 401
            set.headers['set-cookie'] = clearCookie(CONFIRM_CHALLENGE_COOKIE)
            return { error: result.error ?? 'Passkey confirmation failed', reason: 'passkey' }
          }
        } else if (body.code) {
          const result = await deps.verifyOtp(user!.id, body.code, 'delete-account')
          if (result !== 'valid') {
            set.status = 401
            return {
              error:
                result === 'expired'
                  ? 'That code has expired. Request a new one.'
                  : result === 'too-many-attempts'
                    ? 'Too many incorrect attempts. Request a new code.'
                    : 'That code is not valid',
              reason: result,
            }
          }
        } else {
          set.status = 400
          return { error: 'Confirm with the emailed code or a passkey', reason: 'confirmation-required' }
        }

        try {
          const deleted = await deps.deleteAccount(user!.id, user!.id)
          if (!deleted) {
            set.status = 404
            return { error: 'Account not found' }
          }
        } catch (err) {
          if (err instanceof LastAdminError) {
            set.status = 409
            return { error: err.message }
          }
          // Polar could not be told, so the account is kept: deleting it would
          // leave a subscription charging with nothing left to cancel it from.
          if (err instanceof BillingTeardownError) {
            set.status = 502
            return { error: err.message }
          }
          throw err
        }

        // deleteAccount has already ended every session. This clears the
        // cookies so the browser stops presenting a dead one.
        set.headers['set-cookie'] = [deps.blankSessionCookie(), clearCookie(CONFIRM_CHALLENGE_COOKIE)] as never
        return { deleted: true }
      },
      {
        body: t.Object({
          /** The account's own address, typed to confirm. */
          email: t.String({ minLength: 1, maxLength: 320 }),
          /** The code from POST /account/delete/request. */
          code: t.Optional(t.String({ minLength: 4, maxLength: 16 })),
          /** A WebAuthn assertion answering POST /account/delete/passkey-options. */
          passkey: t.Optional(
            t.Object(
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
          ),
        }),
        detail: {
          summary: 'Delete your account',
          description:
            'Permanently deletes the signed-in account with its API keys, usage, credit ledger, sessions and ' +
            'moderation history, then signs it out. Needs the account email typed back, plus either the code ' +
            'from `POST /account/delete/request` or a passkey assertion. With billing enabled, outstanding ' +
            'overage is reported and the subscription revoked first; if that fails the account is kept and ' +
            'the response is 502. The only administrator cannot delete their account (409).',
          tags: ['Account'],
        },
      },
    )

  return new Elysia().use(publicRoutes).use(routes)
}

export const accountRoutes = createAccountRoutes()
