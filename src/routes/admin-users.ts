/**
 * Administrator moderation API: inspect accounts, suspend and reinstate them,
 * and work through the queue of automated abuse signals.
 *
 * Gated with `.onBeforeHandle(adminAuthHandler)` directly on the instance —
 * never via `.use()` of a plugin, which Elysia scopes to the plugin and which
 * has already left routes in this codebase publicly reachable once.
 *
 * The acting administrator is resolved from their session so the audit trail
 * names a person. A caller driving these routes with an admin-scoped API key
 * has no session, but the key resolves to its owner, so those actions are
 * attributed too.
 */
import Elysia, { t } from 'elysia'
import { and, desc, eq, ilike, isNotNull, or, sql as dsql } from 'drizzle-orm'
import { db } from '../db'
import { adminAuthHandler } from '../middleware/auth'
import { resolveSession } from '../middleware/session'
import { users } from '../schema/accounts'
import {
  addNote,
  countOpenSignals,
  deleteAccount,
  describeSuspension,
  listAbuseSignals,
  moderationHistory,
  resolveAbuseSignal,
  suspendUser,
  unsuspendUser,
  warnUser,
} from '../services/moderation.service'
import { listApiKeys, invalidateUserKeys, resolveApiKey } from '../services/api-keys.service'
import { getBalance, setPlan } from '../services/credits.service'
import { usageByDay, usageByKey, usageTotals, currentCycleStart, utcDay } from '../services/usage.service'
import {
  describeTerms,
  findUserById,
  LastAdminError,
  setUserRole,
} from '../services/accounts.service'
import { throttleStats } from '../services/throttle.service'
import { allPlans, getPlan, isPlanId } from '../billing/plans'

/**
 * Recorded as the actor when a caller somehow reaches these routes without a
 * resolvable account. The guard should make that unreachable — both accepted
 * credentials carry a user — so it exists to keep the audit trail non-null
 * rather than to describe a real caller.
 */
const UNKNOWN_ACTOR = 'unknown'

const SUSPENSION_KINDS = ['tos-violation', 'abuse', 'automated-abuse', 'billing', 'spam', 'operator-request'] as const

export interface AdminUserDeps {
  suspendUser: typeof suspendUser
  unsuspendUser: typeof unsuspendUser
  deleteAccount: typeof deleteAccount
  setUserRole: typeof setUserRole
  resolveApiKey: typeof resolveApiKey
  warnUser: typeof warnUser
  addNote: typeof addNote
  resolveAbuseSignal: typeof resolveAbuseSignal
  listAbuseSignals: typeof listAbuseSignals
  countOpenSignals: typeof countOpenSignals
  setPlan: typeof setPlan
  invalidateUserKeys: typeof invalidateUserKeys
  findUserById: typeof findUserById
  resolveSession: typeof resolveSession
  guard: typeof adminAuthHandler
}

const defaultDeps: AdminUserDeps = {
  suspendUser,
  unsuspendUser,
  deleteAccount,
  setUserRole,
  resolveApiKey,
  warnUser,
  addNote,
  resolveAbuseSignal,
  listAbuseSignals,
  countOpenSignals,
  setPlan,
  invalidateUserKeys,
  findUserById,
  resolveSession,
  guard: adminAuthHandler,
}

export function createAdminUserRoutes(overrides: Partial<AdminUserDeps> = {}) {
  const deps = { ...defaultDeps, ...overrides }

  /**
   * Who to record in the moderation log.
   *
   * A session gives the user directly. A caller using an admin-scoped API key
   * has no session, but the key resolves to its owner — attributing those
   * actions to a person is one of the reasons the shared admin secret was
   * retired, so resolve it rather than logging an anonymous placeholder.
   */
  async function actorFor(request: Request): Promise<string> {
    const { user } = await deps.resolveSession(request)
    if (user) return user.id

    const authorization = request.headers.get('authorization')
    const token = authorization?.replace('Bearer ', '').trim()
    if (token) {
      const key = await deps.resolveApiKey(token)
      if (key) return key.userId
    }
    return UNKNOWN_ACTOR
  }

  return new Elysia({ prefix: '/admin' })
  .onBeforeHandle(deps.guard)

  // ── Accounts ────────────────────────────────────────────────────────
  .get(
    '/users',
    async ({ query }) => {
      const limit = Math.min(Number(query.limit ?? 50), 200)
      const offset = Number(query.offset ?? 0)
      const search = query.search?.trim()

      const filters = []
      if (search) {
        filters.push(or(ilike(users.email, `%${search}%`), ilike(users.name, `%${search}%`)))
      }
      if (query.status === 'suspended') filters.push(isNotNull(users.suspendedAt))
      if (query.status === 'paid') filters.push(dsql`${users.plan} <> 'free'`)

      const where = filters.length ? and(...filters) : undefined

      const [rows, [total]] = await Promise.all([
        db
          .select({
            id: users.id,
            email: users.email,
            name: users.name,
            role: users.role,
            plan: users.plan,
            suspendedAt: users.suspendedAt,
            suspendedReason: users.suspendedReason,
            suspendedKind: users.suspendedKind,
            suspendedUntil: users.suspendedUntil,
            tosVersion: users.tosVersion,
            tosAcceptedAt: users.tosAcceptedAt,
            createdAt: users.createdAt,
          })
          .from(users)
          .where(where)
          .orderBy(desc(users.createdAt))
          .limit(limit)
          .offset(offset),
        db.select({ count: dsql<number>`count(*)::int` }).from(users).where(where),
      ])

      return {
        users: rows.map((row) => ({
          ...row,
          suspension: describeSuspension(row),
          terms: describeTerms(row),
        })),
        total: total?.count ?? 0,
        limit,
        offset,
        // The operator's plan picker, sent with the list so changing a plan
        // does not need a detail fetch first.
        plans: allPlans(),
      }
    },
    {
      query: t.Object({
        search: t.Optional(t.String()),
        status: t.Optional(t.Union([t.Literal('all'), t.Literal('suspended'), t.Literal('paid')])),
        limit: t.Optional(t.String()),
        offset: t.Optional(t.String()),
      }),
      detail: { summary: 'List and search accounts', tags: ['Admin'] },
    },
  )

  .get(
    '/users/:id',
    async ({ params, set }) => {
      const [account] = await db.select().from(users).where(eq(users.id, params.id)).limit(1)
      if (!account) {
        set.status = 404
        return { error: 'Account not found' }
      }

      const from = currentCycleStart()
      const to = utcDay()

      const [keys, balance, daily, byKey, lifetime, history] = await Promise.all([
        listApiKeys(account.id, true),
        getBalance(account.id).catch(() => null),
        usageByDay(account.id, from, to).catch(() => []),
        usageByKey(account.id, from, to).catch(() => []),
        usageTotals(account.id).catch(() => null),
        moderationHistory(account.id),
      ])

      return {
        user: {
          id: account.id,
          email: account.email,
          name: account.name,
          role: account.role,
          plan: getPlan(account.plan),
          createdAt: account.createdAt,
          // The sign-up address is only ever a hash — we never stored the
          // address itself, and there is nothing to reveal here.
          signupIpHash: account.signupIpHash,
        },
        suspension: describeSuspension(account),
        terms: describeTerms(account),
        keys,
        balance,
        // Cycle-shaped, like every other usage view — and `lifetime` beside
        // it, because an account that was abusive last month reads as
        // spotless through a cycle window.
        usage: { from, to, daily, byKey },
        lifetime,
        history,
        // Every plan, internal ones included: this is the operator's picker,
        // and `demo` exists precisely to be assigned from here.
        plans: allPlans(),
      }
    },
    { detail: { summary: 'Account detail with usage, keys and moderation history', tags: ['Admin'] } },
  )

  .delete(
    '/users/:id',
    async ({ params, body, request, set }) => {
      const actor = await actorFor(request)

      // Deleting yourself is never the intent behind a click on someone
      // else's row, and unlike a suspension there is no undoing it.
      if (actor === params.id) {
        set.status = 400
        return { error: 'You cannot delete your own account' }
      }

      const account = await deps.findUserById(params.id)
      if (!account) {
        set.status = 404
        return { error: 'Account not found' }
      }

      // Typed confirmation, checked server-side rather than only in the
      // console: this cascades away keys, usage, credits and the audit trail,
      // and an admin-scoped API key can reach it with a one-line curl.
      if (body.email.trim().toLowerCase() !== account.email.trim().toLowerCase()) {
        set.status = 400
        return { error: 'The email you typed does not match this account' }
      }

      try {
        const deleted = await deps.deleteAccount(params.id, actor)
        if (!deleted) {
          set.status = 404
          return { error: 'Account not found' }
        }
        return { deleted: { id: deleted.id, email: deleted.email } }
      } catch (err) {
        if (err instanceof LastAdminError) {
          set.status = 409
          return { error: err.message }
        }
        throw err
      }
    },
    {
      body: t.Object({
        /** The account's own address, to confirm the right row is going. */
        email: t.String({ minLength: 1, maxLength: 320 }),
      }),
      detail: {
        summary: 'Delete an account permanently',
        description:
          'Erases the account and everything attached to it — API keys, usage, the credit ledger, abuse ' +
          'signals and its moderation history — with no way back. Suspending is the reversible option, ' +
          'and the one that keeps a record of the decision. The request body must repeat the account\'s ' +
          'email address. Refuses your own account and the last administrator.',
        tags: ['Admin'],
      },
    },
  )

  .post(
    '/users/:id/suspend',
    async ({ params, body, request, set }) => {
      const actor = await actorFor(request)

      // An admin suspending themselves would lock the console's own operator
      // out mid-action; almost always a misclick on the wrong row.
      if (actor === params.id) {
        set.status = 400
        return { error: 'You cannot suspend your own account' }
      }

      const until = body.hours ? new Date(Date.now() + body.hours * 60 * 60_000) : null
      const updated = await deps.suspendUser({
        userId: params.id,
        reason: body.reason.trim(),
        kind: body.kind,
        actorId: actor,
        until,
      })

      if (!updated) {
        set.status = 404
        return { error: 'Account not found' }
      }
      return { suspension: describeSuspension(updated) }
    },
    {
      body: t.Object({
        reason: t.String({ minLength: 3, maxLength: 500 }),
        kind: t.Union(SUSPENSION_KINDS.map((k) => t.Literal(k))),
        /** Omit for an indefinite suspension. */
        hours: t.Optional(t.Number({ minimum: 1, maximum: 24 * 365 })),
      }),
      detail: {
        summary: 'Suspend an account',
        description:
          'Ends every session and disables every API key immediately. The reason is shown to the user, ' +
          'both at sign-in and on API responses.',
        tags: ['Admin'],
      },
    },
  )

  .post(
    '/users/:id/unsuspend',
    async ({ params, body, request, set }) => {
      const updated = await deps.unsuspendUser(params.id, await actorFor(request), body?.reason)
      if (!updated) {
        set.status = 404
        return { error: 'Account not found' }
      }
      return { suspension: describeSuspension(updated) }
    },
    {
      body: t.Optional(t.Object({ reason: t.Optional(t.String({ maxLength: 500 })) })),
      detail: { summary: 'Reinstate a suspended account', tags: ['Admin'] },
    },
  )

  .post(
    '/users/:id/plan',
    async ({ params, body, request, set }) => {
      if (!isPlanId(body.plan)) {
        set.status = 400
        return { error: `Unknown plan '${body.plan}'` }
      }

      const account = await deps.findUserById(params.id)
      if (!account) {
        set.status = 404
        return { error: 'Account not found' }
      }

      await deps.setPlan(params.id, body.plan)
      // Keys cache the plan alongside the key, so a plan change that skipped
      // this would not take effect until the cache expired — and the whole
      // point of moving an account onto `demo` is that it takes effect now.
      deps.invalidateUserKeys(params.id)

      // Assigning an unmetered plan is granting free API access, so it belongs
      // in the same audit trail as a suspension rather than only in a diff of
      // the users table.
      await deps.addNote(
        params.id,
        `Plan changed from ${account.plan} to ${body.plan}${body.reason ? `: ${body.reason.trim()}` : ''}`,
        await actorFor(request),
      )

      return { plan: getPlan(body.plan) }
    },
    {
      body: t.Object({
        plan: t.String({ minLength: 1, maxLength: 40 }),
        reason: t.Optional(t.String({ maxLength: 500 })),
      }),
      detail: {
        summary: "Change an account's plan",
        description:
          'Operator override, independent of the billing provider. This is how an account is moved onto ' +
          'an internal plan such as `demo`, which serves the API unmetered — so it is recorded in the ' +
          'moderation log like any other privileged action.',
        tags: ['Admin'],
      },
    },
  )

  .post(
    '/users/:id/role',
    async ({ params, body, request, set }) => {
      const account = await deps.findUserById(params.id)
      if (!account) {
        set.status = 404
        return { error: 'Account not found' }
      }
      if (account.role === body.role) {
        return { user: { id: account.id, role: account.role } }
      }

      try {
        const updated = await deps.setUserRole(params.id, body.role)
        if (!updated) {
          set.status = 404
          return { error: 'Account not found' }
        }

        // Keys cache the owner's role alongside the key, so a demotion that
        // skipped this would leave the account's admin-scoped keys reaching
        // /admin/* until the cache expired — long enough to re-promote itself.
        deps.invalidateUserKeys(params.id)

        // Granting or removing administrator access reaches every destructive
        // operation in the console, so it belongs in the same audit trail as a
        // suspension.
        await deps.addNote(
          params.id,
          `Role changed from ${account.role} to ${body.role}${body.reason ? `: ${body.reason.trim()}` : ''}`,
          await actorFor(request),
        )

        return { user: { id: updated.id, role: updated.role } }
      } catch (err) {
        if (err instanceof LastAdminError) {
          set.status = 409
          return { error: err.message }
        }
        throw err
      }
    },
    {
      body: t.Object({
        role: t.Union([t.Literal('user'), t.Literal('admin')]),
        reason: t.Optional(t.String({ maxLength: 500 })),
      }),
      detail: {
        summary: "Change an account's role",
        description:
          'Promote an account to administrator or demote it. Refuses to remove the last administrator — ' +
          'with no shared admin secret, an instance with zero admins can only be recovered from the database.',
        tags: ['Admin'],
      },
    },
  )

  .post(
    '/users/:id/warn',
    async ({ params, body, request }) => {
      await deps.warnUser(params.id, body.reason.trim(), await actorFor(request))
      return { ok: true }
    },
    {
      body: t.Object({ reason: t.String({ minLength: 3, maxLength: 500 }) }),
      detail: {
        summary: 'Record a warning',
        description: 'Logged against the account without restricting it — the step before a suspension.',
        tags: ['Admin'],
      },
    },
  )

  .post(
    '/users/:id/note',
    async ({ params, body, request }) => {
      await deps.addNote(params.id, body.note.trim(), await actorFor(request))
      return { ok: true }
    },
    {
      body: t.Object({ note: t.String({ minLength: 1, maxLength: 1000 }) }),
      detail: { summary: 'Attach an internal note to an account', tags: ['Admin'] },
    },
  )

  // ── Abuse queue ─────────────────────────────────────────────────────
  .get(
    '/abuse',
    async ({ query }) => ({
      signals: await deps.listAbuseSignals({
        includeResolved: query.includeResolved === 'true',
        limit: Math.min(Number(query.limit ?? 100), 500),
      }),
      open: await deps.countOpenSignals(),
      throttle: throttleStats(),
    }),
    {
      query: t.Object({ includeResolved: t.Optional(t.String()), limit: t.Optional(t.String()) }),
      detail: {
        summary: 'Automated abuse signals awaiting review',
        description: 'Detections raised by the periodic sweep. A signal is an observation, not a judgement.',
        tags: ['Admin'],
      },
    },
  )

  .post(
    '/abuse/:id/resolve',
    async ({ params, request, set }) => {
      const resolved = await deps.resolveAbuseSignal(params.id, await actorFor(request))
      if (!resolved) {
        set.status = 404
        return { error: 'Signal not found or already resolved' }
      }
      return { ok: true }
    },
    { detail: { summary: 'Dismiss an abuse signal', tags: ['Admin'] } },
  )
}

export const adminUserRoutes = createAdminUserRoutes()
