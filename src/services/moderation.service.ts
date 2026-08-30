/**
 * Account suspension, bans and the audit trail behind them.
 *
 * Suspending must take effect *now*, not whenever a cache happens to expire.
 * Three things hold a suspended account's access open, and all three are torn
 * down together:
 *
 *   1. Browser sessions — deleted, so the console logs out immediately.
 *   2. The API-key verification cache — evicted, so the next request re-reads
 *      the account rather than serving from a minute-old "fine" verdict.
 *   3. The credit-balance cache — evicted for the same reason.
 *
 * Every action is written to an append-only log. Suspending is a judgement
 * about a person; the columns on `users` hold only the current state, which
 * tells you nothing about who decided what, or why.
 */
import { and, desc, eq, gte, isNull, lte, or, sql as dsql } from 'drizzle-orm'
import { db } from '../db'
import {
  abuseSignals,
  moderationLog,
  users,
  type AbuseSignal,
  type AbuseSignalKind,
  type ModerationAction,
  type SuspensionKind,
  type User,
} from '../schema/accounts'
import { generateId } from '../lib/crypto'
import { LastAdminError } from './accounts.service'
import { endAllSessions } from './auth.service'
import { invalidateUserKeys } from './api-keys.service'
import { invalidateBalance } from './credits.service'

/** Actor id recorded when an automated rule acts rather than a person. */
export const SYSTEM_ACTOR = 'system'

export interface SuspendOptions {
  userId: string
  reason: string
  kind: SuspensionKind
  /** Admin user id, or `SYSTEM_ACTOR`. */
  actorId: string
  /** Auto-expiry. Omit for an indefinite suspension. */
  until?: Date | null
  metadata?: Record<string, unknown>
}

export async function suspendUser(options: SuspendOptions): Promise<User | null> {
  const { userId, reason, kind, actorId, until = null, metadata } = options

  const [updated] = await db
    .update(users)
    .set({
      suspendedAt: new Date(),
      suspendedReason: reason,
      suspendedKind: kind,
      suspendedBy: actorId,
      suspendedUntil: until,
      updatedAt: new Date(),
    })
    .where(eq(users.id, userId))
    .returning()

  if (!updated) return null

  await log({ userId, action: 'suspend', kind, reason, actorId, metadata })
  await revokeAccess(userId)

  console.warn(`[moderation] suspended ${userId} (${kind}) by ${actorId}: ${reason}`)
  return updated
}

export async function unsuspendUser(userId: string, actorId: string, reason?: string): Promise<User | null> {
  const [updated] = await db
    .update(users)
    .set({
      suspendedAt: null,
      suspendedReason: null,
      suspendedKind: null,
      suspendedBy: null,
      suspendedUntil: null,
      updatedAt: new Date(),
    })
    .where(eq(users.id, userId))
    .returning()

  if (!updated) return null

  await log({ userId, action: 'unsuspend', reason: reason ?? null, actorId })
  // Drop the caches again: they may hold the "suspended" verdict.
  invalidateUserKeys(userId)
  invalidateBalance(userId)

  console.log(`[moderation] unsuspended ${userId} by ${actorId}`)
  return updated
}

/**
 * Tear down everything that could keep a suspended account working. Sessions
 * are deleted rather than left to expire, because a session is a live
 * credential and "suspended but still signed in" is not a suspension.
 */
async function revokeAccess(userId: string): Promise<void> {
  await endAllSessions(userId)
  invalidateUserKeys(userId)
  invalidateBalance(userId)
}

/**
 * Erase an account and everything attached to it.
 *
 * The opposite bargain from a suspension. A suspension keeps the person on
 * file — their keys, their usage, the log of what was decided about them — so
 * it can be argued with and undone. Deletion keeps nothing: keys, usage,
 * credit ledger, abuse signals and the moderation log itself all go with the
 * row, by `ON DELETE CASCADE`. That is the point when the request is "remove
 * me", and it is also why the only record left of this action is the server
 * log — the audit trail is one of the things being erased.
 *
 * Prefer `suspendUser()` for anything you might have to justify later.
 *
 * Refuses the last administrator for the same reason `setUserRole()` does:
 * with no shared admin secret, an instance with zero administrators can only
 * be recovered with direct database access. The check and the delete share a
 * transaction with the admin rows locked, so two concurrent deletions cannot
 * each see a count of two and both go through.
 */
export async function deleteAccount(userId: string, actorId: string): Promise<User | null> {
  const deleted = await db.transaction(async (tx) => {
    const admins = await tx
      .select({ id: users.id })
      .from(users)
      .where(eq(users.role, 'admin'))
      .for('update')

    if (admins.length <= 1 && admins.some((a) => a.id === userId)) {
      throw new LastAdminError(
        'This is the only administrator. Promote someone else before deleting this account.',
      )
    }

    const [row] = await tx.delete(users).where(eq(users.id, userId)).returning()
    return row ?? null
  })

  if (!deleted) return null

  // The row is gone but the caches keyed on it are not, and an API-key verdict
  // cached a moment before the delete would keep serving a deleted account
  // until it expired.
  await revokeAccess(userId)

  console.warn(`[moderation] deleted account ${userId} <${deleted.email}> by ${actorId}`)
  return deleted
}

/**
 * Lift suspensions whose expiry has passed. Called from the periodic sweep —
 * a timed suspension that never lifts is just an indefinite one with extra
 * steps.
 */
export async function expireSuspensions(): Promise<number> {
  const expired = await db
    .update(users)
    .set({
      suspendedAt: null,
      suspendedReason: null,
      suspendedKind: null,
      suspendedBy: null,
      suspendedUntil: null,
      updatedAt: new Date(),
    })
    .where(and(dsql`${users.suspendedUntil} IS NOT NULL`, lte(users.suspendedUntil, new Date())))
    .returning({ id: users.id })

  for (const row of expired) {
    await log({ userId: row.id, action: 'unsuspend', reason: 'Suspension expired', actorId: SYSTEM_ACTOR })
    invalidateUserKeys(row.id)
    invalidateBalance(row.id)
  }

  return expired.length
}

// ── Audit log ───────────────────────────────────────────────────────────

interface LogEntry {
  userId: string
  action: ModerationAction
  kind?: SuspensionKind | null
  reason?: string | null
  actorId: string
  metadata?: Record<string, unknown>
}

export async function log(entry: LogEntry): Promise<void> {
  await db.insert(moderationLog).values({
    id: generateId(),
    userId: entry.userId,
    action: entry.action,
    kind: entry.kind ?? null,
    reason: entry.reason ?? null,
    actorId: entry.actorId,
    metadata: entry.metadata ?? null,
  })
}

export async function warnUser(userId: string, reason: string, actorId: string): Promise<void> {
  await log({ userId, action: 'warn', reason, actorId })
}

export async function addNote(userId: string, note: string, actorId: string): Promise<void> {
  await log({ userId, action: 'note', reason: note, actorId })
}

export async function moderationHistory(userId: string, limit = 50) {
  return db
    .select()
    .from(moderationLog)
    .where(eq(moderationLog.userId, userId))
    .orderBy(desc(moderationLog.createdAt))
    .limit(limit)
}

// ── Abuse signals ───────────────────────────────────────────────────────

export interface RecordSignalOptions {
  userId?: string | null
  kind: AbuseSignalKind
  severity: 'low' | 'medium' | 'high'
  detail?: Record<string, unknown>
  ipHash?: string | null
  /**
   * Suppress duplicates for this many milliseconds. A burn-rate detector runs
   * every few minutes against a condition that persists for hours, so without
   * this the queue fills with the same finding and a human stops reading it.
   */
  dedupeWindowMs?: number
}

export async function recordAbuseSignal(options: RecordSignalOptions): Promise<AbuseSignal | null> {
  const { userId = null, kind, severity, detail, ipHash = null, dedupeWindowMs = 6 * 60 * 60_000 } = options

  if (dedupeWindowMs > 0) {
    const since = new Date(Date.now() - dedupeWindowMs)
    const [existing] = await db
      .select({ id: abuseSignals.id })
      .from(abuseSignals)
      .where(
        and(
          eq(abuseSignals.kind, kind),
          gte(abuseSignals.createdAt, since),
          userId ? eq(abuseSignals.userId, userId) : ipHash ? eq(abuseSignals.ipHash, ipHash) : undefined,
        ),
      )
      .limit(1)
    if (existing) return null
  }

  const [row] = await db
    .insert(abuseSignals)
    .values({ id: generateId(), userId, kind, severity, detail: detail ?? null, ipHash })
    .returning()

  if (row) console.warn(`[abuse] ${severity} ${kind}${userId ? ` for ${userId}` : ''}`)
  return row ?? null
}

export async function listAbuseSignals(options: { includeResolved?: boolean; limit?: number } = {}) {
  const { includeResolved = false, limit = 100 } = options

  const query = db
    .select({
      id: abuseSignals.id,
      userId: abuseSignals.userId,
      kind: abuseSignals.kind,
      severity: abuseSignals.severity,
      detail: abuseSignals.detail,
      resolvedAt: abuseSignals.resolvedAt,
      createdAt: abuseSignals.createdAt,
      email: users.email,
      suspendedAt: users.suspendedAt,
    })
    .from(abuseSignals)
    .leftJoin(users, eq(abuseSignals.userId, users.id))

  const filtered = includeResolved ? query : query.where(isNull(abuseSignals.resolvedAt))
  return filtered.orderBy(desc(abuseSignals.createdAt)).limit(limit)
}

export async function resolveAbuseSignal(id: string, actorId: string): Promise<boolean> {
  const [row] = await db
    .update(abuseSignals)
    .set({ resolvedAt: new Date(), resolvedBy: actorId })
    .where(and(eq(abuseSignals.id, id), isNull(abuseSignals.resolvedAt)))
    .returning({ id: abuseSignals.id })
  return Boolean(row)
}

export async function countOpenSignals(): Promise<number> {
  const [row] = await db
    .select({ count: dsql<number>`count(*)::int` })
    .from(abuseSignals)
    .where(isNull(abuseSignals.resolvedAt))
  return row?.count ?? 0
}

// ── Suspension state for callers ────────────────────────────────────────

export interface SuspensionInfo {
  suspended: boolean
  reason: string | null
  kind: SuspensionKind | null
  until: string | null
  /** Whether the user can plausibly do something about it themselves. */
  appealable: boolean
}

export function describeSuspension(user: {
  suspendedAt: Date | null
  suspendedReason: string | null
  suspendedKind: SuspensionKind | null
  suspendedUntil: Date | null
}): SuspensionInfo {
  if (!user.suspendedAt) {
    return { suspended: false, reason: null, kind: null, until: null, appealable: false }
  }
  return {
    suspended: true,
    reason: user.suspendedReason,
    kind: user.suspendedKind,
    until: user.suspendedUntil?.toISOString() ?? null,
    // A billing hold clears itself once payment succeeds; a judgement about
    // conduct needs a human to look again.
    appealable: user.suspendedKind !== 'billing',
  }
}

/** Accounts sharing a sign-up address — the cheap multi-account signal. */
export async function accountsSharingIp(ipHash: string): Promise<Array<{ id: string; email: string }>> {
  return db
    .select({ id: users.id, email: users.email })
    .from(users)
    .where(eq(users.signupIpHash, ipHash))
}

export type { SuspensionKind, AbuseSignalKind }
