/**
 * Everything barrelman holds about one account, as a single JSON document: the
 * data-portability and access half of the GDPR, served from the console.
 *
 * Built in two steps. `loadExportSources()` reads whole rows, secrets and all,
 * and `buildAccountExport()` copies fields out of them by name. The copy is an
 * allowlist on purpose: spreading a row would carry along whatever column is
 * added to it next, and the columns that must never leave are credentials (an
 * API key's hash, a passkey's public key, a session id, which is the cookie
 * value itself). A new column stays out of the export until someone adds it
 * here.
 */
import { desc, eq } from 'drizzle-orm'
import { accountsDb as db } from '../db'
import {
  abuseSignals,
  apiKeys,
  creditLedger,
  moderationLog,
  oauthAccounts,
  passkeys,
  sessions,
  users,
  type AbuseSignal,
  type ApiKey,
  type CreditEntry,
  type ModerationEntry,
  type OAuthAccount,
  type Passkey,
  type Session,
  type User,
} from '../schema/accounts'
import { billing } from '../config/billing.config'
import { getPlan } from '../billing/plans'
import { describeTerms, toPublicUser } from './accounts.service'
import { describeSuspension, SYSTEM_ACTOR } from './moderation.service'
import { getBalance, type CreditBalance } from './credits.service'
import { usageByDay, utcDay, type UsageBucket } from './usage.service'
import { accountRequestSummary, type AccountRequestSummary } from './request-metrics.service'

/** Bumped when a field is renamed or removed, so a consumer can tell versions apart. */
export const EXPORT_FORMAT_VERSION = 1

export interface ExportSources {
  user: User
  oauth: OAuthAccount[]
  passkeys: Passkey[]
  apiKeys: ApiKey[]
  sessions: Session[]
  usage: UsageBucket[]
  requestMetrics: AccountRequestSummary
  ledger: CreditEntry[]
  moderation: ModerationEntry[]
  abuseSignals: AbuseSignal[]
  balance: CreditBalance | null
}

const iso = (value: Date | null | undefined) => value?.toISOString() ?? null

export function buildAccountExport(sources: ExportSources, now: Date = new Date()) {
  const { user } = sources
  const plan = getPlan(user.plan)

  return {
    format: 'barrelman-account-export',
    version: EXPORT_FORMAT_VERSION,
    exportedAt: now.toISOString(),

    profile: {
      ...toPublicUser(user),
      updatedAt: iso(user.updatedAt),
      // A digest of the address the account was created from, kept to limit
      // sign-ups per network. It is about this person, so it is theirs to see.
      signupIpHash: user.signupIpHash,
    },
    terms: describeTerms(user),
    suspension: describeSuspension(user),

    billing: {
      billingEnabled: billing.enabled,
      plan: { id: plan.id, name: plan.name },
      polarCustomerId: user.polarCustomerId,
      subscription: user.subscriptionId
        ? {
            id: user.subscriptionId,
            status: user.subscriptionStatus,
            cancelAtPeriodEnd: user.subscriptionCancelAtPeriodEnd,
            currentPeriodEnd: iso(user.subscriptionPeriodEnd),
          }
        : null,
      balance: sources.balance
        ? {
            monthlyCredits: sources.balance.monthlyCredits,
            used: sources.balance.used,
            purchased: sources.balance.purchased,
            remaining: sources.balance.remaining,
            overage: sources.balance.overage,
          }
        : null,
    },

    // Provider and subject only. Barrelman stores no provider tokens.
    oauthIdentities: sources.oauth.map((row) => ({
      provider: row.provider,
      subject: row.providerAccountId,
      linkedAt: iso(row.createdAt),
    })),

    // Names and dates. No credential id, public key or counter.
    passkeys: sources.passkeys.map((row) => ({
      name: row.name,
      deviceType: row.deviceType,
      backedUp: row.backedUp,
      createdAt: iso(row.createdAt),
      lastUsedAt: iso(row.lastUsedAt),
    })),

    // No hash: the plaintext was shown once at creation and is not stored.
    apiKeys: sources.apiKeys.map((row) => ({
      id: row.id,
      name: row.name,
      prefix: row.prefix,
      last4: row.last4,
      scopes: row.scopes,
      allowedOrigins: row.allowedOrigins,
      createdAt: iso(row.createdAt),
      lastUsedAt: iso(row.lastUsedAt),
      revokedAt: iso(row.revokedAt),
      expiresAt: iso(row.expiresAt),
    })),

    // No session id: it is the cookie value, so exporting it would export a
    // live credential.
    sessions: sources.sessions.map((row) => ({
      userAgent: row.userAgent,
      createdAt: iso(row.createdAt),
      expiresAt: iso(row.expiresAt),
    })),

    usageByDay: sources.usage,
    requestMetrics: sources.requestMetrics,

    creditLedger: sources.ledger.map((row) => ({
      amount: row.amount,
      kind: row.kind,
      description: row.description,
      createdAt: iso(row.createdAt),
    })),

    // Who acted is reduced to "administrator" or "system": the person is owed
    // what was decided about them, not the identity of the staff member.
    moderationLog: sources.moderation.map((row) => ({
      action: row.action,
      kind: row.kind,
      reason: row.reason,
      actor: row.actorId === SYSTEM_ACTOR ? 'system' : 'administrator',
      createdAt: iso(row.createdAt),
    })),

    // What tripped and when, without the detector's thresholds and readings.
    abuseSignals: sources.abuseSignals.map((row) => ({
      kind: row.kind,
      severity: row.severity,
      createdAt: iso(row.createdAt),
      resolvedAt: iso(row.resolvedAt),
    })),
  }
}

export type AccountExport = ReturnType<typeof buildAccountExport>

export async function loadExportSources(userId: string): Promise<ExportSources | null> {
  const [user] = await db.select().from(users).where(eq(users.id, userId)).limit(1)
  if (!user) return null

  const [
    oauth,
    passkeyRows,
    keyRows,
    sessionRows,
    usage,
    requestMetrics,
    ledger,
    moderation,
    signals,
    balance,
  ] = await Promise.all([
    db.select().from(oauthAccounts).where(eq(oauthAccounts.userId, userId)),
    db.select().from(passkeys).where(eq(passkeys.userId, userId)),
    db.select().from(apiKeys).where(eq(apiKeys.userId, userId)).orderBy(desc(apiKeys.createdAt)),
    db.select().from(sessions).where(eq(sessions.userId, userId)).orderBy(desc(sessions.createdAt)),
    // Every day the account has ever been metered, by endpoint group.
    usageByDay(userId, '1970-01-01', utcDay()),
    accountRequestSummary(userId),
    db.select().from(creditLedger).where(eq(creditLedger.userId, userId)).orderBy(desc(creditLedger.createdAt)),
    db.select().from(moderationLog).where(eq(moderationLog.userId, userId)).orderBy(desc(moderationLog.createdAt)),
    db.select().from(abuseSignals).where(eq(abuseSignals.userId, userId)).orderBy(desc(abuseSignals.createdAt)),
    // The balance is a convenience, derived from the rows above; an export
    // should not fail because the cache behind it did.
    getBalance(userId).catch(() => null),
  ])

  return {
    user,
    oauth,
    passkeys: passkeyRows,
    apiKeys: keyRows,
    sessions: sessionRows,
    usage,
    requestMetrics,
    ledger,
    moderation,
    abuseSignals: signals,
    balance,
  }
}

export async function exportAccount(userId: string): Promise<AccountExport | null> {
  const sources = await loadExportSources(userId)
  return sources ? buildAccountExport(sources) : null
}
