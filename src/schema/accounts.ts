/**
 * Account, auth, API-key and usage tables for the public API.
 *
 * Unlike `geo-places.ts` and `gtfs.ts` — which describe tables osm2pgsql and
 * the importers create, and exist here for reference — these definitions are
 * used at runtime: the services below query through drizzle, and Lucia's
 * Drizzle adapter is handed `users` and `sessions` directly.
 *
 * The tables themselves are created by `ensureAccountsSchema()` in
 * `services/accounts.service.ts`, which runs idempotent DDL at startup like
 * every other barrelman table. Keep the two in sync when changing a column.
 */
import {
  pgTable,
  text,
  integer,
  boolean,
  timestamp,
  date,
  bigint,
  jsonb,
  index,
  uniqueIndex,
  primaryKey,
} from 'drizzle-orm/pg-core'

/** Roles are a flat ladder, not a set — `admin` implies everything `user` has. */
export type UserRole = 'user' | 'admin'

/**
 * Why an account is suspended. The distinction matters to the user: `abuse`
 * and `tos-violation` are judgements to appeal, while `billing` and
 * `automated-abuse` clear themselves once the underlying condition does.
 */
export type SuspensionKind =
  | 'tos-violation'
  | 'abuse'
  | 'automated-abuse'
  | 'billing'
  | 'spam'
  | 'operator-request'

export const users = pgTable(
  'accounts_users',
  {
    id: text('id').primaryKey(),
    /** Address as the user typed it — what we send mail to and display back. */
    email: text('email').notNull(),
    /**
     * Lowercased, provider-canonicalised form (gmail dots and `+tags` folded
     * away). Uniqueness lives HERE, not on `email`, so one inbox cannot farm
     * an unlimited number of free monthly credit grants. See lib/email.ts.
     */
    emailNormalized: text('email_normalized').notNull(),
    name: text('name'),
    picture: text('picture'),
    role: text('role').notNull().default('user').$type<UserRole>(),
    /** Current plan id (see billing/plans.ts). Mirrors the Polar subscription. */
    plan: text('plan').notNull().default('free'),
    polarCustomerId: text('polar_customer_id'),
    /**
     * SHA-256 of the IP that created the account. Used only to rate-limit
     * sign-ups per address — we never need the address back, so we don't keep it.
     */
    signupIpHash: text('signup_ip_hash'),
    /** Set when the account is disabled; blocks sign-in and every API key. */
    suspendedAt: timestamp('suspended_at', { withTimezone: true }),
    /**
     * Why it was suspended. Shown verbatim to the user — someone who has been
     * cut off is owed a reason they can act on, and a support thread that
     * starts with "your account is disabled, no idea why" helps nobody.
     */
    suspendedReason: text('suspended_reason'),
    /** Category, for filtering and for deciding whether an appeal is possible. */
    suspendedKind: text('suspended_kind').$type<SuspensionKind>(),
    /** Admin user id, or `system` for an automated action. */
    suspendedBy: text('suspended_by'),
    /** When the suspension lifts on its own. Null means indefinite. */
    suspendedUntil: timestamp('suspended_until', { withTimezone: true }),
    /** Terms version this account has accepted; null means never accepted. */
    tosVersion: text('tos_version'),
    tosAcceptedAt: timestamp('tos_accepted_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('accounts_users_email_normalized_idx').on(t.emailNormalized),
    uniqueIndex('accounts_users_polar_customer_idx').on(t.polarCustomerId),
  ],
)

/**
 * Lucia owns this table: `id`, `user_id` and `expires_at` are the shape its
 * Drizzle adapter expects. The remaining columns are session attributes we
 * populate ourselves for the "active sessions" list in the console.
 */
export const sessions = pgTable(
  'accounts_sessions',
  {
    id: text('id').primaryKey(),
    userId: text('user_id')
      .notNull()
      .references(() => users.id, { onDelete: 'cascade' }),
    expiresAt: timestamp('expires_at', { withTimezone: true, mode: 'date' }).notNull(),
    ipHash: text('ip_hash'),
    userAgent: text('user_agent'),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [index('accounts_sessions_user_idx').on(t.userId)],
)

/**
 * Single-use, short-lived secrets: currently just email sign-in codes. Stored
 * as a digest so a database read never yields a usable code.
 */
export const authTokens = pgTable(
  'accounts_auth_tokens',
  {
    id: text('id').primaryKey(),
    userId: text('user_id')
      .notNull()
      .references(() => users.id, { onDelete: 'cascade' }),
    type: text('type').notNull().$type<'otp'>(),
    hash: text('hash').notNull(),
    /** Failed attempts against this code; five strikes and it is burned. */
    attempts: integer('attempts').notNull().default(0),
    expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [index('accounts_auth_tokens_user_type_idx').on(t.userId, t.type)],
)

export const passkeys = pgTable(
  'accounts_passkeys',
  {
    /** The WebAuthn credential ID (base64url), as issued by the authenticator. */
    id: text('id').primaryKey(),
    userId: text('user_id')
      .notNull()
      .references(() => users.id, { onDelete: 'cascade' }),
    name: text('name').notNull(),
    publicKey: text('public_key').notNull(),
    counter: bigint('counter', { mode: 'number' }).notNull().default(0),
    deviceType: text('device_type').notNull().default('singleDevice'),
    backedUp: boolean('backed_up').notNull().default(false),
    transports: text('transports').notNull().default(''),
    lastUsedAt: timestamp('last_used_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [index('accounts_passkeys_user_idx').on(t.userId)],
)

export type OAuthProvider = 'google' | 'github' | 'gitlab'

/**
 * One row per linked third-party identity. A user may link several providers
 * (and an email code) to the same account; the provider's stable subject id is
 * what we match on, never the email, which providers allow users to change.
 */
export const oauthAccounts = pgTable(
  'accounts_oauth',
  {
    provider: text('provider').notNull().$type<OAuthProvider>(),
    providerAccountId: text('provider_account_id').notNull(),
    userId: text('user_id')
      .notNull()
      .references(() => users.id, { onDelete: 'cascade' }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.provider, t.providerAccountId] }),
    index('accounts_oauth_user_idx').on(t.userId),
  ],
)


export const apiKeys = pgTable(
  'accounts_api_keys',
  {
    id: text('id').primaryKey(),
    userId: text('user_id')
      .notNull()
      .references(() => users.id, { onDelete: 'cascade' }),
    name: text('name').notNull(),
    /**
     * SHA-256 of the full key. The plaintext is shown once at creation and
     * never again — there is no way to recover it, only to roll a new one.
     */
    hash: text('hash').notNull(),
    /** Leading, non-secret portion (`brm_live_a1b2c3…`) for identification in UI. */
    prefix: text('prefix').notNull(),
    /** Final four characters, so a key can be matched against a copy elsewhere. */
    last4: text('last4').notNull(),
    /** Endpoint groups this key may call; `['*']` means every group. */
    scopes: text('scopes').array().notNull().default(['*']),
    /**
     * Origins this key may be used from, as canonical `scheme://host[:port]`
     * with an optional leading `*.` label. Empty means no restriction, which is
     * what every key predating the column has and what a server-side key wants.
     */
    allowedOrigins: text('allowed_origins').array().notNull().default([]),
    lastUsedAt: timestamp('last_used_at', { withTimezone: true }),
    revokedAt: timestamp('revoked_at', { withTimezone: true }),
    expiresAt: timestamp('expires_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('accounts_api_keys_hash_idx').on(t.hash),
    index('accounts_api_keys_user_idx').on(t.userId),
  ],
)

/**
 * Usage rolled up per key, per day, per endpoint group. Barrelman serves
 * autocomplete traffic — a row per request would outgrow `geo_places` — so the
 * metering layer buffers in memory and upserts these counters periodically.
 */
export const usageRecords = pgTable(
  'accounts_usage',
  {
    userId: text('user_id')
      .notNull()
      .references(() => users.id, { onDelete: 'cascade' }),
    /**
     * The key that made the requests, or `-` for usage with no key attached.
     * A sentinel rather than NULL because this is part of the primary key, and
     * NULL never equals itself — every flush would insert a new row instead of
     * incrementing the existing one.
     */
    apiKeyId: text('api_key_id').notNull().default('-'),
    /** UTC day. Cycle boundaries are calendar months in UTC. */
    day: date('day').notNull(),
    /** Endpoint group as billed (`search`, `routing`, …) — see billing/plans.ts. */
    endpoint: text('endpoint').notNull(),
    requests: integer('requests').notNull().default(0),
    credits: integer('credits').notNull().default(0),
    /** Requests refused for want of credits — surfaced in the console. */
    rejected: integer('rejected').notNull().default(0),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    primaryKey({ columns: [t.userId, t.apiKeyId, t.day, t.endpoint] }),
    index('accounts_usage_user_day_idx').on(t.userId, t.day),
  ],
)

export type CreditEntryKind = 'purchase' | 'grant' | 'adjustment' | 'refund'

/**
 * Credits that exist outside the monthly plan allowance: prepaid packs bought
 * through Polar, and manual operator adjustments. Append-only — the balance is
 * the sum, so a mistaken entry is corrected by writing its inverse.
 */
export const creditLedger = pgTable(
  'accounts_credit_ledger',
  {
    id: text('id').primaryKey(),
    userId: text('user_id')
      .notNull()
      .references(() => users.id, { onDelete: 'cascade' }),
    /** Signed: positive adds credits, negative removes them. */
    amount: integer('amount').notNull(),
    kind: text('kind').notNull().$type<CreditEntryKind>(),
    description: text('description'),
    /**
     * Provider-side identifier (a Polar order id). Unique, so a webhook that
     * Polar retries cannot grant the same pack twice.
     */
    externalId: text('external_id'),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    uniqueIndex('accounts_credit_ledger_external_idx').on(t.externalId),
    index('accounts_credit_ledger_user_idx').on(t.userId),
  ],
)

/**
 * Counts sign-ups per source address so one host cannot mint accounts in bulk
 * for their free grants. Keyed by a hash — we never store the address itself.
 */
export const signupAttempts = pgTable(
  'accounts_signup_attempts',
  {
    ipHash: text('ip_hash').notNull(),
    day: date('day').notNull(),
    count: integer('count').notNull().default(0),
  },
  (t) => [primaryKey({ columns: [t.ipHash, t.day] })],
)

/**
 * Moderation actions, append-only. Suspending is a judgement about a person, so
 * it needs a record of who decided what and why — the columns on `users` hold
 * only the current state, which tells you nothing about how it got there.
 */
export const moderationLog = pgTable(
  'accounts_moderation_log',
  {
    id: text('id').primaryKey(),
    userId: text('user_id')
      .notNull()
      .references(() => users.id, { onDelete: 'cascade' }),
    action: text('action').notNull().$type<ModerationAction>(),
    kind: text('kind').$type<SuspensionKind>(),
    reason: text('reason'),
    /** Admin user id, or `system` when an automated rule acted. */
    actorId: text('actor_id').notNull(),
    metadata: jsonb('metadata').$type<Record<string, unknown>>(),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [index('accounts_moderation_log_user_idx').on(t.userId, t.createdAt)],
)

export type ModerationAction = 'suspend' | 'unsuspend' | 'warn' | 'note' | 'flag' | 'dismiss-flag'

/**
 * Automated abuse detections awaiting a human decision. Kept separate from the
 * moderation log because a signal is an observation, not an action — most
 * resolve as false positives and should never imply anyone did anything.
 */
export const abuseSignals = pgTable(
  'accounts_abuse_signals',
  {
    id: text('id').primaryKey(),
    userId: text('user_id').references(() => users.id, { onDelete: 'cascade' }),
    /** What tripped: `burn-rate`, `error-hammering`, `multi-account`, … */
    kind: text('kind').notNull().$type<AbuseSignalKind>(),
    severity: text('severity').notNull().$type<'low' | 'medium' | 'high'>(),
    detail: jsonb('detail').$type<Record<string, unknown>>(),
    /** Hashed source address, when the signal is about an address not a user. */
    ipHash: text('ip_hash'),
    resolvedAt: timestamp('resolved_at', { withTimezone: true }),
    resolvedBy: text('resolved_by'),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (t) => [
    index('accounts_abuse_signals_user_idx').on(t.userId),
    index('accounts_abuse_signals_open_idx').on(t.resolvedAt, t.createdAt),
  ],
)

export type AbuseSignalKind =
  | 'burn-rate'
  | 'error-hammering'
  | 'multi-account'
  | 'quota-exhausted'
  | 'rate-limit-sustained'

export type User = typeof users.$inferSelect
export type NewUser = typeof users.$inferInsert
export type Session = typeof sessions.$inferSelect
export type AuthToken = typeof authTokens.$inferSelect
export type Passkey = typeof passkeys.$inferSelect
export type OAuthAccount = typeof oauthAccounts.$inferSelect
export type ApiKey = typeof apiKeys.$inferSelect
export type UsageRecord = typeof usageRecords.$inferSelect
export type CreditEntry = typeof creditLedger.$inferSelect
export type ModerationEntry = typeof moderationLog.$inferSelect
export type AbuseSignal = typeof abuseSignals.$inferSelect
