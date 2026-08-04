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
  index,
  uniqueIndex,
  primaryKey,
} from 'drizzle-orm/pg-core'

/** Roles are a flat ladder, not a set — `admin` implies everything `user` has. */
export type UserRole = 'user' | 'admin'

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
    /** Set when an operator disables the account; blocks sign-in and API keys. */
    suspendedAt: timestamp('suspended_at', { withTimezone: true }),
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

export type KeyEnvironment = 'live' | 'test'

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
    environment: text('environment').notNull().default('live').$type<KeyEnvironment>(),
    /** Endpoint groups this key may call; `['*']` means every group. */
    scopes: text('scopes').array().notNull().default(['*']),
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
    apiKeyId: text('api_key_id'),
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

export type User = typeof users.$inferSelect
export type NewUser = typeof users.$inferInsert
export type Session = typeof sessions.$inferSelect
export type AuthToken = typeof authTokens.$inferSelect
export type Passkey = typeof passkeys.$inferSelect
export type OAuthAccount = typeof oauthAccounts.$inferSelect
export type ApiKey = typeof apiKeys.$inferSelect
export type UsageRecord = typeof usageRecords.$inferSelect
export type CreditEntry = typeof creditLedger.$inferSelect
