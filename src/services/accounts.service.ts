/**
 * Account storage: schema bootstrap plus user lookup and creation.
 *
 * The DDL here mirrors `schema/accounts.ts` and runs on every startup like
 * barrelman's other `ensure*Schema()` functions — a fresh database gets the
 * tables without a separate migration step, and an existing one is untouched.
 * When you change a column in the drizzle definitions, change it here too.
 */
import { and, desc, eq, isNull, sql as dsql } from 'drizzle-orm'
import { connection as sql, db } from '../db'
import { users, type NewUser, type User, type UserRole } from '../schema/accounts'
import { generateId, sha256Hex } from '../lib/crypto'
import { emailDomain, isDisposableEmail, normalizeEmail } from '../lib/email'
import { adminEmails, registrationMode } from '../config/accounts.config'

let schemaReady: Promise<void> | null = null

export function ensureAccountsSchema(): Promise<void> {
  if (!schemaReady) {
    schemaReady = (async () => {
      await sql`
        CREATE TABLE IF NOT EXISTS accounts_users (
          id                text PRIMARY KEY,
          email             text NOT NULL,
          email_normalized  text NOT NULL,
          name              text,
          picture           text,
          role              text NOT NULL DEFAULT 'user',
          plan              text NOT NULL DEFAULT 'free',
          polar_customer_id text,
          signup_ip_hash    text,
          suspended_at      timestamptz,
          created_at        timestamptz NOT NULL DEFAULT now(),
          updated_at        timestamptz NOT NULL DEFAULT now()
        )`
      await sql`CREATE UNIQUE INDEX IF NOT EXISTS accounts_users_email_normalized_idx ON accounts_users (email_normalized)`
      await sql`CREATE UNIQUE INDEX IF NOT EXISTS accounts_users_polar_customer_idx ON accounts_users (polar_customer_id)`

      await sql`
        CREATE TABLE IF NOT EXISTS accounts_sessions (
          id         text PRIMARY KEY,
          user_id    text NOT NULL REFERENCES accounts_users(id) ON DELETE CASCADE,
          expires_at timestamptz NOT NULL,
          ip_hash    text,
          user_agent text,
          created_at timestamptz NOT NULL DEFAULT now()
        )`
      await sql`CREATE INDEX IF NOT EXISTS accounts_sessions_user_idx ON accounts_sessions (user_id)`

      await sql`
        CREATE TABLE IF NOT EXISTS accounts_auth_tokens (
          id         text PRIMARY KEY,
          user_id    text NOT NULL REFERENCES accounts_users(id) ON DELETE CASCADE,
          type       text NOT NULL,
          hash       text NOT NULL,
          attempts   integer NOT NULL DEFAULT 0,
          expires_at timestamptz NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now()
        )`
      await sql`CREATE INDEX IF NOT EXISTS accounts_auth_tokens_user_type_idx ON accounts_auth_tokens (user_id, type)`

      await sql`
        CREATE TABLE IF NOT EXISTS accounts_passkeys (
          id           text PRIMARY KEY,
          user_id      text NOT NULL REFERENCES accounts_users(id) ON DELETE CASCADE,
          name         text NOT NULL,
          public_key   text NOT NULL,
          counter      bigint NOT NULL DEFAULT 0,
          device_type  text NOT NULL DEFAULT 'singleDevice',
          backed_up    boolean NOT NULL DEFAULT false,
          transports   text NOT NULL DEFAULT '',
          last_used_at timestamptz,
          created_at   timestamptz NOT NULL DEFAULT now()
        )`
      await sql`CREATE INDEX IF NOT EXISTS accounts_passkeys_user_idx ON accounts_passkeys (user_id)`

      await sql`
        CREATE TABLE IF NOT EXISTS accounts_oauth (
          provider            text NOT NULL,
          provider_account_id text NOT NULL,
          user_id             text NOT NULL REFERENCES accounts_users(id) ON DELETE CASCADE,
          created_at          timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (provider, provider_account_id)
        )`
      await sql`CREATE INDEX IF NOT EXISTS accounts_oauth_user_idx ON accounts_oauth (user_id)`

      await sql`
        CREATE TABLE IF NOT EXISTS accounts_api_keys (
          id           text PRIMARY KEY,
          user_id      text NOT NULL REFERENCES accounts_users(id) ON DELETE CASCADE,
          name         text NOT NULL,
          hash         text NOT NULL,
          prefix       text NOT NULL,
          last4        text NOT NULL,
          environment  text NOT NULL DEFAULT 'live',
          scopes       text[] NOT NULL DEFAULT ARRAY['*'],
          last_used_at timestamptz,
          revoked_at   timestamptz,
          expires_at   timestamptz,
          created_at   timestamptz NOT NULL DEFAULT now()
        )`
      await sql`CREATE UNIQUE INDEX IF NOT EXISTS accounts_api_keys_hash_idx ON accounts_api_keys (hash)`
      await sql`CREATE INDEX IF NOT EXISTS accounts_api_keys_user_idx ON accounts_api_keys (user_id)`

      // api_key_id is nullable (usage can predate or outlive a key) and NULL is
      // never equal to itself in a primary key, so the key column is coalesced
      // to a sentinel instead of being left NULL.
      await sql`
        CREATE TABLE IF NOT EXISTS accounts_usage (
          user_id    text NOT NULL REFERENCES accounts_users(id) ON DELETE CASCADE,
          api_key_id text NOT NULL DEFAULT '-',
          day        date NOT NULL,
          endpoint   text NOT NULL,
          requests   integer NOT NULL DEFAULT 0,
          credits    integer NOT NULL DEFAULT 0,
          rejected   integer NOT NULL DEFAULT 0,
          updated_at timestamptz NOT NULL DEFAULT now(),
          PRIMARY KEY (user_id, api_key_id, day, endpoint)
        )`
      await sql`CREATE INDEX IF NOT EXISTS accounts_usage_user_day_idx ON accounts_usage (user_id, day)`

      await sql`
        CREATE TABLE IF NOT EXISTS accounts_credit_ledger (
          id          text PRIMARY KEY,
          user_id     text NOT NULL REFERENCES accounts_users(id) ON DELETE CASCADE,
          amount      integer NOT NULL,
          kind        text NOT NULL,
          description text,
          external_id text,
          created_at  timestamptz NOT NULL DEFAULT now()
        )`
      await sql`CREATE UNIQUE INDEX IF NOT EXISTS accounts_credit_ledger_external_idx ON accounts_credit_ledger (external_id)`
      await sql`CREATE INDEX IF NOT EXISTS accounts_credit_ledger_user_idx ON accounts_credit_ledger (user_id)`

      await sql`
        CREATE TABLE IF NOT EXISTS accounts_signup_attempts (
          ip_hash text NOT NULL,
          day     date NOT NULL,
          count   integer NOT NULL DEFAULT 0,
          PRIMARY KEY (ip_hash, day)
        )`
    })()
  }
  return schemaReady
}

// ── Lookup ──────────────────────────────────────────────────────────────

export async function findUserById(id: string): Promise<User | null> {
  const [row] = await db.select().from(users).where(eq(users.id, id)).limit(1)
  return row ?? null
}

/** Matches on the normalised form, so `a.b+tag@gmail.com` finds `ab@gmail.com`. */
export async function findUserByEmail(email: string): Promise<User | null> {
  const [row] = await db
    .select()
    .from(users)
    .where(eq(users.emailNormalized, normalizeEmail(email)))
    .limit(1)
  return row ?? null
}

export async function findUserByPolarCustomerId(polarCustomerId: string): Promise<User | null> {
  const [row] = await db
    .select()
    .from(users)
    .where(eq(users.polarCustomerId, polarCustomerId))
    .limit(1)
  return row ?? null
}

export async function countUsers(): Promise<number> {
  const [row] = await db.select({ count: dsql<number>`count(*)::int` }).from(users)
  return row?.count ?? 0
}

// ── Creation ────────────────────────────────────────────────────────────

/** Why a sign-up was refused. Each maps to a distinct message in the UI. */
export type SignupRejection = 'disposable-email' | 'invite-only' | 'ip-limit'

export class SignupError extends Error {
  constructor(public reason: SignupRejection, message: string) {
    super(message)
    this.name = 'SignupError'
  }
}

/**
 * The first account on a fresh instance is an admin — otherwise a new
 * deployment has a console nobody can administer. After that, only addresses
 * named in `BARRELMAN_ADMIN_EMAILS` are promoted automatically.
 */
async function roleForNewUser(email: string): Promise<UserRole> {
  if (adminEmails.has(email.trim().toLowerCase())) return 'admin'
  if (adminEmails.has(normalizeEmail(email))) return 'admin'
  return (await countUsers()) === 0 ? 'admin' : 'user'
}

export interface CreateUserOptions {
  email: string
  name?: string | null
  picture?: string | null
  ipHash?: string | null
  /** Skip the invite-only gate — used by operator-driven creation. */
  force?: boolean
}

export async function createUser(options: CreateUserOptions): Promise<User> {
  const { email, name = null, picture = null, ipHash = null, force = false } = options

  if (!force && registrationMode === 'invite') {
    throw new SignupError('invite-only', 'This instance is invite-only — ask an administrator for access.')
  }
  if (!force && isDisposableEmail(email)) {
    throw new SignupError(
      'disposable-email',
      `Addresses at ${emailDomain(email)} are not accepted — please use a permanent email address.`,
    )
  }

  const payload: NewUser = {
    id: generateId(),
    email: email.trim(),
    emailNormalized: normalizeEmail(email),
    name,
    picture,
    role: await roleForNewUser(email),
    plan: 'free',
    signupIpHash: ipHash,
  }

  // A concurrent request for the same address loses the unique-index race;
  // return the winner's row rather than surfacing a constraint violation.
  const [row] = await db.insert(users).values(payload).onConflictDoNothing().returning()
  if (row) return row

  const existing = await findUserByEmail(email)
  if (!existing) throw new Error('Failed to create user')
  return existing
}

/** Fetch by email, creating the account if this is a first-time sign-in. */
export async function findOrCreateUser(options: CreateUserOptions): Promise<{ user: User; created: boolean }> {
  const existing = await findUserByEmail(options.email)
  if (existing) return { user: existing, created: false }
  return { user: await createUser(options), created: true }
}

// ── Mutation ────────────────────────────────────────────────────────────

export async function updateUser(
  id: string,
  patch: Partial<Pick<User, 'name' | 'picture' | 'email' | 'role' | 'plan' | 'polarCustomerId' | 'suspendedAt'>>,
): Promise<User | null> {
  const values: Record<string, unknown> = { ...patch, updatedAt: new Date() }
  if (patch.email) values.emailNormalized = normalizeEmail(patch.email)
  const [row] = await db.update(users).set(values).where(eq(users.id, id)).returning()
  return row ?? null
}

export async function linkPolarCustomer(userId: string, polarCustomerId: string): Promise<void> {
  await db
    .update(users)
    .set({ polarCustomerId, updatedAt: new Date() })
    .where(eq(users.id, userId))
}

export async function deleteUser(id: string): Promise<void> {
  await db.delete(users).where(eq(users.id, id))
}

// ── Operator views ──────────────────────────────────────────────────────

export interface UserListOptions {
  limit?: number
  offset?: number
  search?: string
}

export async function listUsers({ limit = 50, offset = 0, search }: UserListOptions = {}): Promise<User[]> {
  const query = db.select().from(users)
  const filtered = search
    ? query.where(dsql`${users.email} ILIKE ${'%' + search + '%'} OR ${users.name} ILIKE ${'%' + search + '%'}`)
    : query
  return filtered.orderBy(desc(users.createdAt)).limit(limit).offset(offset)
}

export async function countActiveUsers(): Promise<number> {
  const [row] = await db
    .select({ count: dsql<number>`count(*)::int` })
    .from(users)
    .where(isNull(users.suspendedAt))
  return row?.count ?? 0
}

// ── Sign-up throttling ──────────────────────────────────────────────────

/** Accounts one address may create per UTC day before we start refusing. */
const SIGNUPS_PER_IP_PER_DAY = Number(process.env.BARRELMAN_SIGNUPS_PER_IP_PER_DAY ?? 5)

export function hashIp(ip: string): string {
  return sha256Hex(ip)
}

/**
 * Count this address against the daily sign-up budget, returning false when it
 * is spent. Called only on the path that would create a NEW account, so a
 * shared office NAT signing in to existing accounts is never affected.
 */
export async function consumeSignupAllowance(ipHash: string): Promise<boolean> {
  if (SIGNUPS_PER_IP_PER_DAY <= 0) return true
  const [row] = await sql<{ count: number }[]>`
    INSERT INTO accounts_signup_attempts (ip_hash, day, count)
    VALUES (${ipHash}, CURRENT_DATE, 1)
    ON CONFLICT (ip_hash, day) DO UPDATE SET count = accounts_signup_attempts.count + 1
    RETURNING count`
  return (row?.count ?? 1) <= SIGNUPS_PER_IP_PER_DAY
}

/** Drop sign-up counters older than a week — they have no value once stale. */
export async function pruneSignupAttempts(): Promise<void> {
  await sql`DELETE FROM accounts_signup_attempts WHERE day < CURRENT_DATE - 7`
}

// ── Serialisation ───────────────────────────────────────────────────────

/** The account fields a client may see. Excludes internal anti-abuse columns. */
export interface PublicUser {
  id: string
  email: string
  name: string | null
  picture: string | null
  role: UserRole
  plan: string
  /** Null only if the caller genuinely has no creation timestamp to report. */
  createdAt: string | null
}

/**
 * Note the absence of a fallback on `createdAt`: an earlier version defaulted a
 * missing value to `now`, which silently reported every account as created at
 * the moment it was read. A missing timestamp is reported as null so the gap is
 * visible rather than plausible.
 */
export function toPublicUser(user: {
  id: string
  email: string
  name: string | null
  picture: string | null
  role: UserRole
  plan: string
  createdAt?: Date | string | null
}): PublicUser {
  return {
    id: user.id,
    email: user.email,
    name: user.name,
    picture: user.picture,
    role: user.role,
    plan: user.plan,
    createdAt: user.createdAt instanceof Date ? user.createdAt.toISOString() : (user.createdAt ?? null),
  }
}

export { normalizeEmail }
export type { User }
