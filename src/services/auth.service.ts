/**
 * Sign-in mechanics: one-time email codes and browser sessions.
 *
 * Codes are stored as digests and are single-use — a verified code is deleted
 * before the session is issued, so the same code can never buy two sessions.
 * Session lifetime is Lucia's; everything here is about getting a user to the
 * point where Lucia can mint one.
 */
import { and, desc, eq, lt } from 'drizzle-orm'
import type { Session } from 'lucia'
import { accountsDb as db } from '../db'
import { lucia } from '../lib/lucia'
import { authTokens, sessions, type AuthTokenType, type SignInMethod, type User } from '../schema/accounts'
import { generateId, randomNumericCode, safeEqual, sha256Hex } from '../lib/crypto'
import { otpTtlMs } from '../config/accounts.config'
import { hashIp } from './accounts.service'

/** Wrong guesses tolerated per code before it is destroyed. */
const MAX_OTP_ATTEMPTS = 5

/**
 * Fixed code for the address named in `BARRELMAN_TEST_ACCOUNT_EMAIL`, so
 * end-to-end tests and app-store reviewers can sign in without an inbox. Unset
 * in any real deployment.
 */
const TEST_ACCOUNT_EMAIL = (process.env.BARRELMAN_TEST_ACCOUNT_EMAIL || '').trim().toLowerCase()
const TEST_ACCOUNT_CODE = '00000000'

export function isTestAccount(email: string): boolean {
  return TEST_ACCOUNT_EMAIL.length > 0 && email.trim().toLowerCase() === TEST_ACCOUNT_EMAIL
}

// ── One-time codes ──────────────────────────────────────────────────────

/**
 * Issue a one-time code, replacing any code of the same type already
 * outstanding for this user. Returns the plaintext for delivery; it is never
 * recoverable afterwards.
 *
 * The fixed test-account code applies to sign-in only. A deletion code is
 * always random, so a reviewer holding the test address cannot erase the
 * account the next reviewer needs.
 */
export async function createOtp(userId: string, email?: string, type: AuthTokenType = 'otp'): Promise<string> {
  await db.delete(authTokens).where(and(eq(authTokens.userId, userId), eq(authTokens.type, type)))

  const code = type === 'otp' && email && isTestAccount(email) ? TEST_ACCOUNT_CODE : randomNumericCode(8)

  await db.insert(authTokens).values({
    id: generateId(),
    userId,
    type,
    hash: sha256Hex(code),
    expiresAt: new Date(Date.now() + otpTtlMs),
  })

  return code
}

/**
 * `expired` is reported separately from `invalid` so the UI can say "request a
 * new code" instead of making the user doubt what they typed. It reveals
 * nothing an attacker couldn't learn by waiting out the clock.
 */
export type OtpResult = 'valid' | 'invalid' | 'expired' | 'too-many-attempts'

export async function verifyOtp(userId: string, code: string, type: AuthTokenType = 'otp'): Promise<OtpResult> {
  const [token] = await db
    .select()
    .from(authTokens)
    .where(and(eq(authTokens.userId, userId), eq(authTokens.type, type)))
    .orderBy(desc(authTokens.createdAt))
    .limit(1)

  if (!token) return 'invalid'

  if (token.expiresAt.getTime() <= Date.now()) {
    await db.delete(authTokens).where(eq(authTokens.id, token.id))
    return 'expired'
  }

  if (!safeEqual(token.hash, sha256Hex(code.trim()))) {
    const attempts = token.attempts + 1
    if (attempts >= MAX_OTP_ATTEMPTS) {
      // Burn the code rather than let it be ground down by guessing. Eight
      // digits is 100M possibilities, but a code sitting for 15 minutes under
      // unlimited attempts is a different proposition.
      await db.delete(authTokens).where(eq(authTokens.id, token.id))
      return 'too-many-attempts'
    }
    await db.update(authTokens).set({ attempts }).where(eq(authTokens.id, token.id))
    return 'invalid'
  }

  await db.delete(authTokens).where(eq(authTokens.id, token.id))
  return 'valid'
}

// ── Confirmation challenges ─────────────────────────────────────────────

/** A WebAuthn confirmation is a few seconds of interaction; five minutes is generous. */
export const CHALLENGE_TTL_MS = 5 * 60_000

/**
 * Remember a WebAuthn challenge issued to `userId` for one purpose, replacing
 * any earlier one. Kept server-side rather than in a cookie: the client
 * controls its cookies, so a cookie challenge lets someone holding a session
 * answer with any assertion they captured, a sign-in one included.
 */
export async function storeChallenge(userId: string, type: AuthTokenType, challenge: string): Promise<void> {
  await db.delete(authTokens).where(and(eq(authTokens.userId, userId), eq(authTokens.type, type)))
  await db.insert(authTokens).values({
    id: generateId(),
    userId,
    type,
    hash: sha256Hex(challenge),
    expiresAt: new Date(Date.now() + CHALLENGE_TTL_MS),
  })
}

/**
 * Spend the challenge stored for (`userId`, `type`). True only when `challenge`
 * is that challenge and it has not expired. The row is deleted whatever the
 * outcome, so each challenge answers at most one attempt.
 */
export async function consumeChallenge(userId: string, type: AuthTokenType, challenge: string): Promise<boolean> {
  const [token] = await db
    .delete(authTokens)
    .where(and(eq(authTokens.userId, userId), eq(authTokens.type, type)))
    .returning()
  if (!token) return false
  if (token.expiresAt.getTime() <= Date.now()) return false
  return safeEqual(token.hash, sha256Hex(challenge))
}

/** Clear codes that expired without being used. */
export async function pruneExpiredTokens(): Promise<void> {
  await db.delete(authTokens).where(lt(authTokens.expiresAt, new Date()))
}

// ── Sessions ────────────────────────────────────────────────────────────

export interface SessionContext {
  ip?: string
  userAgent?: string | null
  /** Recorded on the row; see `SignInMethod`. Omitted means unknown. */
  method?: SignInMethod
}

/**
 * Start a session and return both the Lucia session and the `Set-Cookie` value
 * for it. Callers decide whether to set the cookie: browsers want it, API
 * clients signing in with a bearer flow just keep the token.
 */
export async function startSession(
  userId: string,
  context: SessionContext = {},
): Promise<{ session: Session; cookie: string }> {
  const session = await lucia.createSession(userId, {
    ipHash: context.ip ? hashIp(context.ip) : null,
    userAgent: context.userAgent?.slice(0, 400) ?? null,
    authMethod: context.method ?? null,
    createdAt: new Date(),
  })

  return { session, cookie: lucia.createSessionCookie(session.id).serialize() }
}

export async function endSession(sessionId: string): Promise<string> {
  await lucia.invalidateSession(sessionId)
  return lucia.createBlankSessionCookie().serialize()
}

/** A `Set-Cookie` value that clears the session cookie. */
export function blankSessionCookie(): string {
  return lucia.createBlankSessionCookie().serialize()
}

export async function endAllSessions(userId: string): Promise<void> {
  await lucia.invalidateUserSessions(userId)
}

export async function endOtherSessions(userId: string, keepSessionId: string): Promise<void> {
  const all = await lucia.getUserSessions(userId)
  await Promise.all(all.filter((s) => s.id !== keepSessionId).map((s) => lucia.invalidateSession(s.id)))
}

export interface SessionSummary {
  id: string
  userAgent: string | null
  createdAt: Date
  expiresAt: Date
  current: boolean
}

export async function listSessions(userId: string, currentSessionId?: string): Promise<SessionSummary[]> {
  const rows = await db
    .select()
    .from(sessions)
    .where(eq(sessions.userId, userId))
    .orderBy(desc(sessions.createdAt))

  return rows.map((row) => ({
    id: row.id,
    userAgent: row.userAgent,
    createdAt: row.createdAt,
    expiresAt: row.expiresAt,
    current: row.id === currentSessionId,
  }))
}

export async function deleteSession(userId: string, sessionId: string): Promise<boolean> {
  const [row] = await db
    .delete(sessions)
    .where(and(eq(sessions.id, sessionId), eq(sessions.userId, userId)))
    .returning({ id: sessions.id })
  return Boolean(row)
}

export type { User }
