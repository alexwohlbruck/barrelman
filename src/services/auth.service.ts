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
import { db } from '../db'
import { lucia } from '../lib/lucia'
import { authTokens, sessions, type User } from '../schema/accounts'
import { generateId, randomNumericCode, sha256Hex } from '../lib/crypto'
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
 * Issue a sign-in code, replacing any code already outstanding for this user.
 * Returns the plaintext for delivery — it is never recoverable afterwards.
 */
export async function createOtp(userId: string, email?: string): Promise<string> {
  await db.delete(authTokens).where(and(eq(authTokens.userId, userId), eq(authTokens.type, 'otp')))

  const code = email && isTestAccount(email) ? TEST_ACCOUNT_CODE : randomNumericCode(8)

  await db.insert(authTokens).values({
    id: generateId(),
    userId,
    type: 'otp',
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

export async function verifyOtp(userId: string, code: string): Promise<OtpResult> {
  const [token] = await db
    .select()
    .from(authTokens)
    .where(and(eq(authTokens.userId, userId), eq(authTokens.type, 'otp')))
    .orderBy(desc(authTokens.createdAt))
    .limit(1)

  if (!token) return 'invalid'

  if (token.expiresAt.getTime() <= Date.now()) {
    await db.delete(authTokens).where(eq(authTokens.id, token.id))
    return 'expired'
  }

  if (token.hash !== sha256Hex(code.trim())) {
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

/** Clear codes that expired without being used. */
export async function pruneExpiredTokens(): Promise<void> {
  await db.delete(authTokens).where(lt(authTokens.expiresAt, new Date()))
}

// ── Sessions ────────────────────────────────────────────────────────────

export interface SessionContext {
  ip?: string
  userAgent?: string | null
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
    createdAt: new Date(),
  })

  return { session, cookie: lucia.createSessionCookie(session.id).serialize() }
}

export async function endSession(sessionId: string): Promise<string> {
  await lucia.invalidateSession(sessionId)
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
