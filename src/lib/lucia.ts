/**
 * Lucia session manager, backed by the same Postgres the rest of barrelman uses.
 *
 * Lucia owns session lifetime, the sliding-expiry renewal and cookie
 * serialisation; everything else about an account (roles, plans, API keys) is
 * ours. Session rows carry a few extra attributes — the device and a hashed
 * source address — purely so the console can show "where am I signed in".
 *
 * Note that Lucia stores session IDs in the clear: the cookie value IS the
 * primary key of the row. That is the library's design, and it matches
 * Parchment's setup, so treat the sessions table as credential material.
 */
import { Lucia, TimeSpan } from 'lucia'
import { DrizzlePostgreSQLAdapter } from '@lucia-auth/adapter-drizzle'
import { db } from '../db'
import { sessions, users, type UserRole } from '../schema/accounts'
import { isProduction, sessionCookieName, sessionTtlMs } from '../config/accounts.config'

const adapter = new DrizzlePostgreSQLAdapter(db, sessions, users)

export const lucia = new Lucia(adapter, {
  sessionExpiresIn: new TimeSpan(sessionTtlMs, 'ms'),
  sessionCookie: {
    name: sessionCookieName,
    // `expires: false` makes it a persistent cookie whose lifetime Lucia
    // refreshes as the session slides forward, rather than a session cookie
    // that dies with the browser window.
    expires: false,
    attributes: {
      secure: isProduction,
      sameSite: 'lax',
      path: '/',
    },
  },
  getUserAttributes: (attrs) => ({
    email: attrs.email,
    emailNormalized: attrs.emailNormalized,
    name: attrs.name,
    picture: attrs.picture,
    role: attrs.role,
    plan: attrs.plan,
    suspendedAt: attrs.suspendedAt,
    createdAt: attrs.createdAt,
  }),
  getSessionAttributes: (attrs) => ({
    userAgent: attrs.userAgent,
    ipHash: attrs.ipHash,
    createdAt: attrs.createdAt,
  }),
})

declare module 'lucia' {
  interface Register {
    Lucia: typeof lucia
    DatabaseUserAttributes: {
      email: string
      emailNormalized: string
      name: string | null
      picture: string | null
      role: UserRole
      plan: string
      suspendedAt: Date | null
      createdAt: Date
    }
    DatabaseSessionAttributes: {
      userAgent: string | null
      ipHash: string | null
      createdAt: Date
    }
  }
}
