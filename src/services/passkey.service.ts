/**
 * WebAuthn passkey registration and sign-in.
 *
 * Registration requires an existing session — a passkey is added to an account,
 * never used to create one, so the account's email is always verified first.
 * Sign-in is passwordless and usernameless: `residentKey: 'required'` means the
 * authenticator stores the credential itself, so the browser can offer it
 * without the user typing an address, and the credential id in the response is
 * enough to find the account.
 *
 * The challenge is round-tripped through a short-lived httpOnly cookie rather
 * than a table. It is single-use and meaningless on its own, and this keeps a
 * failed or abandoned ceremony from leaving a row behind.
 */
import {
  generateAuthenticationOptions,
  generateRegistrationOptions,
  verifyAuthenticationResponse,
  verifyRegistrationResponse,
} from '@simplewebauthn/server'
import type { AuthenticationResponseJSON, RegistrationResponseJSON } from '@simplewebauthn/server'
import { and, eq } from 'drizzle-orm'
import { accountsDb as db } from '../db'
import { passkeys, users, type Passkey, type User } from '../schema/accounts'
import { allowedOrigins, appName, consoleOrigin, rpID } from '../config/accounts.config'
import { passkeyNameFor } from '../lib/passkey-names'

/** Origins a ceremony may legitimately have happened on. */
const expectedOrigin = Array.from(new Set([consoleOrigin, ...allowedOrigins]))

export const challengeCookieName = 'barrelman_webauthn'

// ── Registration ────────────────────────────────────────────────────────

export async function beginRegistration(userId: string, userName: string) {
  const existing = await db
    .select({ id: passkeys.id, transports: passkeys.transports })
    .from(passkeys)
    .where(eq(passkeys.userId, userId))

  return generateRegistrationOptions({
    rpID,
    rpName: appName,
    userName,
    attestationType: 'none',
    // Stops the user silently registering the same authenticator twice, which
    // would leave two rows that are indistinguishable in the console.
    excludeCredentials: existing.map((row) => ({
      id: row.id,
      transports: parseTransports(row.transports),
    })),
    authenticatorSelection: {
      residentKey: 'required',
      userVerification: 'required',
    },
  })
}

export interface CompleteRegistrationResult {
  ok: boolean
  passkey?: Omit<Passkey, 'publicKey'>
  error?: string
}

export async function completeRegistration(
  userId: string,
  response: RegistrationResponseJSON & { name?: string },
  expectedChallenge: string,
  userAgent?: string | null,
): Promise<CompleteRegistrationResult> {
  let verification
  try {
    verification = await verifyRegistrationResponse({
      response,
      expectedChallenge,
      expectedOrigin,
      expectedRPID: rpID,
      requireUserVerification: true,
    })
  } catch (err) {
    return { ok: false, error: (err as Error).message }
  }

  if (!verification.verified || !verification.registrationInfo) {
    return { ok: false, error: 'Passkey registration could not be verified' }
  }

  const { credential, credentialDeviceType, credentialBackedUp, aaguid } = verification.registrationInfo

  const name = response.name?.trim() || passkeyNameFor(aaguid, userAgent)

  const [row] = await db
    .insert(passkeys)
    .values({
      id: credential.id,
      userId,
      name,
      publicKey: Buffer.from(credential.publicKey).toString('base64'),
      counter: credential.counter,
      deviceType: credentialDeviceType,
      backedUp: credentialBackedUp,
      transports: (response.response.transports ?? []).join(','),
    })
    .returning()

  if (!row) return { ok: false, error: 'Passkey already registered' }

  const { publicKey: _publicKey, ...safe } = row
  return { ok: true, passkey: safe }
}

// ── Authentication ──────────────────────────────────────────────────────

export function beginAuthentication() {
  // No allowCredentials: the authenticator offers whichever discoverable
  // credential it holds for this RP, so sign-in needs no email first.
  return generateAuthenticationOptions({ rpID, userVerification: 'required' })
}

export interface CompleteAuthenticationResult {
  ok: boolean
  user?: User
  error?: string
}

export async function completeAuthentication(
  response: AuthenticationResponseJSON,
  expectedChallenge: string,
): Promise<CompleteAuthenticationResult> {
  const [passkey] = await db.select().from(passkeys).where(eq(passkeys.id, response.id)).limit(1)
  if (!passkey) return { ok: false, error: 'Passkey not recognised' }

  let verification
  try {
    verification = await verifyAuthenticationResponse({
      response,
      expectedChallenge,
      expectedOrigin,
      expectedRPID: rpID,
      requireUserVerification: true,
      credential: {
        id: passkey.id,
        publicKey: new Uint8Array(Buffer.from(passkey.publicKey, 'base64')),
        counter: passkey.counter,
        transports: parseTransports(passkey.transports),
      },
    })
  } catch (err) {
    return { ok: false, error: (err as Error).message }
  }

  if (!verification.verified) return { ok: false, error: 'Passkey could not be verified' }

  // The signature counter must never go backwards. When an authenticator
  // reports one at all (many platform authenticators always report 0), a
  // repeat or lower value means the credential has been cloned.
  const { newCounter } = verification.authenticationInfo
  if (passkey.counter > 0 && newCounter <= passkey.counter) {
    return { ok: false, error: 'Passkey signature counter did not advance — possible cloned credential' }
  }

  await db
    .update(passkeys)
    .set({ counter: newCounter, lastUsedAt: new Date() })
    .where(eq(passkeys.id, passkey.id))

  const [user] = await db.select().from(users).where(eq(users.id, passkey.userId)).limit(1)
  if (!user) return { ok: false, error: 'Account no longer exists' }
  if (user.suspendedAt) return { ok: false, error: 'This account has been suspended' }

  return { ok: true, user }
}

// ── Confirmation ────────────────────────────────────────────────────────

/**
 * Options for re-proving presence on an account that is already signed in,
 * before something irreversible. Unlike sign-in, the credentials are listed:
 * only this account's passkeys can answer.
 */
export async function beginConfirmation(userId: string) {
  const own = await db
    .select({ id: passkeys.id, transports: passkeys.transports })
    .from(passkeys)
    .where(eq(passkeys.userId, userId))

  return generateAuthenticationOptions({
    rpID,
    userVerification: 'required',
    allowCredentials: own.map((row) => ({ id: row.id, transports: parseTransports(row.transports) as never })),
  })
}

/**
 * Verify a confirmation assertion. The signature check is sign-in's; on top of
 * it the credential must belong to `userId`, or a passkey for some other
 * account the same person holds would confirm an action on this one.
 */
export async function confirmPasskey(
  userId: string,
  response: AuthenticationResponseJSON,
  expectedChallenge: string,
): Promise<{ ok: boolean; error?: string }> {
  const result = await completeAuthentication(response, expectedChallenge)
  if (!result.ok || !result.user) return { ok: false, error: result.error ?? 'Passkey could not be verified' }
  if (result.user.id !== userId) return { ok: false, error: 'That passkey belongs to a different account' }
  return { ok: true }
}

// ── Management ──────────────────────────────────────────────────────────

export interface PasskeySummary {
  id: string
  name: string
  deviceType: string
  backedUp: boolean
  lastUsedAt: Date | null
  createdAt: Date
}

export async function listPasskeys(userId: string): Promise<PasskeySummary[]> {
  return db
    .select({
      id: passkeys.id,
      name: passkeys.name,
      deviceType: passkeys.deviceType,
      backedUp: passkeys.backedUp,
      lastUsedAt: passkeys.lastUsedAt,
      createdAt: passkeys.createdAt,
    })
    .from(passkeys)
    .where(eq(passkeys.userId, userId))
}

export async function renamePasskey(userId: string, id: string, name: string): Promise<boolean> {
  const [row] = await db
    .update(passkeys)
    .set({ name })
    .where(and(eq(passkeys.id, id), eq(passkeys.userId, userId)))
    .returning({ id: passkeys.id })
  return Boolean(row)
}

export async function deletePasskey(userId: string, id: string): Promise<boolean> {
  const [row] = await db
    .delete(passkeys)
    .where(and(eq(passkeys.id, id), eq(passkeys.userId, userId)))
    .returning({ id: passkeys.id })
  return Boolean(row)
}

export async function countPasskeys(userId: string): Promise<number> {
  return (await db.select({ id: passkeys.id }).from(passkeys).where(eq(passkeys.userId, userId))).length
}

function parseTransports(value: string): string[] {
  return value ? value.split(',').filter(Boolean) : []
}
