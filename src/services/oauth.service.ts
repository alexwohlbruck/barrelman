/**
 * Sign-in with Google, GitHub and GitLab.
 *
 * Identities are matched on the provider's stable subject id, never on the
 * email — providers let users change their address, and matching on a mutable
 * field would hand an account to whoever inherits an old address.
 *
 * An identity IS linked to an existing barrelman account when the provider
 * asserts the same address AND says it has verified it. That check is the whole
 * safety argument: without it, anyone who can get a provider to issue a token
 * carrying an unverified address of their choosing could claim someone else's
 * account. GitHub in particular will happily report an unverified address, so
 * we ask for the verified primary specifically.
 */
import * as arctic from 'arctic'
import { and, eq } from 'drizzle-orm'
import { db } from '../db'
import { oauthAccounts, users, type OAuthProvider, type User } from '../schema/accounts'
import { getOAuthProvider, type OAuthProviderConfig } from '../config/oauth.config'
import { createUser, findUserByEmail, SignupError } from './accounts.service'

export interface OAuthProfile {
  providerAccountId: string
  email: string | null
  /** Whether the provider states it has verified the address above. */
  emailVerified: boolean
  name: string | null
  picture: string | null
}

export interface AuthorizationRequest {
  url: string
  state: string
  /** PKCE verifier — only Google's flow uses one; empty for the others. */
  codeVerifier: string
}

function client(config: OAuthProviderConfig) {
  switch (config.id) {
    case 'google':
      return new arctic.Google(config.clientId, config.clientSecret, config.redirectUri)
    case 'github':
      return new arctic.GitHub(config.clientId, config.clientSecret, config.redirectUri)
    case 'gitlab':
      return new arctic.GitLab(
        config.baseUrl ?? 'https://gitlab.com',
        config.clientId,
        config.clientSecret,
        config.redirectUri,
      )
  }
}

export function createAuthorization(providerId: string): AuthorizationRequest | null {
  const config = getOAuthProvider(providerId)
  if (!config) return null

  const state = arctic.generateState()
  const codeVerifier = arctic.generateCodeVerifier()
  const provider = client(config)

  const url =
    provider instanceof arctic.Google
      ? provider.createAuthorizationURL(state, codeVerifier, config.scopes)
      : provider.createAuthorizationURL(state, config.scopes)

  return { url: url.toString(), state, codeVerifier }
}

/** Exchange the callback code for the caller's profile at the provider. */
export async function fetchProfile(
  providerId: string,
  code: string,
  codeVerifier: string,
): Promise<OAuthProfile | null> {
  const config = getOAuthProvider(providerId)
  if (!config) return null

  const provider = client(config)
  const tokens =
    provider instanceof arctic.Google
      ? await provider.validateAuthorizationCode(code, codeVerifier)
      : await provider.validateAuthorizationCode(code)

  const accessToken = tokens.accessToken()

  switch (config.id) {
    case 'google':
      return fetchGoogleProfile(accessToken)
    case 'github':
      return fetchGitHubProfile(accessToken)
    case 'gitlab':
      return fetchGitLabProfile(accessToken, config.baseUrl ?? 'https://gitlab.com')
  }
}

async function fetchGoogleProfile(accessToken: string): Promise<OAuthProfile> {
  const res = await fetch('https://openidconnect.googleapis.com/v1/userinfo', {
    headers: { authorization: `Bearer ${accessToken}` },
  })
  if (!res.ok) throw new Error(`Google userinfo failed: ${res.status}`)
  const profile = (await res.json()) as {
    sub: string
    email?: string
    email_verified?: boolean
    name?: string
    picture?: string
  }

  return {
    providerAccountId: profile.sub,
    email: profile.email ?? null,
    emailVerified: profile.email_verified === true,
    name: profile.name ?? null,
    picture: profile.picture ?? null,
  }
}

async function fetchGitHubProfile(accessToken: string): Promise<OAuthProfile> {
  const headers = {
    authorization: `Bearer ${accessToken}`,
    accept: 'application/vnd.github+json',
    'user-agent': 'barrelman',
  }

  const res = await fetch('https://api.github.com/user', { headers })
  if (!res.ok) throw new Error(`GitHub user failed: ${res.status}`)
  const profile = (await res.json()) as {
    id: number
    login: string
    name?: string
    email?: string
    avatar_url?: string
  }

  // The profile's `email` is whatever the user chose to make public and carries
  // no verification guarantee — so ask for the address list and take the entry
  // GitHub marks both primary and verified.
  let email: string | null = null
  let emailVerified = false
  const emailsRes = await fetch('https://api.github.com/user/emails', { headers })
  if (emailsRes.ok) {
    const emails = (await emailsRes.json()) as Array<{ email: string; primary: boolean; verified: boolean }>
    const primary = emails.find((e) => e.primary && e.verified) ?? emails.find((e) => e.verified)
    if (primary) {
      email = primary.email
      emailVerified = true
    }
  }

  return {
    providerAccountId: String(profile.id),
    email,
    emailVerified,
    name: profile.name ?? profile.login,
    picture: profile.avatar_url ?? null,
  }
}

async function fetchGitLabProfile(accessToken: string, baseUrl: string): Promise<OAuthProfile> {
  const res = await fetch(`${baseUrl}/api/v4/user`, {
    headers: { authorization: `Bearer ${accessToken}` },
  })
  if (!res.ok) throw new Error(`GitLab user failed: ${res.status}`)
  const profile = (await res.json()) as {
    id: number
    username: string
    name?: string
    email?: string
    confirmed_at?: string | null
    avatar_url?: string
  }

  return {
    providerAccountId: String(profile.id),
    email: profile.email ?? null,
    // GitLab only exposes an address on a confirmed account, and `confirmed_at`
    // is the explicit signal.
    emailVerified: Boolean(profile.email && profile.confirmed_at),
    name: profile.name ?? profile.username,
    picture: profile.avatar_url ?? null,
  }
}

// ── Account resolution ──────────────────────────────────────────────────

export type OAuthSignInResult =
  | { ok: true; user: User; created: boolean }
  | { ok: false; error: string; reason?: string }

/**
 * Resolve a provider profile to an account, in three steps:
 *
 *   1. Known identity → sign in.
 *   2. Verified address matching an existing account → link and sign in.
 *   3. Otherwise → create an account, subject to the usual sign-up gates.
 *
 * `signedInUserId` is set when an already-authenticated user is adding a
 * provider from their settings, which links unconditionally.
 */
export async function resolveOAuthSignIn(
  provider: OAuthProvider,
  profile: OAuthProfile,
  options: { ipHash?: string | null; signedInUserId?: string } = {},
): Promise<OAuthSignInResult> {
  const [existingLink] = await db
    .select()
    .from(oauthAccounts)
    .where(
      and(eq(oauthAccounts.provider, provider), eq(oauthAccounts.providerAccountId, profile.providerAccountId)),
    )
    .limit(1)

  if (options.signedInUserId) {
    if (existingLink && existingLink.userId !== options.signedInUserId) {
      return { ok: false, error: `That ${provider} account is already linked to another barrelman account` }
    }
    if (!existingLink) {
      await db
        .insert(oauthAccounts)
        .values({ provider, providerAccountId: profile.providerAccountId, userId: options.signedInUserId })
        .onConflictDoNothing()
    }
    const [user] = await db.select().from(users).where(eq(users.id, options.signedInUserId)).limit(1)
    return user ? { ok: true, user, created: false } : { ok: false, error: 'Account no longer exists' }
  }

  if (existingLink) {
    const [user] = await db.select().from(users).where(eq(users.id, existingLink.userId)).limit(1)
    if (!user) return { ok: false, error: 'Account no longer exists' }
    if (user.suspendedAt) return { ok: false, error: 'This account has been suspended' }
    return { ok: true, user, created: false }
  }

  if (!profile.email) {
    return {
      ok: false,
      error: `Your ${provider} account has no email address available — sign in with an email code instead`,
    }
  }
  if (!profile.emailVerified) {
    // Refusing here is the difference between "linked because the provider
    // vouched for this address" and "linked because someone typed it in".
    return {
      ok: false,
      error: `Your ${provider} email address is not verified — verify it there, or sign in with an email code`,
    }
  }

  const existingUser = await findUserByEmail(profile.email)
  if (existingUser) {
    if (existingUser.suspendedAt) return { ok: false, error: 'This account has been suspended' }
    await db
      .insert(oauthAccounts)
      .values({ provider, providerAccountId: profile.providerAccountId, userId: existingUser.id })
      .onConflictDoNothing()
    return { ok: true, user: existingUser, created: false }
  }

  try {
    const user = await createUser({
      email: profile.email,
      name: profile.name,
      picture: profile.picture,
      ipHash: options.ipHash ?? null,
    })
    await db
      .insert(oauthAccounts)
      .values({ provider, providerAccountId: profile.providerAccountId, userId: user.id })
      .onConflictDoNothing()
    return { ok: true, user, created: true }
  } catch (err) {
    if (err instanceof SignupError) return { ok: false, error: err.message, reason: err.reason }
    throw err
  }
}

// ── Management ──────────────────────────────────────────────────────────

export async function listLinkedProviders(userId: string): Promise<Array<{ provider: OAuthProvider; createdAt: Date }>> {
  return db
    .select({ provider: oauthAccounts.provider, createdAt: oauthAccounts.createdAt })
    .from(oauthAccounts)
    .where(eq(oauthAccounts.userId, userId))
}

export async function unlinkProvider(userId: string, provider: OAuthProvider): Promise<boolean> {
  const [row] = await db
    .delete(oauthAccounts)
    .where(and(eq(oauthAccounts.userId, userId), eq(oauthAccounts.provider, provider)))
    .returning({ provider: oauthAccounts.provider })
  return Boolean(row)
}
