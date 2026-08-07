/**
 * API key lifecycle and verification.
 *
 * Keys look like `brm_live_<40 base62 chars>`. Only a SHA-256 digest is stored,
 * so the plaintext exists exactly once — in the response that created it. There
 * is no "show key again": a lost key is rolled, not recovered.
 *
 * Verification sits in the hot path of every metered request, so a successful
 * lookup is cached in memory. The cache is keyed by the digest and holds the
 * account's plan and scopes, which means a revoked key stays usable for at most
 * `CACHE_TTL_MS` — revocation therefore also punches the entry out directly.
 */
import { and, desc, eq, inArray, isNull } from 'drizzle-orm'
import { LRUCache } from 'lru-cache'
import { db } from '../db'
import { apiKeys, users, type ApiKey } from '../schema/accounts'
import { generateId, randomBase62, sha256Hex } from '../lib/crypto'
import { normalizeOrigins } from '../lib/origins'
import { isValidScope, type Scope } from '../billing/plans'

const KEY_PREFIX = 'brm'
/** 40 base62 characters ≈ 238 bits — far beyond guessing range. */
const SECRET_LENGTH = 40
/** How much of the key is shown in the console to identify it. */
const DISPLAY_PREFIX_LENGTH = 16

const CACHE_TTL_MS = 60_000
const CACHE_MAX = 5_000

export interface ResolvedKey {
  keyId: string
  userId: string
  scopes: string[]
  /** Origins the key may be used from; empty means unrestricted. */
  allowedOrigins: string[]
  plan: string
  /**
   * Set when the owning account is suspended. The key still RESOLVES — the
   * caller holds a valid credential and is owed a real explanation, not the
   * same "invalid key" a stranger gets. The guard turns this into a 403.
   */
  suspended: boolean
  suspensionReason: string | null
}

const cache = new LRUCache<string, ResolvedKey>({ max: CACHE_MAX, ttl: CACHE_TTL_MS })

/** Keys whose lookup missed, cached briefly so a bad key can't hammer the DB. */
const negativeCache = new LRUCache<string, true>({ max: CACHE_MAX, ttl: 30_000 })

export function clearKeyCache(): void {
  cache.clear()
  negativeCache.clear()
}

// ── Creation ────────────────────────────────────────────────────────────

export interface CreateKeyOptions {
  userId: string
  name: string
  scopes?: string[]
  allowedOrigins?: string[]
  expiresAt?: Date | null
}

export interface CreatedKey {
  /** Full plaintext key. Shown once; never retrievable again. */
  key: string
  record: Omit<ApiKey, 'hash'>
}

export async function createApiKey(options: CreateKeyOptions): Promise<CreatedKey> {
  const { userId, name, expiresAt = null } = options

  const scopes = normalizeScopes(options.scopes)
  const allowedOrigins = normalizeOrigins(options.allowedOrigins)
  const secret = randomBase62(SECRET_LENGTH)
  const key = `${KEY_PREFIX}_live_${secret}`

  const [row] = await db
    .insert(apiKeys)
    .values({
      id: generateId(),
      userId,
      name: name.trim() || 'Untitled key',
      hash: sha256Hex(key),
      prefix: key.slice(0, DISPLAY_PREFIX_LENGTH),
      last4: key.slice(-4),
      scopes,
      allowedOrigins,
      expiresAt,
    })
    .returning()

  if (!row) throw new Error('Failed to create API key')

  const { hash: _hash, ...record } = row
  return { key, record }
}

/**
 * Exported for testing: this decides what a leaked key can reach, and it was
 * previously only exercisable through a database round-trip.
 */
export function normalizeScopes(scopes: string[] | undefined): string[] {
  if (!scopes || scopes.length === 0) return ['*']
  const valid = scopes.filter(isValidScope)
  if (valid.length === 0) return ['*']
  // A key holding `*` alongside narrower scopes is just `*`; storing both
  // invites a reader into thinking the narrow ones constrain anything.
  return valid.includes('*') ? ['*'] : Array.from(new Set(valid))
}

// ── Verification ────────────────────────────────────────────────────────

/**
 * Resolve a presented key to its account. Returns null for anything unknown,
 * revoked or expired. A key belonging to a *suspended* account still resolves,
 * carrying `suspended: true` — the caller holds a real credential and gets a
 * 403 explaining why, rather than the "invalid key" a stranger would see.
 */
export async function resolveApiKey(presented: string): Promise<ResolvedKey | null> {
  const hash = sha256Hex(presented)

  const cached = cache.get(hash)
  if (cached) return cached
  if (negativeCache.has(hash)) return null

  const [row] = await db
    .select({
      keyId: apiKeys.id,
      userId: apiKeys.userId,
      scopes: apiKeys.scopes,
      allowedOrigins: apiKeys.allowedOrigins,
      revokedAt: apiKeys.revokedAt,
      expiresAt: apiKeys.expiresAt,
      plan: users.plan,
      suspendedAt: users.suspendedAt,
      suspendedReason: users.suspendedReason,
    })
    .from(apiKeys)
    .innerJoin(users, eq(apiKeys.userId, users.id))
    .where(eq(apiKeys.hash, hash))
    .limit(1)

  // A key that does not exist, was revoked, or has expired is indistinguishable
  // from a guess, so it gets the negative cache and a flat null.
  if (!row || row.revokedAt || (row.expiresAt && row.expiresAt.getTime() <= Date.now())) {
    negativeCache.set(hash, true)
    return null
  }

  const resolved: ResolvedKey = {
    keyId: row.keyId,
    userId: row.userId,
    scopes: row.scopes,
    allowedOrigins: row.allowedOrigins,
    plan: row.plan,
    suspended: Boolean(row.suspendedAt),
    suspensionReason: row.suspendedReason,
  }

  cache.set(hash, resolved)
  return resolved
}

/**
 * Drop every cached entry for an account. Called when a plan changes or an
 * account is suspended, so the next request sees the new state rather than
 * waiting out the TTL.
 */
export function invalidateUserKeys(userId: string): void {
  for (const [hash, entry] of cache.entries()) {
    if (entry.userId === userId) cache.delete(hash)
  }
}

// ── Management ──────────────────────────────────────────────────────────

export type ApiKeySummary = Omit<ApiKey, 'hash'>

export async function listApiKeys(userId: string, includeRevoked = false): Promise<ApiKeySummary[]> {
  const where = includeRevoked
    ? eq(apiKeys.userId, userId)
    : and(eq(apiKeys.userId, userId), isNull(apiKeys.revokedAt))

  return db
    .select({
      id: apiKeys.id,
      userId: apiKeys.userId,
      name: apiKeys.name,
      prefix: apiKeys.prefix,
      last4: apiKeys.last4,
      scopes: apiKeys.scopes,
      allowedOrigins: apiKeys.allowedOrigins,
      lastUsedAt: apiKeys.lastUsedAt,
      revokedAt: apiKeys.revokedAt,
      expiresAt: apiKeys.expiresAt,
      createdAt: apiKeys.createdAt,
    })
    .from(apiKeys)
    .where(where)
    .orderBy(desc(apiKeys.createdAt))
}

export async function renameApiKey(userId: string, keyId: string, name: string): Promise<boolean> {
  const [row] = await db
    .update(apiKeys)
    .set({ name: name.trim() })
    .where(and(eq(apiKeys.id, keyId), eq(apiKeys.userId, userId)))
    .returning({ id: apiKeys.id })
  return Boolean(row)
}

export async function updateApiKeyScopes(userId: string, keyId: string, scopes: string[]): Promise<boolean> {
  const [row] = await db
    .update(apiKeys)
    .set({ scopes: normalizeScopes(scopes) })
    .where(and(eq(apiKeys.id, keyId), eq(apiKeys.userId, userId)))
    .returning({ hash: apiKeys.hash })
  if (!row) return false
  // Scope changes must take effect now, not in a minute's time.
  cache.delete(row.hash)
  return true
}

/**
 * Replace a key's origin restrictions. An empty list lifts the restriction
 * entirely, which is the only way back to an unrestricted key short of rolling
 * a new one.
 */
export async function updateApiKeyOrigins(
  userId: string,
  keyId: string,
  origins: string[],
): Promise<boolean> {
  const [row] = await db
    .update(apiKeys)
    .set({ allowedOrigins: normalizeOrigins(origins) })
    .where(and(eq(apiKeys.id, keyId), eq(apiKeys.userId, userId)))
    .returning({ hash: apiKeys.hash })
  if (!row) return false
  // Same reason as scopes: locking a key down has to take effect now, not up to
  // a cache TTL later, or the console reports a restriction that is not live.
  cache.delete(row.hash)
  return true
}

/**
 * Revoke a key. The row is kept rather than deleted so its usage history stays
 * attributable and the console can still show what the key did.
 */
export async function revokeApiKey(userId: string, keyId: string): Promise<boolean> {
  const [row] = await db
    .update(apiKeys)
    .set({ revokedAt: new Date() })
    .where(and(eq(apiKeys.id, keyId), eq(apiKeys.userId, userId), isNull(apiKeys.revokedAt)))
    .returning({ hash: apiKeys.hash })

  if (!row) return false
  // Evict immediately — a revoked key that keeps working for another minute is
  // not revoked in any sense the user would accept.
  cache.delete(row.hash)
  negativeCache.set(row.hash, true)
  return true
}

/**
 * Record that a key was used. Written by the metering flush rather than on the
 * request path, since a timestamp update per request would be a write per read.
 */
export async function markKeysUsed(keyIds: string[], at: Date = new Date()): Promise<void> {
  if (keyIds.length === 0) return
  await db.update(apiKeys).set({ lastUsedAt: at }).where(inArray(apiKeys.id, keyIds))
}

export type { ApiKey }
