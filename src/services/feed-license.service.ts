/**
 * Per-feed licence terms at request time: which feeds the licence policy keeps
 * out of responses, and the credit line each contributing feed is owed.
 *
 * gtfs_feeds is small (hundreds to low thousands of rows), so the whole table's
 * terms are read once and held for a few minutes rather than joined into every
 * transit query.
 */
import { sql } from 'drizzle-orm'
import { db } from '../db'
import {
  isExcludedByPolicy,
  licensePolicy,
  licenseAnswer,
  type FeedAttribution,
  type LicenseAnswer,
} from '../lib/feed-license'

interface FeedTerms {
  name: string | null
  attributionText: string | null
  url: string | null
  commercialUseAllowed: LicenseAnswer | null
  redistributionAllowed: LicenseAnswer | null
}

const TTL_MS = 5 * 60_000

let cache: { at: number; terms: Promise<Map<string, FeedTerms>> } | null = null

async function readTerms(): Promise<Map<string, FeedTerms>> {
  const out = new Map<string, FeedTerms>()
  try {
    const rows = (await db.execute(sql`
      SELECT feed_id, name, license_attribution_text, license_url,
             license_commercial_use_allowed, license_redistribution_allowed
      FROM gtfs_feeds
    `)) as any[]
    for (const r of rows) {
      if (typeof r?.feed_id !== 'string') continue
      out.set(r.feed_id, {
        name: r.name ?? null,
        attributionText: r.license_attribution_text ?? null,
        url: r.license_url ?? null,
        commercialUseAllowed: licenseAnswer(r.license_commercial_use_allowed),
        redistributionAllowed: licenseAnswer(r.license_redistribution_allowed),
      })
    }
  } catch (err) {
    // A response without credits is better than no response. The licence
    // columns are added at startup, so this is a database outage, not drift.
    console.error('[feed-license] Failed to read feed terms:', err instanceof Error ? err.message : err)
  }
  return out
}

const FAILED_READ_RETRY_MS = 10_000

function feedTerms(): Promise<Map<string, FeedTerms>> {
  if (!cache || Date.now() - cache.at > TTL_MS) {
    const entry = { at: Date.now(), terms: readTerms() }
    cache = entry
    // A failed read returns an empty map. Keep it only briefly, or under the
    // commercial policy excluded feeds would be served for the whole TTL.
    entry.terms.then(terms => {
      if (terms.size === 0 && cache === entry) entry.at = Date.now() - TTL_MS + FAILED_READ_RETRY_MS
    })
  }
  return cache.terms
}

/** Drop the cached terms, after a licence refresh or in tests. */
export function invalidateFeedTerms(): void {
  cache = null
}

/** Seed the cache directly. Tests only. */
export function __setFeedTermsForTests(rows: Record<string, Partial<FeedTerms>>): void {
  const terms = new Map<string, FeedTerms>()
  for (const [feedId, t] of Object.entries(rows)) {
    terms.set(feedId, {
      name: null,
      attributionText: null,
      url: null,
      commercialUseAllowed: null,
      redistributionAllowed: null,
      ...t,
    })
  }
  cache = { at: Date.now(), terms: Promise.resolve(terms) }
}

/**
 * Feed ids the licence policy keeps out of responses. Empty, without touching
 * the database, when the policy is `off`.
 */
export async function excludedFeedIds(): Promise<Set<string>> {
  const policy = licensePolicy()
  if (policy === 'off') return new Set()
  const excluded = new Set<string>()
  for (const [feedId, terms] of await feedTerms()) {
    if (isExcludedByPolicy(terms, policy)) excluded.add(feedId)
  }
  return excluded
}

/** Keep the items whose feed the policy allows. */
export async function servableOnly<T>(items: T[], feedIdOf: (item: T) => string | undefined): Promise<T[]> {
  const excluded = await excludedFeedIds()
  if (!excluded.size) return items
  return items.filter((item) => {
    const feedId = feedIdOf(item)
    return !feedId || !excluded.has(feedId)
  })
}

/**
 * The credit lines for the feeds that contributed to a response, one per
 * feed, in first-seen order.
 *
 * `agencies` supplies a name for a feed the table does not know, usually the
 * GTFS agency_name the response already carries.
 */
export async function attributionFor(
  feedIds: Iterable<string | undefined | null>,
  agencies: Map<string, string> = new Map(),
): Promise<FeedAttribution[]> {
  const unique = [...new Set([...feedIds].filter((f): f is string => !!f))]
  if (!unique.length) return []
  const terms = await feedTerms()
  return unique.map((feedId) => {
    const t = terms.get(feedId)
    return {
      feedId,
      agency: t?.name ?? agencies.get(feedId) ?? null,
      text: t?.attributionText ?? null,
      url: t?.url ?? null,
    }
  })
}
