import postgres from 'postgres'
import { dbUrl, onnotice } from '../db'
import { brandCache } from './cache'

/**
 * Resolve brand logos + descriptions from Wikidata into the brand_logos table.
 *
 * geo_brands carries each brand's brand:wikidata QID but not its logo — logos
 * live on Wikidata (property P154) and Commons, not in OSM tags. This job
 * batch-fetches the P154 logo filename + English description for every brand
 * QID that doesn't yet have a brand_logos row, and stores a stable Commons
 * Special:FilePath URL (which needs no second API call). It is:
 *   - self-healing (fetches QIDs with no row yet, retries failed fetches, and
 *     re-checks logo-less brands monthly; converges to a near no-op)
 *   - guarded by an advisory lock (one instance at a time)
 *   - polite to the Wikidata API (batched 50/req, small delay between batches)
 * Never throws — logos are a nice-to-have layered onto the working catalog.
 */

const LOGO_LOCK_KEY = 0x5ea2c5
const WIKI_UA = 'Parchment-Barrelman/1.0 (https://github.com/alexwohlbruck/parchment)'
const BATCH = 50
// Safety cap per run so a huge fresh catalog can't hammer Wikidata in one go;
// leftover QIDs are picked up on the next startup.
const MAX_PER_RUN = 4000
// Attempts per batch when Wikidata rate-limits (429) or errors (5xx).
const MAX_ATTEMPTS = 4
// A row with neither a logo nor a description is a fetch that failed: a real
// entity nearly always has an English description. Those are retried after a
// day. A brand with a description but no logo is re-checked monthly, since
// logos get added to Wikidata over time.
const RETRY_FAILED_AFTER = '1 day'
const RECHECK_NO_LOGO_AFTER = '30 days'

type Sql = ReturnType<typeof postgres>

interface LogoMeta {
  logoUrl: string | null
  description: string | null
}

/**
 * Logo + description for each QID Wikidata answered for. A QID missing from the
 * map was not answered (the request failed), so the caller must not record it:
 * on 2026-09-14 a run stored every QID of each failed batch as logo-less, and
 * since only QIDs without a row were ever fetched again, 1,006 brands —
 * Starbucks among them — lost their logo for good.
 */
export async function fetchEntityBatch(qids: string[]): Promise<Map<string, LogoMeta>> {
  const out = new Map<string, LogoMeta>()
  const url =
    `https://www.wikidata.org/w/api.php?action=wbgetentities&ids=${qids.join('|')}` +
    `&props=claims|descriptions&languages=en&format=json`
  let res: Response | null = null
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    res = await fetch(url, { headers: { 'User-Agent': WIKI_UA } })
    if (res.ok || (res.status !== 429 && res.status < 500)) break
    // Rate-limited or a server error: wait as asked, else back off.
    const retryAfter = Number(res.headers.get('retry-after'))
    const waitMs = retryAfter > 0 ? retryAfter * 1000 : 1000 * 2 ** attempt
    await new Promise((r) => setTimeout(r, Math.min(waitMs, 60_000)))
  }
  if (!res?.ok) return out
  const data = (await res.json()) as any
  if (data.error) return out
  const entities = data.entities || {}
  for (const qid of qids) {
    const e = entities[qid]
    // Answered, but no such entity (deleted or merged away): record it as
    // logo-less so it isn't asked for again until the next re-check.
    if (!e || e.missing !== undefined) {
      out.set(qid, { logoUrl: null, description: null })
      continue
    }
    const description = e.descriptions?.en?.value || null
    const filename = e.claims?.P154?.[0]?.mainsnak?.datavalue?.value || null
    const logoUrl = filename
      ? `https://commons.wikimedia.org/wiki/Special:FilePath/${encodeURIComponent(filename)}?width=200`
      : null
    out.set(qid, { logoUrl, description })
  }
  return out
}

/**
 * The QID to ask Wikidata for, from a raw brand:wikidata tag value, or null if
 * it holds none. OSM lists several with ";" ("Q155026;Q7771029"), and Wikidata
 * rejects a whole request with one malformed ID in it — that error, not rate
 * limiting, is what failed batches of 50 at a time. The first ID names the
 * brand itself; the rest are usually its operator or parent.
 */
export function queryableQid(tagValue: string): string | null {
  const first = tagValue.split(';')[0].trim()
  return /^Q[1-9]\d*$/.test(first) ? first : null
}

export async function ensureBrandLogos(): Promise<void> {
  const sql = postgres(dbUrl, { max: 1, onnotice })
  try {
    // Brands with a wikidata id and no logo yet: never fetched, a fetch that
    // failed, or a logo-less brand due for a re-check (see RETRY_FAILED_AFTER).
    // Never-fetched first, then the most popular brands.
    const missing = await sql<{ wikidata: string }[]>`
      SELECT b.wikidata
      FROM geo_brands b
      LEFT JOIN brand_logos l ON l.wikidata = b.wikidata
      WHERE b.wikidata IS NOT NULL
        AND (l.wikidata IS NULL
          OR (l.logo_url IS NULL AND l.description IS NULL
              AND l.fetched_at < NOW() - ${RETRY_FAILED_AFTER}::interval)
          OR (l.logo_url IS NULL AND l.fetched_at < NOW() - ${RECHECK_NO_LOGO_AFTER}::interval))
      GROUP BY b.wikidata, l.wikidata
      ORDER BY (l.wikidata IS NULL) DESC, sum(b.location_count) DESC
      LIMIT ${MAX_PER_RUN}
    `.catch(() => [] as { wikidata: string }[])
    if (missing.length === 0) return

    const [{ locked }] = await sql<{ locked: boolean }[]>`
      SELECT pg_try_advisory_lock(${LOGO_LOCK_KEY}) AS locked
    `
    if (!locked) return

    try {
      console.log(`[brand-logos] Resolving ${missing.length} brand logo(s) from Wikidata…`)
      let done = 0
      for (let i = 0; i < missing.length; i += BATCH) {
        // Rows are keyed by the raw tag value (geo_brands joins on it), and
        // Wikidata is asked by the QID inside it.
        const tagValues = missing.slice(i, i + BATCH).map((r) => r.wikidata)
        const asked = [...new Set(tagValues.map(queryableQid).filter((q): q is string => q != null))]
        let answers: Map<string, LogoMeta>
        try {
          answers = asked.length ? await fetchEntityBatch(asked) : new Map()
        } catch {
          answers = new Map()
        }
        const results = new Map<string, LogoMeta>()
        for (const tagValue of tagValues) {
          const qid = queryableQid(tagValue)
          // Not an ID at all: nothing to ask, so record it as logo-less.
          if (!qid) results.set(tagValue, { logoUrl: null, description: null })
          else if (answers.has(qid)) results.set(tagValue, answers.get(qid)!)
        }
        const qids = tagValues
        // Upsert every QID Wikidata answered for — even those with no logo —
        // so the attempt is recorded. One it didn't answer is left as it was,
        // to be fetched again next run.
        for (const qid of qids) {
          const r = results.get(qid)
          if (!r) continue
          await sql`
            INSERT INTO brand_logos (wikidata, logo_url, description, fetched_at)
            VALUES (${qid}, ${r.logoUrl}, ${r.description}, NOW())
            ON CONFLICT (wikidata) DO UPDATE
              SET logo_url = EXCLUDED.logo_url,
                  description = EXCLUDED.description,
                  fetched_at = NOW()
          `
        }
        done += qids.length
        await new Promise((res) => setTimeout(res, 150)) // be polite
      }
      // Newly-resolved logos invalidate any cached brand lookups.
      brandCache.clear()
      console.log(`[brand-logos] Done (${done}).`)
    } finally {
      await sql`SELECT pg_advisory_unlock(${LOGO_LOCK_KEY})`
    }
  } catch (err) {
    console.error('[brand-logos] Failed:', err)
  } finally {
    await sql.end({ timeout: 5 })
  }
}
