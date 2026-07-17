import postgres from 'postgres'
import { dbUrl } from '../db'
import { brandCache } from './cache'

/**
 * Resolve brand logos + descriptions from Wikidata into the brand_logos table.
 *
 * geo_brands carries each brand's brand:wikidata QID but not its logo — logos
 * live on Wikidata (property P154) and Commons, not in OSM tags. This job
 * batch-fetches the P154 logo filename + English description for every brand
 * QID that doesn't yet have a brand_logos row, and stores a stable Commons
 * Special:FilePath URL (which needs no second API call). It is:
 *   - self-healing (only fetches QIDs with no row yet; converges to a no-op)
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

type Sql = ReturnType<typeof postgres>

interface LogoMeta {
  logoUrl: string | null
  description: string | null
}

async function fetchEntityBatch(qids: string[]): Promise<Map<string, LogoMeta>> {
  const out = new Map<string, LogoMeta>()
  const url =
    `https://www.wikidata.org/w/api.php?action=wbgetentities&ids=${qids.join('|')}` +
    `&props=claims|descriptions&languages=en&format=json`
  const res = await fetch(url, { headers: { 'User-Agent': WIKI_UA } })
  if (!res.ok) return out
  const data = (await res.json()) as any
  const entities = data.entities || {}
  for (const qid of qids) {
    const e = entities[qid]
    if (!e) {
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

export async function ensureBrandLogos(): Promise<void> {
  const sql = postgres(dbUrl, { max: 1 })
  try {
    // Brands with a wikidata id but no brand_logos row yet.
    const missing = await sql<{ wikidata: string }[]>`
      SELECT DISTINCT b.wikidata
      FROM geo_brands b
      LEFT JOIN brand_logos l ON l.wikidata = b.wikidata
      WHERE b.wikidata IS NOT NULL AND l.wikidata IS NULL
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
        const qids = missing.slice(i, i + BATCH).map((r) => r.wikidata)
        let results: Map<string, LogoMeta>
        try {
          results = await fetchEntityBatch(qids)
        } catch {
          results = new Map()
        }
        // Upsert every requested QID — even those with no logo — so we record
        // the attempt and don't re-fetch it on every startup.
        for (const qid of qids) {
          const r = results.get(qid) ?? { logoUrl: null, description: null }
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
