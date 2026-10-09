/**
 * How many geo_places rows a tsquery's words match, estimated from the
 * statistics ANALYZE already keeps for the `ts` column.
 *
 * Postgres's own estimate is useless for the case that matters. It multiplies
 * word frequencies as if words were independent, and the words of a place name
 * never are: "new & york:*" was estimated at 92K rows and matched 1.2M. So the
 * planner picked the GIN bitmap, and sorting the matches inside a 50 km box
 * read 1.8 GB of heap (8.5s).
 *
 * This estimate assumes the opposite, that the words are fully correlated, so
 * a query matches about as many rows as its rarest word. That is an upper
 * bound, which is the safe side for its one use: deciding that a word is too
 * common for the ts index to narrow a search down. Prefix words are counted
 * separately, since the index expands them in full whatever else the query
 * says.
 */

import { db } from '../db'
import { sql } from 'drizzle-orm'

interface LexemeStats {
  /** Lexeme → fraction of rows containing it (the ~1000 most common). */
  freq: Map<string, number>
  rows: number
}

const REFRESH_MS = 60 * 60 * 1000

let stats: LexemeStats | null = null
let loadedAt = 0
let loading: Promise<void> | null = null

async function load(): Promise<void> {
  const rows = await db.execute(sql`
    SELECT s.most_common_elems::text::text[] AS elems, s.most_common_elem_freqs AS freqs,
           c.reltuples::float8 AS rows
    FROM pg_stats s
    JOIN pg_class c ON c.relname = s.tablename
    WHERE s.tablename = 'geo_places' AND s.attname = 'ts'
  `).catch(() => [] as any[])
  const row = (rows as any[])[0]
  loadedAt = Date.now()
  if (!row?.elems || !row?.freqs || !(row.rows > 0)) return
  const freq = new Map<string, number>()
  // most_common_elem_freqs carries three trailing summary values (min, max,
  // null fraction) after the per-element frequencies.
  row.elems.forEach((e: string, i: number) => freq.set(e, Number(row.freqs[i])))
  stats = { freq, rows: Number(row.rows) }
}

/** Kick off a (re)load when stale; never blocks a search on it. */
function current(): LexemeStats | null {
  if (!loading && Date.now() - loadedAt > REFRESH_MS) {
    loading = load().finally(() => { loading = null })
  }
  return stats
}

/** Lexemes in ts are unaccented and lowercased (see build_ts). */
const fold = (s: string) => s.normalize('NFKD').replace(/\p{M}/gu, '').toLowerCase()

export interface MatchEstimate {
  /** Rows the query matches, at most: as many as its rarest word. */
  rows: number
  /** Rows its largest prefix word expands to. GIN collects every row a
   *  prefix matches before it can intersect anything, and that step does not
   *  stop for a statement timeout: "empire & state:*" (state:* is ~2M rows)
   *  ran 2.6s under a 500ms timeout and held its connection throughout.
   *  Exact words don't pay this; GIN skips through their posting lists, so
   *  "harris & teeter" took 7ms though "harris" is in 2.5M rows. */
  prefixRows: number
}

/**
 * Upper-bound estimates for a tsquery built by buildTsQueryText ("a & (b | c)
 * & d:*"), or null while statistics are unavailable.
 *
 * A prefix shorter than three characters counts as matching everything: it
 * expands to thousands of lexemes, most below the statistics' horizon, so
 * their sum would badly undercount ("harris & t:*" took 3s).
 */
export function estimateMatches(tsQueryText: string): MatchEstimate | null {
  const s = current()
  if (!s) return null
  let rarest = 1
  let largestPrefix = 0
  for (const group of tsQueryText.split(' & ')) {
    let groupFreq = 0
    let prefixFreq = 0
    for (const raw of group.replace(/[()]/g, '').split(' | ')) {
      const prefix = raw.endsWith(':*')
      const term = fold(prefix ? raw.slice(0, -2) : raw).trim()
      if (!term) continue
      let f = 0
      if (prefix && term.length < 3) f = 1
      else if (prefix) { for (const [lexeme, lf] of s.freq) if (lexeme.startsWith(term)) f += lf }
      // Not among the most common words, so rarer than all of them.
      else f = s.freq.get(term) ?? 0
      groupFreq += f
      if (prefix) prefixFreq += f
    }
    rarest = Math.min(rarest, Math.min(1, groupFreq))
    largestPrefix = Math.max(largestPrefix, Math.min(1, prefixFreq))
  }
  return { rows: Math.round(rarest * s.rows), prefixRows: Math.round(largestPrefix * s.rows) }
}

/** Test hook: install statistics directly. */
export function setLexemeStats(freq: Record<string, number>, rows: number): void {
  stats = { freq: new Map(Object.entries(freq)), rows }
  loadedAt = Date.now()
}
