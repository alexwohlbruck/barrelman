/**
 * Typo correction for search words, drawn from the words the index holds.
 *
 * The FTS layer matches whole words, so one misspelt word fails the query
 * however close it is: "men's warehouse flatbush" found nothing, because the
 * store is "Men's Wearhouse". The trigram layer, the old typo tolerance,
 * compares the query with names only — "flatbush" is in the address, so the
 * name scored under its cutoff — and typeahead skips it altogether, since a
 * KNN walk over a 5 GB index cannot fit a keystroke.
 *
 * search_lexicon holds every distinct word of the `ts` column (built by
 * import/build-search-lexicon.sql), about 1.4M on the US. Each word is filed
 * under itself and every one-letter deletion of it ("park" under park, ark,
 * prk, pak, par), and a query word is looked up by the same keys. Two words
 * share a key exactly when one edit separates them — a letter inserted,
 * dropped, changed or swapped with its neighbour ("prak" and "park" both drop
 * to "pak") — or a letter moved ("warehouse" and "wearhouse" both drop to
 * "warhouse"). That is an equality lookup on a GIN index: 0.5ms on 1.5M words.
 *
 * A trigram nearest-neighbour search was the first attempt, and failed on
 * both counts at that size: 300-780ms a word, and typos share too few
 * trigrams to rank near the top ("prak" and "park" share one of five, so
 * "park" never made the 60 nearest).
 *
 * The corrections become extra spellings of the word in the tsquery, so they
 * cost one more FTS pass, bounded by the same index, budgets and estimates as
 * the first.
 *
 * Corrections are only ever looked up after the exact words came up short
 * (see searchPlaces), so a well-spelled query never pays for them.
 */

import { sql } from 'drizzle-orm'
import { LRUCache } from 'lru-cache'
import { executeWithin } from '../lib/bounded-query'
import { cleanWords } from '../lib/search-query'

/** Statement timeout per word. A lookup measures under a millisecond. */
const LOOKUP_TIMEOUT_MS = 150

/** Spellings added per word. Each is one more posting list in the index. */
const MAX_CORRECTIONS = 3

/**
 * Edits a word of this length may be away from a correction. A four-letter
 * word has many neighbours ("prak": park, pram, peak), but every other word of
 * the query still has to match, which picks between them; under four there is
 * too little word left to correct.
 */
export function maxEdits(length: number): number {
  return length >= 8 ? 2 : length >= 4 ? 1 : 0
}

/**
 * The keys a word is filed and looked up under: itself and each one-letter
 * deletion. Keep in step with build-search-lexicon.sql, which makes the same
 * keys in SQL. By code point, as SQL's substr() counts.
 */
export function deletionKeys(word: string): string[] {
  const chars = Array.from(word)
  const keys = new Set([word])
  for (let i = 0; i < chars.length; i++) keys.add([...chars.slice(0, i), ...chars.slice(i + 1)].join(''))
  return [...keys]
}

/**
 * Optimal string alignment distance: insertions, deletions, substitutions and
 * swaps of adjacent letters each cost one, so "recieve" is one edit from
 * "receive". Gives up early once every alignment exceeds `limit`, returning
 * limit + 1.
 */
export function editDistance(a: string, b: string, limit: number): number {
  if (Math.abs(a.length - b.length) > limit) return limit + 1
  let prev2: number[] = []
  let prev = Array.from({ length: b.length + 1 }, (_, j) => j)
  for (let i = 1; i <= a.length; i++) {
    const cur = [i]
    let rowMin = i
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1
      let d = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
      if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) d = Math.min(d, prev2[j - 2] + 1)
      cur.push(d)
      if (d < rowMin) rowMin = d
    }
    if (rowMin > limit) return limit + 1
    prev2 = prev
    prev = cur
  }
  return prev[b.length]
}

/**
 * The corrections worth trying for `word` among `candidates` (word, number of
 * places containing it): within maxEdits, the most used first.
 *
 * Not nearest first. The data has its own misspellings, and on a country they
 * sit one edit from every common word: "warehouse" has "warhouse" (8 places)
 * and "wharehouse" (8) at one edit, which crowded out "wearhouse" (505 — the
 * Men's Wearhouse chain) at two. How many places use a word is what says it is
 * the one meant.
 */
export function pickCorrections(word: string, candidates: Array<{ word: string; n: number }>): string[] {
  const limit = maxEdits(word.length)
  return candidates
    .filter((c) => c.word !== word)
    .map((c) => ({ ...c, d: editDistance(word, c.word, limit) }))
    .filter((c) => c.d <= limit)
    .sort((x, y) => y.n - x.n || x.d - y.d)
    .slice(0, MAX_CORRECTIONS)
    .map((c) => c.word)
}

/** The lexicon only changes with an import, so a word's corrections hold. */
const corrections = new LRUCache<string, string[]>({ max: 20_000, ttl: 60 * 60 * 1000 })

/** Lexicon words are unaccented and lowercased, like the tsvector. */
const fold = (s: string) => s.normalize('NFKD').replace(/\p{M}/gu, '')

async function lookup(word: string): Promise<string[] | null> {
  // One aggregated row whatever matched, so no rows at all means the lookup
  // failed or ran out of time, and is not cached as "no corrections".
  //
  // No LIMIT: the planner guesses `&&` matches 5% of the table, and with a
  // LIMIT that makes a sequential scan look cheaper than the index (2.3s
  // instead of 0.5ms). The keys match a handful of words, a hundred at most.
  const rows = await executeWithin(sql`
    SELECT coalesce(json_agg(json_build_object('word', word, 'n', n)), '[]'::json) AS candidates
    FROM search_lexicon
    WHERE keys && ARRAY[${sql.join(deletionKeys(word).map((k) => sql`${k}`), sql`, `)}]::text[]
  `, LOOKUP_TIMEOUT_MS)
  if (rows.length === 0) return null
  return pickCorrections(word, rows[0].candidates ?? [])
}

/**
 * Near-spellings of each correctable query word, keyed by the cleaned word
 * buildTsQueryText expects. Words with none are left out; an empty map means
 * there is nothing to retry with.
 */
export async function correctionsFor(words: string[]): Promise<Map<string, string[]>> {
  const out = new Map<string, string[]>()
  // Letters only: a house number or a code has no spelling to correct.
  const eligible = [...new Set(cleanWords(words))].filter((w) => /^\p{L}+$/u.test(w) && maxEdits(w.length) > 0)
  await Promise.all(eligible.map(async (w) => {
    let found = corrections.get(w)
    if (!found) {
      const looked = await lookup(fold(w))
      if (!looked) return
      corrections.set(w, looked)
      found = looked
    }
    if (found.length) out.set(w, found)
  }))
  return out
}
