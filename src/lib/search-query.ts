/**
 * Query-side abbreviation expansion for the FTS layer.
 *
 * The tsvector uses the 'simple' config — no stemming — so "ave" in a query
 * cannot match "Avenue" in a name, and "heights" cannot match "Hts". Both
 * directions are real: signs abbreviate ("82 St-Jackson Hts") and people
 * abbreviate ("franklin ave medgar"). Rather than reindex 22M names with
 * synonym expansion, each query word becomes an OR-group of every spelling —
 * `ave` → `(av | ave | avenue)` — derived from the same curated table
 * station-name.ts folds with, so the two stay in agreement. An OR-group can
 * only create matches, never hide one.
 */

import { SYNONYMS } from './station-name'

/** Every spelling of a word, keyed by each of its spellings. */
const VARIANTS = new Map<string, string[]>()
{
  const groups = new Map<string, Set<string>>()
  for (const [variant, canonical] of Object.entries(SYNONYMS)) {
    if (!canonical) continue // 'and' folds to nothing — not a spelling group
    const group = groups.get(canonical) ?? new Set([canonical])
    group.add(variant)
    groups.set(canonical, group)
  }
  for (const group of groups.values()) {
    const spellings = [...group].sort()
    for (const word of spellings) VARIANTS.set(word, spellings)
  }
}

/** `42` → `42nd`, `1` → `1st` — so "42 st" finds "42nd Street". */
function ordinalOf(digits: string): string {
  const n = Number(digits)
  const tens = n % 100
  if (tens >= 11 && tens <= 13) return `${digits}th`
  switch (n % 10) {
    case 1: return `${digits}st`
    case 2: return `${digits}nd`
    case 3: return `${digits}rd`
    default: return `${digits}th`
  }
}

const ORDINAL = /^(\d+)(st|nd|rd|th)$/

/**
 * Words people type that the document spells differently. The document's
 * category words come from OSM keys ("shop clothes", "shop supermarket"), and
 * its place names from boundaries ("New York"), so the everyday word never
 * matches on its own. Each entry is an extra spelling; a space makes it a
 * phrase, matched only as adjacent words. Kept small and unambiguous: every
 * entry widens every query that uses the word.
 */
const VOCABULARY: Record<string, string[]> = {
  store: ['shop'],
  stores: ['shop'],
  shops: ['shop'],
  clothing: ['clothes'],
  grocery: ['supermarket', 'greengrocer'],
  groceries: ['supermarket', 'greengrocer'],
  nyc: ['new york'],
}

/**
 * The other number of an English noun: "suit" ↔ "suits", "bakery" ↔
 * "bakeries", "church" ↔ "churches". The 'simple' config doesn't stem, and the
 * document holds tag values in whatever number OSM uses (clothes=suits), so
 * "suit shop" missed every store tagged that way. Crude on purpose: a wrong
 * form is a lexeme nothing contains, which costs one index probe and matches
 * nothing. ASCII words only, since these are English rules.
 */
export function inflections(word: string): string[] {
  if (!/^[a-z]{3,}$/.test(word)) return []
  if (/ies$/.test(word) && word.length >= 5) return [word.slice(0, -3) + 'y']
  if (/(s|x|ch|sh)es$/.test(word) && word.length >= 5) return [word.slice(0, -2), word.slice(0, -1)]
  if (/s$/.test(word)) return /(ss|us|is)$/.test(word) || word.length < 4 ? [] : [word.slice(0, -1)]
  if (/[^aeiou]y$/.test(word)) return [word.slice(0, -1) + 'ies']
  if (/(x|ch|sh)$/.test(word)) return [word + 'es']
  return [word + 's']
}

function spellingsOf(word: string): string[] {
  const known = VARIANTS.get(word)
  if (known) return known
  const ordinal = word.match(ORDINAL)
  if (ordinal) return [word, ordinal[1]] // 42nd → also plain 42
  if (/^\d+$/.test(word)) return [word, ordinalOf(word)] // 42 → also 42nd
  return [word]
}

/** A phrase spelling ("new york") as tsquery text: adjacent words. */
const phrase = (s: string) => s.split(' ').join(' <-> ')

/**
 * The text handed to `to_tsquery('simple', unaccent(...))`: words AND-joined,
 * each expanded to an OR-group of its spellings, the last one optionally a
 * prefix (`:*`) for typeahead — unless it is a known abbreviation ending a
 * multi-word query. Words are reduced to letters and digits so no
 * tsquery operator can be injected — any script's, since the query is passed
 * through unaccent() in SQL to meet the tsvector ("Neukölln" → "neukolln").
 * Returns '' when nothing survives.
 *
 * `corrections` adds near-spellings found in the index for a word (see
 * search-lexicon.service.ts), keyed by the cleaned word. They are exact
 * alternatives, never prefixes: a corrected word is one the user finished.
 */
export function buildTsQueryText(
  words: string[],
  prefixLast: boolean,
  corrections?: ReadonlyMap<string, readonly string[]>,
): string {
  const parts: string[] = []
  const cleaned = cleanWords(words)
  for (let i = 0; i < cleaned.length; i++) {
    const word = cleaned[i]
    // A trailing "ave" or "st" after another word is a finished street type, and
    // as a prefix it expands across millions of words: 9ms became 2.6s.
    const prefix = prefixLast && i === cleaned.length - 1 && !(i > 0 && VARIANTS.has(word))
    const tokens = spellingsOf(word).map((s) => (prefix ? `${s}:*` : s))
    const exact = [
      ...(VARIANTS.has(word) ? [] : inflections(word)),
      ...(VOCABULARY[word] ?? []).map(phrase),
      ...(corrections?.get(word) ?? []),
    ]
    // A prefix already reaches every longer form ("suit:*" covers "suits").
    for (const s of exact) {
      if (prefix && s.startsWith(word)) continue
      if (!tokens.includes(s)) tokens.push(s)
    }
    parts.push(tokens.length > 1 ? `(${tokens.join(' | ')})` : tokens[0])
  }
  return parts.join(' & ')
}

/** Query words as buildTsQueryText sees them: lowercased letters and digits. */
export function cleanWords(words: string[]): string[] {
  return words
    .map((w) => w.normalize('NFC').toLowerCase().replace(/[^\p{L}\p{M}\p{N}]/gu, ''))
    .filter(Boolean)
}

/** Canonical street types (see SYNONYMS): "Elm Street", "Michigan Ave", "Ocean Pkwy". */
const STREET_TYPES = new Set(['st', 'av', 'blvd', 'rd', 'dr', 'pl', 'pkwy', 'ter', 'ct', 'ln', 'hwy', 'sq', 'plz', 'way', 'cir'])

/** True when the query names a street: two or more words, the last a street type. */
export function isStreetQuery(words: string[]): boolean {
  const cleaned = words.map((w) => w.toLowerCase().replace(/[^a-z0-9]/g, '')).filter(Boolean)
  if (cleaned.length < 2) return false
  const last = cleaned[cleaned.length - 1]
  return STREET_TYPES.has(SYNONYMS[last] || last)
}
