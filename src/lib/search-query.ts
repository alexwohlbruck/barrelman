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

function spellingsOf(word: string): string[] {
  const known = VARIANTS.get(word)
  if (known) return known
  const ordinal = word.match(ORDINAL)
  if (ordinal) return [word, ordinal[1]] // 42nd → also plain 42
  if (/^\d+$/.test(word)) return [word, ordinalOf(word)] // 42 → also 42nd
  return [word]
}

/**
 * The text handed to `to_tsquery('simple', unaccent(...))`: words AND-joined,
 * each expanded to an OR-group of its spellings, the last one optionally a
 * prefix (`:*`) for typeahead. Words are reduced to alphanumerics so no
 * tsquery operator can be injected. Returns '' when nothing survives.
 */
export function buildTsQueryText(words: string[], prefixLast: boolean): string {
  const parts: string[] = []
  const cleaned = words
    .map((w) => w.toLowerCase().replace(/[^a-z0-9]/g, ''))
    .filter(Boolean)
  for (let i = 0; i < cleaned.length; i++) {
    const prefix = prefixLast && i === cleaned.length - 1
    const tokens = spellingsOf(cleaned[i]).map((s) => (prefix ? `${s}:*` : s))
    parts.push(tokens.length > 1 ? `(${tokens.join(' | ')})` : tokens[0])
  }
  return parts.join(' & ')
}
