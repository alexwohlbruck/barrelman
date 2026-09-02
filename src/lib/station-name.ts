/**
 * Folding a station name down to what a rider calls it, so an OSM name and a
 * GTFS one can be compared for identity.
 *
 * Distance alone cannot say which station an OSM node *is*. The Brooklyn
 * Bridge–City Hall stop_position sits 37.8 m from the Chambers St (J/Z)
 * platforms and 52.2 m from its own, so nearest-wins hands the station to its
 * neighbour and the board fills with lines that don't serve it. The name
 * settles it — but only after the two spellings are reconciled: OSM writes
 * "Grand Central–42nd Street" with an en dash and the word spelled out, the MTA
 * writes "Grand Central-42 St".
 *
 * The fold is deliberately exact-match, not fuzzy. Trigram similarity ranks
 * "Brooklyn Bridge–City Hall" ≈ "City Hall" (0.42) above "Times Square–42nd
 * Street" ≈ "Times Sq-42 St" (0.41), so any threshold that catches the real
 * pairs also catches wrong ones. Normalising both sides and requiring equality
 * has no threshold to get wrong, and a name that doesn't fold to a match simply
 * falls back to ranking by distance — never worse than having no name at all.
 */

/**
 * Word forms that mean the same thing on a sign. Both sides are folded, so an
 * entry can only ever create a match that distance would have gotten wrong; it
 * cannot hide one. That symmetry is why coarse folds like `park` → `pk` are
 * safe here and would not be in a search index.
 */
const SYNONYMS: Record<string, string> = {
  street: 'st',
  saint: 'st',
  avenue: 'av',
  ave: 'av',
  boulevard: 'blvd',
  square: 'sq',
  road: 'rd',
  drive: 'dr',
  place: 'pl',
  park: 'pk',
  parkway: 'pkwy',
  plaza: 'plz',
  terrace: 'ter',
  heights: 'hts',
  center: 'ctr',
  centre: 'ctr',
  court: 'ct',
  lane: 'ln',
  highway: 'hwy',
  junction: 'jct',
  east: 'e',
  west: 'w',
  north: 'n',
  south: 's',
  northeast: 'ne',
  northwest: 'nw',
  southeast: 'se',
  southwest: 'sw',
  mount: 'mt',
  fort: 'ft',
  and: '',
}

/** `42nd` → `42`, `1st` → `1`. The ordinal suffix is decoration on a number. */
const ORDINAL = /^(\d+)(st|nd|rd|th)$/

/**
 * The comparable form of a station name: lower-cased, stripped of accents and
 * punctuation (every dash, slash and period a feed might use), with each word
 * reduced to its shortest common spelling.
 *
 * Returns '' for a name that folds to nothing, which never matches — an unnamed
 * stop must not claim to be the same station as another unnamed one.
 */
export function normalizeStationName(name: string | null | undefined): string {
  if (!name) return ''

  return name
    .normalize('NFD')
    // Combining marks, so "Cañada" and "Canada" fold together.
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim()
    .split(' ')
    .map((word) => {
      const ordinal = ORDINAL.exec(word)
      if (ordinal) return ordinal[1]
      return SYNONYMS[word] ?? word
    })
    .filter(Boolean)
    .join(' ')
}

/**
 * Whether two names denote the same station.
 *
 * Both empty is not a match: two unnamed stops are unidentified, not identical.
 */
export function sameStationName(
  a: string | null | undefined,
  b: string | null | undefined,
): boolean {
  const left = normalizeStationName(a)
  if (!left) return false
  return left === normalizeStationName(b)
}
