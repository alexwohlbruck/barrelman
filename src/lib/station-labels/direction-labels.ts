/**
 * Direction labels: what the platform sign calls a direction ("Uptown & The
 * Bronx", "Manhattan"), as opposed to the GTFS headsign, which names the
 * terminal. Stored per directional stop in `stop_direction_labels`, one row per
 * source, and resolved here.
 */

/** Where a label came from. Anything not prefixed `osm-` is agency data. */
export type DirectionLabelSource = 'osm-destination' | 'osm-direction' | (string & {})

export interface DirectionLabelRow {
  label: string
  source: DirectionLabelSource
}

/** Values agencies publish where no direction applies. */
const PLACEHOLDERS = /^(last stop|n\/?a|nan|none|null|-+)$/i

/** A label as published, or null for blanks and placeholders like MTA's "Last Stop". */
export function cleanLabel(raw: string | null | undefined): string | null {
  const label = raw?.replace(/\s+/g, ' ').trim()
  if (!label || PLACEHOLDERS.test(label)) return null
  return label
}

/** Compass and in/out words say which way, not what the sign says. */
const GENERIC = /^(north|south|east|west|in|out)\s*-?\s*bound$/i

const isOsm = (row: DirectionLabelRow) => row.source.startsWith('osm-')

/**
 * The label to show. Agency data covers the most stops, so it leads. An OSM
 * `destination` tied to the stop is the sign's own text, so it wins where the
 * agency label is only a compass word ("Southbound") or a short form of it
 * ("Uptown" → "Uptown & The Bronx"). OSM direction words are the last resort.
 */
export function pickDirectionLabel(rows: DirectionLabelRow[]): string | null {
  const agency = rows.find((r) => !isOsm(r))?.label ?? null
  const destination = rows.find((r) => r.source === 'osm-destination')?.label ?? null
  const words = rows.find((r) => r.source === 'osm-direction')?.label ?? null

  if (agency && destination && (GENERIC.test(agency) || elaborates(destination, agency))) {
    return destination
  }
  return agency ?? destination ?? words
}

function elaborates(full: string, short: string): boolean {
  const f = full.toLowerCase()
  const s = short.toLowerCase()
  return f !== s && f.startsWith(s) && /^[\s&,/-]/.test(f.slice(s.length))
}

const COMPASS_WORDS = /\b(uptown|downtown|(?:north|south|east|west|in|out)\s*-?\s*bound)\b/i
const PLACE_BOUND = /\b([A-Z][\w.]*(?:\s[A-Z][\w.]*)*-bound)\b/

/**
 * A direction phrase in a platform's name or description ("Uptown platform",
 * "Brooklyn-bound"), or null. Used only where the platform has no `destination`.
 */
export function directionPhrase(...texts: Array<string | null | undefined>): string | null {
  for (const text of texts) {
    const match = text ? COMPASS_WORDS.exec(text) ?? PLACE_BOUND.exec(text) : null
    if (match) return match[1].replace(/\s+/g, ' ')
  }
  return null
}
