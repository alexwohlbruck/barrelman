/**
 * Entrances that reach only one direction's platform. A station with side
 * platforms and no crossover inside fare control signs each entrance for one
 * direction, and mappers copy that into its name or description: a compass
 * word ("6 trains Uptown & The Bronx") or destinations ("A,C trains to
 * Euclid Avenue, Lefferts Boulevard, Rockaways"), sometimes per line
 * ("all F,G trains; R trains to Manhattan").
 */

/** What an entrance is signed for: a compass heading or named destinations. */
export type EntranceSign = { heading: number } | { toward: string[] }

const HEADING_TERMS: Array<[RegExp, number]> = [
  [/\b(uptown|north\s*-?bound)\b/i, 0],
  [/\b(east\s*-?bound)\b/i, 90],
  [/\b(downtown|south\s*-?bound)\b/i, 180],
  [/\b(west\s*-?bound)\b/i, 270],
]

const TRAINS_CLAUSE = /^\s*(?:all\s+)?(?:([\w<>]{1,4}(?:\s*,\s*[\w<>]{1,4})*)\s+)?trains?\b(.*)$/i
const TOWARD = /^\s*(?:to|toward|towards)\s+(.+?)\s*$/i
const BOUND = /^\s*(.+?)[\s-]+bound\b/i

interface Clause {
  routes: string[] | null
  sign: EntranceSign | null
}

const routeKey = (route: string) => route.replace(/[<>]/g, '').trim().toLowerCase()

function compassHeading(text: string): number | null {
  const headings = new Set(HEADING_TERMS.filter(([re]) => re.test(text)).map(([, h]) => h))
  return headings.size === 1 ? [...headings][0] : null
}

const places = (text: string) => text.split(/\s*(?:,|&|\/|\band\b)\s*/i).map((p) => p.trim()).filter(Boolean)

function parseClause(text: string): Clause {
  const trains = TRAINS_CLAUSE.exec(text)
  const routes = trains?.[1] ? trains[1].split(',').map(routeKey) : null
  const heading = compassHeading(text)
  if (heading != null) return { routes, sign: { heading } }
  const toward = (trains ? TOWARD.exec(trains[2]) ?? BOUND.exec(trains[2]) : BOUND.exec(text))?.[1]
  return { routes, sign: toward ? { toward: places(toward) } : null }
}

const signKey = (sign: EntranceSign) =>
  'heading' in sign ? `h:${sign.heading}` : `t:${sign.toward.join('|').toLowerCase()}`

/**
 * The one direction an entrance is signed for on the given routes, or null
 * when it serves either or the sign can't be read. A clause naming one of
 * the routes decides; with none, every clause on the sign is considered.
 */
export function entranceSign(routes: string[], ...labels: Array<string | null | undefined>): EntranceSign | null {
  const clauses = labels
    .filter((l): l is string => !!l)
    .flatMap((l) => l.split(';'))
    .map(parseClause)
  const wanted = new Set(routes.map(routeKey))
  const own = clauses.filter((c) => c.routes?.some((r) => wanted.has(r)))
  if (own.some((c) => c.sign == null)) return null
  const signs = (own.length ? own : clauses).flatMap((c) => (c.sign ? [c.sign] : []))
  const distinct = new Map(signs.map((s) => [signKey(s), s]))
  return distinct.size === 1 ? [...distinct.values()][0] : null
}

const ABBREVIATIONS: Record<string, string> = {
  avenue: 'av', ave: 'av', boulevard: 'blvd', street: 'st', road: 'rd', parkway: 'pkwy',
  square: 'sq', place: 'pl', center: 'ctr', centre: 'ctr', heights: 'hts', junction: 'jct',
}

function words(text: string): string[] {
  return text.toLowerCase().replace(/[^\p{L}\p{N}]+/gu, ' ').split(' ')
    .filter((w) => w && w !== 'the')
    .map((w) => ABBREVIATIONS[w] ?? w.replace(/^(\d+)(st|nd|rd|th)$/, '$1').replace(/^(\D{3,})s$/, '$1'))
}

/** The place appears in the name word for word: "Lefferts Boulevard" in "Ozone Park-Lefferts Blvd". */
function names(place: string, name: string): boolean {
  const want = words(place), have = new Set(words(name))
  return want.length > 0 && want.every((w) => have.has(w))
}

/** Which way a ride runs: what the platform calls it, the terminal, and the stops ahead. */
export interface RideDirection {
  /** Compass bearing the train leaves the platform on. */
  bearing: number | null
  /** The platform's own sign ("Manhattan"), headsign, and the stops still to come. */
  names: string[]
}

/**
 * True when the sign names this ride's direction, false when it names the
 * other, null when it can't tell. "Uptown" leans with a street grid rather
 * than true north, hence the generous quarter-turn.
 */
export function signMatches(sign: EntranceSign, ride: RideDirection): boolean | null {
  if ('heading' in sign) {
    if (ride.bearing == null) return null
    return Math.abs(((sign.heading - ride.bearing + 540) % 360) - 180) <= 90
  }
  return sign.toward.some((place) => ride.names.some((name) => names(place, name))) ? true : null
}

/**
 * Entrances that reach the ride's platform. A compass sign pointing the
 * other way rules an entrance out. A destination sign the ride doesn't name
 * rules it out only once another entrance's sign does name it, so two signs
 * for one station are told apart; unsigned entrances are always kept.
 */
export function reachingPlatform<E>(entrances: E[], signOf: (e: E) => EntranceSign | null, ride: RideDirection): E[] {
  const verdicts = entrances.map((e) => {
    const sign = signOf(e)
    return { e, sign, match: sign ? signMatches(sign, ride) : null }
  })
  const someNamed = verdicts.some((v) => v.sign && 'toward' in v.sign && v.match === true)
  return verdicts
    .filter(({ sign, match }) => {
      if (!sign || match === true) return true
      if ('heading' in sign) return match !== false
      return !someNamed
    })
    .map((v) => v.e)
}
