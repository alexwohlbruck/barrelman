/**
 * Entrances that reach only one direction's platform. A station with side
 * platforms and no crossover inside fare control signs each entrance for one
 * direction, and mappers copy that into its name or description: a compass
 * word ("6 trains Uptown & The Bronx"), in or out ("Red Line Inbound only"),
 * or destinations ("A,C trains to Euclid Avenue, Lefferts Boulevard,
 * Rockaways", "To Franconia-Springfield", "Richtung Pankow"), sometimes per
 * line ("all F,G trains; R trains to Manhattan").
 */

/**
 * What an entrance is signed for: a compass heading, in or out of town,
 * named destinations, or only other lines than the ride's.
 */
export type EntranceSign = { heading: number } | { inbound: boolean } | { toward: string[] } | { otherLines: true }

const HEADING_TERMS: Array<[RegExp, number]> = [
  [/\b(uptown|north\s*-?bound)\b/i, 0],
  [/\b(east\s*-?bound)\b/i, 90],
  [/\b(downtown|south\s*-?bound)\b/i, 180],
  [/\b(west\s*-?bound)\b/i, 270],
]

/** "A,C trains …", "all F,G trains …". */
const TRAINS = /^\s*(?:all\s+)?(?:([\w<>]{1,4}(?:\s*,\s*[\w<>]{1,4})*)\s+)?trains?\b(.*)$/i
/** "Red Line …", "Line 1 …". */
const LINE = /^\s*(?:all\s+)?((?:[\w-]+\s+){1,3}line|line\s+[\w-]+)\b(.*)$/i
const EITHER_WAY = /\b(all\s+trains|all\s+directions|both\s+directions)\b/i
const IN_OUT = /\b(in|out)\s*-?\s*bound\b/i
/** A destination after a word for "to" or "towards", at the start of a piece of the sign. */
const TOWARD = /^(?:to|toward|towards|direction|dir\.|richtung|dirección|direccion|hacia|direzione|sentido|kierunek|riktning|retning)\s+(.+?)\s*$/i
const BOUND = /^(.+?)[\s-]+bound\b/i

interface Clause {
  routes: string[] | null
  sign: EntranceSign | null
}

/** "Red Line", "Line 1" and "1" are one route's key; an express ("6X", signed "<6>") is its line's. */
export const routeKey = (route: string) =>
  route.replace(/[<>]/g, '').trim().toLowerCase().replace(/\s+line$/, '').replace(/^line\s+/, '').replace(/^(\w{1,2})x$/, '$1')

function compassHeading(text: string): number | null {
  const headings = new Set(HEADING_TERMS.filter(([re]) => re.test(text)).map(([, h]) => h))
  return headings.size === 1 ? [...headings][0] : null
}

const places = (text: string) =>
  text.replace(/\bonly\b/gi, '').split(/\s*(?:,|&|\/|\band\b)\s*/i).map((p) => p.trim()).filter(Boolean)

/** The pieces a sign's text may start a destination in: the whole, and after a dash, colon or bracket. */
const pieces = (text: string) => [text, ...text.split(/\s+[-–—]\s+|:|\(|\)/)].map((p) => p.trim()).filter(Boolean)

function readSign(text: string): EntranceSign | null {
  if (EITHER_WAY.test(text)) return null
  const heading = compassHeading(text)
  if (heading != null) return { heading }
  const inOut = IN_OUT.exec(text)
  if (inOut) return { inbound: inOut[1].toLowerCase() === 'in' }
  for (const piece of pieces(text)) {
    const toward = (TOWARD.exec(piece) ?? BOUND.exec(piece))?.[1]
    if (toward) return { toward: places(toward) }
  }
  return null
}

function parseClause(text: string): Clause {
  const trains = TRAINS.exec(text)
  if (trains) return { routes: trains[1] ? trains[1].split(',').map(routeKey) : null, sign: readSign(trains[2]) }
  const line = LINE.exec(text)
  if (line) return { routes: [routeKey(line[1])], sign: readSign(line[2]) }
  return { routes: null, sign: readSign(text) }
}

const signKey = (sign: EntranceSign) =>
  'heading' in sign ? `h:${sign.heading}` : 'inbound' in sign ? `i:${sign.inbound}` : 'toward' in sign ? `t:${sign.toward.join('|').toLowerCase()}` : 'o'

/**
 * The one direction an entrance is signed for on the given routes, or null
 * when it serves either or the sign can't be read. A clause naming one of
 * the routes decides; with none, every clause on the sign is considered.
 */
export function entranceSign(routes: string[], ...labels: Array<string | null | undefined>): EntranceSign | null {
  const clauses = labels
    .filter((l): l is string => !!l)
    .flatMap((l) => l.split(/[;|\n]/))
    .filter((c) => c.trim())
    .map(parseClause)
  const wanted = new Set(routes.map(routeKey))
  const own = clauses.filter((c) => c.routes?.some((r) => wanted.has(r)))
  if (own.some((c) => c.sign == null)) return null
  const general = clauses.filter((c) => !c.routes)
  if (!own.length && !general.length && clauses.length && wanted.size) return { otherLines: true }
  const signs = (own.length ? own : general).flatMap((c) => (c.sign ? [c.sign] : []))
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
  /** Heading toward the network's hub, where most of its lines meet. */
  inbound: boolean | null
}

const IN_OUT_LABEL = /^\s*(in|out)\s*-?\s*bound\s*$/i

/**
 * True when the sign names this ride's direction, false when it names the
 * other, null when it can't tell. "Uptown" leans with a street grid rather
 * than true north, hence the generous quarter-turn.
 */
export function signMatches(sign: EntranceSign, ride: RideDirection): boolean | null {
  if ('otherLines' in sign) return false
  if ('heading' in sign) {
    const labelled = ride.names.map(compassHeading).find((h) => h != null)
    const heading = labelled ?? ride.bearing
    if (heading == null) return null
    return Math.abs(((sign.heading - heading + 540) % 360) - 180) <= 90
  }
  if ('inbound' in sign) {
    const labelled = ride.names.map((n) => IN_OUT_LABEL.exec(n)).find(Boolean)
    const inbound = labelled ? labelled[1].toLowerCase() === 'in' : ride.inbound
    return inbound == null ? null : inbound === sign.inbound
  }
  return sign.toward.some((place) => ride.names.some((name) => names(place, name))) ? true : null
}

/**
 * Entrances that reach the ride's platform. A compass or in/out sign for
 * the other way rules an entrance out, and so does a sign naming only other
 * lines, while some entrance might serve the ride's. A destination sign the ride doesn't name
 * rules it out only once another entrance's sign does name it, so two signs
 * for one station are told apart; unsigned entrances are always kept.
 */
export function reachingPlatform<E>(entrances: E[], signOf: (e: E) => EntranceSign | null, ride: RideDirection): E[] {
  const verdicts = entrances.map((e) => {
    const sign = signOf(e)
    return { e, sign, match: sign ? signMatches(sign, ride) : null }
  })
  const someNamed = verdicts.some((v) => v.sign && 'toward' in v.sign && v.match === true)
  const servesRide = verdicts.some((v) => !v.sign || !('otherLines' in v.sign))
  return verdicts
    .filter(({ sign, match }) => {
      if (!sign || match === true) return true
      if ('toward' in sign) return !someNamed
      if ('otherLines' in sign) return !servesRide
      return match !== false
    })
    .map((v) => v.e)
}
