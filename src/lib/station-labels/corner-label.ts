/**
 * Corner labels for station entrances, derived from the street grid:
 * "Broadway & West 28th Street · NW corner". Generic, so it works anywhere
 * barrelman has named intersections, and recomputed whenever OSM changes.
 */

export type Cardinal = 'N' | 'E' | 'S' | 'W'
export type Compass8 = Cardinal | 'NE' | 'SE' | 'SW' | 'NW'

export interface LngLat {
  lng: number
  lat: number
}

export interface CornerStreet {
  name: string
  /** Bearing of the street through the intersection, degrees from north. Null when unmeasured. */
  azimuth: number | null
  /** Distance from the entrance to this street, metres. */
  distanceM: number | null
}

export interface CornerInput {
  entrance: LngLat
  intersection: (LngLat & { distanceM: number; streets: CornerStreet[] }) | null
  /** Nearest named road and its distance, for an entrance mid-block. */
  nearestRoad: { name: string; distanceM: number } | null
}

/** Within this, an entrance stands on the intersection's corner. */
export const CORNER_MAX_M = 50
/** Within this, the intersection still names where the entrance is. */
export const NEAR_INTERSECTION_MAX_M = 80
/** Within this of a named road, a mid-block entrance is "on" it. */
export const ON_STREET_MAX_M = 30

const M_PER_DEG = 111_320
const CARDINALS: Cardinal[] = ['N', 'E', 'S', 'W']
const COMPASS8: Compass8[] = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']

/** Entrance offset from the intersection in local metres (x east, y north). */
function offset(from: LngLat, to: LngLat): { x: number; y: number } {
  return {
    x: (to.lng - from.lng) * M_PER_DEG * Math.cos((from.lat * Math.PI) / 180),
    y: (to.lat - from.lat) * M_PER_DEG,
  }
}

const norm = (deg: number) => ((deg % 360) + 360) % 360

export function snapCardinal(bearing: number): Cardinal {
  return CARDINALS[Math.round(norm(bearing) / 90) % 4]
}

export function compass8(bearing: number): Compass8 {
  return COMPASS8[Math.round(norm(bearing) / 45) % 8]
}

/**
 * The side of a street the entrance is on, as a compass point: the street's
 * normal that points toward the entrance, snapped to N/E/S/W.
 */
export function streetSide(azimuth: number, entrance: LngLat, intersection: LngLat): Cardinal {
  const { x, y } = offset(intersection, entrance)
  const a = (azimuth * Math.PI) / 180
  const left = Math.sin(a) * y - Math.cos(a) * x > 0
  return snapCardinal(left ? azimuth - 90 : azimuth + 90)
}

/** "NW" from a north-south side and an east-west side; null when both lie on one axis. */
export function quadrant(sides: Cardinal[]): Compass8 | null {
  const ns = sides.find((s) => s === 'N' || s === 'S')
  const ew = sides.find((s) => s === 'E' || s === 'W')
  return ns && ew ? ((ns + ew) as Compass8) : null
}

/** Bearing from the intersection to the entrance. */
function bearing(from: LngLat, to: LngLat): number {
  const { x, y } = offset(from, to)
  return norm((Math.atan2(x, y) * 180) / Math.PI)
}

/** The two streets that frame the entrance: nearest first, with distinct names. */
function framingStreets(streets: CornerStreet[]): CornerStreet[] {
  const named = streets.filter((s, i) => s.name && streets.findIndex((o) => o.name === s.name) === i)
  if (named.length <= 2) return named
  return [...named]
    .sort((a, b) => (a.distanceM ?? Infinity) - (b.distanceM ?? Infinity))
    .slice(0, 2)
}

/**
 * Which corner of the two streets the entrance stands on. Each street's side
 * gives one half of it; a diagonal street that leaves both on one axis falls
 * back to the bearing from the intersection.
 */
export function cornerOf(entrance: LngLat, intersection: LngLat, streets: CornerStreet[]): Compass8 {
  const sides = streets
    .filter((s): s is CornerStreet & { azimuth: number } => s.azimuth != null)
    .map((s) => streetSide(s.azimuth, entrance, intersection))
  return quadrant(sides) ?? compass8(bearing(intersection, entrance))
}

export function formatCorner(corner: Compass8): string {
  return corner.length === 2 ? `${corner} corner` : `${corner} side`
}

/**
 * The label for one entrance, or null when the streets around it can't say
 * where it is. Tried in order: the corner of the nearest intersection, the
 * intersection alone, the street it stands on.
 */
export function cornerLabel({ entrance, intersection, nearestRoad }: CornerInput): string | null {
  if (intersection) {
    const streets = framingStreets(intersection.streets)
    if (streets.length === 2) {
      const pair = streets.map((s) => s.name).join(' & ')
      if (intersection.distanceM <= CORNER_MAX_M) {
        return `${pair} · ${formatCorner(cornerOf(entrance, intersection, streets))}`
      }
      if (intersection.distanceM <= NEAR_INTERSECTION_MAX_M) return pair
    }
  }
  if (nearestRoad && nearestRoad.distanceM <= ON_STREET_MAX_M) return nearestRoad.name
  return null
}
