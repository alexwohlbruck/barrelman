/**
 * GTFS `route_type` → mode class.
 *
 * The basic types (0-12) plus the extended HVT ranges, collapsed to the
 * nine classes transit renderers actually distinguish. Consumers hold a
 * class, never a raw number: a metro is route_type 1 in one feed and 401
 * in the next, and a caller asking "is this route a metro" must get the
 * same answer either way.
 *
 * Mirrors portolan's `mode.Of` (internal/mode/mode.go) and parchment's
 * `portolanClassOf` — the three have to agree or a route matched across
 * them lands in the wrong class.
 */

export const GTFS_MODE_CLASSES = [
  'metro',
  'tram',
  'regional',
  'monorail',
  'funicular',
  'cable',
  'aerial',
  'ferry',
  'bus',
] as const

export type GtfsModeClass = (typeof GTFS_MODE_CLASSES)[number]

export function isGtfsModeClass(value: unknown): value is GtfsModeClass {
  return GTFS_MODE_CLASSES.includes(value as GtfsModeClass)
}

/** The class a `route_type` belongs to, or null for a type nothing claims
 *  — which matches nothing rather than matching the first thing tried. */
export function gtfsModeClass(routeType?: number | null): GtfsModeClass | null {
  if (routeType == null || Number.isNaN(routeType)) return null

  switch (routeType) {
    case 0: return 'tram'
    case 1: return 'metro'
    case 2: return 'regional'
    case 3: case 11: return 'bus' // bus, trolleybus
    case 4: return 'ferry'
    case 5: return 'cable'
    case 6: return 'aerial'
    case 7: return 'funicular'
    case 12: return 'monorail'
  }

  if (routeType >= 100 && routeType < 300) return 'regional' // rail + coach
  if (routeType === 405) return 'monorail'
  if (routeType >= 400 && routeType < 500) return 'metro'
  if (routeType >= 700 && routeType < 900) return 'bus'
  if (routeType >= 900 && routeType < 1000) return 'tram'
  if (routeType >= 1000 && routeType < 1300) return 'ferry' // water, air, taxi
  if (routeType >= 1300 && routeType < 1400) return 'aerial'
  if (routeType >= 1400 && routeType < 1500) return 'funicular'
  if (routeType >= 1700 && routeType < 1800) return 'cable'

  return null
}
