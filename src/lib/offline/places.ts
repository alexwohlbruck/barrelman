import type { PackagePlace, PackagePlaceAddress, PackageTransitRoute } from '../../types/offline-package.types'
import { round6 } from './bbox'

const ADDRESS_FIELDS = ['housenumber', 'street', 'unit', 'city', 'state', 'postcode'] as const

/** Categories that duplicate GTFS stops or name nothing a person searches for. */
export const SKIPPED_CATEGORY_PREFIXES = ['public_transport/', 'highway/bus_stop', 'highway/intersection']

export function packageAddress(raw: Record<string, unknown> | null | undefined): PackagePlaceAddress | undefined {
  if (!raw) return undefined
  const address: PackagePlaceAddress = {}
  for (const field of ADDRESS_FIELDS) {
    const value = raw[field]
    if (typeof value === 'string' && value.trim()) address[field] = value.trim()
  }
  return Object.keys(address).length ? address : undefined
}

export interface PlaceRow {
  id: string
  name: string
  category: string | null
  lng: number
  lat: number
  address?: Record<string, unknown> | null
  hours?: string | null
  phone?: string | null
  website?: string | null
}

export function packagePlace(row: PlaceRow): PackagePlace {
  const address = packageAddress(row.address)
  return {
    id: row.id,
    name: row.name,
    category: row.category,
    lng: round6(row.lng),
    lat: round6(row.lat),
    ...(address && { address }),
    ...(row.hours && { hours: row.hours }),
    ...(row.phone && { phone: row.phone }),
    ...(row.website && { website: row.website }),
  }
}

export interface StopRow {
  feedId: string
  stopId: string
  name: string
  lng: number
  lat: number
  routes: { shortName: string | null; color: string | null; type: number }[] | null
}

export function packageStop(row: StopRow): PackagePlace {
  const routes: PackageTransitRoute[] = []
  const seen = new Set<string>()
  for (const r of row.routes ?? []) {
    if (!r.shortName || seen.has(r.shortName)) continue
    seen.add(r.shortName)
    routes.push({ shortName: r.shortName, type: r.type, ...(r.color && { color: r.color }) })
  }
  routes.sort((a, b) => a.shortName.localeCompare(b.shortName, undefined, { numeric: true }))
  return {
    id: `stop/${row.feedId}/${row.stopId}`,
    name: row.name,
    category: 'public_transport/station',
    lng: round6(row.lng),
    lat: round6(row.lat),
    transit: { feedId: row.feedId, stopId: row.stopId, routes },
  }
}

/**
 * Drop places already listed: the same OSM id from a neighbouring query cell,
 * or a street name repeated inside one cell.
 */
export function dedupePlaces(places: PackagePlace[]): PackagePlace[] {
  const seen = new Set<string>()
  return places.filter(p => {
    if (seen.has(p.id)) return false
    seen.add(p.id)
    return true
  })
}
