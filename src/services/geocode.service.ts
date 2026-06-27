import { db } from '../db'
import { sql } from 'drizzle-orm'
import { spatialCache } from '../lib/cache'

export interface ReverseGeocodeResult {
  address: Record<string, string>
  hierarchy: any[]
}

// ── Forward geocoding (addresses) via Pelias ─────────────────────────────────
// Barrelman owns the Pelias geocoder (self-hosted OSM + OpenAddresses) and
// proxies it. Barrelman's own PostGIS layers cover POIs/categories; Pelias
// covers street addresses (which have no `name` and so aren't searchable in
// PostGIS). forwardGeocode is folded into /search so a single barrelman call
// returns POIs *and* addresses.

const PELIAS_URL = process.env.PELIAS_URL || 'http://pelias_api:4000'

/**
 * Adapt a Pelias GeoJSON feature into the geo_places result shape that
 * /search emits (and parchment's barrelman adapter consumes).
 */
function adaptPeliasFeature(f: any): any {
  const p = f?.properties ?? {}
  const [lng, lat] = f?.geometry?.coordinates ?? [null, null]

  // Pelias gid looks like "openstreetmap:address:node/4059135157" or
  // "openaddresses:address:<hash>". For OSM-sourced records reuse the real OSM
  // id so the result dedups against barrelman's PostGIS rows and gets an OSM
  // link downstream; otherwise namespace it under pelias/.
  const gid: string = p.gid ?? p.id ?? ''
  const localId = gid.split(':').slice(2).join(':')
  const isOsm = p.source === 'openstreetmap' && /^(node|way|relation)\//.test(localId)
  const id = isOsm ? localId : `pelias/${gid}`
  const osmTypeWord = isOsm ? localId.split('/')[0] : 'pelias'
  const osmIdNum = isOsm ? Number(localId.split('/')[1]) || 0 : 0
  const address = {
    housenumber: p.housenumber ?? null,
    street: p.street ?? null,
    unit: p.unit ?? null,
    city: p.locality ?? p.localadmin ?? p.county ?? null,
    state: p.region_a ?? p.region ?? null,
    postcode: p.postalcode ?? null,
    country: p.country_a ?? p.country ?? null,
  }
  // Mirror into addr:* tags so downstream summary/address builders work too.
  const tags: Record<string, string> = {}
  if (address.housenumber) tags['addr:housenumber'] = address.housenumber
  if (address.street) tags['addr:street'] = address.street
  if (address.city) tags['addr:city'] = address.city
  if (address.state) tags['addr:state'] = address.state
  if (address.postcode) tags['addr:postcode'] = address.postcode

  return {
    id,
    osm_type: osmTypeWord,
    osm_id: osmIdNum,
    name: p.name ?? p.label ?? null,
    name_abbrev: null,
    categories: [`pelias/${p.layer ?? 'address'}`],
    tags,
    address,
    hours: null,
    phones: null,
    websites: null,
    geom_type: 'point',
    geometry: lng != null ? { type: 'Point', coordinates: [lng, lat] } : null,
    text_rank: typeof p.confidence === 'number' ? p.confidence : 0.5,
    distance_m: null,
    _peliasLayer: p.layer,
  }
}

/**
 * Forward-geocode free text via Pelias autocomplete. Returns address/street
 * results adapted to the geo_places shape. Resilient: any failure (Pelias down,
 * timeout) yields [] so search degrades to POI-only rather than erroring.
 */
export async function forwardGeocode(
  text: string,
  opts: { lat?: number; lng?: number; limit?: number; layers?: string } = {},
): Promise<any[]> {
  const { lat, lng, limit = 10, layers = 'address,street' } = opts
  if (!text?.trim()) return []

  const params = new URLSearchParams({ text, size: String(limit) })
  if (lat != null && lng != null) {
    params.set('focus.point.lat', String(lat))
    params.set('focus.point.lon', String(lng))
  }
  if (layers) params.set('layers', layers)

  try {
    const res = await fetch(`${PELIAS_URL}/v1/autocomplete?${params}`, {
      signal: AbortSignal.timeout(2500),
    })
    if (!res.ok) return []
    const data = (await res.json()) as { features?: any[] }
    return (data.features ?? []).map(adaptPeliasFeature)
  } catch {
    return [] // Pelias unavailable — POI search still works.
  }
}

export async function reverseGeocode(lat: number, lng: number): Promise<ReverseGeocodeResult> {
  const cacheKey = `geocode:${lat}:${lng}`
  const cached = spatialCache.get(cacheKey)
  if (cached) return cached

  // Reverse geocode: find admin boundaries containing the point
  const results = await db.execute(sql`
    SELECT
      id, osm_type, osm_id, name, admin_level, area_m2,
      tags->>'place' AS place_type,
      tags->>'boundary' AS boundary_type
    FROM geo_places
    WHERE geom_type = 'area'
    AND admin_level IS NOT NULL
    AND ST_Contains(
      geom,
      ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)
    )
    ORDER BY admin_level DESC
  `)

  // Build address components from hierarchy
  const rows = Array.from(results as any[])
  const address: Record<string, string> = {}

  for (const row of rows) {
    const level = row.admin_level
    if (level >= 8 && !address.city) address.city = row.name
    else if (level >= 6 && !address.county) address.county = row.name
    else if (level >= 4 && !address.state) address.state = row.name
    else if (level >= 2 && !address.country) address.country = row.name
  }

  const result: ReverseGeocodeResult = { address, hierarchy: rows }
  spatialCache.set(cacheKey, result)
  return result
}
