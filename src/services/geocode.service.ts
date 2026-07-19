import { db } from '../db'
import { sql } from 'drizzle-orm'
import { spatialCache } from '../lib/cache'
import { findOsmByAddress } from './place.service'

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

// Backstop only — guards against a hung Pelias, not a slow-but-alive one.
// Cancellation is driven by the caller's request signal (a new keystroke
// supersedes the previous request); this ceiling merely prevents a request
// from hanging forever if Pelias never responds. Kept well above Pelias's
// real p95 (~2.6s) so genuine slow responses still complete and return
// addresses rather than being dropped. See PELIAS_URL geocode notes above.
const PELIAS_HANG_BACKSTOP_MS = 10_000

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
  opts: { lat?: number; lng?: number; limit?: number; layers?: string; signal?: AbortSignal } = {},
): Promise<any[]> {
  const { lat, lng, limit = 10, layers = 'address,street', signal } = opts
  if (!text?.trim()) return []

  // Cancel when the originating request is aborted (a superseding keystroke),
  // OR when the hang backstop fires — whichever comes first. Honoring the
  // caller's signal is what lets a slow-but-valid Pelias response finish
  // instead of being cut at a fixed clock and dropped.
  const backstop = AbortSignal.timeout(PELIAS_HANG_BACKSTOP_MS)
  const fetchSignal = signal ? AbortSignal.any([signal, backstop]) : backstop

  const query = async (withLayers: string): Promise<any[] | null> => {
    const params = new URLSearchParams({ text, size: String(limit) })
    if (lat != null && lng != null) {
      params.set('focus.point.lat', String(lat))
      params.set('focus.point.lon', String(lng))
    }
    if (withLayers) params.set('layers', withLayers)
    const res = await fetch(`${PELIAS_URL}/v1/autocomplete?${params}`, {
      signal: fetchSignal,
    })
    if (!res.ok) return null
    const data = (await res.json()) as { features?: any[] }
    return data.features ?? []
  }

  try {
    let features = await query(layers)
    // A requested layer with zero docs in the index (e.g. `street` when
    // polylines were never imported) makes Pelias return nothing for the whole
    // request rather than falling back to the layers that DO have data. When a
    // filtered call comes back empty, retry unfiltered so addresses still
    // surface. Skip if there was no filter to begin with.
    if (layers && (features == null || features.length === 0)) {
      features = await query('')
    }
    return (features ?? []).map(adaptPeliasFeature)
  } catch {
    return [] // Pelias unavailable/aborted — POI search still works.
  }
}

/**
 * Fetch a single Pelias record by its global id (gid), e.g.
 * "openaddresses:address:us/ny/city_of_new_york:7e5b…". Used to resolve an
 * address place-detail view: Pelias geocoder results have no row in geo_places,
 * so `/place/:osmType/:osmId` can't serve them — this hits Pelias `/v1/place`
 * instead. Returns the geo_places-shaped record (via adaptPeliasFeature) or null.
 */
export async function fetchPeliasPlaceByGid(
  gid: string,
  opts: { signal?: AbortSignal } = {},
): Promise<any | null> {
  if (!gid?.trim()) return null
  const params = new URLSearchParams({ ids: gid })
  const backstop = AbortSignal.timeout(PELIAS_HANG_BACKSTOP_MS)
  const fetchSignal = opts.signal ? AbortSignal.any([opts.signal, backstop]) : backstop
  try {
    const res = await fetch(`${PELIAS_URL}/v1/place?${params}`, { signal: fetchSignal })
    if (!res.ok) return null
    const data = (await res.json()) as { features?: any[] }
    const feature = data.features?.[0]
    if (!feature) return null
    const place = adaptPeliasFeature(feature)

    // Associate with the OSM feature at the same address, if one exists, so the
    // detail view can outline the building perimeter and link to OSM instead of
    // showing a bare geocoder point. We return the OSM object (polygon + tags +
    // osm id), backfilling name/address from the geocoded record when the
    // building itself is unnamed/unaddressed.
    const hn = place.address?.housenumber
    const street = place.address?.street
    const coords = place.geometry?.coordinates
    if (hn && street && Array.isArray(coords)) {
      try {
        const osm = await findOsmByAddress(hn, street, coords[1], coords[0])
        if (osm) {
          // Backfill only the address from the geocoded record when the OSM
          // feature lacks one; the display name is derived from the street
          // address downstream (parchment adaptPlace), so both the address-view
          // and the direct-OSM-view resolve to identical data.
          const emptyAddr = !osm.address || Object.values(osm.address).every((v) => v == null)
          if (emptyAddr) osm.address = place.address
          return osm
        }
      } catch {
        // Association is best-effort — fall back to the bare geocoder point.
      }
    }

    return place
  } catch {
    return null
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
