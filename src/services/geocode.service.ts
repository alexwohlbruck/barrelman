import { db } from '../db'
import { sql } from 'drizzle-orm'
import { spatialCache } from '../lib/cache'
import { findOsmByAddress, getPlace } from './place.service'

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

/** True when a record carries at least one populated address component. */
function hasAddress(place: any): boolean {
  return Boolean(place.address) && Object.values(place.address).some((v) => v != null)
}

/**
 * Associate an adapted Pelias record with the OSM feature at the same street
 * address, when one exists. Geocoder points carry no geometry, tags, or
 * categories; the OSM row does — so the detail view can outline the building
 * perimeter and link to OSM instead of showing a bare point. Best-effort:
 * returns the original record when there's no confident match.
 */
async function associateOsmByAddress(place: any): Promise<any> {
  const hn = place.address?.housenumber
  const street = place.address?.street
  const coords = place.geometry?.coordinates
  if (!hn || !street || !Array.isArray(coords)) return place

  try {
    const osm = await findOsmByAddress(hn, street, coords[1], coords[0])
    if (!osm) return place
    // Backfill only the address from the geocoded record when the OSM feature
    // lacks one; the display name is derived from the street address downstream
    // (parchment adaptPlace), so both the address-view and the direct-OSM-view
    // resolve to identical data.
    if (!hasAddress(osm)) osm.address = place.address
    return osm
  } catch {
    return place // Association is best-effort — fall back to the bare point.
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
    return associateOsmByAddress(adaptPeliasFeature(feature))
  } catch {
    return null
  }
}

// ── Reverse geocoding (coordinate → places) ──────────────────────────────────

/** Pelias layers worth returning for a dropped pin: what's *at* the point. */
const REVERSE_LAYERS = 'venue,address,street'

/** How far from the click a result may be before it stops describing the point. */
const REVERSE_RADIUS_M = 100

/**
 * Reverse-geocode a coordinate to the addressable features at that point,
 * returning geo_places-shaped rows (the same shape `/search` emits).
 *
 * Two tiers, narrowest first:
 *  1. Pelias reverse within `radiusM` — venues, addresses, and streets from the
 *     OSM + OpenAddresses index. Each hit is hydrated into its real geo_places
 *     row where possible, which upgrades a bare geocoder point into the full
 *     OSM feature (polygon, tags, categories, opening hours).
 *  2. Admin fallback — for a point with nothing addressable near it (a lake, a
 *     field), return the smallest administrative area containing it so callers
 *     still get a meaningful label instead of nothing. Widening the Pelias
 *     radius instead would surface an unrelated address hundreds of metres away.
 *
 * Resilient: a Pelias failure degrades to the admin fallback rather than erroring.
 */
export async function reverseGeocodePlaces(
  lat: number,
  lng: number,
  opts: { limit?: number; radiusM?: number; signal?: AbortSignal } = {},
): Promise<any[]> {
  const { limit = 5, radiusM = REVERSE_RADIUS_M, signal } = opts

  const params = new URLSearchParams({
    'point.lat': String(lat),
    'point.lon': String(lng),
    size: String(limit),
    'boundary.circle.radius': String(radiusM / 1000), // Pelias takes kilometres
    layers: REVERSE_LAYERS,
  })

  const backstop = AbortSignal.timeout(PELIAS_HANG_BACKSTOP_MS)
  const fetchSignal = signal ? AbortSignal.any([signal, backstop]) : backstop

  let features: any[] = []
  try {
    const res = await fetch(`${PELIAS_URL}/v1/reverse?${params}`, { signal: fetchSignal })
    if (res.ok) {
      const data = (await res.json()) as { features?: any[] }
      features = data.features ?? []
    }
  } catch {
    // Pelias unavailable/aborted — fall through to the admin tier.
  }

  // Pelias emits one feature per layer for the same OSM element (an `address`
  // and a `venue` row both pointing at way/123). Keep the first — they are
  // returned best-match first — so the caller sees one result per real feature.
  const seen = new Set<string>()
  const unique = features.map(adaptPeliasFeature).filter((p) => {
    if (seen.has(p.id)) return false
    seen.add(p.id)
    return true
  })

  if (unique.length > 0) {
    const hydrated = await Promise.all(unique.map(hydrateReverseResult))

    // The nearest feature to a dropped pin is often an unaddressed node — a
    // statue, a bench, a tree — while the same response also carries the street
    // address of that spot from the address layer. Lend it to the results that
    // have none, so a pin always resolves to a label *and* an address.
    const donor = unique.find((p) => p.address?.housenumber && p.address?.street)
    if (donor) {
      for (const place of hydrated) {
        if (!hasAddress(place)) place.address = donor.address
      }
    }

    return hydrated
  }

  const { hierarchy } = await reverseGeocode(lat, lng)
  const smallest = hierarchy[0]
  if (!smallest) return []
  const [osmType, osmId] = String(smallest.id).split('/')
  const row = await getPlace(osmType, osmId)
  return row ? [row] : []
}

/**
 * Upgrade a single adapted Pelias reverse hit into its real geo_places row.
 * OSM-sourced hits resolve by id; OpenAddresses hits fall back to matching the
 * street address against a nearby building. Keeps the geocoder record when
 * neither resolves (e.g. the Pelias index is ahead of the OSM import).
 */
async function hydrateReverseResult(place: any): Promise<any> {
  const [osmType, osmId] = String(place.id).split('/')
  if (['node', 'way', 'relation'].includes(osmType)) {
    return (await getPlace(osmType, osmId)) ?? place
  }
  return associateOsmByAddress(place)
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
