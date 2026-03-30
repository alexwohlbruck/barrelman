import { db } from '../db'
import { sql } from 'drizzle-orm'
import { spatialCache } from '../lib/cache'

export interface ReverseGeocodeResult {
  address: Record<string, string>
  hierarchy: any[]
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
