import { db } from '../db'
import { sql } from 'drizzle-orm'
import { spatialCache } from '../lib/cache'

export interface ContainingAreasParams {
  lat: number
  lng: number
  exclude?: string
}

export async function findContainingAreas({
  lat,
  lng,
  exclude,
}: ContainingAreasParams): Promise<any[]> {
  const cacheKey = `contains:${lat}:${lng}:${exclude || ''}`
  const cached = spatialCache.get(cacheKey)
  if (cached) return cached

  const excludeFilter = exclude ? sql`AND id != ${exclude}` : sql``

  const results = await db.execute(sql`
    SELECT
      id, osm_type, osm_id, name, categories, tags,
      address, admin_level, area_m2, geom_type,
      ST_AsGeoJSON(centroid)::jsonb AS geometry,
      ST_AsGeoJSON(geom)::jsonb AS full_geometry
    FROM geo_places
    WHERE geom_type = 'area'
    AND name IS NOT NULL
    AND tags->>'building:part' IS NULL
    ${excludeFilter}
    AND ST_Contains(
      geom,
      ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)
    )
    ORDER BY area_m2 ASC NULLS LAST
  `)

  const rows = Array.from(results as any[])
  spatialCache.set(cacheKey, rows)
  return rows
}

export interface ChildrenParams {
  id: string
  categories?: string
  limit?: string
  offset?: string
  lat?: string
  lng?: string
}

export async function findChildren({
  id,
  categories,
  limit = '20',
  offset = '0',
  lat,
  lng,
}: ChildrenParams): Promise<any[]> {
  const categoryList = categories ? categories.split(',').filter(Boolean) : []
  const cacheKey = `children:${id}:${categoryList.join(',')}:${limit}:${offset}:${lat}:${lng}`
  const cached = spatialCache.get(cacheKey)
  if (cached) return cached

  // Named places are always included; category filter only applies to unnamed places.
  // Categories without a '/' are treated as prefixes (e.g. "office" matches "office/lawyer").
  const exactCats = categoryList.filter(c => c.includes('/'))
  const prefixCats = categoryList.filter(c => !c.includes('/'))

  let categoryFilter: ReturnType<typeof sql>
  if (categoryList.length > 0) {
    const conditions: ReturnType<typeof sql>[] = [sql`c.name IS NOT NULL`]
    if (exactCats.length > 0) {
      conditions.push(
        sql`c.categories && ARRAY[${sql.join(exactCats.map(cat => sql`${cat}`), sql`, `)}]::text[]`
      )
    }
    for (const prefix of prefixCats) {
      conditions.push(sql`EXISTS (SELECT 1 FROM unnest(c.categories) AS cat WHERE cat LIKE ${prefix + '/%'} OR cat = ${prefix})`)
    }
    categoryFilter = sql`AND (${sql.join(conditions, sql` OR `)})`
  } else {
    categoryFilter = sql`AND c.name IS NOT NULL`
  }

  // Proximity sort: use provided lat/lng if available, otherwise fall back to parent centroid
  const hasLocation = lat != null && lng != null
  const distanceExpr = hasLocation
    ? sql`ST_Distance(c.centroid, ST_SetSRID(ST_MakePoint(${Number(lng)}, ${Number(lat)}), 4326))`
    : sql`ST_Distance(c.centroid, parent.centroid)`

  const results = await db.execute(sql`
    SELECT
      c.id, c.osm_type, c.osm_id, c.name, c.categories, c.tags,
      c.address, c.hours, c.phones, c.websites, c.geom_type,
      ST_AsGeoJSON(c.centroid)::jsonb AS geometry
    FROM geo_places c, geo_places parent
    WHERE parent.id = ${id}
    AND parent.geom_type = 'area'
    AND ST_Within(c.centroid, parent.geom)
    AND c.id != ${id}
    AND c.tags->>'building:part' IS NULL
    ${categoryFilter}
    ORDER BY
      CASE WHEN c.name IS NOT NULL THEN 0 ELSE 1 END ASC,
      CASE WHEN cardinality(c.categories) > 0 THEN 0 ELSE 1 END ASC,
      ${distanceExpr} ASC,
      c.name ASC NULLS LAST
    LIMIT ${Number(limit)}
    OFFSET ${Number(offset)}
  `)

  const rows = Array.from(results as any[])
  spatialCache.set(cacheKey, rows)
  return rows
}
