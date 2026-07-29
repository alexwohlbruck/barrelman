import { db } from '../db'
import { sql } from 'drizzle-orm'

export async function getPlace(osmType: string, osmId: string): Promise<any | null> {
  const id = `${osmType}/${osmId}`

  const results = await db.execute(sql`
    SELECT
      id, osm_type, osm_id, name, name_abbrev, names,
      categories, tags, address, hours,
      phones, websites, geom_type, admin_level, area_m2,
      ST_AsGeoJSON(centroid)::jsonb AS geometry,
      CASE WHEN geom_type != 'point'
        THEN ST_AsGeoJSON(geom)::jsonb
        ELSE NULL
      END AS full_geometry
    FROM geo_places
    WHERE id = ${id}
    LIMIT 1
  `)

  const rows = Array.from(results as any[])
  return rows.length > 0 ? rows[0] : null
}

/**
 * Find the OSM feature at a given street address near a point — used to
 * associate a Pelias address point with the real OSM building/parcel so the
 * detail view can outline its perimeter. Matches on exact housenumber + street
 * (case-insensitive) within 40m, preferring a polygon that actually contains
 * the point, then the nearest. Returns the same row shape as getPlace (with
 * full_geometry) or null when there's no confident match.
 */
export async function findOsmByAddress(
  housenumber: string,
  street: string,
  lat: number,
  lng: number,
): Promise<any | null> {
  const pt = sql`ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)`

  const results = await db.execute(sql`
    SELECT
      id, osm_type, osm_id, name, name_abbrev, names,
      categories, tags, address, hours,
      phones, websites, geom_type, admin_level, area_m2,
      ST_AsGeoJSON(centroid)::jsonb AS geometry,
      CASE WHEN geom_type != 'point'
        THEN ST_AsGeoJSON(geom)::jsonb
        ELSE NULL
      END AS full_geometry
    FROM geo_places
    WHERE tags->>'addr:housenumber' = ${housenumber}
      AND lower(tags->>'addr:street') = lower(${street})
      AND centroid && ST_Expand(${pt}, 0.0006)
      AND ST_DWithin(centroid::geography, ${pt}::geography, 40)
    ORDER BY
      (geom_type = 'area') DESC,
      ST_Contains(geom, ${pt}) DESC,
      ST_Distance(centroid::geography, ${pt}::geography) ASC
    LIMIT 1
  `)

  const rows = Array.from(results as any[])
  return rows.length > 0 ? rows[0] : null
}
