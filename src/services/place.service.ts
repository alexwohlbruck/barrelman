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
