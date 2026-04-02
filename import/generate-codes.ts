/**
 * Populate the `codes` column for all geo_places from OSM tags.
 * Extracts IATA, ICAO, ref, short_name, abbreviation, and alt_name codes.
 *
 * Uses a single SQL UPDATE for speed (~70K rows in seconds).
 */
import postgres from 'postgres'

const DATABASE_URL = process.env.DATABASE_URL
if (!DATABASE_URL) {
  console.error('DATABASE_URL is required')
  process.exit(1)
}

const sql = postgres(DATABASE_URL)

async function main() {
  console.log('Populating codes from OSM tags...')

  const [result] = await sql`
    WITH updated AS (
      UPDATE geo_places
      SET codes = sub.codes
      FROM (
        SELECT id,
          array_agg(DISTINCT lower(trim(code))) FILTER (WHERE trim(code) <> '') AS codes
        FROM geo_places,
        LATERAL unnest(
          string_to_array(coalesce(tags->>'iata', ''), ';') ||
          string_to_array(coalesce(tags->>'icao', ''), ';') ||
          string_to_array(coalesce(tags->>'ref', ''), ';') ||
          string_to_array(coalesce(tags->>'short_name', ''), ';') ||
          string_to_array(coalesce(tags->>'abbreviation', ''), ';') ||
          string_to_array(coalesce(tags->>'alt_name', ''), ';')
        ) AS code
        WHERE tags IS NOT NULL
          AND (
            tags->>'iata' IS NOT NULL OR
            tags->>'icao' IS NOT NULL OR
            tags->>'ref' IS NOT NULL OR
            tags->>'short_name' IS NOT NULL OR
            tags->>'abbreviation' IS NOT NULL OR
            tags->>'alt_name' IS NOT NULL
          )
        GROUP BY id
      ) sub
      WHERE geo_places.id = sub.id
        AND (geo_places.codes IS NULL OR geo_places.codes <> sub.codes)
      RETURNING 1
    )
    SELECT count(*) AS updated_count FROM updated
  `

  console.log(`Done. Updated codes for ${result.updated_count} places.`)
  await sql.end()
}

main().catch((err) => {
  console.error('Error:', err)
  process.exit(1)
})
