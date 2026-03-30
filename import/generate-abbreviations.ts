/**
 * Generate abbreviations for all named places in geo_places.
 * Updates the name_abbrev column.
 */
import postgres from 'postgres'
import { generateAbbrev } from '../src/lib/abbreviations'

const DATABASE_URL = process.env.DATABASE_URL
if (!DATABASE_URL) {
  console.error('DATABASE_URL is required')
  process.exit(1)
}

const sql = postgres(DATABASE_URL)

const BATCH_SIZE = 1000

async function main() {
  console.log('Generating abbreviations for named places...')

  let offset = 0
  let totalUpdated = 0

  while (true) {
    const rows = await sql`
      SELECT id, name
      FROM geo_places
      WHERE name IS NOT NULL AND name_abbrev IS NULL
      ORDER BY id
      LIMIT ${BATCH_SIZE}
      OFFSET ${offset}
    `

    if (rows.length === 0) break

    const updates: { id: string; abbrev: string }[] = []

    for (const row of rows) {
      const abbrev = generateAbbrev(row.name)
      if (abbrev) {
        updates.push({ id: row.id, abbrev })
      }
    }

    if (updates.length > 0) {
      // Batch update using a VALUES list
      const values = updates.map((u) => sql`(${u.id}, ${u.abbrev})`)
      await sql`
        UPDATE geo_places p
        SET name_abbrev = v.abbrev
        FROM (VALUES ${sql.unsafe(values.map((_, i) => `($${i * 2 + 1}, $${i * 2 + 2})`).join(','))}) AS v(id, abbrev)
        WHERE p.id = v.id
      `.catch(async () => {
        // Fallback: update one by one if batch fails
        for (const u of updates) {
          await sql`UPDATE geo_places SET name_abbrev = ${u.abbrev} WHERE id = ${u.id}`
        }
      })
      totalUpdated += updates.length
    }

    offset += BATCH_SIZE
    if (offset % 10000 === 0) {
      console.log(`  Processed ${offset} rows, ${totalUpdated} abbreviations generated`)
    }
  }

  console.log(`Done. Generated ${totalUpdated} abbreviations.`)
  await sql.end()
}

main().catch((err) => {
  console.error('Error:', err)
  process.exit(1)
})
