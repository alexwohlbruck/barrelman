/**
 * Generate embeddings for all named POIs in geo_places using Ollama nomic-embed-text.
 * Run after import-places.ts. Can be re-run safely (skips already-embedded rows).
 */
import postgres from 'postgres'
import { generateEmbeddings, buildEmbeddingInput } from '../src/lib/embeddings'

const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://barrelman:barrelman@localhost:5434/barrelman'
const sql = postgres(DATABASE_URL)

const BATCH_SIZE = 64

async function embedPlaces() {
  // Count remaining
  const [{ count }] = await sql`
    SELECT count(*) FROM geo_places
    WHERE name IS NOT NULL AND embedding IS NULL
  `
  console.log(`${count} places need embeddings`)

  if (Number(count) === 0) {
    console.log('All places already embedded!')
    return
  }

  let processed = 0
  let errors = 0

  while (true) {
    // Fetch next batch
    const batch = await sql`
      SELECT id, name, categories, address, tags, parent_context
      FROM geo_places
      WHERE name IS NOT NULL AND embedding IS NULL
      ORDER BY id
      LIMIT ${BATCH_SIZE}
    `

    if (batch.length === 0) break

    // Build input texts
    const texts = batch.map((row) =>
      buildEmbeddingInput({
        name: row.name,
        categories: row.categories as string[] | null,
        address: row.address as any,
        osmTags: row.tags as any,
        parentContext: row.parent_context as string | null,
      }),
    )

    try {
      const embeddings = await generateEmbeddings(texts)

      // Update each row
      for (let i = 0; i < batch.length; i++) {
        const embeddingStr = `[${embeddings[i].join(',')}]`
        await sql`
          UPDATE geo_places
          SET embedding = ${embeddingStr}::vector
          WHERE id = ${batch[i].id}
        `
      }

      processed += batch.length
    } catch (e: any) {
      console.error(`  Error embedding batch: ${e.message}`)
      errors += batch.length

      // Mark these as attempted to avoid infinite loop — set embedding to zero vector
      // They can be re-embedded later by setting embedding back to NULL
      for (const row of batch) {
        try {
          const zeroVec = `[${new Array(512).fill(0).join(',')}]`
          await sql`
            UPDATE geo_places SET embedding = ${zeroVec}::vector WHERE id = ${row.id}
          `
        } catch { /* skip */ }
      }
    }

    if (processed % 1000 < BATCH_SIZE) {
      console.log(`  Embedded ${processed} / ${count} places (${errors} errors)...`)
    }
  }

  // Build IVFFlat index
  console.log('\nBuilding IVFFlat vector index...')
  const [{ cnt }] = await sql`SELECT count(*) AS cnt FROM geo_places WHERE embedding IS NOT NULL`
  const lists = Math.min(Math.max(Math.floor(Number(cnt) / 1000), 10), 1000)
  console.log(`  Using ${lists} lists for ${cnt} embedded places`)

  await sql`DROP INDEX IF EXISTS geo_places_embedding_idx`
  await sql.unsafe(`
    CREATE INDEX geo_places_embedding_idx ON geo_places
    USING ivfflat(embedding vector_cosine_ops) WITH (lists = ${lists})
  `)

  console.log(`\nDone! Embedded ${processed} places (${errors} errors)`)
}

async function main() {
  try {
    await embedPlaces()
  } finally {
    await sql.end()
  }
}

main().catch(console.error)
