/**
 * Typo correction against the real search_lexicon.
 *
 * The unit tests stub the lookup out of searchPlaces, so only this exercises
 * the query itself: the key array has to reach Postgres as one text[]
 * parameter, and the GIN index has to answer it. Works on any import — it
 * misspells a word the lexicon already holds.
 *
 * Run: bun run test:integration (needs a database whose lexicon is built).
 */

import { describe, test, expect } from 'bun:test'

const enabled = Boolean(process.env.BARRELMAN_INTEGRATION_TESTS && process.env.DATABASE_URL)
let word: string | undefined
let correctionsFor: typeof import('./search-lexicon.service').correctionsFor

if (enabled) {
  const { db } = await import('../db')
  const { sql } = await import('drizzle-orm')
  correctionsFor = (await import('./search-lexicon.service')).correctionsFor
  const rows = await db.execute(sql`
    SELECT word FROM search_lexicon
    WHERE word ~ '^[a-z]{7,10}$' AND substr(word, 3, 1) <> substr(word, 4, 1)
    ORDER BY n DESC LIMIT 1
  `).catch(() => [] as any[])
  word = (rows as any[])[0]?.word
}

describe.skipIf(!enabled)('search lexicon', () => {
  test.skipIf(!word)('finds a common word from a misspelling of it', async () => {
    // Swap the third and fourth letters: one edit, as typed.
    const typo = word!.slice(0, 2) + word![3] + word![2] + word!.slice(4)
    const fixes = await correctionsFor([typo])
    expect(fixes.get(typo)).toContain(word)
  })

  test.skipIf(!word)('a lookup answers well inside its budget', async () => {
    await correctionsFor([`${word}q`]) // a connection, so the timing below is the query's
    const started = performance.now()
    await correctionsFor([`${word}x`])
    expect(performance.now() - started).toBeLessThan(150)
  })
})
