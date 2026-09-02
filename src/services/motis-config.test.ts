/**
 * Tests for the MOTIS config generator's GBFS scoping.
 *
 * `gbfs_systems` is a worldwide catalog and MOTIS polls every feed it is
 * handed, so handing it the catalog makes a single-metro instance pay for a
 * thousand feeds it can never answer a question about — and report itself
 * unhealthy forever, since its health endpoint is an AND over all of them.
 * What is pinned here is which systems the query is allowed to return.
 */
import { describe, test, expect, mock, beforeEach } from 'bun:test'

/** SQL the generator issued, in order. */
let statements: any[] = []

/**
 * Replace only `db`, keeping every other export.
 *
 * Bun's module registry is shared across the whole test process, so a
 * `mock.module` that returns a bare object deletes the module's other exports
 * for every file that loads afterwards — `connection`, which `region-store`
 * imports, among them. That surfaces as `SyntaxError: Export named 'connection'
 * not found` in unrelated suites, and only when the file order puts this one
 * first, which is why it passed locally and took the 0.2.7 release build down.
 */
const actualDb = await import('../db')

mock.module('../db', () => ({
  ...actualDb,
  db: {
    execute: async (query: any) => {
      statements.push(query)
      return []
    },
  },
}))

const actualRegions = await import('../config/regions')
let resolved: any

mock.module('../config/regions', () => ({
  ...actualRegions,
  resolveRegions: async () => resolved,
}))

const { generateMotisConfig } = await import('./gtfs.service')

/**
 * Split a drizzle statement into its literal SQL and its bound values.
 *
 * The scope clause is built as a nested `sql` fragment and joined, so the
 * chunks arrive as a tree: a StringChunk holds an array of literal SQL, a
 * nested statement holds its own `queryChunks`, and anything else is a value
 * on its way to a parameter.
 */
function split(node: any, out = { text: [] as string[], values: [] as any[] }) {
  if (node == null) return out
  if (Array.isArray(node.value)) out.text.push(...node.value)
  else if (Array.isArray(node.queryChunks)) for (const c of node.queryChunks) split(c, out)
  else out.values.push(node)
  return out
}

const bound = (stmt: any) => split(stmt).values
const sqlText = (stmt: any) => split(stmt).text.join(' ')

const gbfsQuery = () => statements.find((s) => sqlText(s).includes('gbfs_systems'))

describe('GBFS feeds in the MOTIS config', () => {
  beforeEach(() => {
    statements = []
  })

  test('is scoped to each imported region, not their union', async () => {
    // The union of two distant regions is mostly the gap between them, so the
    // boxes go in one at a time.
    resolved = {
      isGlobal: false,
      regions: [
        { bbox: [-84.33, 33.83, -75.4, 36.59] },
        { bbox: [-74.6, 40.3, -73.3, 41.4] },
      ],
    }

    await generateMotisConfig({ includeGbfs: true })

    const query = gbfsQuery()
    expect(sqlText(query)).toContain('BETWEEN')
    // Every edge of both boxes is bound, and neither is widened into the other.
    expect(bound(query)).toEqual([-84.33, -75.4, 33.83, 36.59, -74.6, -73.3, 40.3, 41.4])
  })

  test('asks for every system on a global instance', async () => {
    resolved = { isGlobal: true, regions: [{ bbox: [-180, -90, 180, 90] }] }

    await generateMotisConfig({ includeGbfs: true })

    expect(sqlText(gbfsQuery())).not.toContain('BETWEEN')
  })

  test('asks for every system when no region declares a usable box', async () => {
    // Serving no shared mobility at all is the worse failure of the two.
    resolved = { isGlobal: false, regions: [{ bbox: undefined }, {}] }

    await generateMotisConfig({ includeGbfs: true })

    expect(sqlText(gbfsQuery())).not.toContain('BETWEEN')
  })

  test('asks for nothing at all when GBFS is off', async () => {
    resolved = { isGlobal: false, regions: [{ bbox: [-74.6, 40.3, -73.3, 41.4] }] }

    await generateMotisConfig({ includeGbfs: false })

    expect(gbfsQuery()).toBeUndefined()
  })
})
