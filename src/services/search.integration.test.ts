/**
 * Integration tests for searchPlaces against the real database.
 *
 * These validate end-to-end search ranking behavior for edge cases
 * discovered during development. They require a running Barrelman DB
 * with NC import data on DATABASE_URL (default: localhost:5434).
 *
 * Run: DATABASE_URL="postgresql://barrelman:barrelman@localhost:5434/barrelman" bun test src/services/search.integration.test.ts
 */

import { describe, test, expect } from 'bun:test'

// ── Guard: only run when a real database is reachable ──────────────────────
// When `bun test` runs all files, the unit test file uses mock.module('../db')
// which poisons the module cache — searchPlaces silently returns empty arrays.
// We detect the mock by checking if db.execute has mock metadata (bun attaches
// a .mock property to mock functions), then verify the DB is actually reachable.
const DATABASE_URL = process.env.DATABASE_URL
let searchPlaces: typeof import('./search.service').searchPlaces
let canRun = false

if (DATABASE_URL) {
  try {
    // Check if the db module has been replaced by a mock
    const dbMod = await import('../db')
    if ('mock' in dbMod.db.execute) {
      throw new Error('DB module is mocked by unit tests')
    }

    const mod = await import('./search.service')
    // Quick smoke test — will throw if DB is unreachable
    await mod.searchPlaces({ query: '__health__', autocomplete: true, limit: 1 })
    searchPlaces = mod.searchPlaces
    canRun = true
  } catch {
    // DB not reachable or module is mocked — skip
  }
}

if (!canRun) {
  describe.skip('searchPlaces integration (DB unavailable)', () => {
    test('skipped', () => {})
  })
} else {

  // Charlotte center coordinates
  const CLT_LAT = 35.22
  const CLT_LNG = -80.84

  // Helper: search with Charlotte location defaults
  async function search(query: string, overrides: Record<string, any> = {}) {
    return searchPlaces({
      query,
      lat: CLT_LAT,
      lng: CLT_LNG,
      autocomplete: true,
      limit: 20,
      ...overrides,
    })
  }

  // Helper: get result names
  function names(results: any[]): string[] {
    return results.map((r: any) => r.name)
  }

  // Helper: find first result matching a name pattern
  function findResult(results: any[], pattern: RegExp): any {
    return results.find((r: any) => pattern.test(r.name))
  }

  // Helper: get the index of a result matching a pattern
  function indexOf(results: any[], pattern: RegExp): number {
    return results.findIndex((r: any) => pattern.test(r.name))
  }

  // ── Exact name match prioritization ──────────────────────────────────────

  describe('exact name match beats partial mention', () => {
    test('"carowinds" → Carowinds theme park ranks above Days Inn Near Carowinds', async () => {
      const results = await search('carowinds')
      const themepark = indexOf(results, /^Carowinds$/i)
      const daysInn = indexOf(results, /days inn.*carowinds/i)
      expect(themepark).toBeGreaterThanOrEqual(0)
      expect(themepark).toBeLessThan(daysInn)
    })

    test('"starbucks" → exact Starbucks matches rank first', async () => {
      const results = await search('starbucks')
      // First 3 results should all be "Starbucks"
      expect(results.slice(0, 3).every((r: any) => r.name === 'Starbucks')).toBe(true)
    })
  })

  // ── Airport code search (IATA/ICAO via codes column) ─────────────────────

  describe('airport code search', () => {
    test('"CLT" → Charlotte-Douglas International Airport is #1', async () => {
      const results = await search('CLT')
      expect(results[0].name).toMatch(/charlotte.*douglas/i)
    })

    test('"avl" → Asheville Regional Airport surfaces at top', async () => {
      const results = await search('avl')
      const airport = findResult(results, /asheville.*airport/i)
      expect(airport).toBeDefined()
      expect(indexOf(results, /asheville.*airport/i)).toBe(0)
    })

    test('"RDU" → RDU airport results surface', async () => {
      const results = await search('RDU', { lat: 35.87, lng: -78.78 })
      const airport = findResult(results, /rdu/i)
      expect(airport).toBeDefined()
    })
  })

  // ── Acronym search (name_abbrev column) ──────────────────────────────────

  describe('acronym search', () => {
    test('"uncc" → University of North Carolina at Charlotte appears in results', async () => {
      const results = await search('uncc')
      const university = findResult(results, /university.*north carolina.*charlotte/i)
      expect(university).toBeDefined()
      // Should appear in top 10
      expect(indexOf(results, /university.*north carolina.*charlotte/i)).toBeLessThan(10)
    })

    test('"mphs" → finds Myers Park High School', async () => {
      const results = await search('mphs')
      const mphs = findResult(results, /myers park high/i)
      expect(mphs).toBeDefined()
    })

    test('"mphs" → finds Mount Pleasant High School', async () => {
      const results = await search('mphs')
      const mountPleasant = findResult(results, /mount pleasant high/i)
      expect(mountPleasant).toBeDefined()
    })
  })

  // ── Global search (no hard radius) ───────────────────────────────────────

  describe('global search — no hard radius boundary', () => {
    test('"charlotte douglas" from China still finds the airport', async () => {
      const results = await search('charlotte douglas', { lat: 39.9, lng: 116.4 })
      const airport = findResult(results, /charlotte.*douglas.*airport/i)
      expect(airport).toBeDefined()
    })

    test('"asheville" from Charlotte still finds Asheville results', async () => {
      const results = await search('asheville')
      const asheville = findResult(results, /asheville/i)
      expect(asheville).toBeDefined()
    })
  })

  // ── Local bias for generic queries ───────────────────────────────────────

  describe('generic queries prefer nearby results', () => {
    test('"restaurant" → top 5 results are within 5 km', async () => {
      const results = await search('restaurant')
      const top5 = results.slice(0, 5)
      for (const r of top5) {
        expect(r.distance_m).toBeLessThan(5000)
      }
    })

    test('"starbucks" → top 5 results are within 5 km', async () => {
      const results = await search('starbucks')
      const top5 = results.slice(0, 5)
      for (const r of top5) {
        expect(r.distance_m).toBeLessThan(5000)
      }
    })
  })

  // ── Category-enriched search (tsvector includes category labels) ─────────

  describe('category-enriched search', () => {
    test('"winnifred apartments" → finds The Winnifred apartment building', async () => {
      const results = await search('winnifred apartments')
      const winnifred = findResult(results, /winnifred/i)
      expect(winnifred).toBeDefined()
    })

    test('"carowinds theme park" → finds Carowinds', async () => {
      const results = await search('carowinds theme park')
      const carowinds = findResult(results, /^carowinds$/i)
      expect(carowinds).toBeDefined()
    })

    test('"starbucks cafe" → returns Starbucks results', async () => {
      const results = await search('starbucks cafe')
      expect(results[0].name).toBe('Starbucks')
    })
  })

  // ── Location-qualified search (name + street/city/neighbourhood) ─────────

  describe('location-qualified search', () => {
    test('"bojangles tryon" → Bojangles on Tryon Street ranks first', async () => {
      const results = await search('bojangles tryon', { lat: 35.2007, lng: -80.775 })
      expect(results[0].name).toMatch(/bojangles/i)
      // Should be a Tryon Street location, not just the nearest Bojangles
      expect(results[0].address?.street).toMatch(/tryon/i)
    })

    test('"walmart independence" → Walmart on Independence Blvd ranks first', async () => {
      const results = await search('walmart independence', { lat: 35.2007, lng: -80.775 })
      expect(results[0].name).toMatch(/walmart/i)
    })

    test('"starbucks uptown" → Starbucks in Uptown neighbourhood ranks first', async () => {
      const results = await search('starbucks uptown')
      expect(results[0].name).toBe('Starbucks')
    })

    test('"sabor huntersville" → Sabor in Huntersville ranks first', async () => {
      const results = await search('sabor huntersville')
      expect(results[0].name).toMatch(/sabor/i)
    })
  })

  // ── Category demotion (roads/surveillance deprioritized) ─────────────────

  describe('category demotion', () => {
    test('"avl" → streets (highway/*) rank below non-highway POIs', async () => {
      const results = await search('avl')
      // Airport (aeroway) should come before any street (highway)
      const airportIdx = indexOf(results, /asheville.*airport/i)
      const firstStreet = results.findIndex(
        (r: any) => r.categories?.some((c: string) => c.startsWith('highway/')),
      )
      if (airportIdx >= 0 && firstStreet >= 0) {
        expect(airportIdx).toBeLessThan(firstStreet)
      }
    })

    test('"carowinds" → webcams (man_made/surveillance) rank below named POIs', async () => {
      const results = await search('carowinds')
      const themepark = indexOf(results, /^carowinds$/i)
      const webcam = results.findIndex(
        (r: any) => r.categories?.some((c: string) => c.includes('surveillance')),
      )
      if (themepark >= 0 && webcam >= 0) {
        expect(themepark).toBeLessThan(webcam)
      }
    })
  })

  // ── Codes vs abbreviation priority ───────────────────────────────────────

  describe('codes outrank auto-generated abbreviations', () => {
    test('"avl" → IATA code match (Asheville airport) beats name_abbrev matches (Alta Vista Lane)', async () => {
      const results = await search('avl')
      // Asheville Regional Airport has codes=['avl'], should be first
      // Streets like "Alta Vista Lane" have name_abbrev='avl' but should be below
      const airport = indexOf(results, /asheville.*airport/i)
      expect(airport).toBe(0)
    })

    test('"clt" → IATA code match beats trigram matches (County Line Trail, etc.)', async () => {
      const results = await search('CLT')
      expect(results[0].name).toMatch(/charlotte.*douglas/i)
    })
  })

  // ── Browse mode (no query — spatial only) ────────────────────────────────

  describe('browse mode — no query', () => {
    test('category browse returns results within specified radius', async () => {
      const results = await searchPlaces({
        lat: CLT_LAT,
        lng: CLT_LNG,
        radius: 5000,
        categories: ['amenity/cafe'],
        limit: 5,
      })
      expect(results.length).toBeGreaterThan(0)
      for (const r of results) {
        expect(r.distance_m).toBeLessThan(5000)
      }
    })
  })

  // ── Performance sanity check ─────────────────────────────────────────────

  describe('performance', () => {
    test('autocomplete queries complete within 1 second', async () => {
      const queries = ['restaurant', 'starbucks', 'CLT', 'uncc', 'carowinds']
      for (const q of queries) {
        const start = performance.now()
        await search(q)
        const elapsed = performance.now() - start
        expect(elapsed).toBeLessThan(1000)
      }
    })
  })
}
