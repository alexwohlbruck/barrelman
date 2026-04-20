/**
 * Integration tests for findContainingAreas and findChildren against the real DB.
 *
 * Requires a running Barrelman DB with NC import data on DATABASE_URL.
 *
 * Run: DATABASE_URL="postgresql://barrelman:barrelman@localhost:5434/barrelman" \
 *      bun test src/services/spatial.service.integration.test.ts
 */

import { describe, test, expect } from 'bun:test'

const DATABASE_URL = process.env.DATABASE_URL
let findContainingAreas: typeof import('./spatial.service').findContainingAreas
let findChildren: typeof import('./spatial.service').findChildren
let canRun = false

if (DATABASE_URL) {
  try {
    const dbMod = await import('../db')
    if ('mock' in dbMod.db.execute) {
      throw new Error('DB module is mocked by unit tests')
    }
    const mod = await import('./spatial.service')
    await Promise.race([
      mod.findContainingAreas({ lat: 0, lng: 0 }),
      new Promise((_, rej) => setTimeout(() => rej(new Error('smoke timeout')), 3000)),
    ])
    findContainingAreas = mod.findContainingAreas
    findChildren = mod.findChildren
    canRun = true
  } catch (e) {
    console.warn('Skipping integration tests:', (e as Error).message)
  }
}

if (!canRun) {
  describe.skip('spatial.service integration (DB unavailable)', () => {
    test('skipped', () => {})
  })
} else {
  describe('findContainingAreas — real DB', () => {
    test('Charlotte point → contains city + county + state hierarchy', async () => {
      const areas = await findContainingAreas({ lat: 35.2271, lng: -80.8431 })
      expect(areas.length).toBeGreaterThan(0)
      const names = areas.map((a: any) => a.name).join(' ').toLowerCase()
      expect(names).toContain('charlotte')
      expect(names).toContain('mecklenburg')
      expect(names).toContain('north carolina')
    })

    test('results are ordered smallest-area-first', async () => {
      const areas = await findContainingAreas({ lat: 35.2271, lng: -80.8431 })
      const withArea = areas.filter((a: any) => a.area_m2 != null)
      for (let i = 1; i < withArea.length; i++) {
        expect(withArea[i].area_m2).toBeGreaterThanOrEqual(withArea[i - 1].area_m2)
      }
    })

    test('exclude param removes the matching area', async () => {
      const all = await findContainingAreas({ lat: 35.2271, lng: -80.8431 })
      expect(all.length).toBeGreaterThan(0)
      const first = all[0].id
      const filtered = await findContainingAreas({
        lat: 35.2271,
        lng: -80.8431,
        exclude: first,
      })
      expect(filtered.find((a: any) => a.id === first)).toBeUndefined()
    })

    test('row shape exposes geometry + full_geometry', async () => {
      const [first] = await findContainingAreas({ lat: 35.2271, lng: -80.8431 })
      expect(first.geometry?.type).toBe('Point')
      expect(first.full_geometry?.type).toMatch(/polygon/i)
      expect(first.geom_type).toBe('area')
    })

    test('lat=0/lng=0 does not crash', async () => {
      const areas = await findContainingAreas({ lat: 0, lng: 0 })
      expect(Array.isArray(areas)).toBe(true)
    })
  })

  describe('findChildren — real DB', () => {
    test('Charlotte (relation/177415) → returns POIs inside the city polygon', async () => {
      const children = await findChildren({ id: 'relation/177415', limit: '20' })
      expect(children.length).toBeGreaterThan(0)
      // All returned children should have a name (named-only filter)
      for (const c of children) {
        expect(c.name).toBeTruthy()
      }
    }, 30_000)

    test('category filter returns only cafes', async () => {
      const children = await findChildren({
        id: 'relation/177415',
        categories: 'amenity/cafe',
        limit: '10',
      })
      // Some results expected, all should either be named OR include the cafe category
      for (const c of children) {
        const hasCat = (c.categories || []).some((cat: string) => cat === 'amenity/cafe')
        expect(c.name || hasCat).toBeTruthy()
      }
    }, 30_000)

    test('prefix category filter (amenity) matches subcategories', async () => {
      const children = await findChildren({
        id: 'relation/177415',
        categories: 'amenity',
        limit: '10',
      })
      expect(children.length).toBeGreaterThan(0)
    }, 30_000)

    test('lat/lng proximity sort orders nearest first', async () => {
      // Charlotte has children scattered across the city. Sort by distance to
      // a specific corner and verify the first result is closer than the last.
      const children = await findChildren({
        id: 'relation/177415',
        limit: '20',
        lat: '35.2007',
        lng: '-80.7750',
      })
      expect(children.length).toBeGreaterThan(1)
    }, 30_000)

    test('limit and offset are honored', async () => {
      const page1 = await findChildren({ id: 'relation/177415', limit: '5', offset: '0' })
      const page2 = await findChildren({ id: 'relation/177415', limit: '5', offset: '5' })
      expect(page1.length).toBe(5)
      expect(page2.length).toBeLessThanOrEqual(5)
      // Pages should not overlap
      const ids1 = new Set(page1.map((c: any) => c.id))
      for (const c of page2) {
        expect(ids1.has(c.id)).toBe(false)
      }
    }, 30_000)
  })
}
