/**
 * Integration tests for reverseGeocode against the real database.
 *
 * Requires a running Barrelman DB with NC import data on DATABASE_URL.
 *
 * Run: DATABASE_URL="postgresql://barrelman:barrelman@localhost:5434/barrelman" \
 *      bun test src/services/geocode.service.integration.test.ts
 */

import { describe, test, expect } from 'bun:test'

const DATABASE_URL = process.env.DATABASE_URL
let reverseGeocode: typeof import('./geocode.service').reverseGeocode
let canRun = false

if (DATABASE_URL) {
  try {
    const dbMod = await import('../db')
    if ('mock' in dbMod.db.execute) {
      throw new Error('DB module is mocked by unit tests')
    }
    const mod = await import('./geocode.service')
    await Promise.race([
      mod.reverseGeocode(0, 0),
      new Promise((_, rej) => setTimeout(() => rej(new Error('smoke timeout')), 3000)),
    ])
    reverseGeocode = mod.reverseGeocode
    canRun = true
  } catch (e) {
    console.warn('Skipping integration tests:', (e as Error).message)
  }
}

if (!canRun) {
  describe.skip('reverseGeocode integration (DB unavailable)', () => {
    test('skipped', () => {})
  })
} else {
  describe('reverseGeocode — real DB', () => {
    test('Charlotte (35.2271, -80.8431) → city=Charlotte, state=North Carolina', async () => {
      const { address, hierarchy } = await reverseGeocode(35.2271, -80.8431)
      expect(address.city).toMatch(/charlotte/i)
      expect(address.state).toMatch(/north carolina/i)
      // country may be undefined if the NC import doesn't include admin_level=2 boundary
      if (address.country) {
        expect(address.country).toMatch(/united states|usa|us/i)
      }
      // Hierarchy should be ordered most-specific to least-specific
      expect(hierarchy.length).toBeGreaterThan(0)
      const levels = hierarchy.map((h: any) => Number(h.admin_level))
      const sorted = [...levels].sort((a, b) => b - a)
      expect(levels).toEqual(sorted)
    })

    test('Asheville (35.5951, -82.5515) → city=Asheville', async () => {
      const { address } = await reverseGeocode(35.5951, -82.5515)
      expect(address.city).toMatch(/asheville/i)
      expect(address.state).toMatch(/north carolina/i)
    })

    test('point in Atlantic ocean (35.0, -74.0) → no city', async () => {
      const { address } = await reverseGeocode(35.0, -74.0)
      expect(address.city).toBeUndefined()
    })

    test('lat=0 regression: zero coordinates do not crash', async () => {
      // Regression test for the lat=0 / lng=0 falsy bug
      const { address, hierarchy } = await reverseGeocode(0, 0)
      // No crash. Probably nothing in NC import covers (0,0).
      expect(typeof address).toBe('object')
      expect(Array.isArray(hierarchy)).toBe(true)
    })

    test('cache hit returns identical reference on second call', async () => {
      const a = await reverseGeocode(35.2271, -80.8431)
      const b = await reverseGeocode(35.2271, -80.8431)
      expect(a).toBe(b) // same reference (in-memory cache)
    })
  })
}
