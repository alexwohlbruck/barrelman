/**
 * Integration tests for getPlace against the real database.
 *
 * Requires a running Barrelman DB with NC import data on DATABASE_URL
 * (default: localhost:5434).
 *
 * Run: DATABASE_URL="postgresql://barrelman:barrelman@localhost:5434/barrelman" \
 *      bun test src/services/place.service.integration.test.ts
 */

import { describe, test, expect } from 'bun:test'

// ── Guard: skip when DB is unreachable or mocked by sibling unit tests ─────
const DATABASE_URL = process.env.DATABASE_URL
let getPlace: typeof import('./place.service').getPlace
let canRun = false

if (DATABASE_URL) {
  try {
    const dbMod = await import('../db')
    if ('mock' in dbMod.db.execute) {
      throw new Error('DB module is mocked by unit tests')
    }
    const mod = await import('./place.service')
    // Smoke test with 3s timeout — skip if DB unreachable
    await Promise.race([
      mod.getPlace('node', '0'),
      new Promise((_, rej) => setTimeout(() => rej(new Error('smoke timeout')), 3000)),
    ])
    getPlace = mod.getPlace
    canRun = true
  } catch (e) {
    console.warn('Skipping integration tests:', (e as Error).message)
  }
}

if (!canRun) {
  describe.skip('getPlace integration (DB unavailable)', () => {
    test('skipped', () => {})
  })
} else {
  describe('getPlace — real DB', () => {
    test('returns null for unknown id', async () => {
      const result = await getPlace('node', '0')
      expect(result).toBeNull()
    })

    test('relation/177415 → Charlotte (NC) with full geometry', async () => {
      const result = await getPlace('relation', '177415')
      expect(result).not.toBeNull()
      expect(result.id).toBe('relation/177415')
      expect(result.osm_type).toBe('R')
      expect(result.osm_id).toBe('177415')
      expect(result.name).toMatch(/charlotte/i)
      expect(result.geom_type).toBe('area')
      // Areas must include full_geometry (polygon)
      expect(result.full_geometry).not.toBeNull()
      expect(result.full_geometry.type).toMatch(/polygon/i)
      // And a centroid as point geometry
      expect(result.geometry.type).toBe('Point')
      expect(Array.isArray(result.geometry.coordinates)).toBe(true)
    })

    test('node POI does not include full_geometry', async () => {
      // Find any node and verify shape — ad-hoc lookup via known stable id pattern.
      // We use the Charlotte relation centroid lookup as a control.
      const charlotte = await getPlace('relation', '177415')
      const lng = charlotte.geometry.coordinates[0]
      const lat = charlotte.geometry.coordinates[1]
      expect(typeof lng).toBe('number')
      expect(typeof lat).toBe('number')

      // Verify shape contract: nodes (point geom) should null full_geometry
      // This is a contract test on the SQL CASE WHEN clause.
      // We don't have a guaranteed-stable node id across imports, so we just
      // assert the contract via SQL: any node row must have full_geometry=null.
      const dbMod = await import('../db')
      const { sql } = await import('drizzle-orm')
      const rows = (await dbMod.db.execute(sql`
        SELECT id FROM geo_places
        WHERE geom_type = 'point' AND name IS NOT NULL
        LIMIT 1
      `)) as any[]
      if (rows.length > 0) {
        const [t, i] = rows[0].id.split('/')
        const node = await getPlace(t, i)
        expect(node.full_geometry).toBeNull()
        expect(node.geometry.type).toBe('Point')
      }
    })

    test('returned row exposes the documented columns', async () => {
      const result = await getPlace('relation', '177415')
      // Sanity-check the column set the API contract depends on
      const expected = [
        'id', 'osm_type', 'osm_id', 'name', 'name_abbrev', 'names',
        'categories', 'tags', 'address', 'hours', 'phones', 'websites',
        'geom_type', 'admin_level', 'area_m2', 'geometry', 'full_geometry',
      ]
      for (const k of expected) {
        expect(result).toHaveProperty(k)
      }
    })
  })
}
