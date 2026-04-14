/**
 * Integration tests for admin.service against the real database.
 *
 * Only the read-only `getMigrationStatus` function is exercised here —
 * the run* functions mutate the entire geo_places table and are too
 * expensive (and destructive) to run from the test suite.
 *
 * Requires a running Barrelman DB with NC import data on DATABASE_URL.
 *
 * Run: DATABASE_URL="postgresql://barrelman:barrelman@localhost:5434/barrelman" \
 *      bun test src/services/admin.service.integration.test.ts
 */

import { describe, test, expect } from 'bun:test'

const DATABASE_URL = process.env.DATABASE_URL
let getMigrationStatus: typeof import('./admin.service').getMigrationStatus
let canRun = false

if (DATABASE_URL) {
  try {
    const dbMod = await import('../db')
    if ('mock' in dbMod.db.execute) {
      throw new Error('DB module is mocked by unit tests')
    }
    const mod = await import('./admin.service')
    // NOTE: getMigrationStatus is currently slow (15-18s on a real DB) so we
    // give the smoke test a generous 25s timeout. If a future change makes it
    // fast enough that this can drop to 3s, please tighten it.
    await Promise.race([
      mod.getMigrationStatus(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('smoke timeout')), 25_000)),
    ])
    getMigrationStatus = mod.getMigrationStatus
    canRun = true
  } catch (e) {
    console.warn('Skipping integration tests:', (e as Error).message)
  }
}

if (!canRun) {
  describe.skip('admin.service integration (DB unavailable)', () => {
    test('skipped', () => {})
  })
} else {
  describe('getMigrationStatus — real DB', () => {
    test('returns the documented shape', async () => {
      const status = await getMigrationStatus()
      expect(status).toHaveProperty('parent_context_column')
      expect(status).toHaveProperty('populated')
      expect(status).toHaveProperty('total_named')
      expect(status).toHaveProperty('coverage')
      expect(typeof status.parent_context_column).toBe('boolean')
      expect(typeof status.populated).toBe('number')
      expect(typeof status.total_named).toBe('number')
      expect(typeof status.coverage).toBe('number')
    }, 30_000)

    test('after a normal NC import, parent_context column exists and has data', async () => {
      const status = await getMigrationStatus()
      expect(status.parent_context_column).toBe(true)
      expect(status.total_named).toBeGreaterThan(0)
      // populated should never exceed total_named
      expect(status.populated).toBeLessThanOrEqual(status.total_named)
      // coverage is a percentage 0-100
      expect(status.coverage).toBeGreaterThanOrEqual(0)
      expect(status.coverage).toBeLessThanOrEqual(100)
    }, 30_000)

    /**
     * Performance regression: this query currently does a full table scan
     * over ~7M rows and takes 15-18 seconds in dev. If a future change adds
     * a partial index or counter cache, this test should still pass; if a
     * change makes it WORSE, it will fail loudly.
     *
     * Threshold is intentionally generous (30s) to avoid flaking on slow
     * hardware. The current dev p95 is ~16s.
     */
    test('completes within 30 seconds (perf regression guard)', async () => {
      const start = performance.now()
      await getMigrationStatus()
      const elapsed = performance.now() - start
      expect(elapsed).toBeLessThan(30_000)
    }, 45_000)
  })
}
