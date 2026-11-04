/**
 * The sync loop against a fake store: what reaches the database, in what
 * order, and that a failed write is not lost.
 */
import { describe, test, expect } from 'bun:test'
import { PenaltyBox, type PenaltyRow } from './penalty-box'
import { flushPenalties, refreshPenalties, type PenaltyStore } from './penalty-sync.service'

function box() {
  return new PenaltyBox({ strikesBeforePenalty: 3, decayMs: 600_000, maxPenaltyMs: 1_800_000, hash: (k) => `h(${k})` })
}

function fakeStore(rows: PenaltyRow[] = []) {
  const calls: string[] = []
  const written: PenaltyRow[] = []
  let failNext = false
  const store: PenaltyStore = {
    async load() {
      calls.push('load')
      return rows
    },
    async upsert(r) {
      calls.push('upsert')
      if (failNext) {
        failNext = false
        throw new Error('db down')
      }
      written.push(...r)
    },
    async remove() {
      calls.push('remove')
    },
    async prune() {
      calls.push('prune')
      return 0
    },
  }
  return { store, calls, written, failOnce: () => (failNext = true) }
}

describe('flushPenalties', () => {
  test('issues no query when nothing changed', async () => {
    const { store, calls } = fakeStore()
    await flushPenalties(box(), store)
    expect(calls).toEqual([])
  })

  test('requeues a batch whose write failed', async () => {
    const b = box()
    for (let i = 0; i < 3; i += 1) b.recordRejection('k')
    const fake = fakeStore()
    fake.failOnce()
    await expect(flushPenalties(b, fake.store)).rejects.toThrow('db down')
    await flushPenalties(b, fake.store)
    expect(fake.written.map((r) => r.id)).toEqual(['h(k)'])
  })
})

describe('refreshPenalties', () => {
  test('flushes before it loads, so its own changes are in what it reads', async () => {
    const b = box()
    for (let i = 0; i < 3; i += 1) b.recordRejection('k')
    const { store, calls } = fakeStore()
    await refreshPenalties(b, store)
    expect(calls).toEqual(['upsert', 'remove', 'prune', 'load'])
  })

  test('boxes a caller from a row another replica wrote', async () => {
    const now = Date.now()
    const b = box()
    const { store } = fakeStore([{ id: 'h(ip:5.6.7.8)', strikes: 30, until: now + 120_000, lastStrikeAt: now }])
    await refreshPenalties(b, store)
    expect(b.remaining('ip:5.6.7.8')).toBeGreaterThan(100)
  })
})
