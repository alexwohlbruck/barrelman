/**
 * The penalty box's persistence bookkeeping: what gets written, and what a
 * stored row does to the box when it is read back — which is what decides
 * whether a deploy or a second replica lets a boxed caller through.
 */
import { describe, test, expect } from 'bun:test'
import { PenaltyBox, isLivePenalty, mergePenalty, type PenaltyRow } from './penalty-box'

const DECAY = 10 * 60_000
const T0 = 1_000_000_000_000

function box(hashed: string[] = []) {
  return new PenaltyBox({
    strikesBeforePenalty: 3,
    decayMs: DECAY,
    maxPenaltyMs: 30 * 60_000,
    hash: (key) => {
      hashed.push(key)
      return `h(${key})`
    },
  })
}

function strike(b: PenaltyBox, key: string, times: number, now = T0) {
  for (let i = 0; i < times; i += 1) b.recordRejection(key, now)
}

describe('what is persisted', () => {
  test('nothing until a caller is boxed', () => {
    const b = box()
    strike(b, 'u1@1.2.3.4', 2)
    expect(b.takeDirty().upserts).toEqual([])
  })

  test('a boxed caller, under its hashed key, once per flush', () => {
    const b = box()
    strike(b, 'u1@1.2.3.4', 5)
    const batch = b.takeDirty()
    expect(batch.upserts).toHaveLength(1)
    expect(batch.upserts[0]).toMatchObject({ id: 'h(u1@1.2.3.4)', strikes: 5 })
    expect(batch.upserts[0]!.until).toBeGreaterThan(T0)
    expect(b.takeDirty().upserts).toEqual([])
  })

  test('a success after the box expired deletes the row', () => {
    const b = box()
    strike(b, 'ip:1.2.3.4', 3)
    const { upserts } = b.takeDirty()
    b.recordSuccess('ip:1.2.3.4', upserts[0]!.until + 1)
    expect(b.takeDirty()).toMatchObject({ upserts: [], deletes: ['h(ip:1.2.3.4)'] })
  })

  test('a failed write is retried on the next flush', () => {
    const b = box()
    strike(b, 'k', 3)
    b.requeue(b.takeDirty())
    expect(b.takeDirty().upserts.map((r) => r.id)).toEqual(['h(k)'])
  })
})

describe('reading rows back', () => {
  const stored = (over: Partial<PenaltyRow> = {}): PenaltyRow => ({
    id: 'h(u1@1.2.3.4)',
    strikes: 10,
    until: T0 + 60_000,
    lastStrikeAt: T0,
    ...over,
  })

  test('a row loaded at startup boxes the caller it belongs to', () => {
    const b = box()
    b.applyStored([stored()], T0)
    expect(b.remaining('u1@1.2.3.4', T0)).toBe(60)
    expect(b.remaining('u2@1.2.3.4', T0)).toBe(0)
    expect(b.penalisedCount(T0)).toBe(1)
  })

  test('does not mark loaded rows dirty', () => {
    const b = box()
    b.applyStored([stored()], T0)
    b.remaining('u1@1.2.3.4', T0)
    expect(b.takeDirty().upserts).toEqual([])
  })

  test('skips rows that are no longer live', () => {
    const b = box()
    b.applyStored([stored({ until: T0 - 1, lastStrikeAt: T0 - DECAY - 1 })], T0)
    expect(b.penalisedCount(T0)).toBe(0)
    expect(b.strikes('u1@1.2.3.4', T0)).toBe(0)
  })

  test('another replica cannot shorten a local box', () => {
    const b = box()
    strike(b, 'u1@1.2.3.4', 6)
    const local = b.takeDirty().upserts[0]!
    b.applyStored([stored({ until: T0 + 1_000, strikes: 4 })], T0)
    expect(b.remaining('u1@1.2.3.4', T0)).toBe(Math.ceil((local.until - T0) / 1000))
    expect(b.strikes('u1@1.2.3.4', T0)).toBe(6)
  })

  test('a box from another replica applies to a caller held here below the threshold', () => {
    const b = box()
    b.recordRejection('u1@1.2.3.4', T0)
    b.applyStored([stored({ until: T0 + 600_000 })], T0)
    expect(b.remaining('u1@1.2.3.4', T0)).toBe(600)
  })

  test('a longer box from another replica extends the local one', () => {
    const b = box()
    strike(b, 'u1@1.2.3.4', 3)
    b.takeDirty()
    b.applyStored([stored({ until: T0 + 600_000 })], T0)
    expect(b.remaining('u1@1.2.3.4', T0)).toBe(600)
  })

  test('strikes on an adopted row keep escalating and are written back', () => {
    const b = box()
    b.applyStored([stored({ strikes: 3, until: T0 })], T0)
    b.recordRejection('u1@1.2.3.4', T0 + 1)
    expect(b.strikes('u1@1.2.3.4', T0 + 1)).toBe(4)
    expect(b.takeDirty().upserts[0]).toMatchObject({ id: 'h(u1@1.2.3.4)', strikes: 4 })
  })

  test('hashes nothing on the request path when no row is waiting', () => {
    const hashed: string[] = []
    const b = box(hashed)
    for (let i = 0; i < 100; i += 1) b.remaining(`ip:10.0.0.${i}`, T0)
    expect(hashed).toEqual([])
  })

  test('prune drops expired rows that were never matched', () => {
    const b = box()
    b.applyStored([stored()], T0)
    b.prune(T0 + 60_000 + DECAY + 1)
    expect(b.penalisedCount(T0)).toBe(0)
  })
})

describe('mergePenalty', () => {
  test('a decayed local entry is replaced, not combined', () => {
    const merged = mergePenalty(
      { strikes: 50, until: 0, lastStrikeAt: T0 - DECAY - 1 },
      { strikes: 2, until: 0, lastStrikeAt: T0 },
      T0,
      DECAY,
    )
    expect(merged.strikes).toBe(2)
  })

  test('isLivePenalty matches the prune predicate', () => {
    expect(isLivePenalty({ strikes: 1, until: T0 + 1, lastStrikeAt: 0 }, T0, DECAY)).toBe(true)
    expect(isLivePenalty({ strikes: 1, until: 0, lastStrikeAt: T0 - DECAY }, T0, DECAY)).toBe(true)
    expect(isLivePenalty({ strikes: 1, until: 0, lastStrikeAt: T0 - DECAY - 1 }, T0, DECAY)).toBe(false)
  })
})
