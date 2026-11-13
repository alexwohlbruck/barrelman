import { describe, expect, it } from 'bun:test'
import type { NormalizeStats } from '../src/lib/rt-normalize/normalize'
import { keepNormalizing } from './measure-rt-normalize'

const stats = (s: Partial<NormalizeStats>): NormalizeStats =>
  ({ updates: 0, exact: 0, dated: 0, matched: 0, added: 0, cancelled: 0, skipped: 0, reordered: 0, unresolved: 0, ...s })

describe('keepNormalizing', () => {
  it('keeps a relayed feed through a night-time sample too small to judge', () => {
    expect(keepNormalizing(stats({ updates: 4, exact: 4 }), true)).toBe(true)
  })

  it('turns a relayed feed off once a full sample resolves as published', () => {
    expect(keepNormalizing(stats({ updates: 200, exact: 200 }), true)).toBe(false)
  })

  it('does not turn a feed on from a small sample with nothing to repair', () => {
    expect(keepNormalizing(stats({ updates: 4, exact: 4 }), false)).toBe(false)
  })
})
