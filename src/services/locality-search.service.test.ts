import { describe, test, expect } from 'bun:test'
import { isPostalShaped, mergePasses, qualifierSplits } from './locality-search.service'

describe('isPostalShaped', () => {
  test('accepts postal codes as people type them', () => {
    for (const q of ['11211', '1121', '10997', 'SW1A 1AA', 'H3Z 2Y7', '75008']) {
      expect(isPostalShaped(q)).toBe(true)
    }
  })

  test('rejects names and street addresses', () => {
    // No digit, or too many words to be a code: a house number and its street.
    for (const q of ['brooklyn', 'new jersey', '350 5th ave', '12 elm st', 'flat 2 10 downing']) {
      expect(isPostalShaped(q)).toBe(false)
    }
  })
})

describe('qualifierSplits', () => {
  test('reads trailing words as the state, longest place name first', () => {
    expect(qualifierSplits('charlotte north carolina')).toEqual([
      { name: 'charlotte north', qual: 'carolina' },
      { name: 'charlotte', qual: 'north carolina' },
    ])
    expect(qualifierSplits('clt nc')).toEqual([{ name: 'clt', qual: 'nc' }])
  })

  test('a one-word query has no state, and a name needs three characters', () => {
    expect(qualifierSplits('charlotte')).toEqual([])
    expect(qualifierSplits('ny nc')).toEqual([])
  })

  test('at most three trailing words are a state', () => {
    expect(qualifierSplits('lake in the hills illinois usa').map((s) => s.qual))
      .toEqual(['usa', 'illinois usa', 'hills illinois usa'])
  })
})

describe('mergePasses', () => {
  test('keeps each place once, at its best score', () => {
    const merged = mergePasses([
      [{ id: 'relation/1', text_rank: 0.6 }],
      [{ id: 'relation/1', text_rank: 0.9 }, { id: 'relation/2', text_rank: 0.7 }],
    ], 5)
    expect(merged.map((r) => [r.id, r.text_rank])).toEqual([['relation/1', 0.9], ['relation/2', 0.7]])
  })

  test('drops a label node another pass folded into its boundary', () => {
    const merged = mergePasses([
      [{ id: 'relation/113314', text_rank: 0.9, absorbed_ids: ['node/1'] }],
      [{ id: 'node/1', text_rank: 0.8 }],
    ], 5)
    expect(merged.map((r) => r.id)).toEqual(['relation/113314'])
  })
})
