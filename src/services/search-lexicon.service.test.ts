import { describe, test, expect } from 'bun:test'
import { deletionKeys, editDistance, maxEdits, pickCorrections, looksMisspelt } from './search-lexicon.service'

/** Two words are looked up together when their keys meet. */
const meet = (a: string, b: string) => deletionKeys(a).some((k) => deletionKeys(b).includes(k))

describe('deletionKeys', () => {
  test('a word and each one-letter deletion of it', () => {
    expect(deletionKeys('park').sort()).toEqual(['ark', 'pak', 'par', 'park', 'prk'])
  })

  test('the typos that matter share a key with the word meant', () => {
    expect(meet('prak', 'park')).toBe(true) // letters swapped
    expect(meet('green', 'greene')).toBe(true) // letter dropped
    expect(meet('starbuks', 'starbucks')).toBe(true)
    expect(meet('centre', 'center')).toBe(true) // letters swapped
    expect(meet('warehouse', 'wearhouse')).toBe(true) // letter moved
  })

  test('two changed letters do not meet', () => {
    expect(meet('warehouse', 'morehouse')).toBe(false)
  })

  test('counts by code point, as SQL does', () => {
    expect(deletionKeys('café')).toContain('caf')
  })
})

describe('editDistance', () => {
  test('a swap of neighbours is one edit', () => {
    expect(editDistance('prak', 'park', 2)).toBe(1)
    expect(editDistance('warehouse', 'wearhouse', 2)).toBe(2)
  })

  test('gives up past the limit', () => {
    expect(editDistance('warehouse', 'firehouse', 1)).toBe(2)
    expect(editDistance('a', 'abcdef', 2)).toBe(3)
  })
})

describe('pickCorrections', () => {
  test('the most used first, so the data\'s own typos do not crowd out the word meant', () => {
    // Counts from the US lexicon: one-edit misspellings in the data outnumbered
    // the slots, and "wearhouse" (two edits) was never tried.
    const candidates = [
      { word: 'warehouse', n: 10645 },
      { word: 'wearhouse', n: 505 },
      { word: 'warehouses', n: 89 },
      { word: 'warhouse', n: 8 },
      { word: 'wharehouse', n: 8 },
      { word: 'warehous', n: 3 },
    ]
    expect(pickCorrections('warehouse', candidates)).toEqual(['wearhouse', 'warehouses', 'warhouse'])
  })

  test('a short word may not stray two edits', () => {
    expect(maxEdits(5)).toBe(1)
    expect(pickCorrections('green', [{ word: 'greene', n: 10 }, { word: 'gerne', n: 99 }])).toEqual(['greene'])
  })

  test('words under four letters are never corrected', () => {
    expect(maxEdits(3)).toBe(0)
  })
})

// Counts from the US lexicon.
describe('looksMisspelt', () => {
  const park = { word: 'park', n: 885_814 }
  test('a word far rarer than a one-edit neighbour', () => {
    expect(looksMisspelt('prak', 1, [park, { word: 'prak', n: 1 }], false)).toBe(true)
    expect(looksMisspelt('parc', 1877, [park, { word: 'parc', n: 1877 }], false)).toBe(true)
    expect(looksMisspelt('washingon', 0, [{ word: 'washington', n: 1_356_779 }], false)).toBe(true)
  })

  test('a correct word with a merely commoner neighbour', () => {
    expect(looksMisspelt('divine', 1108, [{ word: 'diving', n: 9000 }], false)).toBe(false)
  })

  test('a last word still being typed', () => {
    const central = { word: 'central', n: 180_348 }
    expect(looksMisspelt('centra', 96, [central], true)).toBe(false)
    expect(looksMisspelt('centra', 96, [central], false)).toBe(true)
    // A typo in the last word is still one.
    expect(looksMisspelt('prak', 1, [park], true)).toBe(true)
  })
})
