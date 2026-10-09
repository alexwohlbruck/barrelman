import { describe, test, expect } from 'bun:test'
import { isNearMissOfStart } from './brands.service'

// Pairs measured against the US brand catalog: trigram scores put each of the
// rejected ones in the same range as a real typo.
describe('isNearMissOfStart', () => {
  test('accepts a typo of the brand name', () => {
    for (const [q, name] of [
      ['starbuks', 'Starbucks'], ['wallmart', 'Walmart'], ['mcdonals', "McDonald's"],
      ['chik fil a', 'Chick-fil-A'], ['harris teter', 'Harris Teeter'], ['walgren', 'Walgreens'],
    ]) expect(isNearMissOfStart(q, name)).toBe(true)
  })

  test('ignores a leading "The"', () => {
    expect(isNearMissOfStart('home depo', 'The Home Depot')).toBe(true)
  })

  test('rejects a different word that shares the letters', () => {
    for (const [q, name] of [
      ['charleston', 'Charles Schwab'], ['charleston', 'Charles Tyrwhitt'],
      ['columbus', 'Columbia'], ['power outlet', 'Home Outlet'],
    ]) expect(isNearMissOfStart(q, name)).toBe(false)
  })

  test('rejects a word that is only somewhere in the name', () => {
    expect(isNearMissOfStart('columbus', 'Knights of Columbus')).toBe(false)
  })
})
