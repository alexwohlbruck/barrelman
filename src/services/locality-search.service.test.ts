import { describe, test, expect } from 'bun:test'
import { isPostalShaped } from './locality-search.service'

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
