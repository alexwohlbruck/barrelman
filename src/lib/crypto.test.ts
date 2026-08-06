/**
 * Tests for the token/digest helpers behind sessions, OTPs and API keys.
 *
 * These generate credential material, so the properties worth asserting are
 * uniformity (no modulo bias in the sampled alphabets), alphabet correctness,
 * and that comparison doesn't short-circuit on length in a way that breaks.
 */
import { describe, test, expect } from 'bun:test'
import { generateId, randomBase62, randomNumericCode, randomToken, safeEqual, sha256Hex } from './crypto'

describe('sha256Hex', () => {
  test('produces the known digest for a known input', () => {
    expect(sha256Hex('abc')).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')
  })

  test('is stable and 64 hex characters', () => {
    const digest = sha256Hex('barrelman')
    expect(digest).toMatch(/^[0-9a-f]{64}$/)
    expect(sha256Hex('barrelman')).toBe(digest)
  })
})

describe('randomBase62', () => {
  test('returns exactly the requested length', () => {
    for (const length of [1, 8, 24, 64]) {
      expect(randomBase62(length)).toHaveLength(length)
    }
  })

  test('stays inside the base62 alphabet', () => {
    expect(randomBase62(512)).toMatch(/^[A-Za-z0-9]+$/)
  })

  test('covers the whole alphabet rather than a biased prefix', () => {
    // Rejection sampling should reach all 62 symbols; a naive `% 62` over a
    // byte would still hit them all but over-represent the first 8. Assert the
    // spread instead: no symbol should take more than 4% of a large sample.
    const sample = randomBase62(20_000)
    const counts = new Map<string, number>()
    for (const char of sample) counts.set(char, (counts.get(char) ?? 0) + 1)

    expect(counts.size).toBe(62)
    const maxShare = Math.max(...counts.values()) / sample.length
    expect(maxShare).toBeLessThan(0.04) // uniform would be ~1.6%
  })

  test('does not repeat across calls', () => {
    const values = new Set(Array.from({ length: 200 }, () => randomBase62(16)))
    expect(values.size).toBe(200)
  })
})

describe('randomNumericCode', () => {
  test('returns the requested number of digits', () => {
    expect(randomNumericCode(8)).toMatch(/^\d{8}$/)
    expect(randomNumericCode(6)).toMatch(/^\d{6}$/)
  })

  test('keeps leading zeros rather than dropping them', () => {
    // Generated as a string precisely so "01234567" stays eight characters.
    const codes = Array.from({ length: 500 }, () => randomNumericCode(8))
    expect(codes.every((c) => c.length === 8)).toBe(true)
  })

  test('spreads across all ten digits', () => {
    const sample = randomNumericCode(10_000)
    const counts = new Map<string, number>()
    for (const char of sample) counts.set(char, (counts.get(char) ?? 0) + 1)

    expect(counts.size).toBe(10)
    const maxShare = Math.max(...counts.values()) / sample.length
    expect(maxShare).toBeLessThan(0.15) // uniform would be 10%
  })
})

describe('randomToken', () => {
  test('is URL-safe base64 with no padding', () => {
    expect(randomToken(32)).toMatch(/^[A-Za-z0-9_-]+$/)
  })

  test('is unique across calls', () => {
    const values = new Set(Array.from({ length: 200 }, () => randomToken()))
    expect(values.size).toBe(200)
  })
})

describe('safeEqual', () => {
  test('matches identical strings', () => {
    expect(safeEqual('secret-value', 'secret-value')).toBe(true)
  })

  test('rejects different strings of equal length', () => {
    expect(safeEqual('secret-value', 'secret-valve')).toBe(false)
  })

  test('rejects different lengths without throwing', () => {
    // timingSafeEqual itself throws on a length mismatch, so the guard matters.
    expect(safeEqual('short', 'considerably-longer')).toBe(false)
    expect(safeEqual('', 'x')).toBe(false)
  })

  test('handles empty strings', () => {
    expect(safeEqual('', '')).toBe(true)
  })
})

describe('generateId', () => {
  test('is 32 hex characters and unique', () => {
    const ids = Array.from({ length: 500 }, generateId)
    expect(ids.every((id) => /^[0-9a-f]{32}$/.test(id))).toBe(true)
    expect(new Set(ids).size).toBe(500)
  })
})
