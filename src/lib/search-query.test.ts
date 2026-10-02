import { describe, test, expect } from 'bun:test'
import { buildTsQueryText, isStreetQuery } from './search-query'

describe('buildTsQueryText', () => {
  test('expands a query-side abbreviation to every spelling', () => {
    // "ave" must reach names spelled "Avenue" (and the fold "Av").
    expect(buildTsQueryText(['franklin', 'ave', 'medgar'], true))
      .toBe('franklin & (av | ave | avenue) & medgar:*')
  })

  test('expands the expanded form to reach abbreviated names', () => {
    // "heights" must reach "82 St-Jackson Hts".
    expect(buildTsQueryText(['82', 'st', 'jackson', 'heights'], true))
      .toBe('(82 | 82nd) & (saint | st | street) & jackson & (heights | hts)')
  })

  test('numbers gain their ordinal and ordinals their number', () => {
    expect(buildTsQueryText(['42', 'st'], false))
      .toBe('(42 | 42nd) & (saint | st | street)')
    expect(buildTsQueryText(['42nd', 'street'], false))
      .toBe('(42nd | 42) & (saint | st | street)')
    expect(buildTsQueryText(['11'], false)).toBe('(11 | 11th)')
    expect(buildTsQueryText(['3'], false)).toBe('(3 | 3rd)')
  })

  test('prefix marker lands on every spelling of the last word only', () => {
    expect(buildTsQueryText(['medgar'], true)).toBe('medgar:*')
    expect(buildTsQueryText(['franklin', 'av'], false))
      .toBe('franklin & (av | ave | avenue)')
  })

  test('a known street type ending a multi-word query is matched whole', () => {
    expect(buildTsQueryText(['353', '5th', 'ave'], true)).toBe('(353 | 353rd) & (5th | 5) & (av | ave | avenue)')
  })

  test('a lone known word stays a prefix, so "st" still reaches "starbucks"', () => {
    expect(buildTsQueryText(['st'], true)).toMatch(/:\*/)
  })

  test('strips characters that would be tsquery operators', () => {
    expect(buildTsQueryText(['(cafe)', 'a|b'], false)).toBe('cafe & ab')
    expect(buildTsQueryText(['&', '!'], false)).toBe('')
  })

  test('plain words pass through unchanged', () => {
    expect(buildTsQueryText(['divine', 'barrel'], true)).toBe('divine & barrel:*')
  })
})

describe('isStreetQuery', () => {
  test('a name followed by a street type, in any spelling', () => {
    expect(isStreetQuery(['elm', 'street'])).toBe(true)
    expect(isStreetQuery(['Michigan', 'Ave'])).toBe(true)
    expect(isStreetQuery(['ocean', 'pkwy.'])).toBe(true)
  })

  test('a lone street type, a place name or a saint is not a street', () => {
    expect(isStreetQuery(['street'])).toBe(false)
    expect(isStreetQuery(['jackson', 'heights'])).toBe(false)
    expect(isStreetQuery(['blue', 'star'])).toBe(false)
  })
})
