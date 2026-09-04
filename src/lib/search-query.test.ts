import { describe, test, expect } from 'bun:test'
import { buildTsQueryText } from './search-query'

describe('buildTsQueryText', () => {
  test('expands a query-side abbreviation to every spelling', () => {
    // "ave" must reach names spelled "Avenue" (and the fold "Av").
    expect(buildTsQueryText(['franklin', 'ave', 'medgar'], true))
      .toBe('franklin & (av | ave | avenue) & medgar:*')
  })

  test('expands the expanded form to reach abbreviated names', () => {
    // "heights" must reach "82 St-Jackson Hts".
    expect(buildTsQueryText(['82', 'st', 'jackson', 'heights'], true))
      .toBe('(82 | 82nd) & (saint | st | street) & jackson & (heights:* | hts:*)')
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

  test('strips characters that would be tsquery operators', () => {
    expect(buildTsQueryText(['(cafe)', 'a|b'], false)).toBe('cafe & ab')
    expect(buildTsQueryText(['&', '!'], false)).toBe('')
  })

  test('plain words pass through unchanged', () => {
    expect(buildTsQueryText(['divine', 'barrel'], true)).toBe('divine & barrel:*')
  })
})
