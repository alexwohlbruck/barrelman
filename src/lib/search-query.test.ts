import { describe, test, expect } from 'bun:test'
import { buildTsQueryText, inflections, isStreetQuery } from './search-query'

describe('inflections', () => {
  test('English plurals both ways', () => {
    expect(inflections('suit')).toEqual(['suits'])
    expect(inflections('suits')).toEqual(['suit'])
    expect(inflections('bakery')).toEqual(['bakeries'])
    expect(inflections('bakeries')).toEqual(['bakery'])
    expect(inflections('church')).toEqual(['churches'])
    expect(inflections('churches')).toEqual(['church', 'churche'])
  })

  test('leaves words that only look plural, short words and non-English ones', () => {
    expect(inflections('bus')).toEqual([])
    expect(inflections('glass')).toEqual([])
    expect(inflections('campus')).toEqual([])
    expect(inflections('42nd')).toEqual([])
    expect(inflections('straße')).toEqual([])
  })
})

describe('buildTsQueryText', () => {
  test('expands a query-side abbreviation to every spelling', () => {
    // "ave" must reach names spelled "Avenue" (and the fold "Av").
    expect(buildTsQueryText(['franklin', 'ave', 'medgar'], true))
      .toBe('(franklin | franklins) & (av | ave | avenue) & medgar:*')
  })

  test('expands the expanded form to reach abbreviated names', () => {
    // "heights" must reach "82 St-Jackson Hts".
    expect(buildTsQueryText(['82', 'st', 'jackson', 'heights'], true))
      .toBe('(82 | 82nd) & (saint | st | street) & (jackson | jacksons) & (heights | hts)')
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
      .toBe('(franklin | franklins) & (av | ave | avenue)')
  })

  test('a known street type ending a multi-word query is matched whole', () => {
    expect(buildTsQueryText(['353', '5th', 'ave'], true)).toBe('(353 | 353rd) & (5th | 5) & (av | ave | avenue)')
  })

  test('a lone known word stays a prefix, so "st" still reaches "starbucks"', () => {
    expect(buildTsQueryText(['st'], true)).toMatch(/:\*/)
  })

  test('strips characters that would be tsquery operators', () => {
    expect(buildTsQueryText(['(cafe)', 'a|b'], false)).toBe('(cafe | cafes) & ab')
    expect(buildTsQueryText(['&', '!'], false)).toBe('')
  })

  test('keeps letters outside ASCII, which unaccent() folds in SQL', () => {
    expect(buildTsQueryText(['Neukölln'], false)).toBe('neukölln')
    expect(buildTsQueryText(['Zürich', 'straße'], true)).toBe('zürich & straße:*')
    // Decomposed input (o + combining diaeresis) composes to the same word.
    expect(buildTsQueryText(['Neuko\u0308lln'], false)).toBe('neukölln')
    expect(buildTsQueryText(['हिन्दी'], false)).toBe('हिन्दी')
  })

  test('a finished word also matches its other number', () => {
    // A shop tagged clothes=suits is found by "suit shop".
    expect(buildTsQueryText(['suit', 'shop'], false)).toBe('(suit | suits) & (shop | shops)')
    expect(buildTsQueryText(['divine', 'barrel'], true)).toBe('(divine | divines) & barrel:*')
  })

  test('a prefix gains only the shorter number, which it does not already reach', () => {
    // "suit:*" covers "suits"; "suits:*" does not cover "suit".
    expect(buildTsQueryText(['suit'], true)).toBe('suit:*')
    expect(buildTsQueryText(['suits'], true)).toBe('(suits:* | suit)')
  })

  test('everyday words reach the words OSM tags use', () => {
    expect(buildTsQueryText(['clothing', 'store'], false)).toBe('(clothing | clothings | clothes) & (store | stores | shop)')
  })

  test('a place nickname becomes the phrase in the address', () => {
    expect(buildTsQueryText(['suits', 'nyc'], false)).toBe('(suits | suit) & (nyc | nycs | new <-> york)')
  })

  test('corrections join a word as exact spellings, never prefixes', () => {
    const fixes = new Map([['warehouse', ['wearhouse']], ['flatbsh', ['flatbush']]])
    expect(buildTsQueryText(['mens', 'warehouse', 'flatbsh'], true, fixes))
      .toBe('(mens | men) & (warehouse | warehouses | wearhouse) & (flatbsh:* | flatbush)')
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
