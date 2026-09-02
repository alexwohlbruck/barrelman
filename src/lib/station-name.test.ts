/**
 * The name fold that decides which station an OSM node *is*.
 *
 * It only has to be right about one question — same station or not — but it is
 * the whole reason a departure board can tell Brooklyn Bridge–City Hall from
 * the Chambers St platforms 14 m nearer, so both directions matter: a pair that
 * should match and doesn't sends the board back to nearest-wins, and a pair
 * that shouldn't match and does hands the station to a neighbour outright.
 */
import { describe, test, expect } from 'bun:test'
import { normalizeStationName, sameStationName } from './station-name'

describe('sameStationName', () => {
  test('reconciles an OSM name with the GTFS one', () => {
    // The reported pairs, exactly as the two sources spell them.
    expect(sameStationName('Brooklyn Bridge–City Hall', 'Brooklyn Bridge-City Hall')).toBe(true)
    expect(sameStationName('Grand Central–42nd Street', 'Grand Central-42 St')).toBe(true)
    expect(sameStationName('Times Square–42nd Street', 'Times Sq-42 St')).toBe(true)
    expect(sameStationName('34th Street–Penn Station', '34 St-Penn Station')).toBe(true)
  })

  test('keeps a shorter name from swallowing a longer one', () => {
    // Trigram similarity ranks this pair (0.42) above Times Sq/Times Square
    // (0.41), which is why the fold compares for equality instead.
    expect(sameStationName('Brooklyn Bridge–City Hall', 'City Hall')).toBe(false)
    expect(sameStationName('Grand Central–42nd Street', '42 St-Bryant Pk')).toBe(false)
    expect(sameStationName('Roosevelt Island Tramway', 'Manhattan Tram Station')).toBe(false)
  })

  test('ignores case, accents and whatever punctuation a feed used', () => {
    expect(sameStationName('E 60 ST/2 AV', 'E 60 St / 2 Av')).toBe(true)
    expect(sameStationName('Cañada College', 'Canada College')).toBe(true)
    expect(sameStationName('St. George', 'Saint George')).toBe(true)
  })

  test('never matches on an absent name', () => {
    // Two unnamed stops are unidentified, not the same station.
    expect(sameStationName('', '')).toBe(false)
    expect(sameStationName(null, undefined)).toBe(false)
    expect(sameStationName('---', '///')).toBe(false)
  })
})

describe('normalizeStationName', () => {
  test('drops the ordinal suffix from a numbered street', () => {
    expect(normalizeStationName('42nd Street')).toBe('42 st')
    expect(normalizeStationName('1st Avenue')).toBe('1 av')
    expect(normalizeStationName('23rd')).toBe('23')
  })

  test('leaves a word that only looks like an ordinal alone', () => {
    // Not `1` + a suffix — the whole token has to be digits.
    expect(normalizeStationName('Forest Hills')).toBe('forest hills')
    expect(normalizeStationName('Ind')).toBe('ind')
  })
})
