import { describe, expect, test } from 'bun:test'
import { entranceSign, reachingPlatform, signMatches, type RideDirection } from './entrance-direction'

const NOSTRAND = [
  { id: 'nw', description: 'A,C trains to Manhattan' },
  { id: 'ne', description: 'A,C trains to Manhattan' },
  { id: 'se', description: 'A,C trains to Euclid Avenue, Lefferts Boulevard, Rockaways' },
  { id: 'sw', description: 'A,C trains to Euclid Avenue, Lefferts Boulevard, Rockaways' },
]
const signOf = (routes: string[]) => (e: { description: string; name?: string }) => entranceSign(routes, e.name, e.description)
const ids = (es: Array<{ id: string }>) => es.map((e) => e.id)

const toManhattan: RideDirection = {
  bearing: 274,
  names: ['Manhattan', 'Inwood-207 St', 'Hoyt-Schermerhorn Sts', 'Jay St-MetroTech', 'High St', 'Fulton St'],
}
const toRockaway: RideDirection = {
  bearing: 94,
  names: ['Far Rockaway-Mott Av', 'Utica Av', 'Broadway Junction', 'Euclid Av'],
}

describe('entranceSign', () => {
  test('reads every destination on the sign', () => {
    expect(entranceSign(['A'], null, NOSTRAND[2].description)).toEqual({ toward: ['Euclid Avenue', 'Lefferts Boulevard', 'Rockaways'] })
  })

  test('a clause for another line does not decide', () => {
    expect(entranceSign(['F'], 'all F,G trains; R trains to Manhattan')).toBeNull()
    expect(entranceSign(['R'], 'all F,G trains; R trains to Manhattan')).toEqual({ toward: ['Manhattan'] })
  })

  test('compass words', () => {
    expect(entranceSign(['6'], '6 trains Uptown & The Bronx')).toEqual({ heading: 0 })
  })
})

describe('reachingPlatform', () => {
  test('a Manhattan-bound ride never boards through the Rockaway entrances', () => {
    expect(ids(reachingPlatform(NOSTRAND, signOf(['A', 'C']), toManhattan))).toEqual(['nw', 'ne'])
  })

  test('a Rockaway-bound ride never boards through the Manhattan entrances', () => {
    expect(ids(reachingPlatform(NOSTRAND, signOf(['A']), toRockaway))).toEqual(['se', 'sw'])
  })

  test('alighting knows only the platform sign and the terminus', () => {
    const alight: RideDirection = { bearing: null, names: ['Euclid - Lefferts - Rockaways', 'Ozone Park-Lefferts Blvd'] }
    expect(ids(reachingPlatform(NOSTRAND, signOf(['A']), alight))).toEqual(['se', 'sw'])
  })

  test('keeps every entrance when no sign names the ride', () => {
    const unknown: RideDirection = { bearing: null, names: ['Somewhere Else'] }
    expect(ids(reachingPlatform(NOSTRAND, signOf(['A']), unknown))).toEqual(['nw', 'ne', 'se', 'sw'])
  })

  test('unsigned entrances stay', () => {
    const es = [...NOSTRAND, { id: 'plain', description: '' }]
    expect(ids(reachingPlatform(es, signOf(['A']), toManhattan))).toEqual(['nw', 'ne', 'plain'])
  })

  test('a compass sign the other way is dropped even with nothing named', () => {
    const es = [
      { id: 'up', description: '6 trains Uptown & The Bronx' },
      { id: 'down', description: '6 trains Downtown & Brooklyn' },
    ]
    expect(ids(reachingPlatform(es, signOf(['6']), { bearing: 20, names: [] }))).toEqual(['up'])
  })
})

describe('signMatches', () => {
  test('a heading leans a quarter turn either way', () => {
    expect(signMatches({ heading: 0 }, { bearing: 60, names: [] })).toBe(true)
    expect(signMatches({ heading: 0 }, { bearing: 150, names: [] })).toBe(false)
  })

  test('abbreviations and plurals match the stop names', () => {
    expect(signMatches({ toward: ['Lefferts Boulevard'] }, { bearing: null, names: ['Ozone Park-Lefferts Blvd'] })).toBe(true)
    expect(signMatches({ toward: ['Rockaways'] }, { bearing: null, names: ['Far Rockaway-Mott Av'] })).toBe(true)
    expect(signMatches({ toward: ['Bay Ridge–95th Street'] }, { bearing: null, names: ['Bay Ridge-95 St'] })).toBe(true)
  })
})
