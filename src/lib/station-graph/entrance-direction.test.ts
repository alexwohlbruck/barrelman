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
  bearing: 274, inbound: null,
  names: ['Manhattan', 'Inwood-207 St', 'Hoyt-Schermerhorn Sts', 'Jay St-MetroTech', 'High St', 'Fulton St'],
}
const toRockaway: RideDirection = {
  bearing: 94, inbound: null,
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

  test('a sign naming only other lines', () => {
    expect(entranceSign(['2'], 'F,<F>,M trains Downtown & Brooklyn; PATH trains to New Jersey')).toEqual({ otherLines: true })
    expect(entranceSign(['2'], 'Main entrance')).toBeNull()
    expect(entranceSign(['6X'], '6,<6> trains Downtown')).toEqual({ heading: 180 })
  })

  test('line names, in and out', () => {
    expect(entranceSign(['Red Line'], 'Red Line Inbound only')).toEqual({ inbound: true })
    expect(entranceSign(['Green Line'], 'Green Line (Inbound) | Red Line (All Trains)')).toEqual({ inbound: true })
    expect(entranceSign(['Red Line'], 'Green Line (Inbound) | Red Line (All Trains)')).toBeNull()
    expect(entranceSign(['Green Line'], 'Lansdowne (Outbound)')).toEqual({ inbound: false })
  })

  test('a bare destination, in other languages too', () => {
    expect(entranceSign(['Blue'], 'To Franconia-Springfield')).toEqual({ toward: ['Franconia-Springfield'] })
    expect(entranceSign(['U2'], 'Zugang – Richtung Pankow')).toEqual({ toward: ['Pankow'] })
    expect(entranceSign(['1'], 'Line 1 direction La Défense')).toEqual({ toward: ['La Défense'] })
  })

  test('mid-sentence directions abroad', () => {
    expect(entranceSign(['1'], 'Comercio-Ascensor. Acceso a línea 1 dirección Pinar de Chamartín')).toEqual({ toward: ['Pinar de Chamartín'] })
    expect(entranceSign(['U2'], 'Aufzug U1 U2 U3 Fahrtrichtung West')).toEqual({ heading: 270 })
    expect(entranceSign(['U9'], 'Bundesplatz', 'Richtung Osloer Straße')).toEqual({ toward: ['Osloer Straße'] })
  })

  test('an exit towards a street is not a train direction', () => {
    expect(entranceSign(['U3'], 'Ausgang Richtung Implerstraße')).toBeNull()
    expect(entranceSign(['U2'], 'Böhmerwaldplatz, Ausgang Richtung Böhmerwaldplatz')).toBeNull()
    expect(entranceSign(['S5'], 'Ausgang Richtung Ringcenter', 'S Frankfurter Allee')).toBeNull()
  })

  test('only a destination opening the sign counts', () => {
    expect(entranceSign(['NJT'], 'Entrance to NJ Transit')).toBeNull()
    expect(entranceSign(['Red'], 'Elevator to Harvard MBTA station')).toBeNull()
  })
})

describe('reachingPlatform', () => {
  test('a ride out of town never boards through an inbound-only entrance', () => {
    const central = [
      { id: 'in', description: 'Red Line Inbound only' },
      { id: 'out', description: 'Red Line Outbound only' },
    ]
    const toAlewife: RideDirection = { bearing: null, names: ['Alewife', 'Harvard'], inbound: false }
    expect(ids(reachingPlatform(central, signOf(['Red Line']), toAlewife))).toEqual(['out'])
  })

  test('a Manhattan-bound ride never boards through the Rockaway entrances', () => {
    expect(ids(reachingPlatform(NOSTRAND, signOf(['A', 'C']), toManhattan))).toEqual(['nw', 'ne'])
  })

  test('a Rockaway-bound ride never boards through the Manhattan entrances', () => {
    expect(ids(reachingPlatform(NOSTRAND, signOf(['A']), toRockaway))).toEqual(['se', 'sw'])
  })

  test('alighting knows only the platform sign and the terminus', () => {
    const alight: RideDirection = { bearing: null, names: ['Euclid - Lefferts - Rockaways', 'Ozone Park-Lefferts Blvd'], inbound: null }
    expect(ids(reachingPlatform(NOSTRAND, signOf(['A']), alight))).toEqual(['se', 'sw'])
  })

  test('keeps every entrance when no sign names the ride', () => {
    const unknown: RideDirection = { bearing: null, names: ['Somewhere Else'], inbound: null }
    expect(ids(reachingPlatform(NOSTRAND, signOf(['A']), unknown))).toEqual(['nw', 'ne', 'se', 'sw'])
  })

  test('unsigned entrances stay', () => {
    const es = [...NOSTRAND, { id: 'plain', description: '' }]
    expect(ids(reachingPlatform(es, signOf(['A']), toManhattan))).toEqual(['nw', 'ne', 'plain'])
  })

  test('an entrance for other lines is dropped while another may serve the ride', () => {
    const es = [
      { id: 'fm', description: 'F,<F>,M trains Downtown & Brooklyn; PATH trains to New Jersey' },
      { id: 'up', description: '1,2,3 trains Uptown & The Bronx' },
      { id: 'plain', description: '' },
    ]
    expect(ids(reachingPlatform(es, signOf(['2']), { bearing: 20, names: ['Uptown'], inbound: null }))).toEqual(['up', 'plain'])
    expect(ids(reachingPlatform(es.slice(0, 1), signOf(['2']), { bearing: 20, names: [], inbound: null }))).toEqual(['fm'])
  })

  test('the platform label says which way when the line bends', () => {
    const es = [{ id: 'up', description: '6 trains Uptown' }, { id: 'down', description: '6 trains Downtown' }]
    expect(ids(reachingPlatform(es, signOf(['6']), { bearing: 200, names: ['Uptown'], inbound: null }))).toEqual(['up'])
  })

  test('a compass sign the other way is dropped even with nothing named', () => {
    const es = [
      { id: 'up', description: '6 trains Uptown & The Bronx' },
      { id: 'down', description: '6 trains Downtown & Brooklyn' },
    ]
    expect(ids(reachingPlatform(es, signOf(['6']), { bearing: 20, names: [], inbound: null }))).toEqual(['up'])
  })
})

describe('signMatches', () => {
  test('in and out follow the platform label, else the way to the hub', () => {
    expect(signMatches({ inbound: true }, { bearing: null, names: ['Inbound', 'Alewife'], inbound: false })).toBe(true)
    expect(signMatches({ inbound: false }, { bearing: null, names: ['Alewife'], inbound: true })).toBe(false)
    expect(signMatches({ inbound: false }, { bearing: null, names: ['Alewife'], inbound: null })).toBeNull()
  })

  test('a heading leans a quarter turn either way', () => {
    expect(signMatches({ heading: 0 }, { bearing: 60, names: [], inbound: null })).toBe(true)
    expect(signMatches({ heading: 0 }, { bearing: 150, names: [], inbound: null })).toBe(false)
  })

  test('abbreviations and plurals match the stop names', () => {
    expect(signMatches({ toward: ['Lefferts Boulevard'] }, { bearing: null, names: ['Ozone Park-Lefferts Blvd'], inbound: null })).toBe(true)
    expect(signMatches({ toward: ['Rockaways'] }, { bearing: null, names: ['Far Rockaway-Mott Av'], inbound: null })).toBe(true)
    expect(signMatches({ toward: ['Bay Ridge–95th Street'] }, { bearing: null, names: ['Bay Ridge-95 St'], inbound: null })).toBe(true)
  })
})
