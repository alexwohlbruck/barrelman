import { describe, expect, test } from 'bun:test'
import { cleanLabel, directionPhrase, pickDirectionLabel } from './direction-labels'
import { parseMtaEntrances, parseMtaStations } from './agency-sources'
import { cornerLabel, cornerOf, quadrant, streetSide } from './corner-label'
import { entranceAccess } from './entrance-access'

describe('cleanLabel', () => {
  test('drops MTA\'s "Last Stop" placeholder and blanks', () => {
    expect(cleanLabel('Last Stop')).toBeNull()
    expect(cleanLabel(' last stop ')).toBeNull()
    expect(cleanLabel('NaN')).toBeNull()
    expect(cleanLabel('')).toBeNull()
    expect(cleanLabel(undefined)).toBeNull()
  })

  test('keeps real labels, collapsing whitespace', () => {
    expect(cleanLabel('  Uptown  &  The Bronx ')).toBe('Uptown & The Bronx')
  })
})

describe('pickDirectionLabel', () => {
  const mta = (label: string) => ({ label, source: 'mta-subway-stations' })
  const dest = (label: string) => ({ label, source: 'osm-destination' })
  const words = (label: string) => ({ label, source: 'osm-direction' })

  test('agency data leads', () => {
    expect(pickDirectionLabel([dest('Jamaica'), mta('Manhattan')])).toBe('Manhattan')
  })

  test('the signed OSM text wins where it spells out the agency label', () => {
    expect(pickDirectionLabel([mta('Uptown'), dest('Uptown & The Bronx')])).toBe('Uptown & The Bronx')
    expect(pickDirectionLabel([mta('Downtown'), dest('Downtown & Brooklyn')])).toBe('Downtown & Brooklyn')
  })

  test('the signed OSM text wins over a bare compass word', () => {
    expect(pickDirectionLabel([mta('Southbound'), dest('Coney Island & Bay Ridge')])).toBe('Coney Island & Bay Ridge')
    expect(pickDirectionLabel([mta('Outbound'), dest('Far Rockaway')])).toBe('Far Rockaway')
  })

  test('a word prefix is not an elaboration', () => {
    expect(pickDirectionLabel([mta('Queens'), dest('Queensbridge')])).toBe('Queens')
  })

  test('OSM fills in where the agency has nothing', () => {
    expect(pickDirectionLabel([dest('Manhattan')])).toBe('Manhattan')
    expect(pickDirectionLabel([words('Uptown')])).toBe('Uptown')
    expect(pickDirectionLabel([words('Uptown'), dest('Uptown & Queens')])).toBe('Uptown & Queens')
    expect(pickDirectionLabel([])).toBeNull()
  })
})

describe('directionPhrase', () => {
  test('finds compass and place-bound words', () => {
    expect(directionPhrase('Uptown platform')).toBe('Uptown')
    expect(directionPhrase(null, 'trains to Brooklyn-bound')).toBe('Brooklyn-bound')
    expect(directionPhrase('Northbound')).toBe('Northbound')
  })

  test('ignores text without one', () => {
    expect(directionPhrase('14 St-Union Sq', 'Platform 2')).toBeNull()
  })
})

describe('parseMtaStations', () => {
  test('maps both labels onto the N and S platforms, without "Last Stop"', () => {
    const rows = [
      { gtfs_stop_id: 'R01', north_direction_label: 'Last Stop', south_direction_label: 'Manhattan' },
      { gtfs_stop_id: 'R20', north_direction_label: 'Uptown', south_direction_label: 'Downtown' },
      { north_direction_label: 'Uptown' },
    ]
    expect(parseMtaStations(rows)).toEqual([
      { stopId: 'R01S', label: 'Manhattan' },
      { stopId: 'R20N', label: 'Uptown' },
      { stopId: 'R20S', label: 'Downtown' },
    ])
  })
})

describe('parseMtaEntrances', () => {
  test('reads the entry and exit flags', () => {
    const [e] = parseMtaEntrances([{
      gtfs_stop_id: 'R31', entrance_type: 'Stair', entry_allowed: 'NO', exit_allowed: 'YES',
      entrance_latitude: '40.683905', entrance_longitude: '-73.978879',
    }])
    expect(e).toMatchObject({ stopId: 'R31', entryAllowed: false, exitAllowed: true, lat: 40.683905 })
  })

  test('one point listed twice allows what either row allows', () => {
    const at = { entrance_latitude: '40.7', entrance_longitude: '-73.9' }
    const out = parseMtaEntrances([
      { ...at, entry_allowed: 'NO', exit_allowed: 'YES' },
      { ...at, entry_allowed: 'YES', exit_allowed: 'YES' },
      { entrance_latitude: 'x', entrance_longitude: '-73.9' },
    ])
    expect(out).toHaveLength(1)
    expect(out[0].entryAllowed).toBe(true)
  })
})

describe('corner math', () => {
  const at = { lng: -73.99, lat: 40.75 }
  // ~14 m east and ~11 m north of the intersection
  const ne = { lng: at.lng + 0.00017, lat: at.lat + 0.0001 }
  const sw = { lng: at.lng - 0.00017, lat: at.lat - 0.0001 }

  test('streetSide gives the compass side of a street', () => {
    expect(streetSide(0, ne, at)).toBe('E') // north-south avenue, entrance east of it
    expect(streetSide(90, ne, at)).toBe('N') // east-west street, entrance north of it
    expect(streetSide(270, sw, at)).toBe('S') // the same street drawn the other way
    expect(streetSide(180, sw, at)).toBe('W')
  })

  test('Manhattan\'s tilted grid still snaps to the right sides', () => {
    // Avenues run ~29° east of north; streets ~119°.
    expect(cornerOf(ne, at, [
      { name: 'Avenue', azimuth: 29, distanceM: 5 },
      { name: 'Street', azimuth: 119, distanceM: 5 },
    ])).toBe('NE')
  })

  test('quadrant needs one side on each axis', () => {
    expect(quadrant(['N', 'W'])).toBe('NW')
    expect(quadrant(['E', 'S'])).toBe('SE')
    expect(quadrant(['N', 'S'])).toBeNull()
  })

  test('two streets on one axis fall back to the bearing', () => {
    expect(cornerOf(ne, at, [
      { name: 'A', azimuth: 0, distanceM: 5 },
      { name: 'B', azimuth: 10, distanceM: 5 },
    ])).toBe('NE')
  })
})

describe('cornerLabel', () => {
  const entrance = { lng: -73.99, lat: 40.75 }
  const streets = [
    { name: 'Broadway', azimuth: 0, distanceM: 4 },
    { name: 'West 28th Street', azimuth: 90, distanceM: 6 },
  ]
  const near = (distanceM: number, s = streets) => ({
    lng: entrance.lng - 0.0001, lat: entrance.lat - 0.0001, distanceM, streets: s,
  })

  test('names the corner of a close intersection', () => {
    expect(cornerLabel({ entrance, intersection: near(20), nearestRoad: null }))
      .toBe('Broadway & West 28th Street · NE corner')
  })

  test('names only the streets a little further out', () => {
    expect(cornerLabel({ entrance, intersection: near(65), nearestRoad: null }))
      .toBe('Broadway & West 28th Street')
  })

  test('picks the two nearest streets at a three-way intersection', () => {
    const three = [...streets, { name: '6th Avenue', azimuth: 30, distanceM: 40 }]
    expect(cornerLabel({ entrance, intersection: near(20, three), nearestRoad: null }))
      .toBe('Broadway & West 28th Street · NE corner')
  })

  test('falls back to the street a mid-block entrance is on', () => {
    expect(cornerLabel({ entrance, intersection: near(120), nearestRoad: { name: 'Fulton Street', distanceM: 12 } }))
      .toBe('Fulton Street')
    expect(cornerLabel({ entrance, intersection: null, nearestRoad: { name: 'Fulton Street', distanceM: 60 } }))
      .toBeNull()
  })
})

describe('entranceAccess', () => {
  const mta = { entryAllowed: false, exitAllowed: true, source: 'mta-subway-entrances' }

  test('OSM entrance=exit and entrance=entry decide', () => {
    expect(entranceAccess('exit', null)).toEqual({ entryAllowed: false, exitAllowed: true, source: 'osm' })
    expect(entranceAccess('entry', mta)).toEqual({ entryAllowed: true, exitAllowed: false, source: 'osm' })
  })

  test('agency flags fill in where OSM is silent', () => {
    expect(entranceAccess('yes', mta)).toEqual(mta)
    expect(entranceAccess(null, null)).toEqual({ entryAllowed: null, exitAllowed: null, source: null })
  })
})
