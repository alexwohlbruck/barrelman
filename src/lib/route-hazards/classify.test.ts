import { describe, expect, test } from 'bun:test'
import {
  appliesTo,
  controlsRoute,
  facesRoute,
  maxheightMeters,
  parseDirection,
  pointHazards,
  wayHazards,
  type WayAtNode,
} from './classify'

describe('pointHazards', () => {
  test('signals read their own direction tag before the generic one', () => {
    expect(pointHazards({ highway: 'traffic_signals', 'traffic_signals:direction': 'forward' })[0].directionKey).toBe('traffic_signals:direction')
    expect(pointHazards({ highway: 'traffic_signals' })[0].directionKey).toBe('direction')
  })

  test('an all-way stop says so', () => {
    expect(pointHazards({ highway: 'stop', stop: 'all' })[0]).toMatchObject({ kind: 'stop', detail: 'all' })
  })

  test('a raised crosswalk is traffic calming of its own sort', () => {
    expect(pointHazards({ highway: 'crossing', traffic_calming: 'table' })).toEqual([
      { kind: 'traffic_calming', reach: 3, detail: 'raised_crossing' },
    ])
  })

  test('school zone and flooding signs get their own kinds; other hazards keep their value', () => {
    expect(pointHazards({ hazard: 'school_zone' })[0].kind).toBe('school_zone')
    expect(pointHazards({ hazard: 'flooding' })[0].kind).toBe('flood_prone')
    expect(pointHazards({ hazard: 'animal_crossing' })[0]).toMatchObject({ kind: 'hazard', detail: 'animal_crossing' })
  })

  test('cameras and signs may stand at the roadside', () => {
    expect(pointHazards({ highway: 'speed_camera', maxspeed: '50' })[0]).toMatchObject({ reach: 15, detail: '50' })
  })
})

describe('wayHazards', () => {
  test('only embedded tram rails are a hazard to ride along', () => {
    expect(wayHazards({ railway: 'tram', embedded: 'yes' })).toEqual([{ kind: 'tram_tracks' }])
    expect(wayHazards({ railway: 'tram' })).toEqual([])
  })

  test('a road with no sidewalk on either side', () => {
    expect(wayHazards({ highway: 'residential', sidewalk: 'no' })).toEqual([{ kind: 'no_sidewalk', detail: 'residential' }])
    expect(wayHazards({ highway: 'residential', 'sidewalk:left': 'no', 'sidewalk:right': 'no' })).toHaveLength(1)
    expect(wayHazards({ highway: 'residential', 'sidewalk:left': 'no', 'sidewalk:right': 'yes' })).toEqual([])
    expect(wayHazards({ highway: 'footway', sidewalk: 'no' })).toEqual([])
  })

  test('several tags on one way give several hazards', () => {
    expect(wayHazards({ highway: 'path', lit: 'no', flood_prone: 'yes' }).map(h => h.kind)).toEqual(['unlit', 'flood_prone'])
  })
})

describe('maxheightMeters', () => {
  test.each([
    ['3.5', 3.5],
    ['3.5 m', 3.5],
    [`12'6"`, 3.81],
    [`13'`, 3.96],
    ['below_default', 0],
    ['default', null],
    ['none', null],
  ])('%s', (value, expected) => {
    expect(maxheightMeters(value)).toBe(expected)
  })
})

describe('profiles', () => {
  test('each gets what matters to it', () => {
    expect(appliesTo('tram_tracks', 'bike')).toBe(true)
    expect(appliesTo('tram_tracks', 'car')).toBe(false)
    expect(appliesTo('speed_camera', 'car')).toBe(true)
    expect(appliesTo('no_sidewalk', 'foot')).toBe(true)
    expect(appliesTo('stop', 'foot')).toBe(false)
  })
})

describe('facing', () => {
  const eastbound: WayAtNode = { azimuth: 90, highway: 'residential' }

  test('forward and backward are along the way the node is on', () => {
    expect(facesRoute(parseDirection('forward'), 90, eastbound)).toBe(true)
    expect(facesRoute(parseDirection('forward'), 270, eastbound)).toBe(false)
    expect(facesRoute(parseDirection('backward'), 270, eastbound)).toBe(true)
  })

  test('a compass direction is the way the sign faces, toward oncoming traffic', () => {
    expect(facesRoute(parseDirection('W'), 90, null)).toBe(true)
    expect(facesRoute(parseDirection('270'), 270, null)).toBe(false)
  })

  test('no direction, or one that cannot be placed, applies both ways', () => {
    expect(facesRoute(parseDirection(undefined), 0, null)).toBe(true)
    expect(facesRoute(parseDirection('forward'), 0, null)).toBe(true)
    expect(parseDirection('both')).toEqual({ type: 'any' })
  })
})

describe('controlsRoute', () => {
  const primary = (azimuth: number): WayAtNode => ({ azimuth, highway: 'primary' })
  const residential = (azimuth: number): WayAtNode => ({ azimuth, highway: 'residential' })

  test('a junction sign is for the minor road', () => {
    const junction = [primary(90), primary(90), residential(0), residential(0)]
    expect(controlsRoute(junction, 0)).toBe(true)
    expect(controlsRoute(junction, 90)).toBe(false)
  })

  test('a sign before the junction, or between equal roads, counts', () => {
    expect(controlsRoute([residential(0), residential(0)], 0)).toBe(true)
    expect(controlsRoute([residential(90), residential(0)], 90)).toBe(true)
  })

  test('footways through the node are not crossing roads', () => {
    expect(controlsRoute([primary(90), { azimuth: 0, highway: 'footway' }], 90)).toBe(true)
  })
})
