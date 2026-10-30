import { describe, expect, test } from 'bun:test'
import { readFileSync } from 'fs'
import { join } from 'path'
import {
  getRouteHazards,
  POINT_PREDICATE,
  RouteHazardsRequestError,
  WAY_PREDICATE,
  type HazardSource,
  type PointRow,
  type WayRow,
} from './route-hazards.service'
import type { WayAtNode } from '../lib/route-hazards/classify'
import type { LonLat } from '../lib/route-hazards/route-path'

const at = (east: number, north = 0): LonLat => [-73.99 + east / (111_320 * Math.cos(40.75 * Math.PI / 180)), 40.75 + north / 111_320]
/** Eastbound, 600 m. */
const route = [at(0), at(300), at(600)]

function source(points: Omit<PointRow, 'piece'>[], ways: Omit<WayRow, 'pieces'>[] = [], waysAt: Record<number, WayAtNode[]> = {}): HazardSource {
  return {
    points: async pieces => points.flatMap(p => pieces.map((_, piece) => ({ ...p, piece }))),
    ways: async pieces => ways.map(w => ({ ...w, pieces: pieces.map((_, i) => i) })),
    waysAtNodes: async nodes => new Map(nodes.filter(n => waysAt[n.osmId]).map(n => [n.osmId, waysAt[n.osmId]])),
  }
}

const eastWest: WayAtNode = { azimuth: 90, highway: 'residential' }

describe('getRouteHazards', () => {
  test('places a signal on the route by its distance along it, once', async () => {
    const { hazards } = await getRouteHazards(
      { coordinates: route, profile: 'car' },
      source([{ osmId: 1, tags: { highway: 'traffic_signals' }, coordinate: at(150, 1) }]),
    )
    expect(hazards).toHaveLength(1)
    expect(hazards[0]).toMatchObject({ kind: 'traffic_signals', osm: 'node/1' })
    expect(hazards[0].distance).toBeCloseTo(150, 0)
  })

  test('a signal on the next street over is not on the route', async () => {
    const { hazards } = await getRouteHazards(
      { coordinates: route, profile: 'car' },
      source([{ osmId: 1, tags: { highway: 'traffic_signals' }, coordinate: at(150, 10) }]),
    )
    expect(hazards).toEqual([])
  })

  test('a stop sign facing the other way is not for the route', async () => {
    const tags = { highway: 'stop', direction: 'backward' }
    const run = (coordinates: LonLat[]) =>
      getRouteHazards({ coordinates, profile: 'car' }, source([{ osmId: 2, tags, coordinate: at(200) }], [], { 2: [eastWest] }))
    expect((await run(route)).hazards).toEqual([])
    expect((await run([...route].reverse())).hazards.map(h => h.kind)).toEqual(['stop'])
  })

  test('kinds a profile has no use for are left out', async () => {
    const points = [{ osmId: 3, tags: { highway: 'stop' }, coordinate: at(100) }]
    expect((await getRouteHazards({ coordinates: route, profile: 'foot' }, source(points))).hazards).toEqual([])
    expect((await getRouteHazards({ coordinates: route, profile: 'car', kinds: ['traffic_signals'] }, source(points))).hazards).toEqual([])
  })

  test('a way ridden becomes a stretch, joined across a split', async () => {
    const { hazards } = await getRouteHazards(
      { coordinates: route, profile: 'bike' },
      source([], [
        { osmId: 10, tags: { highway: 'residential', lit: 'no' }, coordinates: [at(100), at(250)] },
        { osmId: 11, tags: { highway: 'residential', lit: 'no' }, coordinates: [at(250), at(400)] },
      ]),
    )
    expect(hazards).toHaveLength(1)
    expect(hazards[0]).toMatchObject({ kind: 'unlit', osm: 'way/10' })
    expect(hazards[0].distance).toBeCloseTo(100, -1)
    expect(hazards[0].endDistance).toBeCloseTo(400, -1)
  })

  test('tram rails crossed at a shallow angle, not square on', async () => {
    const { hazards } = await getRouteHazards(
      { coordinates: route, profile: 'bike' },
      source([], [
        { osmId: 20, tags: { railway: 'tram' }, coordinates: [at(150, -10), at(250, 10)] },
        { osmId: 21, tags: { railway: 'tram' }, coordinates: [at(400, -10), at(400, 10)] },
      ]),
    )
    expect(hazards.map(h => [h.kind, h.osm])).toEqual([['tram_crossing', 'way/20']])
  })

  test('refuses a route too short to measure', async () => {
    expect(getRouteHazards({ coordinates: [at(0)], profile: 'car' }, source([]))).rejects.toBeInstanceOf(RouteHazardsRequestError)
  })
})

describe('index predicates', () => {
  const normalise = (s: string) => s.replace(/\s+/g, ' ').trim()
  const file = normalise(readFileSync(join(import.meta.dir, '../../import/create-route-hazard-indexes.sql'), 'utf8'))

  test('the indexes are built over exactly the predicates the queries use', () => {
    expect(file).toContain(`WHERE ${normalise(POINT_PREDICATE)};`)
    expect(file).toContain(`WHERE ${normalise(WAY_PREDICATE)};`)
  })
})
