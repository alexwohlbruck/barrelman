import { describe, expect, test } from 'bun:test'
import { readFileSync } from 'fs'
import { join } from 'path'
import { getRouteContext, NAMED_ROAD_PREDICATE, OVERPASS_PREDICATE, PARK_PREDICATE, type RouteContextSource } from './route-context.service'
import type { LonLat } from '../lib/route-hazards/route-path'

const at = (east: number, north = 0): LonLat => [-73.99 + east / (111_320 * Math.cos(40.75 * Math.PI / 180)), 40.75 + north / 111_320]
const coordinates = [at(0), at(300), at(600)]
const distances = [0, 300, 600]

function source(overrides: Partial<RouteContextSource> = {}): RouteContextSource & { asked: string[] } {
  const asked: string[] = []
  return {
    asked,
    crossedRoads: async () => (asked.push('crossedRoads'), []),
    structures: async () => (asked.push('structures'), []),
    wayLevels: async () => (asked.push('wayLevels'), new Map()),
    states: async () => (asked.push('states'), []),
    parks: async () => (asked.push('parks'), []),
    ...overrides,
  }
}

describe('getRouteContext', () => {
  test('looks for parks on foot only', async () => {
    const car = source()
    await getRouteContext({ coordinates, distances, profile: 'car' }, car)
    expect(car.asked).not.toContain('parks')
    const foot = source()
    await getRouteContext({ coordinates, distances, profile: 'foot' }, foot)
    expect(foot.asked).toContain('parks')
  })

  test('reads the route way\'s own layer under an overpass', async () => {
    const bridge = { osmId: 9, firstSegment: 0, lastSegment: 1, tags: { highway: 'primary', bridge: 'yes' }, coordinates: [at(150, -50), at(150, 50)] }
    const details = { osm_way_id: [[0, 2, 42]], road_environment: [[0, 2, 'road']] }
    const context = (level: number) => getRouteContext({ coordinates, distances, details }, source({
      structures: async () => [bridge],
      wayLevels: async ids => new Map(ids.map(id => [id, level])),
    }))
    expect((await context(0)).overpasses).toHaveLength(1)
    expect((await context(1)).overpasses).toEqual([])
  })

  test('a failing lookup leaves only its own part out', async () => {
    const { overpasses, states } = await getRouteContext({ coordinates, distances }, source({
      structures: async () => { throw new Error('timeout') },
      states: async () => [{ id: 'relation/1', name: 'New York', code: 'US-NY', piece: 0, covers: true }],
    }))
    expect(overpasses).toEqual([])
    expect(states[0]).toMatchObject({ name: 'New York', code: 'US-NY' })
  })
})

describe('index predicates', () => {
  const normalise = (s: string) => s.replace(/\s+/g, ' ').trim()
  const file = normalise(readFileSync(join(import.meta.dir, '../../import/create-route-context-indexes.sql'), 'utf8'))
  test('match the queries, or the planner cannot use the indexes', () => {
    expect(file).toContain(`WHERE ${normalise(NAMED_ROAD_PREDICATE)};`)
    expect(file).toContain(`WHERE ${normalise(OVERPASS_PREDICATE)};`)
    expect(file).toContain(`WHERE ${normalise(PARK_PREDICATE)};`)
  })
})
