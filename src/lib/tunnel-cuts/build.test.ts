import { describe, expect, test } from 'bun:test'
import { along, boundsOf, type Point } from '../bridge-mesh/decks'
import { classOf, groundPoints, openings, solveCuts, type Surroundings, type Way } from './build'
import { HEADROOM, ROOF } from './tunnels'

// About a metre in mercator units at Charlotte's latitude.
const M = 1 / 32780000
const [x0, y0] = [0.2754, 0.4006]
const at = (east: number, north = 0): Point => [x0 + east * M, y0 - north * M]
const eastOf = (p: Point) => (p[0] - x0) / M
const WORLD = { minX: 0, minY: 0, maxX: 1, maxY: 1 }

/** Flat at 200 m west of the origin, rising 20% east of it: a hillside a road tunnels into. */
const hill = (p: Point) => 200 + Math.max(0, eastOf(p)) * 0.2

const way = (points: Point[], extra: Partial<Way> = {}): Way => ({ points, cls: 'secondary', layer: 0, brunnel: null, ...extra })
const tunnel = (points: Point[], extra: Partial<Way> = {}) => way(points, { brunnel: 'tunnel', layer: -1, ...extra })
const scene = (ways: Way[], extra: Partial<Surroundings> = {}): Surroundings => ({ ways, surfaces: [], buildings: [], walls: [], ...extra })

const solved = (s: Surroundings, owns = (_: Point) => true) => solveCuts(openings(s, WORLD), hill, owns)

describe('solveCuts', () => {
  const road = scene([way([at(-200), at(0)]), tunnel([at(0), at(150)])])

  test('digs the cut down to where a road tunnels into a hillside', () => {
    const cuts = solved(road)
    expect(cuts).toHaveLength(1)
    const [{ kind, points, at: i, cut }] = cuts
    expect(kind).toBe('road')
    expect(eastOf(points[i])).toBeCloseTo(0, 0)
    // Headroom under the hillside over the bore, reached down a ramp from the west.
    expect(cut.floor[i]).toBeLessThan(hill(at(20)) - HEADROOM - ROOF + 0.01)
    expect(eastOf(points[cut.open])).toBeLessThan(-5)
    expect(cut.floor[cut.open]).toBeCloseTo(200, 0)
  })

  test('keeps the line from just before the cut opens, every array a value per vertex', () => {
    const [{ points, at: i, cut }] = solved(road)
    expect(cut.open).toBeLessThanOrEqual(1)
    for (const values of [cut.floor, ...cut.walls]) expect(values).toHaveLength(points.length)
    expect(along(points.slice(i)).at(-1)).toBeGreaterThan(30)
  })

  test('names a portal by where it is, the same however often it is built', () => {
    expect(solved(road)[0].id).toBe(solved(road)[0].id)
    const moved = scene([way([at(-200), at(5)]), tunnel([at(5), at(150)])])
    expect(solved(moved)[0].id).not.toBe(solved(road)[0].id)
  })

  test('leaves a cut to the box that owns its portal', () => {
    expect(solved(road, p => eastOf(p) > 10)).toHaveLength(0)
  })

  test('digs nothing for a passage under a building, mapped with no layer', () => {
    expect(solved(scene([way([at(-200), at(0)]), tunnel([at(0), at(150)], { layer: 0 })]))).toHaveLength(0)
  })

  test('takes no portal where a tunnel runs on past what was read', () => {
    const read = boundsOf([at(-250), at(100)])
    expect(solveCuts(openings(road, read), hill, () => true)).toHaveLength(1)
    const inward = scene([way([at(250), at(150)]), tunnel([at(150), at(0)])])
    expect(solveCuts(openings(inward, read), hill, () => true)).toHaveLength(0)
  })

  test('fits the cut to the carriageway surfaced around the road', () => {
    const surface = [at(-200, 4), at(0, 4), at(0, -3), at(-200, -3), at(-200, 4)]
    const [{ edges }] = solved(scene(road.ways, { surfaces: [surface] }))
    expect(edges[0]).toBeCloseTo(3, 0)
    expect(edges[1]).toBeCloseTo(4, 0)
  })
})

describe('groundPoints', () => {
  test('spans every vertex a cut is solved along', () => {
    const found = openings(scene([way([at(-200), at(0)]), tunnel([at(0), at(150)])]), WORLD)
    const box = boundsOf(groundPoints(found))
    for (const p of [at(-150), at(0), at(40)]) {
      expect(p[0]).toBeGreaterThan(box.minX)
      expect(p[0]).toBeLessThan(box.maxX)
    }
  })
})

describe('classOf', () => {
  test('classes ways as the basemap does', () => {
    expect(classOf('motorway_link', null)).toBe('motorway')
    expect(classOf('residential', null)).toBe('minor')
    expect(classOf('steps', null)).toBe('path')
    expect(classOf(null, 'subway')).toBe('transit')
    expect(classOf('track', null)).toBeNull()
    expect(classOf(null, 'platform')).toBeNull()
  })
})
