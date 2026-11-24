import { describe, expect, it } from 'bun:test'
import { STREET_EDGE_STRIDE, STREET_FLAGS } from '../../types/offline-package.types'
import { buildStreetGraph, streetFlags, type StreetWay } from './street-graph'

const bounds: [number, number, number, number] = [-74, 40, -73, 41]

function decode(pkg: ReturnType<typeof buildStreetGraph>) {
  const nodes: [number, number][] = []
  let x = 0
  let y = 0
  for (let i = 0; i < pkg.nodes.length; i += 2) {
    x += pkg.nodes[i]
    y += pkg.nodes[i + 1]
    nodes.push([x / 1e6, y / 1e6])
  }
  const edges = []
  for (let i = 0; i < pkg.edges.length; i += STREET_EDGE_STRIDE) {
    const [from, to, length, flags, name, shape] = pkg.edges.slice(i, i + STREET_EDGE_STRIDE)
    edges.push({ from, to, length, flags, name: name >= 0 ? pkg.names[name] : null, shape })
  }
  return { nodes, edges }
}

describe('buildStreetGraph', () => {
  // A street running east, a side street meeting it in the middle, and a
  // bridge crossing above it with no shared vertex.
  const ways: StreetWay[] = [
    { id: 'way/1', tags: { highway: 'residential', name: 'Main St' }, coords: [[-73.5, 40.5], [-73.499, 40.5], [-73.498, 40.5], [-73.497, 40.5]] },
    { id: 'way/2', tags: { highway: 'residential', name: 'Side St' }, coords: [[-73.499, 40.5], [-73.499, 40.501]] },
    { id: 'way/3', tags: { highway: 'footway', bridge: 'yes' }, coords: [[-73.4975, 40.4995], [-73.4975, 40.5005]] },
    { id: 'way/4', tags: { highway: 'motorway' }, coords: [[-73.5, 40.6], [-73.4, 40.6]] },
  ]
  const pkg = buildStreetGraph(ways, { bounds })
  const { nodes, edges } = decode(pkg)

  it('splits a street where a side street meets it', () => {
    const main = edges.filter(e => e.name === 'Main St')
    expect(main.length).toBe(2)
    const junction = nodes.findIndex(([lng, lat]) => lng === -73.499 && lat === 40.5)
    expect(junction).toBeGreaterThanOrEqual(0)
    expect(main[0].to).toBe(junction)
    expect(main[1].from).toBe(junction)
    expect(main[1].shape).toBe(1)
    expect(edges.find(e => e.name === 'Side St')!.from).toBe(junction)
  })

  it('leaves a bridge unconnected to the street under it', () => {
    const bridge = edges.find(e => e.flags & STREET_FLAGS.CAR_FREE)!
    const touches = (e: (typeof edges)[number], n: number) => e.from === n || e.to === n
    expect(edges.filter(e => e !== bridge && (touches(e, bridge.from) || touches(e, bridge.to)))).toEqual([])
  })

  it('drops ways nobody may walk or ride, and measures length in decimetres', () => {
    expect(edges.length).toBe(4)
    expect(nodes.length).toBe(6)
    const side = edges.find(e => e.name === 'Side St')!
    expect(side.length).toBeGreaterThan(1100)
    expect(side.length).toBeLessThan(1120)
  })

  it('round-trips shape points through the delta encoding', () => {
    expect(pkg.shapes).toEqual([1000, 0])
  })
})

describe('streetFlags', () => {
  const { FOOT, BIKE, BIKE_FORWARD_ONLY, BIKE_BACKWARD_ONLY, STEPS, BIKE_LANE, CAR_FREE, BUSY } = STREET_FLAGS

  it('makes one-way streets one-way for bikes only', () => {
    expect(streetFlags({ highway: 'residential', oneway: 'yes' })).toBe(FOOT | BIKE | BIKE_FORWARD_ONLY)
    expect(streetFlags({ highway: 'residential', oneway: '-1' })).toBe(FOOT | BIKE | BIKE_BACKWARD_ONLY)
    expect(streetFlags({ highway: 'residential', oneway: 'yes', 'oneway:bicycle': 'no' })).toBe(FOOT | BIKE)
    expect(streetFlags({ highway: 'residential', oneway: 'yes', cycleway: 'opposite_lane' })).toBe(FOOT | BIKE | BIKE_LANE)
  })

  it('applies access tags', () => {
    expect(streetFlags({ highway: 'footway' })).toBe(FOOT | CAR_FREE)
    expect(streetFlags({ highway: 'footway', bicycle: 'yes' })).toBe(FOOT | BIKE | CAR_FREE)
    expect(streetFlags({ highway: 'steps' })).toBe(FOOT | STEPS | CAR_FREE)
    expect(streetFlags({ highway: 'service', access: 'private' })).toBe(0)
    expect(streetFlags({ highway: 'service', access: 'private', foot: 'yes' })).toBe(FOOT)
    expect(streetFlags({ highway: 'cycleway', foot: 'no' })).toBe(BIKE | BIKE_LANE | CAR_FREE)
    expect(streetFlags({ highway: 'trunk' })).toBe(0)
    expect(streetFlags({ highway: 'trunk', sidewalk: 'both' })).toBe(FOOT | BUSY)
    expect(streetFlags({ highway: 'primary', 'cycleway:right': 'track' })).toBe(FOOT | BIKE | BIKE_LANE | BUSY)
    expect(streetFlags({ highway: 'motorway', foot: 'yes' })).toBe(0)
  })
})
