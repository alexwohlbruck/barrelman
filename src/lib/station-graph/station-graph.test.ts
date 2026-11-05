import { describe, expect, test } from 'bun:test'
import { StationGraph } from './graph'
import { DEFAULT_PARAMS } from './params'
import { solveComplex } from './solve'
import { estimatePair, estimateStreet, straightLinePair, straightLineStreet } from './estimate'
import { agencyStationTimes, agencyTimesByOsm } from './agency'
import { clusterStations, assignToComplexes, transferLinks } from './complexes'
import { platformsForStop, stopPairTime } from './stops'
import { platformRefs, levelOf, levelsOf } from './tags'
import { vertexKey } from './geometry'
import type { OsmNode, OsmWay, StationPath, Tags } from './types'

// Small hand-drawn complexes in metres around a point in Manhattan.
const LON0 = -73.98, LAT0 = 40.75
const KX = Math.cos((LAT0 * Math.PI) / 180) * 111320
const pt = (x: number, y: number) => [Math.round((LON0 + x / KX) * 1e7) / 1e7, Math.round((LAT0 + y / 110540) * 1e7) / 1e7]
const line = (id: string, tags: Tags, pts: Array<[number, number]>): OsmWay =>
  ({ id, tags, geometry: { type: 'LineString', coordinates: pts.map(([x, y]) => pt(x, y)) } })
const rect = (id: string, tags: Tags, x0: number, y0: number, x1: number, y1: number): OsmWay =>
  ({ id, tags, geometry: { type: 'Polygon', coordinates: [[[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]].map(([x, y]) => pt(x, y))] } })
const node = (id: string, tags: Tags, x: number, y: number): OsmNode => {
  const [lon, lat] = pt(x, y)
  return { id, tags, lon, lat }
}
const platform = (level: string) => ({ railway: 'platform', subway: 'yes', level, location: 'underground' })
const stair = (level: string) => ({ highway: 'steps', level })

function row(paths: StationPath[], from: string, to: string, profile = 'foot') {
  return paths.find((p) => p.fromId === from && p.toId === to && p.profile === profile)
}
const kinds = (p?: StationPath) => p?.steps?.map((s) => s.type) ?? []

describe('level-aware vertex split', () => {
  // Two stacked platforms drawn on the same footprint share every corner node.
  const ways = [
    rect('way/1', platform('-1'), 0, 0, 100, 8),
    rect('way/2', platform('-2'), 0, 0, 100, 8),
    line('way/3', stair('-1;-2'), [[40, 4], [55, 4]]),
  ]

  test('a node shared by ways on disjoint levels is not a junction', () => {
    const g = new StationGraph({ ways, nodes: [] }, DEFAULT_PARAMS)
    const [lon, lat] = pt(0, 0)
    expect(g.splitKeys.has(vertexKey(lon, lat))).toBe(true)
  })

  test('the transfer climbs the stair instead of crossing at the glued corner', () => {
    const r = solveComplex({ platformIds: ['way/1', 'way/2'], entranceIds: [], ways, nodes: [] })
    const p = row(r.paths, 'way/1', 'way/2')!
    expect(p.source).toBe('graph')
    expect(kinds(p)).toContain('stairs')
    expect(p.levels).toBe(1)
  })
})

describe('level-aware snap', () => {
  // A -1 corridor runs from the -1 platform to the top of a stair whose loose
  // bottom end lies over both stacked platforms. It may join only the -2 one.
  const ways = [
    rect('way/1', platform('-1'), 0, 0, 100, 8),
    rect('way/2', platform('-2'), 0, -2, 100, 6),
    line('way/3', { highway: 'footway', indoor: 'yes', level: '-1' }, [[20, 4], [20, 30], [60, 30]]),
    line('way/4', stair('-1;-2'), [[60, 30], [60, 3]]),
  ]
  const r = solveComplex({ platformIds: ['way/1', 'way/2'], entranceIds: [], ways, nodes: [] })

  test('the stair end over the stacked footprint joins only its own level', () => {
    const p = row(r.paths, 'way/1', 'way/2')!
    expect(p.source).toBe('graph')
    expect(kinds(p)).toContain('stairs')
    expect(p.metres).toBeGreaterThan(60)
  })

  test('a stair with both ends loose is still climbed, whichever end is the bottom', () => {
    const loose = solveComplex({
      platformIds: ['way/1', 'way/2'], entranceIds: [], nodes: [],
      ways: [rect('way/1', platform('-1'), 0, 0, 100, 8), rect('way/2', platform('-2'), 0, -2, 100, 6), line('way/5', stair('-1;-2'), [[30, 3], [45, 3]])],
    })
    expect(kinds(row(loose.paths, 'way/1', 'way/2'))).toContain('stairs')
    expect(kinds(row(loose.paths, 'way/2', 'way/1'))).toContain('stairs')
  })
})

describe('surfacing needs a climb', () => {
  const plat = rect('way/1', platform('-1'), 0, 0, 100, 8)
  const entrance = node('node/9', { railway: 'subway_entrance' }, 50, 40)

  test('an indoor footway from an underground platform is not a way out', () => {
    const r = solveComplex({
      platformIds: ['way/1'], entranceIds: ['node/9'], nodes: [entrance],
      ways: [plat, line('way/2', { highway: 'footway', indoor: 'yes' }, [[50, 8], [50, 40]])],
    })
    expect(row(r.paths, 'way/1', 'node/9')!.source).toBe('estimated')
  })

  test('with a stair on the way, the path is on the graph and surfaces', () => {
    const r = solveComplex({
      platformIds: ['way/1'], entranceIds: ['node/9'], nodes: [entrance],
      ways: [plat, line('way/2', stair('-1;0'), [[50, 8], [50, 25]]), line('way/3', { highway: 'footway', indoor: 'yes', level: '0' }, [[50, 25], [50, 40]])],
    })
    const out = row(r.paths, 'way/1', 'node/9')!
    expect(out.source).toBe('graph')
    expect(out.surfaces).toBe(true)
    expect(kinds(out)).toContain('stairs')
    const back = row(r.paths, 'node/9', 'way/1')!
    expect(back.source).toBe('graph')
    expect(back.geometry![0]).toEqual(pt(50, 40) as [number, number])
  })
})

describe('bridged (T1)', () => {
  // Two platforms whose stairs end in a mezzanine nobody mapped.
  const stairsTo = (gap: number): OsmWay[] => [
    rect('way/1', platform('-2'), 0, 0, 100, 8),
    rect('way/2', platform('-2'), 0, 60, 100, 68),
    line('way/3', stair('-2;-1'), [[50, 8], [50, 20]]),
    line('way/4', stair('-2;-1'), [[50 + gap, 60], [50 + gap, 48]]),
    line('way/5', { highway: 'footway', indoor: 'yes', level: '-1' }, [[50, 20], [50, 30]]),
    line('way/6', { highway: 'footway', indoor: 'yes', level: '-1' }, [[50 + gap, 48], [50 + gap, 40]]),
  ]

  test('a gap of up to 30 m is bridged, and the bridge is marked', () => {
    const r = solveComplex({ platformIds: ['way/1', 'way/2'], entranceIds: [], ways: stairsTo(0), nodes: [] })
    const p = row(r.paths, 'way/1', 'way/2')!
    expect(p.source).toBe('bridged')
    expect(kinds(p)).toContain('bridge')
    expect(p.geometry!.length).toBeGreaterThan(2)
  })

  test('a wider gap is estimated, with no line or steps', () => {
    const r = solveComplex({ platformIds: ['way/1', 'way/2'], entranceIds: [], ways: stairsTo(80), nodes: [] })
    const p = row(r.paths, 'way/1', 'way/2')!
    expect(p.source).toBe('estimated')
    expect(p.geometry).toBeNull()
    expect(p.steps).toBeNull()
  })

  test('two platforms are never bridged across the tracks', () => {
    const r = solveComplex({
      platformIds: ['way/1', 'way/2'], entranceIds: [], nodes: [],
      ways: [rect('way/1', platform('-1'), 0, 0, 100, 8), rect('way/2', platform('-1'), 0, 13, 100, 21)],
    })
    expect(row(r.paths, 'way/1', 'way/2')!.source).toBe('estimated')
  })
})

describe('elevated station and lifts', () => {
  const plat = rect('way/1', { railway: 'platform', train: 'yes', layer: '1', level: '1' }, 0, 0, 100, 8)
  const entrance = node('node/9', { railway: 'train_station_entrance' }, 50, 30)

  test('stairs down from an elevated platform reach the street; the wheelchair profile has no way', () => {
    const r = solveComplex({
      platformIds: ['way/1'], entranceIds: ['node/9'], nodes: [entrance],
      ways: [plat, line('way/2', stair('1;0'), [[50, 8], [50, 30]])],
    })
    const p = row(r.paths, 'way/1', 'node/9')!
    expect(p.source).toBe('graph')
    expect(kinds(p)).toContain('stairs')
    expect(row(r.paths, 'way/1', 'node/9', 'wheelchair')).toBeUndefined()
  })

  test('a lift drawn as its shaft costs a ride and serves the wheelchair profile', () => {
    const shaft = rect('way/7', { highway: 'elevator', level: '0;1', indoor: 'yes' }, 48, 6, 52, 10)
    const exit = line('way/8', { highway: 'footway', indoor: 'yes', level: '0' }, [[52, 10], [50, 30]])
    const r = solveComplex({ platformIds: ['way/1'], entranceIds: ['node/9'], nodes: [entrance], ways: [plat, shaft, exit] })
    const p = row(r.paths, 'way/1', 'node/9', 'wheelchair')!
    expect(p.source).toBe('graph')
    expect(kinds(p)).toContain('elevator')
    expect(p.seconds).toBeGreaterThanOrEqual(DEFAULT_PARAMS.costs.elevatorS * DEFAULT_PARAMS.costs.scale)
  })
})

describe('tier precedence', () => {
  const ways = [rect('way/1', platform('-1'), 0, 0, 100, 8), rect('way/2', platform('-2'), 0, 40, 100, 48)]

  test('agency outranks an estimate', () => {
    const r = solveComplex({ platformIds: ['way/1', 'way/2'], entranceIds: [], ways, nodes: [], agency: new Map([['way/1>way/2', 77]]) })
    const p = row(r.paths, 'way/1', 'way/2')!
    expect(p.source).toBe('agency')
    expect(p.seconds).toBe(77)
    expect(row(r.paths, 'way/2', 'way/1')!.source).toBe('estimated')
  })

  test('the graph outranks agency, and every row carries its confidence', () => {
    const linked = [...ways, line('way/3', stair('-1;-2'), [[50, 8], [50, 40]])]
    const r = solveComplex({ platformIds: ['way/1', 'way/2'], entranceIds: [], ways: linked, nodes: [], agency: new Map([['way/1>way/2', 77]]) })
    const p = row(r.paths, 'way/1', 'way/2')!
    expect(p.source).toBe('graph')
    expect(p.uncertaintyS).toBe(DEFAULT_PARAMS.uncertaintyS.graph.pair)
    expect(p.confidence).toBeGreaterThan(0)
    expect(p.confidence).toBeLessThanOrEqual(1)
    expect(r.report.tiers.pair.graph).toBe(2)
  })

  test('the best car position is where the stair meets the platform', () => {
    const linked = [...ways, line('way/3', stair('-1;-2'), [[90, 8], [90, 40]])]
    const r = solveComplex({ platformIds: ['way/1', 'way/2'], entranceIds: [], ways: linked, nodes: [] })
    const p = row(r.paths, 'way/1', 'way/2')!
    const bearing = r.platforms.find((x) => x.id === 'way/1')!.axisBearing
    // The axis runs east (90°), so the stair at x=90 sits near the head.
    expect(bearing).toBeCloseTo(90, 0)
    expect(p.bestPosition!).toBeGreaterThan(0.8)
    expect(p.bestSeconds!).toBeLessThan(p.seconds)
  })
})

describe('estimates', () => {
  test('T2 and T3 follow the fitted formulas', () => {
    const P = DEFAULT_PARAMS
    expect(estimatePair(P, { edgeM: 10, centroidM: 50, levelA: -1, levelB: -2 })).toBeCloseTo(23.24 + 3.62 + 46.95 + 8.55, 1)
    expect(estimatePair(P, { edgeM: 0, centroidM: 10, levelA: -1, levelB: -1 })).toBeCloseTo(23.24 + 9.39, 1)
    expect(estimatePair(P, { edgeM: 20, centroidM: 30, levelA: null, levelB: -1 })).toBeCloseTo(23.24 + 7.24 + 28.17 + 54.05, 1)
    expect(estimateStreet(P, { edgeM: 40, level: -2 })).toBeCloseTo(41.92 + 34.16 + 23.68, 1)
    expect(estimateStreet(P, { edgeM: 40, level: null })).toBeCloseTo(41.92 + 34.16 + 11.84, 1)
    expect(straightLinePair(P, 100)).toBeCloseTo(164.83, 1)
    expect(straightLineStreet(P, 100)).toBeCloseTo(128.6, 1)
  })
})

describe('tags', () => {
  test('levels, ranges and refs', () => {
    expect([...levelsOf({ level: '-2--1' })]).toEqual([-2, -1])
    expect([...levelsOf({ level: '0;-1' })]).toEqual([0, -1])
    expect(levelOf({ layer: '-2' })).toBe(-2)
    expect(levelOf({ tunnel: 'yes' })).toBe(-1)
    expect(platformRefs({ name: 'Times Square–42nd Street (N,Q,R,W)' })).toEqual(['N', 'Q', 'R', 'W'])
    expect(platformRefs({ route_ref: 'U2;U5', name: 'x (1)' })).toEqual(['U2', 'U5'])
  })
})

describe('complexes', () => {
  test('stations chain into complexes within the radius', () => {
    const at = (id: string, x: number) => { const [lon, lat] = pt(x, 0); return { id, name: id === 'node/2' ? 'B' : 'A', lon, lat } }
    const cx = clusterStations([at('node/3', 0), at('node/2', 120), at('node/1', 240), at('node/4', 600)], 150)
    expect(cx.length).toBe(2)
    const big = cx.find((c) => c.members.length === 3)!
    expect(big.id).toBe('node/1')
    expect(big.name).toBe('A')
  })

  test('an agency transfer joins stations further apart than the radius', () => {
    const at = (id: string, x: number) => { const [lon, lat] = pt(x, 0); return { id, name: null, lon, lat } }
    const els = [at('node/1', 0), at('node/2', 260), at('node/3', 900)]
    const [fl, fb] = pt(5, 0), [tl, tb] = pt(255, 0), [xl, xb] = pt(890, 0)
    const links = transferLinks(els, [{ fromLon: fl, fromLat: fb, toLon: tl, toLat: tb }, { fromLon: fl, fromLat: fb, toLon: xl + 1, toLat: xb }], 150)
    expect(links).toEqual([['node/1', 'node/2']])
    expect(clusterStations(els, 150, links).map((c) => c.members.length).sort()).toEqual([1, 2])
  })

  test('a platform belongs to the complex whose station is nearest its geometry', () => {
    const at = (id: string, x: number) => { const [lon, lat] = pt(x, 0); return { id, name: null, lon, lat } }
    const cx = clusterStations([at('node/1', 0), at('node/2', 400)], 150)
    const long = rect('way/1', platform('-1'), 200, -4, 390, 4)
    const out = assignToComplexes([long], cx, 150)
    expect(out.get(long)!.id).toBe('node/2')
  })
})

describe('agency pathways', () => {
  const [l0, b0] = pt(0, 0), [l1, b1] = pt(0, 40), [le, be] = pt(30, 20)
  const feed: Record<'stops' | 'pathways' | 'levels', Array<Record<string, string>>> = {
    stops: [
      { stop_id: 'A', location_type: '0', stop_lon: String(l0), stop_lat: String(b0), level_id: 'L1' },
      { stop_id: 'B', location_type: '0', stop_lon: String(l1), stop_lat: String(b1), level_id: 'L2' },
      { stop_id: 'E', location_type: '2', stop_lon: String(le), stop_lat: String(be), level_id: 'L0' },
      { stop_id: 'N', location_type: '3', stop_lon: String(le), stop_lat: String(be), level_id: 'L1' },
    ],
    pathways: [
      { from_stop_id: 'A', to_stop_id: 'N', pathway_mode: '1', is_bidirectional: '1', traversal_time: '30' },
      { from_stop_id: 'N', to_stop_id: 'B', pathway_mode: '2', is_bidirectional: '1', stair_count: '50' },
      { from_stop_id: 'N', to_stop_id: 'E', pathway_mode: '1', is_bidirectional: '0', length: '13' },
      { from_stop_id: 'E', to_stop_id: 'N', pathway_mode: '4', is_bidirectional: '0', traversal_time: '20' },
    ],
    levels: [{ level_id: 'L1', level_index: '-1' }, { level_id: 'L2', level_index: '-2' }, { level_id: 'L0', level_index: '0' }],
  }

  test('times come from traversal_time, stair_count or length', () => {
    const { times } = agencyStationTimes(feed)
    const t = (a: string, b: string) => times.find((x) => x.from === a && x.to === b)?.seconds
    expect(t('A', 'B')).toBe(60)
    expect(t('A', 'E')).toBe(40)
    expect(t('E', 'B')).toBe(50)
  })

  test('quays map onto the OSM platforms they stand on', () => {
    const shapes = [
      { id: 'way/1', level: -1, rings: [[pt(-5, -3), pt(5, -3), pt(5, 3), pt(-5, 3), pt(-5, -3)]], area: true },
      { id: 'way/2', level: -2, rings: [[pt(-5, 37), pt(5, 37), pt(5, 43), pt(-5, 43), pt(-5, 37)]], area: true },
    ]
    const byOsm = agencyTimesByOsm(agencyStationTimes(feed), shapes, [{ id: 'node/9', lon: le, lat: be }])
    expect(byOsm.get('way/1>way/2')).toBe(60)
    expect(byOsm.get('way/1>node/9')).toBe(40)
  })
})

describe('GTFS stops onto platforms', () => {
  const c = (platformId: string, distanceM: number, refs: string[] = [], gtfsStopIds: string[] = []) =>
    ({ platformId, complexId: 'node/1', distanceM, refs, gtfsStopIds })

  test('a platform naming the stop wins, then the platforms serving its routes', () => {
    const stop = { stopId: 'R16N', parentId: 'R16', routes: ['N', 'Q'] }
    expect(platformsForStop(stop, [c('way/1', 20, ['N', 'Q'], ['R16N']), c('way/2', 10, ['N', 'Q'])])).toEqual(['way/1'])
    expect(platformsForStop(stop, [c('way/1', 20, ['N', 'Q']), c('way/2', 10, ['N', 'Q']), c('way/3', 5, ['7'])]).sort()).toEqual(['way/1', 'way/2'])
  })

  test('another line\'s platform is never taken; an unlabelled one only when close', () => {
    const stop = { stopId: '127N', parentId: '127', routes: ['1'] }
    expect(platformsForStop(stop, [c('way/3', 5, ['7'])])).toEqual([])
    expect(platformsForStop(stop, [c('way/4', 30), c('way/5', 20)])).toEqual(['way/5'])
    expect(platformsForStop(stop, [c('way/4', 90)])).toEqual([])
  })

  test('stop pairs take the best source over their platforms, and straight line without one', () => {
    const P = DEFAULT_PARAMS
    const lookup = (a: string, b: string) => ({
      'p1>p3': { seconds: 100, source: 'estimated' as const },
      'p2>p3': { seconds: 80, source: 'graph' as const },
    } as Record<string, { seconds: number; source: 'graph' | 'estimated' }>)[`${a}>${b}`]
    expect(stopPairTime(P, { platforms: ['p1', 'p2'] }, { platforms: ['p3'] }, 50, lookup)).toEqual({ seconds: 80, source: 'graph', uncertaintyS: P.uncertaintyS.graph.pair })
    expect(stopPairTime(P, { platforms: ['p1'] }, { platforms: ['p1'] }, 0, lookup)).toBeNull()
    expect(stopPairTime(P, { platforms: [] }, { platforms: ['p3'] }, 100, lookup)!.source).toBe('straight_line')
  })
})
