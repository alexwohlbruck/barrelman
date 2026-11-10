/**
 * The station graph against agency truth: VBB (Berlin) and MBTA (Boston)
 * pathways.txt times, matched onto OSM by agency.ts. The fixture is every
 * complex of the PAR-350 spike's Berlin and Boston sets whose OSM extract is
 * under 130 KB (the large hubs are left out to keep it small), with only the
 * station's own ways. Bars from the ticket: graph within 30 s median error,
 * the estimate within 35 s.
 */
import { describe, expect, test } from 'bun:test'
import { gunzipSync } from 'zlib'
import { estimatePair, estimateStreet } from './estimate'
import { dist, pointWayDistance, wayDistance } from './geometry'
import { StationGraph } from './graph'
import { DEFAULT_PARAMS as P } from './params'
import { solveComplex, type ComplexInput } from './solve'

type Fixture = ComplexInput & { name: string; city: string; truth: Record<string, number> }

const fixtures: Fixture[] = JSON.parse(
  gunzipSync(new Uint8Array(await Bun.file(new URL('./fixtures/agency-regression.json.gz', import.meta.url)).arrayBuffer())).toString(),
)

const median = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b)
  return s.length % 2 ? s[s.length >> 1] : (s[s.length / 2 - 1] + s[s.length / 2]) / 2
}

const errors = { graphPair: [] as number[], graphStreet: [] as number[], estimatePair: [] as number[], estimateStreet: [] as number[], flat: [] as number[] }
for (const fx of fixtures) {
  const g = new StationGraph(fx, P)
  for (const p of solveComplex(fx).paths) {
    const truth = fx.truth[`${p.fromId}>${p.toId}`]
    if (p.profile !== 'foot' || truth === undefined) continue
    const platform = g.platforms.get(p.fromKind === 'platform' ? p.fromId : p.toId)!
    if (p.toKind === 'platform' && p.fromKind === 'platform') {
      const other = g.platforms.get(p.toId)!
      if (p.source === 'graph') errors.graphPair.push(Math.abs(p.seconds - truth))
      const centroidM = dist(g.xy.get(g.centre.get(p.fromId)!)!, g.xy.get(g.centre.get(p.toId)!)!)
      const t2 = estimatePair(P, { edgeM: wayDistance(platform, other), centroidM, levelA: platform.level, levelB: other.level })
      errors.estimatePair.push(Math.abs(t2 - truth))
      errors.flat.push(Math.abs(180 - truth))
    } else {
      if (p.source === 'graph') errors.graphStreet.push(Math.abs(p.seconds - truth))
      const entrance = g.xy.get(g.entranceKey.get(p.fromKind === 'entrance' ? p.fromId : p.toId)!)!
      errors.estimateStreet.push(Math.abs(estimateStreet(P, { edgeM: pointWayDistance(entrance, platform.pm, platform.area).d, level: platform.level }) - truth))
    }
  }
}

describe('station graph against VBB and MBTA pathways', () => {
  test('the fixture covers both feeds', () => {
    expect(new Set(fixtures.map((f) => f.city))).toEqual(new Set(['vbbberlin', 'mbtaboston']))
    expect(errors.graphPair.length).toBeGreaterThan(50)
    expect(errors.graphStreet.length).toBeGreaterThan(200)
  })

  test('graph times are within 30 s median error', () => {
    expect(median(errors.graphPair)).toBeLessThanOrEqual(30)
    expect(median(errors.graphStreet)).toBeLessThanOrEqual(30)
  })

  test('the estimate is within 35 s median error, and beats a flat 180 s', () => {
    expect(median(errors.estimatePair)).toBeLessThanOrEqual(35)
    expect(median(errors.estimateStreet)).toBeLessThanOrEqual(35)
    expect(median(errors.estimatePair)).toBeLessThan(median(errors.flat))
  })
})
