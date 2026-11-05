/**
 * Every platform-to-platform and platform-to-entrance time in one complex,
 * each from the best tier available for that pair:
 *
 *   graph > bridged > agency > estimated
 *
 * `graph` and `bridged` come with a drawn path and steps; `agency` and
 * `estimated` are times only. (`straight_line` is for GTFS stops with no OSM
 * platform at all, so it is applied where stops are matched — see stops.ts.)
 */
import { StationGraph, type GraphEdge } from './graph'
import { components, shortestPaths, type Adjacency, type Tree } from './dijkstra'
import { estimatePair, estimateStreet } from './estimate'
import { dist, pointWayDistance, wayDistance } from './geometry'
import { confidenceOf, DEFAULT_PARAMS, uncertaintyOf, type Endpoints, type StationPathParams } from './params'
import { walk } from './steps'
import { gtfsStopIdsOf, platformRefs } from './tags'
import type { ComplexReport, OsmNode, OsmWay, PathSource, Profile, StationPath, StationPlatform } from './types'

export interface ComplexInput {
  platformIds: string[]
  entranceIds: string[]
  ways: OsmWay[]
  nodes: OsmNode[]
  /** Agency pathway times keyed `${fromId}>${toId}`, OSM ids on both sides. */
  agency?: Map<string, number>
}

export interface ComplexResult {
  platforms: StationPlatform[]
  paths: StationPath[]
  report: ComplexReport
}

interface Search {
  adj: Adjacency
  edges: GraphEdge[]
}

const PROFILES: Profile[] = ['foot', 'wheelchair']

export function solveComplex(input: ComplexInput, params: StationPathParams = DEFAULT_PARAMS): ComplexResult {
  const g = new StationGraph({ ways: input.ways, nodes: input.nodes }, params)
  const platformIds = input.platformIds.filter((id) => g.platforms.has(id))
  const entranceIds = input.entranceIds.filter((id) => g.entranceKey.has(id))
  const entranceKeys = new Set(g.entranceKey.values())
  const streetKeys = new Set([...g.streetKeys()].filter((k) => !entranceKeys.has(k)))
  const paths: StationPath[] = []
  let componentCount = 0

  for (const profile of PROFILES) {
    const search = (opts: { reverse?: boolean; ban?: Set<string> }, bridged: boolean): Search => {
      const base = g.adjacency({ profile, ...opts })
      if (!bridged) return { adj: base, edges: g.edges }
      const extra = g.bridges(base)
      return { adj: g.adjacency({ profile, ...opts, extra }), edges: [...g.edges, ...extra] }
    }
    const lazy = <T>(make: () => T) => { let v: T | undefined; return () => (v ??= make()) }
    const pairs = [search({ ban: entranceKeys }, false), lazy(() => search({ ban: entranceKeys }, true))] as const
    const outs = [search({}, false), lazy(() => search({}, true))] as const
    const ins = [search({ reverse: true }, false), lazy(() => search({ reverse: true }, true))] as const

    if (profile === 'foot') {
      const find = components(pairs[0].adj)
      componentCount = new Set(platformIds.map((id) => find(g.centre.get(id)!))).size
    }

    for (const pid of platformIds) {
      const plat = g.platforms.get(pid)!
      const centre = g.centre.get(pid)!
      const positions = g.positions.get(pid)!
      const climb = plat.underground
      const trees = (s: Search, needClimb: boolean) => ({
        centre: shortestPaths(s.adj, [centre], { needClimb }),
        best: shortestPaths(s.adj, positions.nodes, { needClimb }),
        edges: s.edges,
      })
      const pairTrees = [lazy(() => trees(pairs[0], false)), lazy(() => trees(pairs[1](), false))]
      const outTrees = [lazy(() => trees(outs[0], climb)), lazy(() => trees(outs[1](), climb))]
      const inTrees = [lazy(() => trees(ins[0], climb)), lazy(() => trees(ins[1](), climb))]

      const fromTrees = (
        t: Array<() => ReturnType<typeof trees>>, target: string, reversed: boolean,
        ends: { fromId: string; fromKind: 'platform' | 'entrance'; toId: string; toKind: 'platform' | 'entrance' },
        endpoints: Endpoints,
      ): StationPath | null => {
        for (const [i, source] of (['graph', 'bridged'] as const).entries()) {
          const tr = t[i]()
          const seconds = tr.centre.cost(target)
          if (seconds === undefined) continue
          return graphRow(tr, target, seconds, reversed, source, ends, endpoints)
        }
        return null
      }

      const graphRow = (
        tr: { centre: Tree; best: Tree; edges: GraphEdge[] }, target: string, seconds: number, reversed: boolean,
        source: 'graph' | 'bridged', ends: Pick<StationPath, 'fromId' | 'fromKind' | 'toId' | 'toKind'>, endpoints: Endpoints,
      ): StationPath => {
        const hops = tr.best.path(target)!
        let nodes = hops.map((h) => h.node)
        let edges = hops.slice(1).map((h) => tr.edges[h.edge])
        if (reversed) { nodes = nodes.reverse(); edges = edges.reverse() }
        const w = walk(g, nodes, edges, streetKeys)
        const origin = tr.best.origin(target)!
        const uncertaintyS = uncertaintyOf(params, source, endpoints)
        return {
          ...ends, profile, source, uncertaintyS, confidence: confidenceOf(params, uncertaintyS),
          seconds: Math.round(seconds),
          bestSeconds: Math.round(tr.best.cost(target)!),
          bestPosition: positions.fractions[positions.nodes.indexOf(origin)] ?? null,
          metres: w.metres, levels: w.levels, crossesFareGate: w.crossesFareGate,
          surfaces: endpoints === 'street' ? true : w.passesStreet,
          geometry: w.geometry, vertexLevels: w.vertexLevels, steps: w.steps,
        }
      }

      const timeOnly = (
        source: 'agency' | 'estimated', seconds: number, ends: Pick<StationPath, 'fromId' | 'fromKind' | 'toId' | 'toKind'>,
        endpoints: Endpoints, metres: number, levels: number,
      ): StationPath => {
        const uncertaintyS = uncertaintyOf(params, source, endpoints)
        return {
          ...ends, profile, source, uncertaintyS, confidence: confidenceOf(params, uncertaintyS),
          seconds: Math.round(seconds), bestSeconds: null, bestPosition: null, metres: Math.round(metres), levels,
          crossesFareGate: false, surfaces: endpoints === 'street' ? true : null,
          geometry: null, vertexLevels: null, steps: null,
        }
      }

      for (const qid of platformIds) {
        if (qid === pid) continue
        const ends = { fromId: pid, fromKind: 'platform', toId: qid, toKind: 'platform' } as const
        const row = fromTrees(pairTrees, g.centre.get(qid)!, false, ends, 'pair')
        if (row) { paths.push(row); continue }
        if (profile !== 'foot') continue
        const other = g.platforms.get(qid)!
        const centroidM = dist(g.xy.get(centre)!, g.xy.get(g.centre.get(qid)!)!)
        const levels = plat.level !== null && other.level !== null ? Math.abs(plat.level - other.level) : 0
        const agency = input.agency?.get(`${pid}>${qid}`)
        if (agency !== undefined) { paths.push(timeOnly('agency', agency, ends, 'pair', centroidM, levels)); continue }
        const edgeM = wayDistance(plat, other)
        paths.push(timeOnly('estimated', estimatePair(params, { edgeM, centroidM, levelA: plat.level, levelB: other.level }), ends, 'pair', centroidM, levels))
      }

      for (const eid of entranceIds) {
        const key = g.entranceKey.get(eid)!
        const out = { fromId: pid, fromKind: 'platform', toId: eid, toKind: 'entrance' } as const
        const inn = { fromId: eid, fromKind: 'entrance', toId: pid, toKind: 'platform' } as const
        const rows = [fromTrees(outTrees, key, false, out, 'street'), fromTrees(inTrees, key, true, inn, 'street')]
        if (profile !== 'foot') { rows.forEach((r) => r && paths.push(r)); continue }
        const edgeM = pointWayDistance(g.xy.get(key)!, plat.pm, plat.area).d
        rows.forEach((row, i) => {
          if (row) return paths.push(row)
          const ends = i === 0 ? out : inn
          const agency = input.agency?.get(`${ends.fromId}>${ends.toId}`)
          const depth = Math.abs(plat.level ?? 1)
          if (agency !== undefined) return paths.push(timeOnly('agency', agency, ends, 'street', edgeM, depth))
          paths.push(timeOnly('estimated', estimateStreet(params, { edgeM, level: plat.level }), ends, 'street', edgeM, depth))
        })
      }
    }
  }

  const tiers: ComplexReport['tiers'] = { pair: {}, street: {} }
  for (const p of paths) {
    if (p.profile !== 'foot') continue
    const kind = p.toKind === 'platform' && p.fromKind === 'platform' ? 'pair' : 'street'
    tiers[kind][p.source] = (tiers[kind][p.source] ?? 0) + 1
  }
  const notGraph = paths.some((p) => p.profile === 'foot' && p.source !== 'graph')

  return {
    platforms: platformIds.map((id) => {
      const pl = g.platforms.get(id)!
      const way = input.ways.find((w) => w.id === id)!
      return {
        id, name: pl.tags.name ?? null, level: pl.level, underground: pl.underground, axisBearing: pl.axisBearing,
        lengthM: pl.lengthM, refs: platformRefs(pl.tags), gtfsStopIds: gtfsStopIdsOf(pl.tags), geometry: way.geometry,
      }
    }),
    paths,
    report: { tiers, components: componentCount, disconnected: notGraph || componentCount > 1 },
  }
}

export const sourceRank = (s: PathSource) => ['graph', 'bridged', 'agency', 'estimated', 'straight_line'].indexOf(s)
