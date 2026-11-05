import type { GraphEdge, StationGraph } from './graph'
import type { PathStep, StepType } from './types'

export interface WalkedPath {
  geometry: Array<[number, number]>
  vertexLevels: Array<number | null>
  steps: PathStep[]
  metres: number
  levels: number
  crossesFareGate: boolean
  passesStreet: boolean
}

function stepType(g: StationGraph, e: GraphEdge): StepType {
  if (e.kind === 'bridge') return 'bridge'
  if (e.vertical === 'steps') return 'stairs'
  if (e.vertical === 'escalator') return 'escalator'
  if (e.vertical === 'elevator') return 'elevator'
  if (e.kind === 'area' && e.wayId && g.platforms.has(e.wayId)) return 'platform'
  return 'walk'
}

const round1 = (v: number) => Math.round(v * 10) / 10

/**
 * The drawn line and compact step list of a path walked through `nodes`
 * along `edges` (edges[i] joins nodes[i] and nodes[i + 1]). Consecutive
 * edges of one kind merge into one step; a lift node and a fare gate are
 * steps of their own.
 */
export function walk(g: StationGraph, nodes: string[], edges: GraphEdge[], streetKeys: Set<string>): WalkedPath {
  const geometry: Array<[number, number]> = []
  const vertexLevels: Array<number | null> = []
  const vertexOf: number[] = []
  for (const n of nodes) {
    const ll = g.lonLat(n)
    const last = geometry[geometry.length - 1]
    if (!last || last[0] !== ll[0] || last[1] !== ll[1]) {
      geometry.push(ll)
      vertexLevels.push(g.vertexLevel(n))
    } else if (vertexLevels[vertexLevels.length - 1] === null) {
      vertexLevels[vertexLevels.length - 1] = g.vertexLevel(n)
    }
    vertexOf.push(geometry.length - 1)
  }

  const lift = g.params.costs.elevatorS * g.params.costs.scale
  const steps: PathStep[] = []
  const event = (type: StepType, i: number, seconds: number) => steps.push({
    type, metres: 0, seconds: round1(seconds), fromLevel: g.vertexLevel(nodes[i - 1] ?? nodes[i]),
    toLevel: g.vertexLevel(nodes[i + 1] ?? nodes[i]), fromVertex: vertexOf[i], toVertex: vertexOf[i],
  })
  let metres = 0
  let crossesFareGate = false
  let passesStreet = false
  edges.forEach((e, i) => {
    const from = nodes[i]
    const seconds = e.a === from ? e.costAB : e.costBA
    metres += e.len
    const type = stepType(g, e)
    const prev = steps[steps.length - 1]
    if (prev && prev.type === type && type !== 'elevator') {
      prev.metres = round1(prev.metres + e.len)
      prev.seconds = round1(prev.seconds + seconds)
      prev.toVertex = vertexOf[i + 1]
      prev.toLevel = g.vertexLevel(nodes[i + 1])
    } else {
      steps.push({
        type, metres: round1(e.len), seconds: round1(seconds), fromLevel: g.vertexLevel(from),
        toLevel: g.vertexLevel(nodes[i + 1]), fromVertex: vertexOf[i], toVertex: vertexOf[i + 1],
      })
    }
    const next = g.baseKey(nodes[i + 1])
    if (i + 1 < edges.length && g.elevatorKeys.has(next)) event('elevator', i + 1, lift)
    if (g.gateKeys.has(next)) {
      crossesFareGate = true
      event('fare_gate', i + 1, 0)
    }
    if (streetKeys.has(next)) passesStreet = true
  })

  const levels = steps
    .filter((s) => s.type === 'stairs' || s.type === 'escalator' || s.type === 'elevator')
    .reduce((n, s) => n + (s.fromLevel !== null && s.toLevel !== null ? Math.abs(s.toLevel - s.fromLevel) : 1), 0)
  return { geometry, vertexLevels, steps, metres: Math.round(metres), levels, crossesFareGate, passesStreet }
}
