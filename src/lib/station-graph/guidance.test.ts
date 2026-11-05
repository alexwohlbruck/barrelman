import { describe, expect, test } from 'bun:test'
import {
  arrivalBearing, boardingHint, chooseEntrance, classifyTransfer, FARE_GATE_DELAY_S, guidanceSteps, positionIsMeasured,
  representativeRow, type TransferFacts,
} from './guidance'
import type { PathStep, StationPath } from './types'

const step = (type: PathStep['type'], metres: number, seconds: number, fromLevel: number | null, toLevel: number | null, v: number): PathStep =>
  ({ type, metres, seconds, fromLevel, toLevel, fromVertex: v, toVertex: v + 1 })

const path = (over: Partial<StationPath> = {}): StationPath => ({
  fromId: 'way/1', fromKind: 'platform', toId: 'way/2', toKind: 'platform', profile: 'foot', source: 'graph',
  confidence: 0.7, uncertaintyS: 75, seconds: 90, bestSeconds: 70, bestPosition: 0.8, metres: 60, levels: 2,
  crossesFareGate: false, surfaces: false, geometry: [[0, 0], [0, 0.0001]], vertexLevels: [-2, -4], steps: [], ...over,
})

// Times Sq, 1/2/3 to the 7 on the station graph: stairs to the mezzanine, a corridor, an escalator down.
const timesSq = path({
  steps: [
    step('platform', 3.3, 3.1, -2, -2, 0), step('stairs', 3.9, 11.1, -2, -3, 1), step('walk', 43.3, 41, -3, -3, 2),
    step('escalator', 9, 14.8, -3, -4, 3), step('platform', 1.4, 1.3, -4, -4, 4),
  ],
})

const facts = (over: Partial<TransferFacts> = {}): TransferFacts =>
  ({ samePlatform: false, row: timesSq, inSystem: true, namedApart: false, oneFare: null, ...over })

describe('classifyTransfer', () => {
  test('a shared platform is cross-platform', () => {
    expect(classifyTransfer(facts({ samePlatform: true })).class).toBe('same_platform')
  })

  test('a walked path inside one station keeps its surfaces flag and adds no gate time', () => {
    expect(classifyTransfer(facts())).toEqual({ class: 'in_station', surfaces: false, reentryFree: null, fareGateDelayS: 0 })
  })

  test('an estimate still classifies, but cannot say whether it surfaces', () => {
    const c = classifyTransfer(facts({ row: path({ source: 'estimated', surfaces: null, steps: null }) }))
    expect(c.class).toBe('in_station')
    expect(c.surfaces).toBeNull()
  })

  test('a line the agency does not join to the station means leaving fare control, gates and all', () => {
    // Jay St–MetroTech to Borough Hall: no transfers.txt row, so out of system whatever the map says.
    const c = classifyTransfer(facts({ row: null, inSystem: false, oneFare: true }))
    expect(c).toEqual({ class: 'out_of_system', surfaces: true, reentryFree: true, fareGateDelayS: 2 * FARE_GATE_DELAY_S })
  })

  test('a walked path through a fare gate leaves fare control even inside one complex', () => {
    expect(classifyTransfer(facts({ row: path({ crossesFareGate: true }) })).class).toBe('out_of_system')
    // An estimate never asserts a gate.
    expect(classifyTransfer(facts({ row: path({ source: 'estimated', crossesFareGate: true }) })).class).toBe('in_station')
  })

  test('a long paid walk between differently named stations is a passage', () => {
    const long = path({ metres: 220 })
    expect(classifyTransfer(facts({ row: long, namedApart: true })).class).toBe('in_system_passage')
    expect(classifyTransfer(facts({ row: long, namedApart: false })).class).toBe('in_station')
    expect(classifyTransfer(facts({ row: path({ metres: 60 }), namedApart: true })).class).toBe('in_station')
  })

  test('without a shared complex, only the agency keeps a transfer inside', () => {
    expect(classifyTransfer(facts({ row: null, inSystem: true })).class).toBe('in_system_passage')
    expect(classifyTransfer(facts({ row: null, inSystem: null })).class).toBe('out_of_system')
  })
})

describe('representativeRow', () => {
  test('takes the best tier, then the median of its times', () => {
    const rows = [path({ source: 'estimated', seconds: 40 }), path({ seconds: 120 }), path({ seconds: 80 }), path({ seconds: 100 })]
    expect(representativeRow(rows)?.seconds).toBe(100)
    expect(representativeRow([])).toBeNull()
  })
})

describe('chooseEntrance', () => {
  test('weighs the street walk against the walk inside, not distance to the street alone', () => {
    const at = { lat: 40.75, lng: -73.98 }
    const near = { id: 'near', lat: 40.7502, lng: -73.98, row: path({ seconds: 300 }) } // ~22 m, deep stairs
    const far = { id: 'far', lat: 40.7512, lng: -73.98, row: path({ seconds: 40 }) } // ~133 m, straight down
    const choice = chooseEntrance(at, [near, far], 1.25)!
    expect(choice.candidate.id).toBe('far')
    expect(choice.totalSeconds).toBe(choice.streetSeconds + 40)
    expect(chooseEntrance(at, [], 1.25)).toBeNull()
  })
})

describe('boardingHint', () => {
  const platform = { axisBearing: 29, lengthM: 166 }

  test('heading along the axis, the platform head is the front of the train', () => {
    expect(boardingHint(platform, 0.82, 29, 1.3, 1.23)).toMatchObject({ section: 'front', fraction: 0.82 })
  })

  test('heading against it, the same spot is the back', () => {
    expect(boardingHint(platform, 0.82, 209, 1.3, 1.23)).toMatchObject({ section: 'back', fraction: 0.18 })
    expect(boardingHint(platform, 0.5, 209, 1.3, 1.23)?.section).toBe('middle')
  })

  test('a short platform gets no hint', () => {
    expect(boardingHint({ axisBearing: 0, lengthM: 40 }, 0.9, 0, 1.3)).toBeNull()
  })
})

describe('arrivalBearing', () => {
  test('is the heading over the ride\'s last stretch', () => {
    const north: Array<[number, number]> = [[-73.98, 40.74], [-73.98, 40.745], [-73.98, 40.75]]
    expect(Math.round(arrivalBearing(north)!)).toBe(0)
    expect(Math.round(arrivalBearing([...north].reverse())!)).toBe(180)
    expect(arrivalBearing([[-73.98, 40.74]])).toBeNull()
  })
})

describe('positionIsMeasured', () => {
  test('needs a walked path that is not bridged before its first stair', () => {
    expect(positionIsMeasured(timesSq)).toBe(true)
    expect(positionIsMeasured(path({ source: 'estimated', steps: null, bestPosition: null }))).toBe(false)
    expect(positionIsMeasured(path({ source: 'bridged', steps: [step('bridge', 12, 15, -2, -2, 0), step('stairs', 4, 10, -2, -1, 1)] }))).toBe(false)
    expect(positionIsMeasured(path({ source: 'bridged', steps: [step('stairs', 4, 10, -2, -1, 0), step('bridge', 12, 15, -1, -1, 1)] }))).toBe(true)
  })
})

describe('guidanceSteps', () => {
  test('a transfer reads as signs, levels and the platform to board', () => {
    const steps = guidanceSteps(timesSq, { kind: 'transfer', sign: 'the 7 · Queens' })
    expect(steps.map((s) => s.instruction)).toEqual([
      'Follow signs for the 7 · Queens and take the stairs down to the mezzanine',
      'Walk 45 m',
      'Take the escalator down to the platform',
      'Board the 7 · Queens',
    ])
    // Folded steps keep a contiguous vertex range and every metre walked.
    expect(steps[0]).toMatchObject({ fromVertex: 0, toVertex: 2, levelChange: -1, landmark: 'the 7 · Queens' })
    expect(steps.reduce((m, s) => m + s.distance, 0)).toBeCloseTo(60.9, 1)
  })

  test('an exit ends at the entrance it uses, and gates are tapped out of', () => {
    const egress = path({
      toKind: 'entrance', toId: 'node/9', steps: [
        step('platform', 30, 25, -1, -1, 0), step('stairs', 6, 12, -1, 0, 1), step('fare_gate', 0, 0, 0, 0, 2), step('walk', 20, 16, 0, 0, 2),
      ],
    })
    const steps = guidanceSteps(egress, { kind: 'egress', entrance: 'Broadway & West 28th Street · NW corner' })
    expect(steps.map((s) => s.instruction)).toEqual([
      'Walk 30 m along the platform',
      'Take the stairs up to street level',
      'Exit through the fare gates',
      'Walk 20 m',
      'Exit at Broadway & West 28th Street · NW corner',
    ])
  })

  test('a transfer through the gates taps out, then back in', () => {
    const out = path({
      steps: [step('walk', 20, 16, -1, -1, 0), step('fare_gate', 0, 0, -1, -1, 1), step('walk', 60, 50, -1, -1, 1), step('fare_gate', 0, 0, -1, -1, 2), step('walk', 20, 16, -1, -1, 2)],
    })
    const text = guidanceSteps(out, { kind: 'transfer', reentryFree: true }).map((s) => s.instruction)
    expect(text).toContain('Exit through the fare gates')
    expect(text).toContain('Re-enter through the fare gates · free transfer')
  })

  test('an estimate has no steps', () => {
    expect(guidanceSteps(path({ source: 'estimated', steps: null }), { kind: 'transfer' })).toEqual([])
  })
})
