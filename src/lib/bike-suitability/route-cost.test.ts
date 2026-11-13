import { describe, expect, test } from 'bun:test'
import { pathCost, pathScore, penaltyRules, scoreSegments, stressFactor, wayStretches, withRules } from './route-cost'

const scores = new Map([[1, 100], [2, 20], [3, 60]])
const stretches = [
  { wayId: 1, from: 0, to: 100 },
  { wayId: 2, from: 100, to: 200 },
  { wayId: 3, from: 200, to: 300 },
]

describe('bike suitability route cost', () => {
  test('stress costs nothing at fastest and most on a red street', () => {
    expect(stressFactor(20, 0)).toBe(1)
    expect(stressFactor(100, 1)).toBe(1)
    expect(stressFactor(20, 1)).toBeGreaterThan(stressFactor(60, 1))
  })

  test('a route on calm streets costs less than one of equal time on busy ones', () => {
    const calm = pathCost([{ wayId: 1, from: 0, to: 300 }], scores, 600, 1)
    const busy = pathCost([{ wayId: 2, from: 0, to: 300 }], scores, 600, 1)
    expect(calm).toBe(600)
    expect(busy).toBeGreaterThan(calm * 2)
  })

  test('the route score is weighted by distance', () => {
    expect(pathScore(stretches, scores)).toBe(60)
    expect(pathScore([], scores)).toBeNull()
  })

  test('stretches come from the osm_way_id detail by point index', () => {
    expect(wayStretches({ osm_way_id: [[0, 2, 7], [2, 2, 8]] }, [0, 5, 12])).toEqual([{ wayId: 7, from: 0, to: 12 }])
  })

  test('segments merge consecutive stretches of one score', () => {
    expect(scoreSegments([...stretches, { wayId: 3, from: 300, to: 350 }], scores).map(s => [s.from, s.to, s.score]))
      .toEqual([[0, 100, 100], [100, 200, 20], [200, 350, 60]])
  })

  test('penalties group into one rule per factor, appended to the request model', () => {
    const rules = penaltyRules(new Map([[5, 0.5], [6, 0.51], [7, 1]]))
    expect(rules).toEqual([{ if: 'osm_way_id == 5 || osm_way_id == 6', multiply_by: 0.5 }])
    expect(withRules({ distance_influence: 20, priority: [{ if: 'a', multiply_by: 0.9 }] }, rules).priority).toHaveLength(2)
    expect(withRules(undefined, [])).toBeUndefined()
  })
})
