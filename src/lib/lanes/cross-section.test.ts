import { describe, expect, test } from 'bun:test'
import { busLanes, crossSection, turnBays } from './cross-section'

const rht = { americas: true, side: 'right' as const }

describe('crossSection', () => {
  test('a two-way street splits its default lanes either side of the centre', () => {
    const s = crossSection({ highway: 'residential' }, rht)
    expect(s.width).toBe(6)
    expect(s.forward.map(l => l.offset)).toEqual([-1.5])
    expect(s.backward.map(l => l.offset)).toEqual([1.5])
  })

  test('forward lanes run left to right from the centre on a tagged two-way road', () => {
    const s = crossSection({ highway: 'secondary', lanes: '4', 'lanes:forward': '2', 'lanes:backward': '2' }, rht)
    expect(s.laneWidth).toBeCloseTo(3.3)
    expect(s.forward.map(l => +l.offset.toFixed(2))).toEqual([-1.65, -4.95])
    // Backward lanes as their own traffic sees them: the one by the centre first.
    expect(s.backward.map(l => +l.offset.toFixed(2))).toEqual([1.65, 4.95])
  })

  test('turn arrows land on their lanes, and a right-only bay is noted', () => {
    const s = crossSection({ highway: 'primary', oneway: 'yes', lanes: '3', 'turn:lanes': 'left|through|right' }, rht)
    expect(s.forward.map(l => l.turns)).toEqual([['left'], ['through'], ['right']])
    expect(turnBays('left|through|right', 3)).toEqual([1, 1])
  })

  test('kerbside bus lanes count from the kerb on the side traffic keeps to', () => {
    expect(busLanes(undefined, 1, 3, 'right')).toEqual([3])
    expect(busLanes(undefined, 1, 3, 'left')).toEqual([1])
    expect(busLanes('|designated|', null, 3, 'right')).toEqual([2])
  })

  test('parking and a bike lane narrow the travel lanes inside a tagged width', () => {
    const s = crossSection({
      highway: 'residential', oneway: 'yes', lanes: '1', width: '8',
      'parking:right': 'lane', 'cycleway:left': 'lane',
    }, rht)
    const car = s.forward.find(l => !l.bike)!
    const bike = s.forward.find(l => l.bike)!
    expect(car.width).toBeCloseTo(8 - 2.4 - 1.6)
    // The bike lane is on the left, the parking on the right.
    expect(bike.offset).toBeGreaterThan(car.offset)
    expect(car.offset).toBeCloseTo(0.4)
  })

  test('left-hand traffic puts the forward lanes on the left of a two-way road', () => {
    const s = crossSection({ highway: 'residential' }, { americas: false, side: 'left' })
    expect(s.forward[0].offset).toBe(1.5)
    expect(s.backward[0].offset).toBe(-1.5)
  })

  test('a reversed one-way keeps its lanes in its traffic direction', () => {
    const s = crossSection({ highway: 'primary', oneway: '-1', lanes: '2' }, rht)
    expect(s.flip).toBe(true)
    expect(s.forward).toHaveLength(2)
    expect(s.backward).toHaveLength(0)
  })
})
