import { describe, test, expect } from 'bun:test'
import {
  backfillTripShapes,
  chooseDonorShape,
  chooseGeometricShape,
  shapeFollowsStops,
  isOrderedSubsequence,
  listShapeIds,
  type DonorTrip,
} from './inject-trip-shapes'
import { groupStopTimes } from '../src/services/gtfs.service'
import { parseCsv } from '../src/lib/csv'
import { routePath, type LonLat } from '../src/lib/route-hazards/route-path'

/** A point `m` metres east of the origin, `n` metres north. */
const at = (m: number, n = 0): LonLat => [m / 84_000, n / 111_000 + 40]

const donor = (shapeId: string, stops: string, directionId = '0'): DonorTrip => ({
  shapeId,
  directionId,
  stops: stops.split(' '),
})

describe('isOrderedSubsequence', () => {
  test('allows gaps but not reordering', () => {
    expect(isOrderedSubsequence(['a', 'c'], ['a', 'b', 'c'])).toBe(true)
    expect(isOrderedSubsequence(['c', 'a'], ['a', 'b', 'c'])).toBe(false)
    expect(isOrderedSubsequence(['a', 'd'], ['a', 'b', 'c'])).toBe(false)
  })

  test('counts a repeated stop on a loop once per visit', () => {
    expect(isOrderedSubsequence(['a', 'b', 'a'], ['a', 'b', 'c', 'a'])).toBe(true)
    expect(isOrderedSubsequence(['a', 'b', 'a'], ['a', 'b', 'c'])).toBe(false)
  })
})

describe('chooseDonorShape', () => {
  test('prefers the donor with the fewest extra stops', () => {
    const donors = [donor('full', 'a b c d e f'), donor('short', 'a b c d')]
    expect(chooseDonorShape({ directionId: '0', stops: ['b', 'd'] }, donors, new Map())).toBe('short')
  })

  test('breaks a tie on the shape most of the route runs', () => {
    const donors = [donor('rare', 'a b c'), donor('common', 'a x c')]
    const counts = new Map([['rare', 2], ['common', 40]])
    expect(chooseDonorShape({ directionId: '0', stops: ['a', 'c'] }, donors, counts)).toBe('common')
  })

  test('prefers the same direction, falling back to the other', () => {
    const donors = [donor('north', 'a b c', '0'), donor('south-long', 'a b c d', '1')]
    expect(chooseDonorShape({ directionId: '1', stops: ['a', 'c'] }, donors, new Map())).toBe('south-long')
    expect(chooseDonorShape({ directionId: '1', stops: ['a', 'b'] }, [donor('north', 'a b c', '0')], new Map()))
      .toBe('north')
  })

  test('returns null when no donor covers the stops in order', () => {
    expect(chooseDonorShape({ directionId: '0', stops: ['c', 'a'] }, [donor('s', 'a b c')], new Map())).toBeNull()
    expect(chooseDonorShape({ directionId: '0', stops: ['a'] }, [donor('s', 'a b c')], new Map())).toBeNull()
  })
})

describe('backfillTripShapes', () => {
  const stopTimes = groupStopTimes(
    [
      'trip_id,stop_id,stop_sequence',
      // Out of file order on purpose: stop_sequence decides.
      't1,b,2', 't1,a,1', 't1,c,3', 't1,d,4',
      't2,a,1', 't2,c,2',
      't3,d,1', 't3,a,2',
      't4,a,1', 't4,c,2',
      'u1,x,1', 'u1,y,2',
      'u2,x,1', 'u2,y,2',
    ].join('\n'),
  )

  test('fills empty and dangling shape ids from a covering trip, and leaves the rest', async () => {
    const trips = [
      'route_id,trip_id,direction_id,shape_id',
      'R,t1,0,R-full',
      'R,t2,0,',
      'R,t3,0,',
      'R,t4,0,R-gone',
      'U,u1,0,',
      'U,u2,0,',
    ].join('\n')

    const result = await backfillTripShapes(trips, new Set(['R-full']), { stopTimes: () => stopTimes })
    expect(result).toMatchObject({ shapeless: 5, filled: 2, missing: 3 })
    expect(parseCsv(result.tripsTxt!).map(t => t.shape_id)).toEqual(['R-full', 'R-full', '', 'R-full', '', ''])
  })

  test('does not read stop_times for a feed with every trip shaped', async () => {
    const result = await backfillTripShapes('route_id,trip_id,shape_id\nR,t1,s\n', new Set(['s']), {
      stopTimes: () => {
        throw new Error('read stop_times')
      },
    })
    expect(result).toEqual({ shapeless: 0, filled: 0, missing: 0 })
  })

  test('falls back to geometry for a local whose only shaped sibling runs express', async () => {
    const times = groupStopTimes('trip_id,stop_id,stop_sequence\nx,s1,1\nx,s3,2\nl,s1,1\nl,s2,2\nl,s3,3\n')
    const result = await backfillTripShapes(
      'route_id,trip_id,direction_id,shape_id\n7,x,0,7-express\n7,l,0,\n',
      new Set(['7-express']),
      {
        stopTimes: () => times,
        geometry: async () => ({
          stops: new Map([['s1', at(0)], ['s2', at(500, 40)], ['s3', at(1000)]]),
          shapes: new Map([['7-express', [at(0), at(1000)]]]),
        }),
      },
    )
    expect(result).toMatchObject({ filled: 1, missing: 0 })
    expect(parseCsv(result.tripsTxt!).map(t => t.shape_id)).toEqual(['7-express', '7-express'])
  })

  test('reports a feed without shapes.txt as unfillable', async () => {
    const result = await backfillTripShapes('route_id,trip_id\nR,t1\n', new Set(), { stopTimes: () => stopTimes })
    expect(result).toMatchObject({ skipped: 'no-shapes', missing: 1, filled: 0 })
    expect(result.tripsTxt).toBeUndefined()
  })
})

describe('shapeFollowsStops', () => {
  const line = routePath([at(0), at(1000), at(2000)])

  test('accepts stops beside the shape, in order', () => {
    expect(shapeFollowsStops(line, [at(100, 30), at(900, -40), at(1900)])).toBe(true)
  })

  test('rejects a stop off the shape, or stops out of order', () => {
    expect(shapeFollowsStops(line, [at(100), at(900, 300)])).toBe(false)
    expect(shapeFollowsStops(line, [at(1900), at(100)])).toBe(false)
  })

  test('follows a loop past the same place twice', () => {
    const loop = routePath([at(0), at(1000), at(1000, 500), at(0, 500), at(0, 20)])
    expect(shapeFollowsStops(loop, [at(10), at(1000, 250), at(10, 20)])).toBe(true)
  })
})

describe('chooseGeometricShape', () => {
  const shape = (shapeId: string, points: LonLat[], trips = 1, directions = ['0']) => ({
    shapeId, trips, path: routePath(points), directions: new Set(directions),
  })

  test('picks the shortest shape running past every stop', () => {
    const shapes = [shape('long', [at(0), at(3000)], 50), shape('short', [at(0), at(1500)], 2)]
    expect(chooseGeometricShape({ directionId: '0', coords: [at(100), at(1400)] }, shapes)).toBe('short')
  })

  test('prefers a same-direction shape over a shorter opposite one', () => {
    const shapes = [shape('back', [at(0), at(1500)], 1, ['1']), shape('ahead', [at(0), at(3000)])]
    expect(chooseGeometricShape({ directionId: '0', coords: [at(100), at(1400)] }, shapes)).toBe('ahead')
  })

  test('returns null when no shape passes a stop', () => {
    const shapes = [shape('s', [at(0), at(3000)])]
    expect(chooseGeometricShape({ directionId: '0', coords: [at(100), at(1400, 500)] }, shapes)).toBeNull()
  })
})

describe('listShapeIds', () => {
  test('reads the shape_id column wherever it sits', () => {
    const txt = 'shape_pt_lat,shape_id,shape_pt_lon\r\n1,"a",2\r\n1,a,2\r\n1,b,2\r\n'
    expect([...listShapeIds(txt)]).toEqual(['a', 'b'])
  })
})
