/**
 * The two pieces of the fleet's placement that are pure: which station a
 * train has just left, and how long one hop takes.
 */

import { describe, it, expect } from 'bun:test'
import { stopBefore, segmentSeconds } from './subway-position'

// The 4, both ways, as trip patterns store them (parent station ids).
const SOUTH = ['415', '416', '418', '419', '420', '423']
const NORTH = ['423', '420', '419', '418', '416', '415']

describe('stopBefore', () => {
  it('takes the station before the one being approached', () => {
    expect(stopBefore([SOUTH], '419', '420')).toBe('418')
  })

  it('uses the following stop to pick the direction', () => {
    // 419 sits in both patterns; only the next stop says which way the
    // train is going, and the answer differs.
    expect(stopBefore([SOUTH, NORTH], '419', '420')).toBe('418')
    expect(stopBefore([SOUTH, NORTH], '419', '418')).toBe('420')
  })

  it('has no answer at a terminal, or for a stop off the pattern', () => {
    expect(stopBefore([SOUTH], '415', '416')).toBeNull()
    expect(stopBefore([SOUTH], '999', null)).toBeNull()
  })

  it('falls back to the busiest pattern when nothing follows', () => {
    // Patterns arrive ordered by trip count, so the first match is the
    // one most trains run.
    expect(stopBefore([SOUTH, NORTH], '419', null)).toBe('418')
  })
})

describe('segmentSeconds', () => {
  const stu = (t: number) => ({ arrival: { time: t } })

  it('measures the gap between the next two arrivals', () => {
    expect(segmentSeconds([stu(1000), stu(1120)])).toBe(120)
  })

  it('rejects a gap that cannot be one hop', () => {
    expect(segmentSeconds([stu(1000), stu(1000)])).toBeNull()   // no time
    expect(segmentSeconds([stu(1000), stu(9000)])).toBeNull()   // a layover
    expect(segmentSeconds([stu(1000)])).toBeNull()              // last stop
  })

  it('ignores stops with no arrival time', () => {
    expect(segmentSeconds([{}, stu(1000), stu(1090)])).toBe(90)
  })
})
