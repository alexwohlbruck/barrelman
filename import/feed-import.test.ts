import { describe, expect, it } from 'bun:test'
import { flatTransferTime, mergeTransfersTxt, type StationTime } from './feed-import'

const HEADER = 'from_stop_id,to_stop_id,transfer_type,min_transfer_time'

/** Parse a merged file back into transfer_type by pair. */
function kinds(text: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const line of text.split('\n').slice(1)) {
    if (!line.trim()) continue
    const [from, to, type] = line.split(',')
    out[`${from}>${to}`] = type
  }
  return out
}

describe('mergeTransfersTxt', () => {
  it('keeps the agency transfers a computed file would have overwritten', () => {
    // The MTA states 90s between the two Jay St-MetroTech halves. Our
    // GraphHopper walk says 150s. The agency knows its own station.
    const feed = `${HEADER}\nA41,R29,2,90\nR29,A41,2,90\n`
    const computed = `${HEADER}\nA41,R29,2,150\nR29,A41,2,150\n`

    const merged = mergeTransfersTxt(feed, computed)
    expect(merged).toContain('A41,R29,2,90')
    expect(merged).not.toContain('A41,R29,2,150')
  })

  it('never re-adds a pair the feed forbids', () => {
    // portolan derives transfer_type=3 for Borough Hall <-> Jay St: out of
    // system, a second fare. A 240m walk is well inside the 500m the
    // transfer computation considers, so without this it comes straight back.
    const feed = `${HEADER}\n423,A41,3,\nA41,423,3,\n`
    const computed = `${HEADER}\n423,A41,2,240\nA41,423,2,240\n`

    const merged = mergeTransfersTxt(feed, computed)
    const k = kinds(merged)
    expect(k['423>A41']).toBe('3')
    expect(k['A41>423']).toBe('3')
    expect(merged).not.toContain(',2,240')
  })

  it('adds computed transfers for pairs the feed says nothing about', () => {
    // The point of computing them: a bus stop outside a station entrance
    // that no agency files.
    const feed = `${HEADER}\nA41,R29,2,90\n`
    const computed = `${HEADER}\n303884,239,2,60\n`

    const merged = mergeTransfersTxt(feed, computed)
    expect(merged).toContain('A41,R29,2,90')
    expect(merged).toContain('303884,239,2,60')
  })

  it('treats a feed row as covering both directions', () => {
    // A feed that states A->B has described the connection; a computed
    // B->A with a different time would contradict its own other half.
    const feed = `${HEADER}\n232,R28,2,180\n`
    const computed = `${HEADER}\nR28,232,2,400\n`

    expect(mergeTransfersTxt(feed, computed)).not.toContain('400')
  })

  it('falls back to the computed file when the feed has none', () => {
    const computed = `${HEADER}\n303884,239,2,60\n`
    expect(mergeTransfersTxt(null, computed)).toBe(computed)
    expect(mergeTransfersTxt('', computed)).toBe(computed)
  })

  it('reads columns by name, not position', () => {
    // Feeds order transfers.txt however they like, and some carry the
    // route/trip columns in between.
    const feed = 'to_stop_id,transfer_type,from_stop_id,min_transfer_time\nA41,3,423,\n'
    const computed = `${HEADER}\n423,A41,2,240\n`

    expect(mergeTransfersTxt(feed, computed)).not.toContain(',2,240')
  })

  it('keeps every row the feed shipped, including same-station dwells', () => {
    const feed = `${HEADER}\n232,232,2,180\n232,423,2,300\n`
    const merged = mergeTransfersTxt(feed, `${HEADER}\n`)
    expect(merged).toContain('232,232,2,180')
    expect(merged).toContain('232,423,2,300')
  })
})

describe('mergeTransfersTxt — parent station resolution', () => {
  // Production's computed transfers are PLATFORM level (423N -> A41N) while a
  // prohibition is declared between STATIONS (423 -> A41), because that is the
  // pair a rider recognises and what portolan derives. Matching ids exactly
  // lets every platform pairing under a forbidden station slip back in.
  const STOPS =
    'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n' +
    '423,Borough Hall,40.692404,-73.990151,1,\n' +
    '423N,Borough Hall,40.692404,-73.990151,0,423\n' +
    '423S,Borough Hall,40.692404,-73.990151,0,423\n' +
    'A41,Jay St-MetroTech,40.692338,-73.987342,1,\n' +
    'A41N,Jay St-MetroTech,40.692338,-73.987342,0,A41\n' +
    'A41S,Jay St-MetroTech,40.692338,-73.987342,0,A41\n'

  it('blocks platform pairs under a forbidden station pair', () => {
    const feed = `${HEADER}\n423,A41,3,\nA41,423,3,\n`
    const computed =
      `${HEADER}\n423N,A41N,2,216\n423N,A41S,2,216\n423S,A41N,2,216\n`

    const merged = mergeTransfersTxt(feed, computed, STOPS)
    expect(merged).not.toContain('423N,A41N')
    expect(merged).not.toContain('423N,A41S')
    expect(merged).not.toContain('423S,A41N')
  })

  it('still allows platform pairs under stations nobody forbade', () => {
    const feed = `${HEADER}\n423,A41,3,\n`
    const computed = `${HEADER}\n423N,423S,2,60\n`

    expect(mergeTransfersTxt(feed, computed, STOPS)).toContain('423N,423S,2,60')
  })

  it('works unchanged when no stops.txt is available', () => {
    const feed = `${HEADER}\n423,A41,3,\n`
    const computed = `${HEADER}\n423,A41,2,216\n`
    expect(mergeTransfersTxt(feed, computed, null)).not.toContain(',2,216')
  })
})

describe('mergeTransfersTxt — station path times', () => {
  // Station path times are the one computed source that outranks the feed:
  // the MTA files a flat 180 s between every connected pair of stations.
  const STOPS =
    'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n' +
    '127,Times Sq,40.75529,-73.987495,1,\n' +
    '127N,Times Sq,40.75529,-73.987495,0,127\n' +
    '127S,Times Sq,40.75529,-73.987495,0,127\n' +
    '725,Times Sq,40.755477,-73.987691,1,\n' +
    '725N,Times Sq,40.755477,-73.987691,0,725\n' +
    '423,Borough Hall,40.692404,-73.990151,1,\n' +
    '423N,Borough Hall,40.692404,-73.990151,0,423\n' +
    'A41,Jay St,40.692338,-73.987342,1,\n' +
    'A41N,Jay St,40.692338,-73.987342,0,A41\n'
  const key = (a: string, b: string) => `${a}\u0000${b}`
  const t = (seconds: number, source: StationTime['source'] = 'graph'): StationTime => ({ seconds, source })

  it('replaces the flat station time with the median of its platforms\' times', () => {
    const feed = `${HEADER}\n127,725,2,180\n`
    const computed = `${HEADER}\n127N,725N,2,90\n127S,725N,2,130\n`
    const times = new Map([[key('127N', '725N'), t(90)], [key('127S', '725N'), t(130)]])
    const merged = mergeTransfersTxt(feed, computed, STOPS, times)
    expect(merged).toContain('127,725,2,110')
    expect(merged).toContain('127N,725N,2,90')
    expect(merged).not.toContain('127,725,2,180')
  })

  it('replaces the feed\'s own time for the same platform pair', () => {
    const feed = `${HEADER}\n127N,725N,0,\n`
    const merged = mergeTransfersTxt(feed, `${HEADER}\n127N,725N,2,95\n`, STOPS, new Map([[key('127N', '725N'), t(95)]]))
    expect(merged.trim().split('\n').slice(1)).toEqual(['127N,725N,2,95'])
  })

  it('never touches a forbidden transfer, nor re-adds one under it', () => {
    const feed = `${HEADER}\n423,A41,3,\n`
    const times = new Map([[key('423N', 'A41N'), t(120)]])
    const merged = mergeTransfersTxt(feed, `${HEADER}\n423N,A41N,2,120\n`, STOPS, times)
    expect(merged).toContain('423,A41,3,')
    expect(merged).not.toContain('423N,A41N')
  })

  it('leaves timed and in-seat transfers alone', () => {
    const feed = `${HEADER}\n127N,725N,1,\n`
    const merged = mergeTransfersTxt(feed, `${HEADER}\n127N,725N,2,95\n`, STOPS, new Map([[key('127N', '725N'), t(95)]]))
    expect(merged).toContain('127N,725N,1,')
    expect(merged).not.toContain('127N,725N,2,95')
  })

  it('without station times the feed still wins', () => {
    const feed = `${HEADER}\n127,725,2,180\n`
    expect(mergeTransfersTxt(feed, `${HEADER}\n127N,725N,2,90\n`, STOPS)).toContain('127,725,2,180')
  })

  describe('against a time the agency set for the pair', () => {
    // 14 St–8 Av: the MTA files 90 s for A/C/E (A31) to L (L01), against its flat 180 s elsewhere.
    const STOPS_14 = STOPS +
      'A31,14 St,40.740893,-74.00169,1,\n' +
      'A31N,14 St,40.740893,-74.00169,0,A31\n' +
      'L01,8 Av,40.739777,-74.002578,1,\n' +
      'L01N,8 Av,40.739777,-74.002578,0,L01\n'
    const feed = `${HEADER}\n127,127,2,180\n127,725,2,180\nA31,L01,2,90\n`
    const computed = `${HEADER}\n127N,725N,2,140\nA31N,L01N,2,147\n`

    it('keeps the agency time over an estimate, on the station row and the platform rows under it', () => {
      const times = new Map([[key('127N', '725N'), t(140, 'estimated')], [key('A31N', 'L01N'), t(147, 'estimated')]])
      const merged = mergeTransfersTxt(feed, computed, STOPS_14, times)
      expect(merged).toContain('A31,L01,2,90')
      expect(merged).toContain('A31N,L01N,2,90')
      // The flat default is still replaced, whatever the tier.
      expect(merged).toContain('127,725,2,140')
    })

    it('a walked path still overrides it', () => {
      const times = new Map([[key('A31N', 'L01N'), t(147, 'bridged')]])
      const merged = mergeTransfersTxt(feed, computed, STOPS_14, times)
      expect(merged).toContain('A31,L01,2,147')
      expect(merged).toContain('A31N,L01N,2,147')
    })
  })
})

describe('flatTransferTime', () => {
  it('is what the feed files on a station to itself', () => {
    expect(flatTransferTime([
      { from: '101', to: '101', seconds: 180 }, { from: 'A31', to: 'L01', seconds: 90 }, { from: '127', to: '725', seconds: 300 },
    ])).toBe(180)
  })

  it('else a value most rows share, and none when times vary', () => {
    const rows = (xs: number[]) => xs.map((seconds, i) => ({ from: `a${i}`, to: `b${i}`, seconds }))
    expect(flatTransferTime(rows([120, 120, 120, 60, 300]))).toBe(120)
    expect(flatTransferTime(rows([120, 90, 60, 300, 240]))).toBeNull()
  })
})

describe('mergeTransfersTxt — fare gates on street transfers', () => {
  const STOPS =
    'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n' +
    '423,Borough Hall,40.692404,-73.990151,1,\n' +
    '423N,Borough Hall,40.692404,-73.990151,0,423\n' +
    'A41,Jay St,40.692338,-73.987342,1,\n' +
    'A41N,Jay St,40.692338,-73.987342,0,A41\n' +
    'B1,Bus stop,40.6924,-73.9890,0,\n'
  const rail = new Set(['423N', 'A41N'])

  it('a walk between rail stations the feed does not connect pays the gates twice', () => {
    const merged = mergeTransfersTxt(`${HEADER}\n101,101,2,180\n`, `${HEADER}\n423N,A41N,2,221\nA41N,B1,2,90\n`, STOPS, new Map(), rail)
    expect(merged).toContain('423N,A41N,2,241')
    // A bus stop has no gates.
    expect(merged).toContain('A41N,B1,2,90')
  })

  it('a pair the feed connects keeps its time', () => {
    const merged = mergeTransfersTxt(`${HEADER}\n423,A41,2,300\n`, `${HEADER}\n423N,A41N,2,221\n`, STOPS, new Map(), rail)
    expect(merged).toContain('423N,A41N,2,221')
  })
})
