import { describe, expect, it } from 'bun:test'
import { flatTransferTime, freeReentry, mergeTransfersTxt, type StationTime } from './feed-import'

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

  it('a walk between rail stations the feed does not connect pays the gates twice and a second fare', () => {
    const merged = mergeTransfersTxt(`${HEADER}\n101,101,2,180\n`, `${HEADER}\n423N,A41N,2,221\nA41N,B1,2,90\n`, STOPS, new Map(), rail)
    expect(merged).toContain('423N,A41N,2,841')
    // A bus stop has no gates.
    expect(merged).toContain('A41N,B1,2,90')
  })

  it('a pair the feed connects keeps its time', () => {
    const merged = mergeTransfersTxt(`${HEADER}\n423,A41,2,300\n`, `${HEADER}\n423N,A41N,2,221\n`, STOPS, new Map(), rail)
    expect(merged).toContain('423N,A41N,2,221')
  })
})

describe('mergeTransfersTxt — out-of-system walks lose to the in-system change', () => {
  // The G meets the 7 in-system at Court Sq (G22/719). 21 St (G24) and
  // Hunters Point Av (720) are 200m apart, but walking between them leaves
  // fare control. With only the gates charged, the walk caught the same 7
  // train a stop earlier and won the tie.
  const STOPS =
    'stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n' +
    'G22,Court Sq,40.746554,-73.943832,1,\n' +
    'G22N,Court Sq,40.746554,-73.943832,0,G22\n' +
    '719,Court Sq,40.747023,-73.945264,1,\n' +
    '719N,Court Sq,40.747023,-73.945264,0,719\n' +
    'G24,21 St,40.744065,-73.949724,1,\n' +
    'G24N,21 St,40.744065,-73.949724,0,G24\n' +
    '720,Hunters Point Av,40.742216,-73.948916,1,\n' +
    '720N,Hunters Point Av,40.742216,-73.948916,0,720\n'
  const rail = new Set(['G22N', '719N', 'G24N', '720N'])
  const MTA = `${HEADER}\nG22,719,2,180\n719,G22,2,180\nG24,G24,2,180\n720,720,2,180\n`

  it('prices the walk as a second fare', () => {
    const merged = mergeTransfersTxt(MTA, `${HEADER}\nG24N,720N,2,205\n`, STOPS, new Map(), rail)
    // The G reaches 21 St at :10 and the 7 leaves Hunters Point Av at :18.
    expect(Number(minTimes(merged)['G24N>720N'])).toBeGreaterThan(8 * 60)
  })

  it('charges only the gates where the fare covers re-entry', () => {
    const merged = mergeTransfersTxt(MTA, `${HEADER}\nG24N,720N,2,205\n`, STOPS, new Map(), rail, true)
    expect(merged).toContain('G24N,720N,2,225')
  })

  it('leaves the in-system change at its own time', () => {
    const merged = mergeTransfersTxt(MTA, `${HEADER}\nG22N,719N,2,105\n`, STOPS, new Map(), rail)
    expect(merged).toContain('G22N,719N,2,105')
  })
})

function minTimes(text: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const line of text.split('\n').slice(1)) {
    const [from, to, , min] = line.split(',')
    if (from) out[`${from}>${to}`] = min
  }
  return out
}

describe('freeReentry', () => {
  const ATTRS = 'fare_id,price,currency_type,payment_method,transfers,transfer_duration'

  it('reads the base v1 fare\'s transfer allowance', () => {
    expect(freeReentry(`${ATTRS}\nadult,2.50,USD,1,,5400\n`, null)).toBe(true)
    expect(freeReentry(`${ATTRS}\nadult,2.50,USD,1,1,7200\n`, null)).toBe(true)
    expect(freeReentry(`${ATTRS}\nadult,2.50,USD,1,0,\n`, null)).toBe(false)
  })

  it('treats a v2 transfer rule with no product as free', () => {
    const rules = 'from_leg_group_id,to_leg_group_id,transfer_count,duration_limit,duration_limit_type,fare_transfer_type,fare_product_id'
    expect(freeReentry(null, `${rules}\nlg,lg,1,7200,1,0,\n`)).toBe(true)
    expect(freeReentry(null, `${rules}\nlg,lg,1,7200,1,0,fp_transfer\n`)).toBe(false)
  })

  it('assumes a second fare when the feed publishes none', () => {
    expect(freeReentry(null, null)).toBe(false)
  })
})
