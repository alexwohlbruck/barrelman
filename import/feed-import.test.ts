import { describe, expect, it } from 'bun:test'
import { mergeTransfersTxt } from './feed-import'

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
