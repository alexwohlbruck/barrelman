import { describe, expect, it } from 'bun:test'
import { parseCsvRows, rewriteCsvColumn } from './csv'

describe('rewriteCsvColumn', () => {
  const rename = (map: Record<string, string>) => (v: string) => map[v]

  it('rewrites only the named column and copies other lines verbatim', () => {
    const text = 'trip_id,stop_id\r\nA_1,  s1 \r\nB_2,s2\r\n'
    const out = rewriteCsvColumn(text, 'trip_id', rename({ A_1: '1' }))
    expect(out.changed).toBe(1)
    // The CRLF and the untouched line's spacing survive.
    expect(out.text).toBe('trip_id,stop_id\r\n1,  s1 \r\nB_2,s2\r\n')
  })

  it('matches on the trimmed value', () => {
    expect(rewriteCsvColumn('trip_id\n A \n', 'trip_id', rename({ A: 'B' })).text).toBe('trip_id\nB\n')
  })

  it('keeps quoted fields with commas and newlines in one record', () => {
    const text = 'stop_headsign,trip_id\n"Main St, then\nDowntown",A\nplain,A\n'
    const out = rewriteCsvColumn(text, 'trip_id', rename({ A: 'Z' }))
    expect(out.changed).toBe(2)
    expect(parseCsvRows(out.text)).toEqual([
      ['stop_headsign', 'trip_id'],
      ['Main St, then\nDowntown', 'Z'],
      ['plain', 'Z'],
    ])
  })

  it('finds the column behind a byte-order mark', () => {
    expect(rewriteCsvColumn('﻿trip_id,x\nA,1\n', 'trip_id', rename({ A: 'B' })).text).toBe('﻿trip_id,x\nB,1\n')
  })

  it('returns the same string when nothing changes or the column is absent', () => {
    const text = 'trip_id\nA\n'
    expect(rewriteCsvColumn(text, 'trip_id', () => undefined).text).toBe(text)
    expect(rewriteCsvColumn(text, 'route_id', () => 'x')).toEqual({ text, changed: 0 })
  })
})
