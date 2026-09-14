import { describe, test, expect } from 'bun:test'
import { defaultBikesAllowed } from './inject-bikes-allowed'
import { parseCsvRows } from './csv'

/** Value of bikes_allowed per data row, in file order. */
function column(tripsTxt: string): string[] {
  const rows = parseCsvRows(tripsTxt)
  const idx = rows[0].map((h) => h.trim()).indexOf('bikes_allowed')
  return rows.slice(1).filter((r) => r.length > 1).map((r) => r[idx])
}

describe('defaultBikesAllowed', () => {
  test('adds the column when the feed omits it entirely', () => {
    const result = defaultBikesAllowed(
      'route_id,service_id,trip_id\nr1,s1,t1\nr1,s1,t2\n',
    )

    expect(result.addedColumn).toBe(true)
    expect(result.filled).toBe(2)
    expect(column(result.tripsTxt!)).toEqual(['1', '1'])
  })

  test('fills "no information" but never overrides a refusal', () => {
    const result = defaultBikesAllowed(
      'trip_id,bikes_allowed\nt1,0\nt2,\nt3,2\nt4,1\n',
    )

    expect(column(result.tripsTxt!)).toEqual(['1', '1', '2', '1'])
    expect(result.filled).toBe(2)
    expect(result.forbidden).toBe(1)
    expect(result.declared).toBe(1)
  })

  test('leaves a feed alone when every trip already states its policy', () => {
    const result = defaultBikesAllowed('trip_id,bikes_allowed\nt1,1\nt2,2\n')

    expect(result.tripsTxt).toBeUndefined()
    expect(result.filled).toBe(0)
    expect(result.forbidden).toBe(1)
  })

  test('keeps quoted fields intact rather than corrupting the row', () => {
    const result = defaultBikesAllowed(
      'trip_id,trip_headsign\nt1,"Downtown, via Main"\n',
    )

    const rows = parseCsvRows(result.tripsTxt!)
    expect(rows[1][1]).toBe('Downtown, via Main')
    expect(column(result.tripsTxt!)).toEqual(['1'])
  })

  test('pads a short row so the value lands under its own header', () => {
    // trip_headsign omitted on the second row — a real shape in the wild.
    const result = defaultBikesAllowed(
      'trip_id,trip_headsign,bikes_allowed\nt1,Downtown,0\nt2\n',
    )

    const rows = parseCsvRows(result.tripsTxt!)
    expect(rows[2]).toEqual(['t2', '', '1'])
  })

  test('leaves bus trips unstated, since most take only a folding bike', () => {
    const routes = 'route_id,route_type\nbus1,3\ntrolley,11\nexpress,702\nsub,1\n'
    const result = defaultBikesAllowed(
      'trip_id,route_id\nt1,bus1\nt2,trolley\nt3,express\nt4,sub\n',
      routes,
    )

    expect(result.bus).toBe(3)
    expect(result.filled).toBe(1)
    expect(column(result.tripsTxt!)).toEqual(['', '', '', '1'])
  })

  test('fills bus trips when the network is known to carry full-size bikes', () => {
    const result = defaultBikesAllowed(
      'trip_id,route_id\nt1,bus1\nt2,sub\n',
      'route_id,route_type\nbus1,3\nsub,1\n',
      { allowBus: true },
    )

    expect(result.bus).toBe(0)
    expect(column(result.tripsTxt!)).toEqual(['1', '1'])
  })

  test('still honours an explicit bus policy either way', () => {
    const result = defaultBikesAllowed(
      'trip_id,route_id,bikes_allowed\nt1,bus1,1\nt2,bus1,2\nt3,bus1,\n',
      'route_id,route_type\nbus1,3\n',
    )

    expect(result.declared).toBe(1)
    expect(result.forbidden).toBe(1)
    expect(result.bus).toBe(1)
    expect(result.tripsTxt).toBeUndefined()
  })

  test('fills everything when the feed has no routes.txt to classify with', () => {
    const result = defaultBikesAllowed('trip_id,route_id\nt1,bus1\n', null)

    expect(result.bus).toBe(0)
    expect(column(result.tripsTxt!)).toEqual(['1'])
  })

  test('reports feeds with no trips rather than inventing any', () => {
    expect(defaultBikesAllowed(null).skipped).toBe('no-trips')
    expect(defaultBikesAllowed('trip_id,bikes_allowed\n').skipped).toBe('empty-trips')
  })
})
