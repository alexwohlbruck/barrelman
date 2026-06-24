import { describe, test, expect } from 'bun:test'
import { parseCsv, convertFaresV1toV2 } from './inject-fares-v2'

const TRAM_V1 = `agency_id,fare_id,price,currency_type,payment_method,transfers,transfer_duration
805,2357,2.50,USD,0,0,
805,2358,1.25,USD,0,0,
805,2359,0.00,USD,0,0,
`

describe('parseCsv', () => {
  test('parses quoted fields and CRLF', () => {
    const rows = parseCsv('a,b\r\n"x,1","he said ""hi"""\r\n')
    expect(rows).toEqual([{ a: 'x,1', b: 'he said "hi"' }])
  })

  test('skips blank lines', () => {
    expect(parseCsv('a,b\n1,2\n\n')).toEqual([{ a: '1', b: '2' }])
  })
})

describe('convertFaresV1toV2', () => {
  test('flat fares: rider categories with first row default', () => {
    const r = convertFaresV1toV2(TRAM_V1, null, false)
    expect(r.skipped).toBeUndefined()
    expect(r.shape).toBe('flat')
    expect(r.productCount).toBe(3)
    // First fare (2.50 adult) is the default rider category
    const cats = parseCsv(r.files!['rider_categories.txt'])
    expect(cats[0]).toMatchObject({ rider_category_id: 'rc_2357', is_default_fare_category: '1' })
    expect(cats[1].is_default_fare_category).toBe('0')
    const prods = parseCsv(r.files!['fare_products.txt'])
    expect(prods[0]).toMatchObject({ fare_product_id: 'fp_2357', amount: '2.50', currency: 'USD' })
    // All leg rules share one group, no networks needed
    const legs = parseCsv(r.files!['fare_leg_rules.txt'])
    expect(legs).toHaveLength(3)
    expect(new Set(legs.map((l) => l.leg_group_id))).toEqual(new Set(['lg_all']))
    expect(r.files!['networks.txt']).toBeUndefined()
    // transfers=0 → no transfer rules
    expect(r.files!['fare_transfer_rules.txt']).toBeUndefined()
  })

  test('route-scoped fares: networks + first-fare-wins on duplicate routes', () => {
    const attrs = `fare_id,price,currency_type,payment_method,transfers,transfer_duration
local,2.20,USD,0,,5400
express,4.40,USD,0,0,
`
    const rules = `fare_id,route_id
local,R1
local,R2
express,R2
express,R3
`
    const r = convertFaresV1toV2(attrs, rules, false)
    expect(r.shape).toBe('route-scoped')
    const rn = parseCsv(r.files!['route_networks.txt'])
    // R2 claimed by local (listed first); express keeps R3 only
    expect(rn).toEqual([
      { network_id: 'net_local', route_id: 'R1' },
      { network_id: 'net_local', route_id: 'R2' },
      { network_id: 'net_express', route_id: 'R3' },
    ])
    // local: unlimited free transfers for 90 min
    const tr = parseCsv(r.files!['fare_transfer_rules.txt'])
    expect(tr).toHaveLength(1)
    expect(tr[0]).toMatchObject({
      from_leg_group_id: 'lg_local',
      to_leg_group_id: 'lg_local',
      transfer_count: '-1',
      duration_limit: '5400',
      fare_transfer_type: '0',
      fare_product_id: '',
    })
  })

  test('flat fare with free transfer window emits a transfer rule', () => {
    const attrs = `fare_id,price,currency_type,payment_method,transfers,transfer_duration
base,3.00,USD,0,,7200
`
    const r = convertFaresV1toV2(attrs, null, false)
    const tr = parseCsv(r.files!['fare_transfer_rules.txt'])
    expect(tr[0]).toMatchObject({ transfer_count: '-1', duration_limit: '7200' })
  })

  test('zone-based feeds are skipped', () => {
    const rules = `fare_id,route_id,origin_id,destination_id
z,,1,2
`
    const r = convertFaresV1toV2(TRAM_V1, rules, false)
    expect(r.skipped).toBe('zone-based')
  })

  test('already-v2 and fare-less feeds are skipped', () => {
    expect(convertFaresV1toV2(TRAM_V1, null, true).skipped).toBe('already-v2')
    expect(convertFaresV1toV2(null, null, false).skipped).toBe('no-v1-fares')
  })
})
