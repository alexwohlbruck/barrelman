import { describe, expect, it } from 'bun:test'
import JSZip from 'jszip'
import { sharedTrips, strippedSide, stripTrips, tripSignatures } from './dedupe-feed-trips'

const stopTimes = (rows: string[]) => 'trip_id,arrival_time,departure_time,stop_id,stop_sequence\n' + rows.join('\n') + '\n'

describe('dedupe feed trips', () => {
  it('counts a trip as shared when it calls at the same stops, even at other times', () => {
    const main = tripSignatures(stopTimes(['T1,08:00:00,08:00:00,A,1', 'T1,08:05:00,08:05:00,B,2', 'T2,09:00:00,09:00:00,A,1', 'T3,09:00:00,09:00:00,A,1']))
    const flex = tripSignatures(stopTimes(['T1,08:05:00,08:05:00,B,2', 'T1,08:00:00,08:00:00,A,1', 'T2,09:01:00,09:01:00,A,1', 'T3,09:00:00,09:00:00,C,1', 'F1,10:00:00,10:00:00,Z,1']))
    expect([...sharedTrips(main, flex)]).toEqual(['T1', 'T2'])
  })

  it('strips the feed with more trips, and leaves exact copies alone', () => {
    expect(strippedSide(21813, 22083, 21813)).toBe('b')
    expect(strippedSide(88, 88, 88)).toBeNull()
    expect(strippedSide(500, 300, 0)).toBeNull()
  })

  it('removes the trips, their stop times and the routes they leave empty', async () => {
    const zip = new JSZip()
    zip.file('routes.txt', 'route_id,route_type\nR1,3\nFLEX,3\n')
    zip.file('trips.txt', 'route_id,service_id,trip_id\nR1,WK,T1\nFLEX,WK,F1\n')
    zip.file('stop_times.txt', stopTimes(['T1,08:00:00,08:00:00,A,1', 'F1,10:00:00,10:00:00,Z,1']))
    await stripTrips(zip, new Set(['T1']))
    expect(await zip.file('trips.txt')!.async('string')).not.toContain('T1')
    expect(await zip.file('stop_times.txt')!.async('string')).toBe(stopTimes(['F1,10:00:00,10:00:00,Z,1']))
    expect((await zip.file('routes.txt')!.async('string')).trim().split('\n')).toEqual(['route_id,route_type', 'FLEX,3'])
  })
})
