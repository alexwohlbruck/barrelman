import JSZip from 'jszip'
import GtfsRealtimeBindings, { type transit_realtime } from 'gtfs-realtime-bindings'
import { buildStaticIndex, type StaticIndex } from './static-index'

const FeedMessage = () => GtfsRealtimeBindings.transit_realtime.FeedMessage

export function decodeFeed(buf: Uint8Array): transit_realtime.IFeedMessage {
  const FM = FeedMessage()
  return FM.toObject(FM.decode(buf), { longs: Number }) as transit_realtime.IFeedMessage
}

export function encodeFeed(message: object): Uint8Array {
  const FM = FeedMessage()
  return FM.encode(FM.fromObject(message)).finish()
}

/** stop_times.txt beyond this is not indexed: barrelman would hold it in memory for every poll. */
export const MAX_STOP_TIMES_CHARS = 400_000_000

export async function indexFromZip(buffer: ArrayBuffer | Uint8Array): Promise<StaticIndex> {
  const zip = await JSZip.loadAsync(buffer)
  const text = (name: string) => zip.file(name)?.async('string')
  const [trips, stopTimes] = await Promise.all([text('trips.txt'), text('stop_times.txt')])
  if (!trips || !stopTimes) throw new Error('feed has no trips.txt or stop_times.txt')
  if (stopTimes.length > MAX_STOP_TIMES_CHARS) throw new Error('stop_times.txt is too large to index')
  return buildStaticIndex({
    trips,
    stopTimes,
    agency: await text('agency.txt'),
    routes: await text('routes.txt'),
    stops: await text('stops.txt'),
    calendar: await text('calendar.txt'),
    calendarDates: await text('calendar_dates.txt'),
  })
}
