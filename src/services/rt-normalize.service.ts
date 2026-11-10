/**
 * Serves a feed's GTFS-RT rewritten by src/lib/rt-normalize for MOTIS to poll.
 * Only URLs the import marked `normalize` are routed here; see
 * import/measure-rt-normalize.ts.
 */
import { existsSync, statSync, readFileSync } from 'fs'
import { join } from 'path'
import JSZip from 'jszip'
import GtfsRealtimeBindings, { type transit_realtime } from 'gtfs-realtime-bindings'
import { eq } from 'drizzle-orm'
import { db } from '../db'
import { gtfsFeeds, type RtUrlEntry } from '../schema/gtfs'
import { envString } from '../config/env'
import { buildStaticIndex, type StaticIndex } from '../lib/rt-normalize/static-index'
import { normalizeFeed, type NormalizeStats } from '../lib/rt-normalize/normalize'

const FeedMessage = () => GtfsRealtimeBindings.transit_realtime.FeedMessage

const GTFS_DATA_DIR = envString<string>('GTFS_DATA_DIR', './data/gtfs')
/** How long one rewritten message is served before the upstream is fetched again. */
const RESULT_TTL_MS = 15_000
const FEED_TTL_MS = 5 * 60_000

/** Where MOTIS reaches this service, from inside the compose network. */
export const normalizeBaseUrl = () => envString<string>('MOTIS_RT_NORMALIZE_URL', 'http://barrelman:5001')

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

export function decodeFeed(buf: Uint8Array): transit_realtime.IFeedMessage {
  const FM = FeedMessage()
  return FM.toObject(FM.decode(buf), { longs: Number }) as transit_realtime.IFeedMessage
}

export function encodeFeed(message: object): Uint8Array {
  const FM = FeedMessage()
  return FM.encode(FM.fromObject(message)).finish()
}

export async function fetchRealtime(rt: RtUrlEntry, fetchFn: typeof fetch = fetch) {
  const response = await fetchFn(rt.url, { headers: rt.headers ?? {}, signal: AbortSignal.timeout(15_000) })
  if (!response.ok) throw new Error(`upstream ${response.status}`)
  return decodeFeed(new Uint8Array(await response.arrayBuffer()))
}

// The zip on disk is the one MOTIS loaded at its last rebuild; reload when it changes.
const indexes = new Map<string, { mtimeMs: number; index: Promise<StaticIndex> }>()

function staticIndex(feedId: string): Promise<StaticIndex> {
  const path = join(GTFS_DATA_DIR, `${feedId}.zip`)
  if (!existsSync(path)) return Promise.reject(new Error(`no static feed at ${path}`))
  const { mtimeMs } = statSync(path)
  const cached = indexes.get(feedId)
  if (cached?.mtimeMs === mtimeMs) return cached.index
  const index = indexFromZip(readFileSync(path))
  indexes.set(feedId, { mtimeMs, index })
  index.catch(() => indexes.delete(feedId))
  return index
}

const feedUrls = new Map<string, { at: number; urls: Promise<RtUrlEntry[]> }>()

function rtUrlsOf(feedId: string): Promise<RtUrlEntry[]> {
  const cached = feedUrls.get(feedId)
  if (cached && Date.now() - cached.at < FEED_TTL_MS) return cached.urls
  const urls = db
    .select({ rtUrls: gtfsFeeds.rtUrls })
    .from(gtfsFeeds)
    .where(eq(gtfsFeeds.feedId, feedId))
    .then(rows => rows[0]?.rtUrls ?? [])
  feedUrls.set(feedId, { at: Date.now(), urls })
  return urls
}

export interface NormalizedFeed {
  body: Uint8Array
  /** Null when the feed was passed through unrewritten. */
  stats: NormalizeStats | null
}

const results = new Map<string, { at: number; result: Promise<NormalizedFeed> }>()

/**
 * The rewritten message for one of a feed's realtime URLs, or null when the
 * feed has no such URL marked for normalization. Never an open proxy: only
 * URLs already on the feed's record are fetched.
 */
export async function getNormalizedFeed(feedId: string, urlIndex: number): Promise<NormalizedFeed | null> {
  const key = `${feedId}/${urlIndex}`
  const cached = results.get(key)
  if (cached && Date.now() - cached.at < RESULT_TTL_MS) return cached.result

  const rt = (await rtUrlsOf(feedId))[urlIndex]
  if (!rt?.normalize) return null

  const result = (async () => {
    const message = await fetchRealtime(rt)
    try {
      const { message: out, stats } = normalizeFeed(await staticIndex(feedId), message)
      return { body: encodeFeed(out), stats }
    } catch (err) {
      // Unrewritten realtime still resolves what it always did.
      console.warn(`[rt-normalize] ${feedId}: passing through unrewritten: ${err instanceof Error ? err.message : err}`)
      return { body: encodeFeed(message), stats: null }
    }
  })()
  results.set(key, { at: Date.now(), result })
  result.catch(() => results.delete(key))
  return result
}
