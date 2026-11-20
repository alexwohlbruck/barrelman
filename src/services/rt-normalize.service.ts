/**
 * Serves a feed's GTFS-RT rewritten by src/lib/rt-normalize for MOTIS to poll.
 * Only URLs the import marked `normalize` are routed here; see
 * import/measure-rt-normalize.ts.
 */
import { join } from 'path'
import { eq } from 'drizzle-orm'
import { db } from '../db'
import { gtfsFeeds, type RtUrlEntry } from '../schema/gtfs'
import { envString } from '../config/env'
import { decodeFeed } from '../lib/rt-normalize/feed-codec'
import type { NormalizedFeed } from '../lib/rt-normalize/rewrite'
import { workerRewriter } from '../lib/rt-normalize/rewrite-worker'
import { isTripUpdates, viaRelay } from '../lib/rt-normalize/motis-sources'
import type { LiveFeed } from '../lib/rt-normalize/exact-times'
import { rtEgressProxy } from './rt-url-selection'

export type { NormalizedFeed }

const GTFS_DATA_DIR = envString<string>('GTFS_DATA_DIR', './data/gtfs')
/** How long one rewritten message is served before the upstream is fetched again. */
const RESULT_TTL_MS = 15_000
/**
 * A refetch slower than this is answered with the previous message, if it is
 * no older than the limit. MOTIS waits for every feed before applying any, so
 * one slow agency would hold back all.
 */
const FRESH_WAIT_MS = 5_000
const STALE_LIMIT_MS = 3 * 60_000
/** MARTA's feed takes up to a minute to download. */
const RELAY_UPSTREAM_TIMEOUT_MS = 90_000
const FEED_TTL_MS = 5 * 60_000

/** Where MOTIS reaches this service, from inside the compose network. */
export const normalizeBaseUrl = () => envString<string>('MOTIS_RT_NORMALIZE_URL', 'http://barrelman:5001')

/** A realtime URL's message as the upstream sent it, undecoded. */
export async function fetchRealtimeBytes(rt: RtUrlEntry, fetchFn: typeof fetch = fetch, timeoutMs = 15_000) {
  const proxy = rt.egress ? rtEgressProxy() : undefined
  const response = await fetchFn(rt.url, { headers: rt.headers ?? {}, signal: AbortSignal.timeout(timeoutMs), ...(proxy && { proxy }) } as RequestInit)
  if (!response.ok) throw new Error(`upstream ${response.status}`)
  return new Uint8Array(await response.arrayBuffer())
}

export async function fetchRealtime(rt: RtUrlEntry, fetchFn: typeof fetch = fetch, timeoutMs = 15_000) {
  return decodeFeed(await fetchRealtimeBytes(rt, fetchFn, timeoutMs))
}

const rewriter = workerRewriter()

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

interface Relayed {
  last?: { at: number; feed: NormalizedFeed }
  pending?: Promise<NormalizedFeed>
}

const results = new Map<string, Relayed>()

async function relay(feedId: string, rt: RtUrlEntry): Promise<NormalizedFeed> {
  const raw = await fetchRealtimeBytes(rt, fetch, RELAY_UPSTREAM_TIMEOUT_MS)
  return rewriter.rewrite({ zipPath: join(GTFS_DATA_DIR, `${feedId}.zip`), raw, normalize: !!rt.normalize })
}

/** The last message for `key`, refreshed after RESULT_TTL_MS; a slow refresh is answered with the previous one. */
function latest(key: string, load: () => Promise<NormalizedFeed>): Promise<NormalizedFeed> {
  let entry = results.get(key)
  if (!entry) results.set(key, (entry = {}))
  const age = entry.last ? Date.now() - entry.last.at : Infinity
  if (age >= RESULT_TTL_MS && !entry.pending) {
    const relayed = entry
    relayed.pending = load()
      .then(feed => (relayed.last = { at: Date.now(), feed }).feed)
      .finally(() => { relayed.pending = undefined })
    relayed.pending.catch(() => {})
  }
  if (!entry.pending) return Promise.resolve(entry.last!.feed)
  const previous = entry.last && age < STALE_LIMIT_MS ? entry.last.feed : null
  const late = Bun.sleep(FRESH_WAIT_MS).then(() => {
    if (previous) return previous
    throw new Error('upstream has not answered yet')
  })
  return Promise.race([previous ? entry.pending.catch(() => previous) : entry.pending, late])
}

/**
 * The rewritten message for one of a feed's realtime URLs, or null when the
 * feed has no such URL routed through the relay. A URL routed here only to
 * be fetched, because it redirects or refuses this network, is passed through
 * as published. Never an open proxy:
 * only URLs already on the feed's record are fetched.
 */
export async function getNormalizedFeed(feedId: string, urlIndex: number): Promise<NormalizedFeed | null> {
  const rt = (await rtUrlsOf(feedId))[urlIndex]
  if (!rt || !viaRelay(rt)) return null
  return latest(`${feedId}/${urlIndex}`, () => relay(feedId, rt))
}

const callIndexes = new WeakMap<Uint8Array, Promise<LiveFeed>>()

/**
 * Every stop prediction in a feed's trip updates, under the trip ids MOTIS
 * holds: relayed URLs as rewritten, the rest as published, since MOTIS
 * only resolves those that already match.
 */
export async function liveCalls(feedId: string): Promise<LiveFeed> {
  const urls = await rtUrlsOf(feedId)
  const feeds = await Promise.allSettled(
    urls.map((rt, i) => {
      if (!isTripUpdates(rt)) return Promise.resolve(null)
      if (viaRelay(rt)) return latest(`${feedId}/${i}`, () => relay(feedId, rt))
      return latest(`${feedId}/${i}/direct`, async () => ({ body: await fetchRealtimeBytes(rt), stats: null }))
    }),
  )
  const live: LiveFeed = { calls: new Map(), trips: new Set(), routes: new Set() }
  for (const f of feeds) {
    if (f.status !== 'fulfilled' || !f.value) continue
    let pending = callIndexes.get(f.value.body)
    if (!pending) callIndexes.set(f.value.body, (pending = rewriter.calls(f.value.body)))
    const index = await pending.catch(() => null)
    if (!index) continue
    for (const [k, v] of index.calls) live.calls.set(k, v)
    for (const t of index.trips) live.trips.add(t)
    for (const r of index.routes) live.routes.add(r)
  }
  return live
}
