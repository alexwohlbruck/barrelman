import type { RtUrlEntry } from '../../schema/gtfs'

/**
 * The realtime URLs MOTIS polls for a feed. One URL often serves trip updates
 * and vehicle positions both, so each is listed once. A URL the import marked
 * `normalize`, or that redirects, is polled through barrelman's /gtfs-rt relay
 * instead, addressed by its position in the feed's `rt_urls`.
 */
export function motisRtSources(
  feedId: string,
  rtUrls: RtUrlEntry[],
  relay: { baseUrl: string; serviceKey: string },
): RtUrlEntry[] {
  return rtUrls.flatMap((rt, i) => {
    if (rtUrls.findIndex(o => o.url === rt.url) !== i) return []
    if (!rtUrls.some(o => o.url === rt.url && (o.normalize || o.redirects))) return [rt]
    return [{
      url: `${relay.baseUrl}/gtfs-rt/${encodeURIComponent(feedId)}/${i}`,
      ...(relay.serviceKey && { headers: { Authorization: `Bearer ${relay.serviceKey}` } }),
    }]
  })
}
