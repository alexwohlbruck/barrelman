import type { RtUrlEntry } from '../../schema/gtfs'

/**
 * The realtime URLs MOTIS polls for a feed. One URL often serves trip updates
 * and vehicle positions both, so each is listed once. A URL the import marked
 * `normalize`, or that redirects or needs the egress proxy, is polled through barrelman's /gtfs-rt relay
 * instead, addressed by its position in the feed's `rt_urls`.
 */
export const viaRelay = (rt: RtUrlEntry) => !!(rt.normalize || rt.redirects || rt.egress)

export function motisRtSources(
  feedId: string,
  rtUrls: RtUrlEntry[],
  relay: { baseUrl: string; serviceKey: string },
): RtUrlEntry[] {
  return rtUrls.flatMap((rt, i) => {
    if (rtUrls.findIndex(o => o.url === rt.url) !== i) return []
    if (!rtUrls.some(o => o.url === rt.url && viaRelay(o))) return [rt]
    return [{
      url: `${relay.baseUrl}/gtfs-rt/${encodeURIComponent(feedId)}/${i}`,
      ...(relay.serviceKey && { headers: { Authorization: `Bearer ${relay.serviceKey}` } }),
    }]
  })
}
