/**
 * Named tile bundles.
 *
 * Martin addresses a composite source by joining names with commas —
 * `/tiles/a,b,c/{z}/{x}/{y}` — which works, but writes the server's schema
 * into every client's style: the membership cannot change without shipping a
 * new client, and a public URL ends up naming private relations. A bundle is a
 * stable public name for one of those lists, so a map declares `detail` once
 * and the set behind it stays ours to change.
 *
 * Why bundle at all: a map view is thirty to sixty tiles *per source*, each a
 * separate metered request. Measured against the public deployment, a six-tile
 * viewport over Manhattan went from 18 requests to 6, and from 2.04 s to
 * 1.14 s warm.
 *
 * Membership rules, both verified against Martin rather than assumed:
 *
 *  - A member BELOW its own minzoom contributes nothing and costs nothing —
 *    the composite still answers 200 carrying the other members' layers.
 *  - A composite 404s only when EVERY member is above its maxzoom.
 *
 * So a bundle may mix minzooms freely, but it must not mix *maxzooms*. The
 * union's top is what a client will request, and any member that stops below
 * that would silently vanish there instead of over-zooming from its own last
 * tile. That is why `street_furniture` is absent: it is minzoom 17, so it can
 * contribute nothing to a bundle a client reads at z≤16, and folding it in
 * would take street furniture off the map rather than speed it up. It folds in
 * here the day its minzoom drops to 16 — at which point the bundle's client
 * can drop its own separate source for it.
 */
export const TILE_BUNDLES: Record<string, readonly string[]> = {
  /** Every detail overlay a client reads at z≤16, as one source. */
  detail: [
    'buildings_3d',
    'parking_areas',
    'bicycle_ways',
    'street_trees',
    'tree_rows',
  ],
}

/**
 * The Martin source list a request should actually address.
 *
 * Anything that is not a bundle name passes through untouched, so the raw
 * source names and Martin's own comma syntax keep working — the bundle is a
 * convenience over that surface, not a replacement for it.
 */
export function resolveTileBundle(source: string): string {
  const members = TILE_BUNDLES[source]
  return members ? members.join(',') : source
}
