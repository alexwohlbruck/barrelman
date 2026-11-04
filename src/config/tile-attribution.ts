/**
 * The credits each tile source owes, in one table.
 *
 * TileJSON's `attribution` is what MapLibre shows in its attribution control,
 * so getting it right here is how a map built on our TileJSON or style meets
 * the licences in docs/content/docs/usage/attribution.mdx without its author
 * reading that page. Keep the two in step: a credit added there belongs here.
 */

import { resolveTileBundle } from './tile-bundles'

type Credit = 'osm' | 'openmaptiles' | 'mapterhorn'

/** HTML, because that is what MapLibre renders in the control. */
const CREDIT_HTML: Record<Credit, string> = {
  osm: '<a href="https://www.openstreetmap.org/copyright" target="_blank">© OpenStreetMap contributors</a>',
  openmaptiles: '<a href="https://openmaptiles.org" target="_blank">© OpenMapTiles</a>',
  mapterhorn: '<a href="https://mapterhorn.com/attribution" target="_blank">© Mapterhorn</a>',
}

/** The order credits appear in when a bundle owes several. */
const CREDIT_ORDER: readonly Credit[] = ['osm', 'openmaptiles', 'mapterhorn']

/**
 * Sources owing more than OpenStreetMap. Anything absent is OSM-derived and
 * owes OSM alone, which is every Martin table today.
 *
 * The Mapterhorn rows are the layers whose heights are read from its terrain
 * at import. Landmarks owe OSM for the buildings they stand in for; a model's
 * own credit travels per feature, in its `attribution` property.
 */
const SOURCE_CREDITS: Record<string, readonly Credit[]> = {
  basemap: ['osm', 'openmaptiles'],
  bridge_decks: ['osm', 'mapterhorn'],
  tunnel_cuts: ['osm', 'mapterhorn'],
  bicycle_ways: ['osm', 'mapterhorn'],
  landmarks: ['osm'],
}

const DEFAULT_CREDITS: readonly Credit[] = ['osm']

/**
 * The attribution string for a source, a bundle, or a comma-joined composite:
 * the union of its members' credits, each once.
 */
export function tileAttribution(source: string): string {
  const owed = new Set<Credit>()
  for (const member of resolveTileBundle(source).split(',').filter(Boolean)) {
    for (const credit of SOURCE_CREDITS[member] ?? DEFAULT_CREDITS) owed.add(credit)
  }
  return CREDIT_ORDER.filter((credit) => owed.has(credit))
    .map((credit) => CREDIT_HTML[credit])
    .join(' ')
}
