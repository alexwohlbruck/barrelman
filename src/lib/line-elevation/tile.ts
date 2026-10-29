/**
 * Lines in a vector tile given the heights of the decks and tunnels they ride,
 * as an `elevation` property a 3D map raises or sinks them by.
 */
import { gtfsModeClass, isGtfsModeClass } from '../gtfs-modes'
import { metresPerUnit, type Point } from '../bridge-mesh/decks'
import { encodeElevation, RELATIVE_PREFIX } from './encode'
import { sampleLine, SourceIndex, type Rides, type Sample } from './match'
import { readLayer, withProperty, type MvtFeature } from './mvt'
import type { HeightSource } from './sources'

export const ELEVATION_PROPERTY = 'elevation'
/** The lowest zoom lines are given heights at. */
export const ELEVATION_MIN_ZOOM = 13

/** Which sources a feature of a layer may ride, or null for none. */
export type RidesOf = (feature: MvtFeature) => Rides | null

/** A cycle way rides only the decks and tunnels of its own OSM way. */
export const bicycleRides: RidesOf = f => (f.id === undefined ? null : { way: f.id })

/** A transit ribbon rides the ways it was drawn along, where its tile names them; otherwise what its mode runs on: rail on track decks and rail tunnels, a bus on roads. */
export const ribbonRides: RidesOf = f => {
  const ways = typeof f.properties.ways === 'string' ? f.properties.ways.split(',').map(Number).filter(Number.isFinite) : []
  if (ways.length) return { ways }
  const mode = gtfsModeClass(typeof f.properties.route_type === 'number' ? f.properties.route_type : null) ?? (isGtfsModeClass(f.properties.mode) ? f.properties.mode : null)
  if (mode === 'bus') return { kinds: ['road'] }
  if (mode && ['metro', 'tram', 'regional', 'monorail', 'funicular', 'cable'].includes(mode)) return { kinds: ['rail'] }
  return null
}

/** Layers whose lines are given heights, and what each rides. */
export const ELEVATED_LAYERS = { bicycle_ways: bicycleRides, ribbons: ribbonRides } as const satisfies Record<string, RidesOf>

/** Whether a tile has features in `layer` worth reading sources for. */
export function hasLines(tile: Uint8Array, layer: string): boolean {
  return (readLayer(tile, layer)?.features.length ?? 0) > 0
}

/** The point layers of a transit tile, each a station or a mark on the ribbons of the routes it names. */
export const RIBBON_POINT_LAYERS = ['stations', 'markers', 'cat']
/** Metres from a station's point to the ribbon of one of its routes it takes the height of. */
const POINT_REACH = 40

/** Each line of `layer`'s features read against `sources`, in feature order; null for a feature that rides nothing. */
function sampledFeatures(tile: Uint8Array, layer: string, z: number, x: number, y: number, index: SourceIndex, rides: RidesOf): Array<{ feature: MvtFeature; parts: Sample[][] } | null> {
  const read = readLayer(tile, layer)
  if (!read) return []
  const n = 2 ** z
  return read.features.map(feature => {
    const how = rides(feature)
    if (!how || feature.type !== 2) return null
    return { feature, parts: feature.lines.map(line => sampleLine(line.map(([px, py]) => [(x + px / read.extent) / n, (y + py / read.extent) / n]), index, how)) }
  })
}

/** The tile with `elevation` on every feature of `layer` that rides one of `sources`. */
export function elevateLayer(tile: Uint8Array, layer: string, z: number, x: number, y: number, sources: HeightSource[], rides: RidesOf): Uint8Array {
  if (!sources.length) return tile
  const sampled = sampledFeatures(tile, layer, z, x, y, new SourceIndex(sources), rides)
  let i = 0
  return withProperty(tile, layer, ELEVATION_PROPERTY, () => {
    const lines = sampled[i++]
    return lines ? encodeElevation(lines.parts) : null
  })
}

/**
 * A transit tile with its ribbons raised and sunk, and each station, stop mark and route bullet given the height of
 * the ribbon of one of its own routes beside it: metres in the terrain's datum, or over the terrain after `~`.
 */
export function elevateRibbonTile(tile: Uint8Array, z: number, x: number, y: number, sources: HeightSource[]): Uint8Array {
  if (!sources.length) return tile
  const sampled = sampledFeatures(tile, 'ribbons', z, x, y, new SourceIndex(sources), ribbonRides)
  let i = 0
  let out = withProperty(tile, 'ribbons', ELEVATION_PROPERTY, () => {
    const lines = sampled[i++]
    return lines ? encodeElevation(lines.parts) : null
  })
  const riding = sampled.flatMap(s => {
    if (!s) return []
    const routes = String(s.feature.properties.routes ?? '').split(',').filter(Boolean)
    return s.parts.flat().map(sample => ({ sample, routes }))
  })
  const n = 2 ** z
  for (const layer of RIBBON_POINT_LAYERS) {
    out = withProperty(out, layer, ELEVATION_PROPERTY, (feature, extent) => {
      const at = feature.lines[0]?.[0]
      if (feature.type !== 1 || !at) return null
      const p: Point = [(x + at[0] / extent) / n, (y + at[1] / extent) / n]
      const routes = new Set(String(feature.properties.routes ?? feature.properties.route ?? '').split(',').filter(Boolean))
      return pointHeight(p, routes, riding)
    })
  }
  return out
}

/** The height of the nearest ribbon sample of one of `routes` within reach of `p`, as the point's property, or null on the ground. */
export function pointHeight(p: Point, routes: Set<string>, riding: Array<{ sample: Sample; routes: string[] }>): string | null {
  const reach = POINT_REACH / metresPerUnit(p[1])
  let best: Sample | null = null
  let nearest = reach
  for (const { sample, routes: theirs } of riding) {
    const d = Math.hypot(sample.p[0] - p[0], sample.p[1] - p[1])
    if (d <= nearest && theirs.some(r => routes.has(r))) [best, nearest] = [sample, d]
  }
  if (best?.height == null) return null
  return `${best.relative ? RELATIVE_PREFIX : ''}${Math.round(best.height * 10) / 10}`
}

/**
 * A tile's lines in `layer` raised and sunk, or the tile as it came where that
 * fails. `complete` is false only when the sources could not be read, as a
 * tile that cannot be elevated at all is as good as it will get.
 */
export async function elevateTile(
  tile: Uint8Array, layer: keyof typeof ELEVATED_LAYERS, z: number, x: number, y: number,
  sources: (z: number, x: number, y: number) => Promise<HeightSource[]>,
): Promise<{ bytes: Uint8Array; complete: boolean }> {
  const fallback = (error: unknown, complete: boolean) => {
    console.warn(`[line elevation] ${layer} ${z}/${x}/${y}:`, error)
    return { bytes: tile, complete }
  }
  if (z < ELEVATION_MIN_ZOOM) return { bytes: tile, complete: true }
  try {
    if (!hasLines(tile, layer)) return { bytes: tile, complete: true }
  } catch (error) {
    return fallback(error, true)
  }
  let read: HeightSource[]
  try {
    read = await sources(z, x, y)
  } catch (error) {
    return fallback(error, false)
  }
  try {
    return { bytes: layer === 'ribbons' ? elevateRibbonTile(tile, z, x, y, read) : elevateLayer(tile, layer, z, x, y, read, ELEVATED_LAYERS[layer]), complete: true }
  } catch (error) {
    return fallback(error, true)
  }
}
