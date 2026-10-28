/**
 * Lines in a vector tile given the heights of the decks and tunnels they ride,
 * as an `elevation` property a 3D map raises or sinks them by.
 */
import { gtfsModeClass, isGtfsModeClass } from '../gtfs-modes'
import { encodeElevation } from './encode'
import { sampleLine, SourceIndex, type Rides } from './match'
import { readLayer, withProperty, type MvtFeature } from './mvt'
import type { HeightSource } from './sources'

export const ELEVATION_PROPERTY = 'elevation'
/** The lowest zoom lines are given heights at. */
export const ELEVATION_MIN_ZOOM = 13

/** Which sources a feature of a layer may ride, or null for none. */
export type RidesOf = (feature: MvtFeature) => Rides | null

/** A cycle way rides only the decks and tunnels of its own OSM way. */
export const bicycleRides: RidesOf = f => (f.id === undefined ? null : { way: f.id })

/** A transit ribbon rides what its mode runs on: rail on track decks and rail tunnels, a bus on roads. */
export const ribbonRides: RidesOf = f => {
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

/** The tile with `elevation` on every feature of `layer` that rides one of `sources`. */
export function elevateLayer(tile: Uint8Array, layer: string, z: number, x: number, y: number, sources: HeightSource[], rides: RidesOf): Uint8Array {
  if (!sources.length) return tile
  const index = new SourceIndex(sources)
  const n = 2 ** z
  return withProperty(tile, layer, ELEVATION_PROPERTY, (feature, extent) => {
    const how = rides(feature)
    if (!how || feature.type !== 2) return null
    const parts = feature.lines.map(line => sampleLine(line.map(([px, py]) => [(x + px / extent) / n, (y + py / extent) / n]), index, how))
    return encodeElevation(parts)
  })
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
    return { bytes: elevateLayer(tile, layer, z, x, y, read, ELEVATED_LAYERS[layer]), complete: true }
  } catch (error) {
    return fallback(error, true)
  }
}
