import { resolve } from 'path'
import { envString } from './env'

/**
 * Where `portolan sync` writes its tile pyramids and the per-feed indexes
 * (tiles.json, routes.json, stops.json). Shared by the /tiles routes that
 * serve those files and the stop-link loader that ingests stops.json.
 */
export function resolvePortolanTilesDir(override?: string): string {
  return resolve(override || envString('PORTOLAN_TILES_DIR', './data/portolan/build/tiles'))
}
