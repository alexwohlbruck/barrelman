/**
 * Whether an entrance can be used to get in, out, or both. OSM's own
 * `entrance=exit|entry` decides; agency data fills in where OSM says nothing.
 * Null means unknown, which callers treat as allowed.
 */
export interface EntranceAccess {
  entryAllowed: boolean | null
  exitAllowed: boolean | null
  source: string | null
}

export function entranceAccess(
  osmEntrance: string | null | undefined,
  agency: EntranceAccess | null,
): EntranceAccess {
  if (osmEntrance === 'exit') return { entryAllowed: false, exitAllowed: true, source: 'osm' }
  if (osmEntrance === 'entry') return { entryAllowed: true, exitAllowed: false, source: 'osm' }
  if (agency && (agency.entryAllowed != null || agency.exitAllowed != null)) return agency
  return { entryAllowed: null, exitAllowed: null, source: null }
}
