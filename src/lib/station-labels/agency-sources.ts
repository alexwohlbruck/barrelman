/**
 * Agency open data that enriches station labels. Every source is optional:
 * labels are derived from OSM alone wherever none applies, so a source only
 * ever adds rows for the feeds it names.
 */
import { cleanLabel } from './direction-labels'

export interface StopLabel {
  stopId: string
  label: string
}

export interface AgencyEntrance {
  ref: string
  stopId: string | null
  entranceType: string | null
  entryAllowed: boolean | null
  exitAllowed: boolean | null
  lat: number
  lon: number
}

export interface DirectionLabelSourceDef {
  id: string
  /** Transitland onestop ids of the feeds whose stop ids the data uses. */
  onestopIds: string[]
  url: string
  parse(rows: unknown[]): StopLabel[]
}

export interface EntranceSourceDef {
  id: string
  /** Fetched only on an instance that carries one of these feeds. */
  onestopIds: string[]
  url: string
  parse(rows: unknown[]): AgencyEntrance[]
}

type Row = Record<string, unknown>
const str = (v: unknown) => (typeof v === 'string' && v.trim() ? v.trim() : null)
const yesNo = (v: unknown) => {
  const s = str(v)?.toUpperCase()
  return s === 'YES' ? true : s === 'NO' ? false : null
}

/**
 * MTA Subway Stations (data.ny.gov 39hk-dx4f). Its `gtfs_stop_id` is the parent
 * station; the subway feed's directional platforms are that id plus N or S.
 */
export function parseMtaStations(rows: unknown[]): StopLabel[] {
  return (rows as Row[]).flatMap((row) => {
    const parent = str(row.gtfs_stop_id)
    if (!parent) return []
    return ([['N', row.north_direction_label], ['S', row.south_direction_label]] as const)
      .flatMap(([suffix, raw]) => {
        const label = cleanLabel(str(raw))
        return label ? [{ stopId: parent + suffix, label }] : []
      })
  })
}

/** MTA Subway Entrances and Exits (data.ny.gov i9wp-a4ja). No stable id, so rows key on position. */
export function parseMtaEntrances(rows: unknown[]): AgencyEntrance[] {
  const byRef = new Map<string, AgencyEntrance>()
  for (const row of rows as Row[]) {
    const lat = Number(row.entrance_latitude)
    const lon = Number(row.entrance_longitude)
    if (!Number.isFinite(lat) || !Number.isFinite(lon) || (lat === 0 && lon === 0)) continue
    const ref = `${lat.toFixed(6)},${lon.toFixed(6)}`
    const entrance: AgencyEntrance = {
      ref,
      stopId: str(row.gtfs_stop_id),
      entranceType: str(row.entrance_type),
      entryAllowed: yesNo(row.entry_allowed),
      exitAllowed: yesNo(row.exit_allowed),
      lat,
      lon,
    }
    // One point listed for two stations is one opening; it allows what either row allows.
    const seen = byRef.get(ref)
    byRef.set(ref, seen ? {
      ...seen,
      entryAllowed: either(seen.entryAllowed, entrance.entryAllowed),
      exitAllowed: either(seen.exitAllowed, entrance.exitAllowed),
    } : entrance)
  }
  return [...byRef.values()]
}

const either = (a: boolean | null, b: boolean | null) => (a === true || b === true ? true : a ?? b)

export const DIRECTION_LABEL_SOURCES: DirectionLabelSourceDef[] = [
  {
    id: 'mta-subway-stations',
    onestopIds: ['f-dr5r-nyctsubway'],
    url: 'https://data.ny.gov/resource/39hk-dx4f.json?$limit=5000',
    parse: parseMtaStations,
  },
]

export const ENTRANCE_SOURCES: EntranceSourceDef[] = [
  {
    id: 'mta-subway-entrances',
    onestopIds: ['f-dr5r-nyctsubway'],
    url: 'https://data.ny.gov/resource/i9wp-a4ja.json?$limit=10000',
    parse: parseMtaEntrances,
  },
]
