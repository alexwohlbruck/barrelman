/**
 * Parsing for GBFS feeds that differ between v2 and v3.
 *
 * GBFS v3 changed several fields in place rather than adding new ones, so code
 * written against v2 does not fail loudly on a v3 feed — it reads the wrong
 * shape and carries on. These helpers accept both, and are shared by the
 * catalog importer and the live-status path so the two cannot drift.
 */

export interface GbfsVehicleType {
  vehicleTypeId: string
  formFactor: string // bicycle, cargo_bicycle, scooter, scooter_standing, moped, car, other
  propulsionType: string // human, electric_assist, electric, combustion
  name?: string
}

/**
 * A GBFS text field. v2 sends a plain string; v3 sends localized text,
 * `[{ text, language }]`. English is preferred, then the first entry.
 */
export function localizedText(value: unknown): string {
  if (typeof value === 'string') return value
  if (!Array.isArray(value)) return ''
  const entries = value.filter(
    (v): v is { text: string; language?: string } => typeof v?.text === 'string',
  )
  const english = entries.find((v) => v.language?.toLowerCase().startsWith('en'))
  return (english ?? entries[0])?.text ?? ''
}

/**
 * `last_reported` as an ISO string, or null. v2 sends POSIX seconds; v3 sends
 * an RFC 3339 timestamp. Multiplying the v3 string by 1000 yields NaN, and an
 * invalid Date throws from toISOString().
 */
export function lastReportedIso(value: unknown): string | null {
  let date: Date
  if (typeof value === 'number') date = new Date(value * 1000)
  else if (typeof value === 'string' && value) date = new Date(value)
  else return null
  return Number.isNaN(date.getTime()) ? null : date.toISOString()
}

/**
 * The stored vehicle_types column holds the feed's raw snake_case objects.
 * Normalize them to the camelCase shape the API documents. Rows already in
 * that shape pass through.
 */
export function normalizeVehicleTypes(raw: unknown): GbfsVehicleType[] {
  if (!Array.isArray(raw)) return []
  return raw.map((vt: any) => ({
    vehicleTypeId: String(vt?.vehicle_type_id ?? vt?.vehicleTypeId ?? ''),
    formFactor: vt?.form_factor ?? vt?.formFactor ?? 'other',
    propulsionType: vt?.propulsion_type ?? vt?.propulsionType ?? 'human',
    ...(vt?.name != null ? { name: localizedText(vt.name) } : {}),
  }))
}

export function isScooter(formFactor: string): boolean {
  return formFactor.startsWith('scooter')
}

export function isEbike(vt: GbfsVehicleType): boolean {
  return vt.formFactor.includes('bicycle') && vt.propulsionType !== 'human'
}

export interface StationAvailability {
  numBikesAvailable: number // non-electric bikes
  numEbikesAvailable: number
  numScootersAvailable: number
  numDocksAvailable: number
  isRenting: boolean
  isReturning: boolean
  lastReported: string | null
}

/**
 * One station_status entry. v3 renamed `num_bikes_available` to
 * `num_vehicles_available` and dropped the per-kind counts, so e-bikes and
 * scooters come from `vehicle_types_available` matched against the system's
 * vehicle types. The id-substring check is a last resort for systems that
 * publish no vehicle_types feed.
 */
export function parseStationStatus(
  s: any,
  vehicleTypes: GbfsVehicleType[],
): StationAvailability {
  const total = s.num_bikes_available ?? s.num_vehicles_available ?? 0
  const types = new Map(vehicleTypes.map((vt) => [vt.vehicleTypeId, vt]))
  const available: Array<{ vehicle_type_id?: string; count?: number }> =
    Array.isArray(s.vehicle_types_available) ? s.vehicle_types_available : []

  const countWhere = (match: (vt: GbfsVehicleType | undefined, id: string) => boolean) =>
    available.reduce((sum, a) => {
      const id = String(a.vehicle_type_id ?? '')
      return match(types.get(id), id) ? sum + (a.count ?? 0) : sum
    }, 0)

  const ebikes = typeof s.num_ebikes_available === 'number'
    ? s.num_ebikes_available
    : countWhere((vt, id) => (vt ? isEbike(vt) : id.includes('electric')))
  const scooters = typeof s.num_scooters_available === 'number'
    ? s.num_scooters_available
    : countWhere((vt, id) => (vt ? isScooter(vt.formFactor) : id.includes('scooter')))

  return {
    numBikesAvailable: Math.max(0, total - ebikes - scooters),
    numEbikesAvailable: ebikes,
    numScootersAvailable: scooters,
    numDocksAvailable: s.num_docks_available ?? 0,
    isRenting: s.is_renting !== false,
    isReturning: s.is_returning !== false,
    lastReported: lastReportedIso(s.last_reported),
  }
}
