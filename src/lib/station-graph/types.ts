export type Tags = Record<string, string>

export type Geometry =
  | { type: 'LineString'; coordinates: number[][] }
  | { type: 'MultiLineString'; coordinates: number[][][] }
  | { type: 'Polygon'; coordinates: number[][][] }
  | { type: 'MultiPolygon'; coordinates: number[][][][] }

export interface OsmWay {
  id: string
  tags: Tags
  geometry: Geometry
}

export interface OsmNode {
  id: string
  tags: Tags
  lon: number
  lat: number
}

/** How a time was made, best first. */
export const SOURCES = ['graph', 'bridged', 'agency', 'estimated', 'straight_line'] as const
export type PathSource = (typeof SOURCES)[number]

export type Profile = 'foot' | 'wheelchair'
export type EndpointKind = 'platform' | 'entrance'

export type StepType = 'walk' | 'platform' | 'stairs' | 'escalator' | 'elevator' | 'bridge' | 'fare_gate'

export interface PathStep {
  type: StepType
  metres: number
  seconds: number
  /** Level at either end, where the map says. */
  fromLevel: number | null
  toLevel: number | null
  /** Vertex range in the path geometry this step covers. */
  fromVertex: number
  toVertex: number
}

export interface StationPath {
  fromId: string
  fromKind: EndpointKind
  toId: string
  toKind: EndpointKind
  profile: Profile
  source: PathSource
  confidence: number
  uncertaintyS: number
  /** From (or to) the platform centre: what a transfer is timed with. */
  seconds: number
  /** From the best car position on the row's platform end; graph and bridged only. */
  bestSeconds: number | null
  /** 0–1 along the platform's axis (see StationPlatform.axisBearing). */
  bestPosition: number | null
  metres: number
  levels: number
  crossesFareGate: boolean
  /** Null when the source can't tell. */
  surfaces: boolean | null
  geometry: Array<[number, number]> | null
  vertexLevels: Array<number | null> | null
  steps: PathStep[] | null
}

export interface StationPlatform {
  id: string
  name: string | null
  level: number | null
  underground: boolean
  /** Compass bearing of the platform's long axis; position 0 is its tail, 1 its head. */
  axisBearing: number
  lengthM: number
  refs: string[]
  gtfsStopIds: string[]
  geometry: Geometry
}

export interface ComplexReport {
  tiers: Record<'pair' | 'street', Partial<Record<PathSource, number>>>
  components: number
  disconnected: boolean
}
