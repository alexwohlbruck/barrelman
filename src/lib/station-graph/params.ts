import config from '../../../config/station-paths.json'
import type { PathSource } from './types'

/** Costs, limits and fitted tier constants. Data rather than code, so a refit only edits the JSON. */
export type StationPathParams = Omit<typeof config, '_comment'>

export const DEFAULT_PARAMS: StationPathParams = config

export type Endpoints = 'pair' | 'street'

export function uncertaintyOf(params: StationPathParams, source: PathSource, endpoints: Endpoints): number {
  return params.uncertaintyS[source][endpoints]
}

/** 1 for a certain time, falling linearly with the tier's p90 error. */
export function confidenceOf(params: StationPathParams, uncertaintyS: number): number {
  return Math.round(Math.max(0.05, Math.min(1, 1 - uncertaintyS / params.confidenceSpanS)) * 100) / 100
}
