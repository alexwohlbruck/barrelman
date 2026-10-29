import { mercator, metresPerUnit, type Point } from '../bridge-mesh/decks'

export const ORIGIN = mercator([-74, 40.7])
const scale = metresPerUnit(ORIGIN[1])

/** A mercator point `east` and `north` metres from ORIGIN. */
export const at = (east: number, north: number): Point => [ORIGIN[0] + east / scale, ORIGIN[1] - north / scale]
