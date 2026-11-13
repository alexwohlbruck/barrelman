/**
 * Tunnel cut faults: the floor not meeting the draped road where the cut
 * opens, a headwall standing proud of the hill it is set in, ground too thin
 * over the bore (the carve shows daylight), and a floor no driver could take.
 */
import { along, lngLat, type Point } from '../bridge-decks/profile'
import { profileIssues, round } from './profile'
import type { ServedCut } from './served'

export type CutIssue = { kind: 'grade' | 'kink' | 'spike' | 'seam' | 'proud' | 'daylight'; cut: string; at: [number, number]; size: number }

/** Metres the floor may sit off the terrain where the cut opens. */
export const SEAM = 0.3
/** Metres a headwall may stand over the ground just behind the portal. */
export const PROUD = 2
/** Metres of bore, past the portal, that must stay under the ground, and the most its roof may stand out of it: cut-and-cover bores run just under the street. */
export const DAYLIGHT = { reach: 60, cover: -1 }

export function cutIssues(cut: ServedCut, ground: (p: Point) => number): CutIssue[] {
  const d = along(cut.points)
  const step = d[d.length - 1] / Math.max(1, d.length - 1)
  const at = (i: number) => lngLat(cut.points[Math.max(0, Math.min(cut.points.length - 1, i))])
  const out: CutIssue[] = profileIssues(cut.floor, step, cut.kind).map(i => ({ kind: i.kind, cut: cut.id, at: at(Math.round(i.at / step)), size: i.size }))
  const seam = cut.floor[cut.open] - ground(cut.points[cut.open])
  if (Math.abs(seam) > SEAM) out.push({ kind: 'seam', cut: cut.id, at: at(cut.open), size: round(seam) })
  const behind = ground(cut.points[Math.min(cut.points.length - 1, cut.at + 2)])
  if (cut.crown - behind > PROUD) out.push({ kind: 'proud', cut: cut.id, at: at(cut.at), size: round(cut.crown - behind) })
  let thinnest = Infinity, where = -1
  for (let i = cut.at + 2; i < cut.points.length && d[i] - d[cut.at] <= DAYLIGHT.reach; i++) {
    const cover = ground(cut.points[i]) - Math.max(cut.walls[0][i], cut.walls[1][i])
    if (cover < thinnest) { thinnest = cover; where = i }
  }
  if (where >= 0 && thinnest < DAYLIGHT.cover) out.push({ kind: 'daylight', cut: cut.id, at: at(where), size: round(-thinnest) })
  return out
}
