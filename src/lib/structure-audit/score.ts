/**
 * One number per structure for ranking: each issue counts by how far past its
 * threshold it is, capped so a single gross fault does not drown the rest.
 */
type Issue = { structure: string; kind: string; size: number; at: [number, number] }

const WEIGHT: Record<string, (size: number) => number> = {
  seam: s => Math.abs(s) / 0.3,
  buried: s => s,
  clash: s => (5.3 - s) / 1.5,
  lurch: s => s / 2,
  grade: s => s / 9,
  kink: s => 4 / Math.max(s, 0.5),
  spike: s => Math.abs(s) / 0.4,
  floating: s => s / 1.5,
  step: s => s / 0.15,
  dangling: () => 2,
  overlap: () => 2,
  width: () => 1,
  bulge: () => 1,
  orphan: () => 1,
  proud: s => s / 2,
  daylight: s => 1 + s,
}
const CAP = 5

const weight = (i: Issue) => Math.min(CAP, (WEIGHT[i.kind] ?? (() => 1))(i.size))

/** Each structure's score, its worst issue's place, and its issues listed; worst first. */
export function score(issues: Issue[]): Array<{ structure: string; score: number; at: [number, number]; summary: string }> {
  const by = new Map<string, Issue[]>()
  for (const i of issues) {
    const list = by.get(i.structure)
    if (list) list.push(i)
    else by.set(i.structure, [i])
  }
  return [...by].map(([structure, list]) => ({
    structure,
    score: Math.round(list.reduce((t, i) => t + weight(i), 0) * 10) / 10,
    at: list.reduce((w, i) => (weight(i) > weight(w) ? i : w)).at,
    summary: list.map(i => `${i.kind} ${i.size}`).join(', '),
  })).sort((a, b) => b.score - a.score)
}
