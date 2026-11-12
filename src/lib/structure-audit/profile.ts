/**
 * Where a deck's or tunnel's long profile would not read as a driven road:
 * grades past what a ramp allows, vertical curves sharper than the slowest
 * design speed's, and lone bumps. Heights arrive in decimetres, so grade and
 * curvature are measured over chords long enough to sit above that rounding.
 */
export type ProfileIssue = { kind: 'grade' | 'kink' | 'spike'; at: number; size: number }

/** Steepest grade by kind: AASHTO ramps (8%, 9% descending), mainline rail. */
export const GRADE = { road: 0.09, path: 0.12, rail: 0.04 } as const
/** Shortest crest/sag K (metres per 1% grade change), 40 km/h crest: below it a ramp lurches. */
export const MIN_K = 4
/** Metres a single sample may stand off the line through its neighbours'. */
export const SPIKE = 0.4
/** Chord, in metres, over which grade and curvature are measured. */
export const CHORD = 18

export function profileIssues(heights: number[], step: number, kind: keyof typeof GRADE): ProfileIssue[] {
  const out: ProfileIssue[] = []
  const n = heights.length
  const k = Math.max(1, Math.round(CHORD / step))
  const span = k * step
  let worst: ProfileIssue | null = null
  const flush = () => { if (worst) out.push(worst); worst = null }
  const keep = (issue: ProfileIssue) => {
    if (worst && worst.kind === issue.kind && issue.at - worst.at <= span) { if (issue.size > worst.size) worst = issue; return }
    flush()
    worst = issue
  }
  for (let i = 0; i + k < n; i++) {
    const g = Math.abs(heights[i + k] - heights[i]) / span
    if (g > GRADE[kind]) keep({ kind: 'grade', at: (i + k / 2) * step, size: round(g * 100) })
  }
  flush()
  for (let i = k; i + k < n; i++) {
    const change = Math.abs(heights[i + k] - 2 * heights[i] + heights[i - k]) / span * 100
    const K = change > 0 ? span / change : Infinity
    if (K < MIN_K && change > 2) keep({ kind: 'kink', at: i * step, size: round(K) })
  }
  flush()
  for (let i = 1; i + 1 < n; i++) {
    const off = heights[i] - (heights[i - 1] + heights[i + 1]) / 2
    if (Math.abs(off) > SPIKE && Math.sign(heights[i] - heights[i - 1]) !== Math.sign(heights[i + 1] - heights[i]))
      keep({ kind: 'spike', at: i * step, size: round(off) })
  }
  flush()
  return out
}

export const round = (v: number) => Math.round(v * 100) / 100
