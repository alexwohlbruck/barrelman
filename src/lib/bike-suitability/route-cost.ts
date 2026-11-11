/**
 * Bike suitability as a routing cost. A way's stress is how far its score
 * (0-100, import/create-bike-suitability.sql) falls short of 100, raised to
 * 2.5 so a red arterial weighs far more than a yellow street; `weight` (0 =
 * fastest, 1 = safest) scales how many seconds of riding that stress is worth.
 * At safest a red street feels ~5x its riding time and a yellow one ~2x.
 */

/** Taken for a way the database has no score for: neither steered to nor away from. */
export const UNKNOWN_SCORE = 70

const STRESS_SCALE = 10

export interface WayStretch {
  wayId: number
  from: number
  to: number
}

/** Metres along the route each OSM way covers, from GraphHopper's `osm_way_id` detail. */
export function wayStretches(details: Record<string, unknown[]> | undefined, distances: number[]): WayStretch[] {
  return (details?.osm_way_id ?? [])
    .filter((e): e is [number, number, number] => Array.isArray(e) && typeof e[2] === 'number')
    .map(([a, b, wayId]) => ({ wayId, from: distances[a] ?? 0, to: distances[b] ?? 0 }))
    .filter(s => s.to > s.from)
}

/** How many times longer a way feels than it takes to ride. */
export function stressFactor(score: number, weight: number): number {
  const stress = (100 - score) / 100
  return 1 + weight * STRESS_SCALE * stress ** 2.5
}

const scoreOf = (scores: Map<number, number>, id: number) => scores.get(id) ?? UNKNOWN_SCORE

/** Riding time with every stretch stretched by its stress, in the time's own units. */
export function pathCost(stretches: WayStretch[], scores: Map<number, number>, time: number, weight: number): number {
  const total = stretches.reduce((sum, s) => sum + s.to - s.from, 0)
  if (!total) return time
  const felt = stretches.reduce((sum, s) => sum + (s.to - s.from) * stressFactor(scoreOf(scores, s.wayId), weight), 0)
  return time * felt / total
}

/** The distance-weighted mean score of a route. */
export function pathScore(stretches: WayStretch[], scores: Map<number, number>): number | null {
  const total = stretches.reduce((sum, s) => sum + s.to - s.from, 0)
  if (!total) return null
  return Math.round(stretches.reduce((sum, s) => sum + (s.to - s.from) * scoreOf(scores, s.wayId), 0) / total)
}

/** Consecutive stretches of one score, merged, for drawing the route by score. */
export function scoreSegments(stretches: WayStretch[], scores: Map<number, number>) {
  const out: { from: number; to: number; score: number | null }[] = []
  for (const s of stretches) {
    const score = scores.get(s.wayId) ?? null
    const last = out[out.length - 1]
    if (last && last.score === score && Math.abs(last.to - s.from) < 0.5) last.to = s.to
    else out.push({ from: Math.round(s.from * 10) / 10, to: s.to, score })
  }
  return out.map(s => ({ ...s, to: Math.round(s.to * 10) / 10 }))
}

/**
 * GraphHopper priority rules that push the router off these ways, one rule per
 * factor so the expression count stays small. Factors are below 1, which keeps
 * the model valid on a landmark (LM) profile.
 */
export function penaltyRules(penalties: Map<number, number>) {
  const byFactor = new Map<number, number[]>()
  for (const [id, factor] of penalties) {
    const f = Math.round(factor * 20) / 20
    if (f >= 1) continue
    byFactor.set(f, [...(byFactor.get(f) ?? []), id])
  }
  return [...byFactor].sort(([a], [b]) => a - b).map(([f, ids]) => ({
    if: ids.map(id => `osm_way_id == ${id}`).join(' || '),
    multiply_by: f,
  }))
}

/** A request's own custom model with these rules appended to its priority. */
export function withRules(model: any, rules: { if: string; multiply_by: number }[]) {
  if (!rules.length) return model
  return { ...(model ?? {}), priority: [...(model?.priority ?? []), ...rules] }
}

/** The box the points span, grown by a quarter of its size and at least `minPad` degrees. */
export function corridor(points: number[][], minPad = 0.005): [number, number, number, number] {
  const xs = points.map(p => p[0]), ys = points.map(p => p[1])
  const [x0, x1, y0, y1] = [Math.min(...xs), Math.max(...xs), Math.min(...ys), Math.max(...ys)]
  const px = Math.max(minPad, (x1 - x0) / 4), py = Math.max(minPad, (y1 - y0) / 4)
  return [x0 - px, y0 - py, x1 + px, y1 + py]
}
