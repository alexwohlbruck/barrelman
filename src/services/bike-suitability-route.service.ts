/**
 * Steers a bike route by the suitability scores. GraphHopper cannot read them,
 * so the worst streets in the trip's corridor are deranked by `osm_way_id`
 * up front, then each pass routes, scores the ways the route took, and routes
 * again with the stressful ones deranked too; the cheapest pass by
 * stress-weighted time wins. Its custom model is what the caller then routes
 * with, so the enriched route is exactly that pass.
 */
import { sql } from 'drizzle-orm'
import { executeWithin } from '../lib/bounded-query'
import { graphHopperUrl } from '../lib/graphhopper'
import {
  corridor, pathCost, penaltyRules, stressFactor, wayStretches, withRules, UNKNOWN_SCORE,
} from '../lib/bike-suitability/route-cost'
import { buildCumulativeDistances, type FetchFn } from './route.service'

export type ScoreLookup = (ids: number[]) => Promise<Map<number, number>>
export type CorridorLookup = (box: [number, number, number, number], below: number, limit: number) => Promise<Map<number, number>>

const PASSES = 4
const BUDGET_MS = 800
/** Ways that feel at least this much longer than they are get deranked. */
const PENALISE_ABOVE = 1.1
/** Red and orange streets in the corridor are deranked before the first pass. */
const SEED_BELOW = 50
const SEED_LIMIT = 600
/** Past this span (degrees) the corridor is too big to read in budget. */
const SEED_MAX_SPAN = 0.25

export const lookupScores: ScoreLookup = async ids => {
  if (!ids.length) return new Map()
  const rows: Array<{ osm_id: string | number; score: number }> = await executeWithin(sql`
    SELECT osm_id, bike_suitability_score(tags, bike_facility(tags)) AS score
    FROM geo_places WHERE osm_type = 'W' AND osm_id = ANY(${`{${ids.join(',')}}`}::bigint[])`, BUDGET_MS)
  return new Map(rows.map(r => [Number(r.osm_id), r.score]))
}

export const lookupCorridor: CorridorLookup = async ([x0, y0, x1, y1], below, limit) => {
  const rows: Array<{ osm_id: string | number; score: number }> = await executeWithin(sql`
    SELECT (fid - 1) / 4 AS osm_id, score FROM bike_suitability
    WHERE geom && ST_MakeEnvelope(${x0}, ${y0}, ${x1}, ${y1}, 4326) AND score < ${below}
    ORDER BY score LIMIT ${limit}`, 1500)
  return new Map(rows.map(r => [Number(r.osm_id), r.score]))
}

/** Scores for every id, looked up only for those not already in `known`. */
export async function scoresFor(ids: number[], known: Map<number, number>, lookup: ScoreLookup) {
  const missing = [...new Set(ids)].filter(id => !known.has(id))
  if (missing.length) for (const [id, s] of await lookup(missing)) known.set(id, s)
  return known
}

/** The custom model whose route is cheapest by stress-weighted time. */
export async function suitableCustomModel(
  body: any,
  weight: number,
  fetchFn: FetchFn,
  lookup: ScoreLookup = lookupScores,
  lookupBox: CorridorLookup = lookupCorridor,
): Promise<{ model: any; scores: Map<number, number> }> {
  const scores = new Map<number, number>()
  const penalties = new Map<number, number>()
  let best: { cost: number; model: any } | null = null

  // The first pass routes as asked, so the unsteered route is always a candidate.
  const box = corridor(body.points)
  const seeds = box[2] - box[0] < SEED_MAX_SPAN && box[3] - box[1] < SEED_MAX_SPAN
    ? lookupBox(box, SEED_BELOW, SEED_LIMIT)
    : Promise.resolve(new Map<number, number>())

  for (let pass = 0; pass < PASSES; pass++) {
    const model = withRules(body.custom_model, penaltyRules(penalties))
    const res = await fetchFn(`${graphHopperUrl()}/route`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        ...body, custom_model: model, instructions: false, calc_points: true,
        points_encoded: false, details: ['osm_way_id'], algorithm: undefined,
      }),
    })
    if (!res.ok) break
    const path = ((await res.json()) as any).paths?.[0]
    if (!path) break

    const stretches = wayStretches(path.details, buildCumulativeDistances(path.points?.coordinates ?? []))
    await scoresFor(stretches.map(s => s.wayId), scores, lookup)
    const cost = pathCost(stretches, scores, path.time, weight)
    if (!best || cost < best.cost) best = { cost, model }

    let added = 0
    if (pass === 0) {
      for (const [id, score] of await seeds) {
        scores.set(id, score)
        penalties.set(id, 1 / stressFactor(score, weight))
        added++
      }
    }
    for (const { wayId } of stretches) {
      const factor = stressFactor(scores.get(wayId) ?? UNKNOWN_SCORE, weight)
      if (factor > PENALISE_ABOVE && !penalties.has(wayId)) {
        penalties.set(wayId, 1 / factor)
        added++
      }
    }
    if (!added) break
  }

  return { model: best?.model ?? body.custom_model, scores }
}
