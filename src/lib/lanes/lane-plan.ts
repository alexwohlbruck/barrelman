import type { CrossSectionLane, DrivingSide } from './cross-section'

/** A maneuver's turn, in turn:lanes vocabulary. */
export type Turn = 'through' | 'left' | 'right' | 'slight_left' | 'slight_right' | 'sharp_left' | 'sharp_right' | 'reverse'

export const TURNS: readonly Turn[] = ['through', 'left', 'right', 'slight_left', 'slight_right', 'sharp_left', 'sharp_right', 'reverse']

export interface PlanLane {
  turns: string[]
  valid: boolean
  recommended: boolean
  bus: boolean
}

/** Degrees right of straight on (negative left) to the turn that names it. */
export function turnFromAngle(degrees: number): Turn {
  const a = Math.abs(degrees)
  const side = degrees < 0 ? 'left' : 'right'
  if (a < 25) return 'through'
  if (a < 60) return `slight_${side}`
  if (a < 135) return side
  if (a < 165) return `sharp_${side}`
  return 'reverse'
}

/** The turn:lanes arrows a lane needs for a maneuver, best first. */
function needs(turn: Turn, side: DrivingSide): string[] {
  switch (turn) {
    case 'left': case 'sharp_left': return ['left', 'sharp_left']
    case 'right': case 'sharp_right': return ['right', 'sharp_right']
    case 'slight_left': return ['slight_left', 'merge_to_left', 'left', 'through']
    case 'slight_right': return ['slight_right', 'merge_to_right', 'right', 'through']
    case 'reverse': return ['reverse', side === 'right' ? 'left' : 'right']
    default: return ['through', 'merge_to_left', 'merge_to_right', 'slight_left', 'slight_right']
  }
}

const leftward = (turn: Turn, side: DrivingSide) => turn.includes('left') || (turn === 'reverse' && side === 'right')
const rightward = (turn: Turn, side: DrivingSide) => turn.includes('right') || (turn === 'reverse' && side === 'left')

/**
 * Which approach lanes lead on to `turn`, left to right as the traffic sees
 * them, or null without tagged turn arrows on at least two lanes. A lane with
 * no arrow goes straight on; bus lanes are never recommended.
 */
export function lanePlan(lanes: CrossSectionLane[], turn: Turn, side: DrivingSide): PlanLane[] | null {
  const motor = lanes.filter(l => !l.bike)
  if (motor.length < 2 || !motor.some(l => l.turns.length > 0)) return null
  const wanted = needs(turn, side)
  const plan = motor.map(l => {
    const turns = l.turns.length ? l.turns : ['through']
    return { turns: l.turns, valid: turns.some(t => wanted.includes(t)), recommended: false, bus: l.bus }
  })
  const pick = recommendedIndex(plan, turn, side)
  if (pick == null) return null
  plan[pick].recommended = true
  return plan
}

function recommendedIndex(plan: { valid: boolean; bus: boolean }[], turn: Turn, side: DrivingSide): number | null {
  const valid = plan.map((l, i) => (l.valid ? i : -1)).filter(i => i >= 0)
  const open = valid.filter(i => !plan[i].bus)
  const from = open.length ? open : valid
  if (from.length === 0) return null
  if (leftward(turn, side)) return from[0]
  if (rightward(turn, side)) return from.at(-1)!
  return from[Math.floor((from.length - 1) / 2)]
}

/**
 * The lane to drive in for `turn` where no arrows say: the outermost on the
 * turn's side, or for straight on the one nearest `offset`, else the kerb's;
 * out of bus lanes where another is open.
 */
export function laneFor(lanes: CrossSectionLane[], turn: Turn | null, side: DrivingSide, offset: number | null): CrossSectionLane | null {
  const motor = lanes.filter(l => !l.bike)
  const open = motor.filter(l => !l.bus)
  const from = open.length ? open : motor
  if (from.length === 0) return null
  if (turn && leftward(turn, side)) return from[0]
  if (turn && rightward(turn, side)) return from.at(-1)!
  if (offset == null) return side === 'right' ? from.at(-1)! : from[0]
  return from.reduce((best, l) => (Math.abs(l.offset - offset) < Math.abs(best.offset - offset) ? l : best))
}

/** The lane a plan recommends, among the approach's motor lanes. */
export function recommendedLane(lanes: CrossSectionLane[], plan: PlanLane[] | null): CrossSectionLane | null {
  const i = plan?.findIndex(l => l.recommended) ?? -1
  return i >= 0 ? lanes.filter(l => !l.bike)[i] ?? null : null
}
