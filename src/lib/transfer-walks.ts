/** The legs of one itinerary, as far as timing a transfer walk needs them. */
export interface TimedLeg {
  transitLeg: boolean
  startTime: string
  endTime: string
  duration: number
  distance: number
  to: { arrival?: string }
  transfer?: { seconds: number | null; walk?: { metres: number; estimate: boolean } | null } | null
}

export interface TimedItinerary {
  walkTime: number
  waitingTime: number
  walkDistance: number
  legs: TimedLeg[]
}

/**
 * Time each walk between two rides as the walk itself. MOTIS reports a
 * transfer's footpath with the planner's per-interchange pad added, so a
 * 90 s platform change read as a 5 min walk. The pad is waiting, not walking;
 * a station-path time for the transfer, where one exists, is the walk, and a
 * walked station path its length too.
 */
export function settleTransferWalks(itinerary: TimedItinerary, padSeconds: number): void {
  itinerary.legs.forEach((leg, i) => {
    const prev = itinerary.legs[i - 1], next = itinerary.legs[i + 1]
    if (leg.transitLeg || !prev?.transitLeg || !next?.transitLeg) return
    const walked = next.transfer?.walk
    if (walked && !walked.estimate) {
      itinerary.walkDistance = Math.max(0, itinerary.walkDistance + walked.metres - leg.distance)
      leg.distance = walked.metres
    }
    const start = Date.parse(leg.startTime)
    const gap = Math.max(0, (Date.parse(next.startTime) - start) / 1000)
    const footpath = Math.max(0, leg.duration - padSeconds)
    const seconds = Math.round(Math.min(next.transfer?.seconds ?? footpath, gap))
    if (seconds === leg.duration) return
    const shift = leg.duration - seconds
    leg.duration = seconds
    leg.endTime = new Date(start + seconds * 1000).toISOString()
    if (leg.to.arrival) leg.to.arrival = leg.endTime
    itinerary.walkTime = Math.max(0, itinerary.walkTime - shift)
    itinerary.waitingTime = Math.max(0, itinerary.waitingTime + shift)
  })
}
