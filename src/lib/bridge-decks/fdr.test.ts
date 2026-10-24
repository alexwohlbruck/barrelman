/**
 * The FDR Drive viaduct where it passes under the Brooklyn Bridge, recorded
 * from OSM with its terrain: twin carriageways over a street grid, ramps
 * leaving them for the bridge, and the bridge's own decks over them.
 */
import { describe, expect, it } from 'bun:test'
import { buildDecks, type Deck } from './build'
import { beside, sideAt, type Point } from './profile'
import { recorded } from './recorded'

const { input, ground } = await recorded('fdr-brooklyn-bridge')
const decks = await buildDecks(input, ground)
const carrying = (way: number) => decks.find(d => d.ways.includes(way))!
/** FDR Drive northbound and southbound, by one of their ways. */
const [northbound, southbound] = [carrying(32935108), carrying(420890069)]

const heightAt = (deck: Deck, p: Point) => {
  const near = beside(deck.points, p)
  const j = near.segment
  return { near, z: deck.heights[j - 1] + (deck.heights[j] - deck.heights[j - 1]) * near.t }
}

describe('FDR Drive under the Brooklyn Bridge', () => {
  it('draws every roadway once: no deck lies inside another of its layer at a different height', () => {
    let samples = 0
    let stacked = 0
    for (const a of decks.filter(d => d.kind !== 'rail'))
      for (const p of a.points) {
        samples++
        const i = a.points.indexOf(p)
        stacked += +decks.some(b => {
          if (b === a || b.kind === 'rail' || b.layer !== a.layer) return false
          const { near, z } = heightAt(b, p)
          const half = sideAt({ edges: b.edges, sides: b.sides ?? undefined }, near.left ? 0 : 1, near.segment - 1, near.t)
          return near.alongside && near.distance < half - 0.5 && Math.abs(z - a.heights[i]) > 0.3
        })
      }
    // A ramp climbing out of its road's gore overlaps it for a few samples.
    expect(stacked / samples).toBeLessThan(0.01)
  })

  it('runs each carriageway as one deck, level over the streets beneath', () => {
    for (const deck of [northbound, southbound]) {
      expect(deck.length).toBeGreaterThan(700)
      const z = deck.heights
      for (let i = 3; i < z.length - 3; i++) {
        // No sag: never lower than the highest it reaches on both sides.
        expect(Math.min(Math.max(...z.slice(0, i + 1)), Math.max(...z.slice(i))) - z[i]).toBeLessThan(0.3)
        expect(Math.abs(z[i + 1] - 2 * z[i] + z[i - 1])).toBeLessThanOrEqual(0.1 + 1e-9)
      }
    }
  })

  it('carries the twin carriageways at one height where they run side by side', () => {
    const gaps = northbound.points.flatMap((p, i) => {
      const { near, z } = heightAt(southbound, p)
      return near.alongside && near.distance < 12 ? [Math.abs(z - northbound.heights[i])] : []
    })
    expect(gaps.length).toBeGreaterThan(20)
    expect(Math.max(...gaps)).toBeLessThan(0.6)
  })
})
