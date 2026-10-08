import { describe, test, expect } from 'bun:test'
import { estimateMatches, setLexemeStats } from './lexeme-stats.service'

const ROWS = 219_000_000

describe('estimateMatches', () => {
  test('a query matches about as many rows as its rarest word', () => {
    // Words in a place name travel together: "new york" matched 1.2M rows,
    // where multiplying the two frequencies predicted 92K.
    setLexemeStats({ new: 0.07, york: 0.04 }, ROWS)
    expect(estimateMatches('new & york')!.rows).toBe(Math.round(0.04 * ROWS))
  })

  test('a word outside the most-common list counts as rare', () => {
    setLexemeStats({ harris: 0.0117 }, ROWS)
    expect(estimateMatches('harris & teeter:*')).toEqual({ rows: 0, prefixRows: 0 })
  })

  test('a prefix sums every common word it expands to', () => {
    setLexemeStats({ street: 0.2, streetcar: 0.01, park: 0.03 }, ROWS)
    expect(estimateMatches('stree:*')).toEqual({ rows: Math.round(0.21 * ROWS), prefixRows: Math.round(0.21 * ROWS) })
  })

  test('a common prefix is counted even when another word is rare', () => {
    // "empire & state:*": empire is rare, but the index expands state:* in full.
    setLexemeStats({ state: 0.009 }, ROWS)
    expect(estimateMatches('empire & state:*')).toEqual({ rows: 0, prefixRows: Math.round(0.009 * ROWS) })
  })

  test('a one- or two-letter prefix counts as matching everything', () => {
    // "harris t" expands t:* to thousands of words; it took 3s.
    setLexemeStats({ harris: 0.0117 }, ROWS)
    expect(estimateMatches('harris & t:*')).toEqual({ rows: Math.round(0.0117 * ROWS), prefixRows: ROWS })
  })

  test('spelling alternatives add up', () => {
    setLexemeStats({ st: 0.05, street: 0.2 }, ROWS)
    expect(estimateMatches('(saint | st | street)')!.rows).toBe(Math.round(0.25 * ROWS))
  })

  test('accents fold the way build_ts folds them', () => {
    setLexemeStats({ neukolln: 0.001 }, ROWS)
    expect(estimateMatches('neukölln')!.rows).toBe(Math.round(0.001 * ROWS))
  })
})
