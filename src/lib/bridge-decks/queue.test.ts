import { describe, expect, it } from 'bun:test'
import { blockOf, cellAt, cellBox, cellsOver, entriesOver, entryCells, maxCellsFrom, pick, skipReason, strays, type Entry } from './queue'

const entry = (id: string, box: Entry['box'], anchors: Entry['anchors'] = []): Entry => ({ id, box, anchors })

describe('cells', () => {
  it('cuts a box into the grid cells under it, edges included', () => {
    expect(cellsOver([-80.85, 35.21, -80.83, 35.215])).toEqual(['-4043,1760', '-4042,1760'])
    expect(cellBox('-4043,1760')[0]).toBeCloseTo(-80.86)
    expect(cellAt([-80.8501, 35.2101])).toBe('-4043,1760')
  })

  it('takes the cells where the decks crossing a box are anchored, however far along them', () => {
    const cells = entryCells(entry('1', [-80.851, 35.211, -80.85, 35.212], [[-80.79, 35.25]]))
    expect([...cells]).toEqual(['-4043,1760', cellAt([-80.79, 35.25])])
  })
})

describe('blockOf', () => {
  it('groups cells three by three, negative ones included', () => {
    expect(['0,0', '2,2', '3,0', '-1,0', '-3,-1', '-4,0'].map(blockOf)).toEqual(['0,0', '0,0', '1,0', '-1,0', '-1,-1', '-2,0'])
  })
})

describe('pick', () => {
  const a = entry('1', [0.001, 0.001, 0.002, 0.002])
  const b = entry('2', [0.003, 0.003, 0.004, 0.004])
  const c = entry('3', [0.5, 0.5, 0.55, 0.55])

  it('takes whole entries while their cells fit, sharing cells between them', () => {
    const { picked, cells } = pick([a, b, c], 2)
    expect(picked.map(e => e.id)).toEqual(['1', '2'])
    expect(cells.get('0,0')).toEqual(['1', '2'])
  })

  it('always takes the first entry, however many cells it spans', () => {
    const { picked, cells } = pick([c, a], 1)
    expect(picked.map(e => e.id)).toEqual(['3'])
    expect(cells.size).toBe(9)
  })
})

describe('strays', () => {
  const picked = [entry('1', [0.001, 0.001, 0.002, 0.002])]
  const planned = new Map([['0,0', ['1']]])

  it('finds the cell a changed deck is now anchored in, and only for decks the change reached', () => {
    const out = strays([
      { id: 'grown', anchor: [0.05, 0.001], box: [0.0015, 0, 0.1, 0.002] },
      { id: 'home', anchor: [0.001, 0.001], box: [0, 0, 0.002, 0.002] },
      { id: 'elsewhere', anchor: [0.07, 0.001], box: [0.06, 0, 0.08, 0.002] },
    ], picked, planned)
    expect([...out]).toEqual([['2,0', ['1']]])
  })
})

describe('entriesOver', () => {
  it('collects each entry over a failed cell once', () => {
    const cells = new Map([['0,0', ['1', '2']], ['1,0', ['2']], ['2,0', ['3']]])
    expect([...entriesOver(['0,0', '1,0'], cells)]).toEqual(['1', '2'])
  })
})

describe('maxCellsFrom', () => {
  it('takes a whole number of at least one, and the default when blank', () => {
    expect(maxCellsFrom(undefined, 1000)).toBe(1000)
    expect(maxCellsFrom(' ', 1000)).toBe(1000)
    expect(maxCellsFrom('50', 1000)).toBe(50)
    for (const bad of ['0', '-5', '2.5', 'many']) expect(() => maxCellsFrom(bad, 1000)).toThrow('at least 1')
  })
})

describe('skipReason', () => {
  it('skips without its tables, a queue or terrain, in that order', () => {
    expect(skipReason({ missing: ['bridge_deck_cells'], queue: true, terrain: true }))
      .toBe('bridge_deck_cells does not exist; start the API once, or run import/create-detail-views.sql.')
    expect(skipReason({ missing: ['bridge_decks', 'bridge_deck_cells'], queue: true, terrain: true }))
      .toStartWith('bridge_decks and bridge_deck_cells do not exist')
    expect(skipReason({ missing: [], queue: false, terrain: false })).toBe('nothing queued.')
    expect(skipReason({ missing: [], queue: true, terrain: false })).toContain('BRIDGE_DECKS_DEM_TILES')
    expect(skipReason({ missing: [], queue: true, terrain: true })).toBeNull()
  })
})
