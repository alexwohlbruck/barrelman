/**
 * Unit tests for the portolan sync wrapper's pure pieces: RESULT-line
 * parsing, sync argv construction, and the exported-zip → barrelman feed id
 * mapping. No portolan binary and no database — the process/DB edges are
 * exercised by running the wrapper itself.
 */

import { describe, test, expect } from 'bun:test'
import {
  parseResultLine,
  buildSyncArgs,
  keyToOnestopFromConfig,
  mapExportedZips,
} from './portolan-sync'

// ── RESULT parsing ───────────────────────────────────────────────────────────

describe('parseResultLine', () => {
  const RESULT =
    'RESULT {"changed":["a"],"affected":["a","b"],"rebuilt":["a"],' +
    '"groups_rewritten":false,"tiles":{"written":12,"unchanged":34,"removed":5},' +
    '"exported":["a.zip"],"errors":[]}'

  test('parses the final RESULT line under progress noise', () => {
    const out = ['building a…', 'tiling a…', RESULT, ''].join('\n')
    const r = parseResultLine(out)
    expect(r).not.toBeNull()
    expect(r!.changed).toEqual(['a'])
    expect(r!.affected).toEqual(['a', 'b'])
    expect(r!.rebuilt).toEqual(['a'])
    expect(r!.exported).toEqual(['a.zip'])
    expect(r!.errors).toEqual([])
    expect(r!.tiles).toEqual({ written: 12, unchanged: 34, removed: 5 })
    expect(r!.groups_rewritten).toBe(false)
  })

  test('takes the LAST result line when progress output mentions RESULT', () => {
    const out = [
      'RESULT {"changed":["stale"],"exported":["stale.zip"]}',
      'more progress…',
      RESULT,
    ].join('\n')
    expect(parseResultLine(out)!.exported).toEqual(['a.zip'])
  })

  test('tolerates a missing trailing newline and surrounding whitespace', () => {
    expect(parseResultLine(`progress\n  ${RESULT}`)).not.toBeNull()
  })

  test('defaults absent arrays to empty (a minimal check run)', () => {
    const r = parseResultLine('RESULT {"changed":[]}')
    expect(r).not.toBeNull()
    expect(r!.changed).toEqual([])
    expect(r!.exported).toEqual([])
    expect(r!.errors).toEqual([])
  })

  test('returns null for malformed JSON', () => {
    expect(parseResultLine('RESULT {not json')).toBeNull()
  })

  test('returns null when no RESULT line exists (crashed run)', () => {
    expect(parseResultLine('building a…\npanic: boom\n')).toBeNull()
  })

  test('errors are surfaced whatever their shape', () => {
    const r = parseResultLine('RESULT {"errors":[{"feed":"a","stage":"match"},"plain string"]}')
    expect(r!.errors).toHaveLength(2)
  })
})

// ── argv construction ────────────────────────────────────────────────────────

describe('buildSyncArgs', () => {
  test('global: workspace-derived paths, --flag=value form, --json', () => {
    const args = buildSyncArgs('global', '/ws')
    expect(args.slice(0, 2)).toEqual(['sync', 'global'])
    expect(args).toContain('--config=/ws/portolan.json')
    expect(args).toContain('--data=/ws/data/gtfs')
    expect(args).toContain('--build=/ws/build')
    expect(args).toContain('--tiles=/ws/build/tiles')
    expect(args).toContain('--export-gtfs=/ws/build/export')
    expect(args).toContain('--state=/ws/build/sync-state.json')
    expect(args).toContain('--style-dir=/ws/style')
    expect(args).toContain('--json')
    // every value-carrying flag uses = form (the parseArgs/leading-minus
    // footgun documented in scripts/download-gtfs.sh)
    for (const a of args) {
      if (a.startsWith('--') && a !== '--json' && a !== '--dry-run') {
        expect(a).toContain('=')
      }
    }
  })

  test('patch carries the feed list', () => {
    const args = buildSyncArgs('patch', '/ws', { feeds: 'mta-subway,marc' })
    expect(args.slice(0, 2)).toEqual(['sync', 'patch'])
    expect(args).toContain('--feeds=mta-subway,marc')
  })

  test('check passes no --feeds', () => {
    const args = buildSyncArgs('check', '/ws')
    expect(args.slice(0, 2)).toEqual(['sync', 'check'])
    expect(args.some(a => a.startsWith('--feeds'))).toBe(false)
  })

  test('jobs and dry-run propagate', () => {
    const args = buildSyncArgs('global', '/ws', { jobs: '4', dryRun: true })
    expect(args).toContain('--jobs=4')
    expect(args).toContain('--dry-run')
  })
})

// ── key → onestop (portolan.json) ────────────────────────────────────────────

describe('keyToOnestopFromConfig', () => {
  test('reads onestop per feed entry, skipping entries without one', () => {
    const map = keyToOnestopFromConfig({
      sketches: 'sketches',
      feeds: {
        'mta-subway': { name: 'MTA Subway', onestop: 'f-dr5r-nyct' },
        'no-onestop': { name: 'Local feed' },
        marc: { onestop: 'f-dqc-marc' },
      },
    })
    expect(map.get('mta-subway')).toBe('f-dr5r-nyct')
    expect(map.get('marc')).toBe('f-dqc-marc')
    expect(map.has('no-onestop')).toBe(false)
  })

  test('degenerate configs yield an empty map, not a crash', () => {
    expect(keyToOnestopFromConfig(null).size).toBe(0)
    expect(keyToOnestopFromConfig({}).size).toBe(0)
    expect(keyToOnestopFromConfig({ feeds: null }).size).toBe(0)
  })
})

// ── exported zip → barrelman feed id ─────────────────────────────────────────

describe('mapExportedZips', () => {
  const keyToOnestop = new Map([
    ['mta-subway', 'f-dr5r-nyct'],
    ['orphan', 'f-xxxx-nowhere'],
  ])
  const onestopToFeedId = new Map([
    ['f-dr5r-nyct', 'f-dr5r-nyct'],
    ['f-dqc-marc', 'marc_feed'],
  ])

  test('maps through key → onestop → feed id', () => {
    const { mapped, unmapped } = mapExportedZips(['mta-subway.zip'], keyToOnestop, onestopToFeedId)
    expect(unmapped).toEqual([])
    expect(mapped).toEqual([
      { zip: 'mta-subway.zip', key: 'mta-subway', onestop: 'f-dr5r-nyct', feedId: 'f-dr5r-nyct' },
    ])
  })

  test('a key portolan.json does not give an onestop for is unmapped, with the reason', () => {
    const { mapped, unmapped } = mapExportedZips(['mystery.zip'], keyToOnestop, onestopToFeedId)
    expect(mapped).toEqual([])
    expect(unmapped).toHaveLength(1)
    expect(unmapped[0].zip).toBe('mystery.zip')
    expect(unmapped[0].reason).toContain('portolan.json')
  })

  test('an onestop no barrelman feed carries is unmapped, with the reason', () => {
    const { mapped, unmapped } = mapExportedZips(['orphan.zip'], keyToOnestop, onestopToFeedId)
    expect(mapped).toEqual([])
    expect(unmapped[0].reason).toContain('f-xxxx-nowhere')
  })

  test('mixed batches split cleanly', () => {
    const { mapped, unmapped } = mapExportedZips(
      ['mta-subway.zip', 'orphan.zip', 'mystery.zip'],
      keyToOnestop,
      onestopToFeedId,
    )
    expect(mapped).toHaveLength(1)
    expect(unmapped).toHaveLength(2)
  })
})
