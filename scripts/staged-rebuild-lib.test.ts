import { describe, expect, it } from 'bun:test'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

/**
 * The decisions rebuild-graphhopper.sh and rebuild-motis.sh make before they
 * touch a live dataset: whether there is room for a second copy, whether the
 * old rollback copy may be given up, whether the box can afford a trial
 * server, and whether a new dataset looks complete. Each is a bash function in
 * staged-rebuild-lib.sh, called here the way the scripts call it.
 */
const LIB = join(import.meta.dir, 'staged-rebuild-lib.sh')

function call(fn: string, ...args: (string | number)[]): string {
  const quoted = args.map((a) => `'${String(a).replace(/'/g, `'\\''`)}'`).join(' ')
  const proc = Bun.spawnSync(['bash', '-c', `set -euo pipefail; . '${LIB}'; ${fn} ${quoted}`])
  if (proc.exitCode !== 0) throw new Error(`${fn} exited ${proc.exitCode}: ${proc.stderr.toString()}`)
  return proc.stdout.toString().trim()
}

const GB = 1024 * 1024 // KiB

describe('staged_need_kb', () => {
  it('asks for 1.2x the current dataset', () => {
    expect(Number(call('staged_need_kb', 28 * GB, 0))).toBe(Math.ceil(28 * GB * 1.2))
  })

  it('falls back to the estimate on a first build', () => {
    expect(call('staged_need_kb', 0, 5 * GB)).toBe(String(5 * GB))
  })
})

describe('staged_disk_decision', () => {
  const need = Math.ceil(28 * GB * 1.2) // the 28 GB US graph

  it('builds when there is room, keeping the rollback copy', () => {
    expect(call('staged_disk_decision', 40 * GB, need, 28 * GB, 1)).toBe('ok')
  })

  it('gives up the rollback copy only when that makes enough room and the live dataset serves', () => {
    expect(call('staged_disk_decision', 10 * GB, need, 28 * GB, 1)).toBe('drop-prev')
  })

  it('keeps the rollback copy when the live dataset is not serving, since it may be the only good one', () => {
    expect(call('staged_disk_decision', 10 * GB, need, 28 * GB, 0)).toBe('refuse')
  })

  it('refuses when even deleting the rollback copy is not enough', () => {
    expect(call('staged_disk_decision', 2 * GB, need, 10 * GB, 1)).toBe('refuse')
    expect(call('staged_disk_decision', 2 * GB, need, 0, 1)).toBe('refuse')
  })

  it('treats exactly enough as enough', () => {
    expect(call('staged_disk_decision', need, need, 0, 0)).toBe('ok')
  })
})

describe('java_xmx_kb', () => {
  it('reads the heap from JAVA_OPTS in each unit', () => {
    expect(call('java_xmx_kb', '-Xmx6g -Xms1g')).toBe(String(6 * GB))
    expect(call('java_xmx_kb', '-Xms512m -Xmx1536m')).toBe(String(1536 * 1024))
    expect(call('java_xmx_kb', '-Xmx2097152k')).toBe(String(2 * GB))
  })

  it('takes the last -Xmx, as the JVM does', () => {
    expect(call('java_xmx_kb', '-Xmx2g -Xmx8g')).toBe(String(8 * GB))
  })

  it('is 0 without one', () => {
    expect(call('java_xmx_kb', '-Xms1g')).toBe('0')
    expect(call('java_xmx_kb', '')).toBe('0')
  })
})

describe('staged_trial_decision', () => {
  it('runs on a big box with room to spare', () => {
    expect(call('staged_trial_decision', 'auto', 64 * GB, 48, 30 * GB, 9 * GB)).toBe('run')
  })

  it('skips on a box under the RAM threshold, however much is free right now', () => {
    expect(call('staged_trial_decision', 'auto', 32 * GB, 48, 20 * GB, 9 * GB)).toMatch(/^skip: host has 32\.0 GB/)
  })

  it('skips when the trial would not fit in available memory', () => {
    expect(call('staged_trial_decision', 'auto', 64 * GB, 48, 4 * GB, 9 * GB)).toMatch(/^skip: 4\.0 GB available/)
  })

  it('obeys an explicit setting over the measurements', () => {
    expect(call('staged_trial_decision', '1', 8 * GB, 48, 1 * GB, 9 * GB)).toBe('run')
    expect(call('staged_trial_decision', '0', 64 * GB, 48, 60 * GB, 1 * GB)).toBe('skip: turned off')
  })
})

describe('staged_size_check', () => {
  it('flags a graph under half the size of the one it replaces', () => {
    expect(call('staged_size_check', 10 * GB, 28 * GB, 50)).toBe('small')
  })

  it('accepts normal drift and first builds', () => {
    expect(call('staged_size_check', 27 * GB, 28 * GB, 50)).toBe('ok')
    expect(call('staged_size_check', 1 * GB, 0, 50)).toBe('ok')
  })
})

describe('motis_metrics_trips', () => {
  const dir = mkdtempSync(join(tmpdir(), 'motis-metrics-'))

  it('sums noTrips over every dataset in the import report', () => {
    // The shape `motis import` writes to data/timetable_metrics.json (2.11.4).
    const f = join(dir, 'ok.json')
    writeFileSync(
      f,
      '[{"idx":0,"firstDay":"2026-08-18","lastDay":"2026-09-06","noLocations":1884,"noTrips":33107,"transportsXDays":170661},' +
        '{"idx":1,"firstDay":"2026-08-17","lastDay":"2027-08-19","noLocations":3919,"noTrips":3673,"transportsXDays":301801}]',
    )
    expect(call('motis_metrics_trips', f)).toBe(String(33107 + 3673))
  })

  it('is 0 for an empty or missing report, which the rebuild refuses to swap in', () => {
    const empty = join(dir, 'empty.json')
    writeFileSync(empty, '')
    expect(call('motis_metrics_trips', empty)).toBe('0')
    expect(call('motis_metrics_trips', join(dir, 'missing.json'))).toBe('0')
    const zero = join(dir, 'zero.json')
    writeFileSync(zero, '[{"idx":0,"noLocations":0,"noTrips":0}]')
    expect(call('motis_metrics_trips', zero)).toBe('0')
    const other = join(dir, 'other.json')
    writeFileSync(other, '[]')
    expect(call('motis_metrics_trips', other)).toBe('0')
  })
})

describe('gh_route_candidates', () => {
  it('spreads test points inside the bounding box GraphHopper reports', () => {
    // /info's bbox for a Monaco build: minLon, minLat, maxLon, maxLat
    const lines = call('gh_route_candidates', 7.406437, 43.723224, 7.444726, 43.754369).split('\n')
    expect(lines.length).toBe(9)
    expect(lines[0]).toBe('43.73880,7.42558 43.74880,7.43558') // the centre first
    for (const line of lines) {
      const [lat, lon] = line.split(' ')[0].split(',').map(Number)
      expect(lat).toBeGreaterThan(43.723224)
      expect(lat).toBeLessThan(43.754369)
      expect(lon).toBeGreaterThan(7.406437)
      expect(lon).toBeLessThan(7.444726)
    }
  })
})
