import { describe, expect, it } from 'bun:test'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { SCRIPTS, getScript } from './scripts-manifest'
import { SEEDS } from '../services/schedules.service'

/**
 * `postScripts` is documentation the console renders, not something the job
 * runner executes, so nothing at runtime would ever notice it going stale. A
 * renamed or deleted script id would just render as a blank row in the "Then
 * runs" list — and quietly stop telling operators that an OSM update also
 * rebuilds the routing graph and the basemap.
 */
describe('postScripts', () => {
  it('references only ids that exist in the manifest', () => {
    const dangling: string[] = []
    for (const script of SCRIPTS) {
      for (const step of script.postScripts || []) {
        if (!getScript(step.script)) dangling.push(`${script.id} → ${step.script}`)
      }
    }
    expect(dangling).toEqual([])
  })

  it('does not let a script list itself', () => {
    const selfReferential = SCRIPTS.filter((s) => s.postScripts?.some((step) => step.script === s.id))
    expect(selfReferential.map((s) => s.id)).toEqual([])
  })
})

describe('manifest integrity', () => {
  it('has unique script ids', () => {
    const ids = SCRIPTS.map((s) => s.id)
    expect(ids.length).toBe(new Set(ids).size)
  })

  /**
   * The whole manifest is serialised to the browser, so a function anywhere in
   * it would silently vanish through JSON and take its behaviour with it.
   */
  it('is pure data', () => {
    const walk = (value: unknown, path: string): string[] => {
      if (typeof value === 'function') return [path]
      if (Array.isArray(value)) return value.flatMap((v, i) => walk(v, `${path}[${i}]`))
      if (value && typeof value === 'object') {
        return Object.entries(value).flatMap(([k, v]) => walk(v, `${path}.${k}`))
      }
      return []
    }
    expect(walk(SCRIPTS, 'SCRIPTS')).toEqual([])
  })
})

/**
 * Backup and restore are only exercised the night something breaks, so the
 * wiring between the manifest, the scripts and the seeded schedule is checked
 * here instead.
 */
describe('backup and restore entries', () => {
  const root = resolve(import.meta.dir, '../..')
  const scriptText = (id: string) => {
    const script = getScript(id)
    if (script?.exec.kind !== 'process') throw new Error(`${id} is not a process script`)
    return readFileSync(resolve(root, script.exec.args[0]), 'utf8')
  }

  it('passes only flags the scripts parse', () => {
    for (const id of ['db-backup', 'db-restore-drill']) {
      const text = scriptText(id)
      const flags = (getScript(id)?.params ?? []).filter((p) => p.apply === 'flag').map((p) => p.flag ?? `--${p.name}`)
      expect(flags.length).toBeGreaterThan(0)
      expect(flags.filter((f) => !text.includes(f))).toEqual([])
    }
  })

  it('never offers the live-database restore from the console', () => {
    const params = getScript('db-restore-drill')?.params ?? []
    expect(params.some((p) => p.name.includes('live') || p.envVar === 'BACKUP_RESTORE_CONFIRM')).toBe(false)
  })

  it('seeds a nightly backup, and every seed resolves', () => {
    expect(SEEDS.find((s) => s.scriptId === 'db-backup')?.cron).toBe('15 2 * * *')
    expect(SEEDS.filter((s) => !getScript(s.scriptId)).map((s) => s.scriptId)).toEqual([])
  })
})
