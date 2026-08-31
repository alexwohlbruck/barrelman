import { describe, expect, it } from 'bun:test'
import { SCRIPTS, getScript } from './scripts-manifest'

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
