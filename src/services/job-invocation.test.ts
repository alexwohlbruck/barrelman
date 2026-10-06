import { describe, expect, test } from 'bun:test'
import { buildInvocation } from './job-invocation'
import type { ScriptDef } from '../admin/scripts-manifest'

const script: ScriptDef = {
  id: 'test-script',
  name: 'Test',
  description: 'Test',
  category: 'osm',
  danger: 'safe',
  longRunning: false,
  confirm: false,
  exec: { kind: 'process', command: 'bash', args: ['scripts/test.sh'] },
  params: [
    { name: 'REBUILD', label: 'Rebuild', type: 'boolean', apply: 'env', envVar: 'REBUILD', default: true },
    { name: 'FORCE', label: 'Force', type: 'boolean', apply: 'env', envVar: 'FORCE', default: false },
  ],
  source: 'scripts/test.sh',
}

function env(params: Record<string, unknown>) {
  const inv = buildInvocation(script, params)
  if (inv.kind !== 'process') throw new Error('expected a process invocation')
  return inv.env
}

describe('buildInvocation boolean env params', () => {
  test('an unticked switch is passed as 0, so a script defaulting it on sees it off', () => {
    expect(env({ REBUILD: false }).REBUILD).toBe('0')
    expect(env({ REBUILD: 'false' }).REBUILD).toBe('0')
  })

  test('a ticked switch is passed as 1', () => {
    expect(env({ FORCE: true }).FORCE).toBe('1')
    expect(env({ FORCE: 'true' }).FORCE).toBe('1')
  })

  test('an omitted switch takes its default', () => {
    const e = env({})
    expect(e.REBUILD).toBe('1')
    expect(e.FORCE).toBe('0')
  })
})
