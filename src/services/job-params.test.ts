import { describe, expect, test } from 'bun:test'
import { buildInvocation, ParamValidationError, validateParams } from './job-invocation'
import { SCRIPTS, type ScriptDef, type ScriptParam } from '../admin/scripts-manifest'
import { SEEDS } from './schedules.service'

function script(params: ScriptParam[]): ScriptDef {
  return {
    id: 'test-script',
    name: 'Test',
    description: 'Test',
    category: 'osm',
    danger: 'safe',
    longRunning: false,
    confirm: false,
    exec: { kind: 'process', command: 'bun', args: ['run', 'import/x.ts'] },
    params,
  }
}

function argv(s: ScriptDef, params: Record<string, unknown>) {
  const inv = buildInvocation(s, params)
  if (inv.kind !== 'process') throw new Error('expected a process invocation')
  return inv.args.slice(2)
}

const refuses = (s: ScriptDef, params: Record<string, unknown>) =>
  expect(() => buildInvocation(s, params)).toThrow(ParamValidationError)

describe('positional params', () => {
  const single = script([{ name: 'source', label: 'Backup', type: 'string', format: 'path', apply: 'positional' }])
  const list = script([
    { name: 'feeds', label: 'Feeds', type: 'string', apply: 'positional', multiple: true },
  ])

  test('a single positional is one argv element', () => {
    expect(argv(single, { source: 'backups/barrelman-1.dump' })).toEqual(['backups/barrelman-1.dump'])
  })

  test('a single positional is never split into extra arguments', () => {
    // This used to become three argv elements, the last two of them flags.
    refuses(single, { source: 'latest --drop-live --yes' })
  })

  test('a list positional gives each item its own element', () => {
    expect(argv(list, { feeds: '  f-one   f-two ' })).toEqual(['f-one', 'f-two'])
  })

  test('a list item that is a flag is refused', () => {
    refuses(list, { feeds: 'f-one --all' })
    refuses(list, { feeds: '-x' })
  })
})

describe('flag and env params', () => {
  const s = script([
    { name: 'feed-id', label: 'Feed', type: 'string', apply: 'flag', flag: '--feed-id' },
    { name: 'dir', label: 'Dir', type: 'string', format: 'path', apply: 'flag', flag: '--dir' },
    { name: 'bbox', label: 'Area', type: 'string', format: 'bbox', apply: 'flag', flag: '--bbox' },
    { name: 'jobs', label: 'Jobs', type: 'number', apply: 'flag', flag: '--jobs' },
    { name: 'cell', label: 'Cell', type: 'number', decimal: true, apply: 'flag', flag: '--cell' },
    { name: 'REGIONS', label: 'Regions', type: 'string', format: 'regions', apply: 'env' },
    {
      name: 'MODE',
      label: 'Mode',
      type: 'select',
      apply: 'env',
      options: [
        { label: 'A', value: 'replication' },
        { label: 'B', value: 'full' },
      ],
    },
    { name: 'search', label: 'Search', type: 'string', format: 'text', apply: 'flag', flag: '--search' },
    { name: 'since', label: 'Since', type: 'string', format: 'date', apply: 'flag', flag: '--since' },
  ])

  test('valid values pass through', () => {
    const inv = buildInvocation(s, {
      'feed-id': 'f-dr5r-mta~nyct',
      dir: './data/gtfs',
      bbox: '-109.06,36.99,-102.04,41.0',
      jobs: 4,
      cell: '0.25',
      REGIONS: 'north-carolina,nyc-metro',
      MODE: 'full',
      search: 'north carolina',
      since: '2026-11-04',
    })
    if (inv.kind !== 'process') throw new Error('expected process')
    expect(inv.args).toContain('--bbox=-109.06,36.99,-102.04,41.0')
    expect(inv.args).toContain('north carolina')
    expect(inv.env).toMatchObject({ REGIONS: 'north-carolina,nyc-metro', MODE: 'full' })
  })

  test('a value starting with a dash is refused unless the param allows it', () => {
    refuses(s, { 'feed-id': '--output=/etc/passwd' })
    refuses(s, { jobs: '-1' })
    expect(() =>
      buildInvocation(script([{ name: 'n', label: 'N', type: 'number', apply: 'flag', allowDash: true }]), { n: '-1' }),
    ).not.toThrow()
  })

  test('select values must be one of the options', () => {
    refuses(s, { MODE: 'replication; rm -rf /' })
  })

  test('numbers are numbers, and whole unless declared decimal', () => {
    refuses(s, { jobs: '4abc' })
    refuses(s, { jobs: '2.5' })
    refuses(s, { cell: '1e3' })
  })

  test('paths stay relative and inside the working directory', () => {
    refuses(s, { dir: '/etc' })
    refuses(s, { dir: 'data/../../etc' })
    refuses(s, { dir: 'data gtfs' })
  })

  test('region lists and dates are checked', () => {
    refuses(s, { REGIONS: 'nc,$(id)' })
    refuses(s, { since: '2026-02-30' })
    refuses(s, { since: 'yesterday' })
  })

  test('text is one line', () => {
    refuses(s, { search: 'colorado\n--evil' })
  })

  test('an identifier refuses shell and path characters', () => {
    for (const bad of ['a b', 'a/b', 'a;b', '$(x)']) refuses(s, { 'feed-id': bad })
  })

  test('required params must be present', () => {
    const r = script([{ name: 'x', label: 'X', type: 'string', apply: 'flag', required: true }])
    expect(() => validateParams(r, {})).toThrow('X: required')
  })
})

describe('the real manifest', () => {
  test('every default value passes its own validation', () => {
    for (const s of SCRIPTS) {
      // A required param with no default is filled in by the operator.
      const params = Object.fromEntries(
        (s.params ?? []).map((p) => [p.name, p.default ?? (p.required ? 'sample' : undefined)]),
      )
      expect(() => validateParams(s, params)).not.toThrow()
    }
  })

  test('every seeded schedule passes validation', () => {
    for (const seed of SEEDS) {
      const s = SCRIPTS.find((x) => x.id === seed.scriptId)!
      expect(() => validateParams(s, seed.params ?? {})).not.toThrow()
    }
  })

  test('the restore drill takes its backup as one argument', () => {
    const drill = SCRIPTS.find((s) => s.id === 'db-restore-drill')!
    const inv = buildInvocation(drill, { source: 'latest' })
    if (inv.kind !== 'process') throw new Error('expected process')
    expect(inv.args.filter((a) => a === 'latest')).toHaveLength(1)
    refuses(drill, { source: '--drop-live' })
  })
})
