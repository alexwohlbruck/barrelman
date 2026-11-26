/**
 * Provider matches survive every way an OSM import rewrites geo_places:
 * a full import (import-osm.sh, osm2pgsql --create), a replication cycle
 * without middle tables (replicate-extract.sh's delete-and-copy swap), and a
 * diff applied through the middle tables (osm2pgsql --append, what
 * osm2pgsql-replication runs).
 *
 * Needs osm2pgsql, osmium, psql and wget, so it runs inside a barrelman-db
 * container against a throwaway database:
 *
 *   docker exec -w /app -e BARRELMAN_INTEGRATION_TESTS=1 -e PGPASSWORD=… \
 *     -e DATABASE_URL=postgresql://barrelman@localhost:5432/scratch \
 *     <container> bun test import/provider-matches-reimport.integration.test.ts
 */
import { afterAll, beforeAll, describe, expect, test } from 'bun:test'
import { mkdtempSync, rmSync, writeFileSync, mkdirSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import postgres from 'postgres'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined
const enabled = Boolean(DATABASE_URL && Bun.which('osm2pgsql') && Bun.which('osmium') && Bun.which('wget'))
const PROJECT = join(import.meta.dir, '..')
const T0 = '2026-01-01T00:00:00Z'

function tags(entries: Record<string, string>) {
  return Object.entries(entries).map(([k, v]) => `<tag k="${k}" v="${v}"/>`).join('')
}

function elements(version: number, cafe: string, shop: string) {
  const ts = `version="${version}" timestamp="2026-01-0${version}T00:00:00Z"`
  const corners = [[1002, 35.2270, -80.8440], [1003, 35.2270, -80.8436], [1004, 35.2274, -80.8436], [1005, 35.2274, -80.8440]]
  return [
    `<node id="1001" ${ts} lat="35.2271" lon="-80.8431">${tags({ amenity: 'cafe', name: cafe })}</node>`,
    ...(version === 1 ? corners.map(([id, lat, lon]) => `<node id="${id}" ${ts} lat="${lat}" lon="${lon}"/>`) : []),
    `<way id="2001" ${ts}>${[1002, 1003, 1004, 1005, 1002].map((n) => `<nd ref="${n}"/>`).join('')}${tags({ building: 'yes', shop: 'books', name: shop })}</way>`,
  ].join('\n')
}

/** Async, so the replication feed served from this process can answer the script's downloads. */
async function run(cmd: string[], env: Record<string, string> = {}) {
  const proc = Bun.spawn(cmd, { cwd: PROJECT, env: { ...process.env, ...env }, stdout: 'pipe', stderr: 'pipe' })
  const [stdout, stderr, code] = await Promise.all([new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited])
  if (code !== 0) throw new Error(`${cmd.join(' ')} exited ${code}\n${stdout.slice(-3000)}\n${stderr.slice(-3000)}`)
  return stdout
}

describe.skipIf(!enabled)('provider matches across OSM re-imports', () => {
  let dir: string
  let sql: postgres.Sql
  let feed: ReturnType<typeof Bun.serve>
  let service: typeof import('../src/services/provider-matches.service')
  const pbf = () => join(dir, 'region.osm.pbf')
  const env = () => ({
    DATABASE_URL: DATABASE_URL!,
    IMPORT_PBF: pbf(),
    OSM_PBF_FILE: pbf(),
    BARRELMAN_DATA_DIR: join(dir, 'data'),
    REPLICATION_WORK_DIR: join(dir, 'replication'),
  })

  const placeName = async (osmType: 'N' | 'W', osmId: number) =>
    (await sql`SELECT name FROM geo_places WHERE osm_type = ${osmType} AND osm_id = ${osmId}`)[0]?.name
  const matches = async () =>
    (await sql`SELECT osm_type, osm_id::text, provider, provider_place_id, confirmed, label FROM place_provider_matches ORDER BY 1, 2, 3`)
      .map((r) => ({ ...r }))

  const expected = [
    { osm_type: 'node', osm_id: '1001', provider: 'foursquare', provider_place_id: '4cd3fixture', confirmed: true, label: null },
    { osm_type: 'node', osm_id: '1001', provider: 'opentable', provider_place_id: null, confirmed: true, label: null },
    { osm_type: 'way', osm_id: '2001', provider: 'google', provider_place_id: 'ChIJfixture', confirmed: false, label: 'Fixture Books' },
  ]

  beforeAll(async () => {
    dir = mkdtempSync(join(tmpdir(), 'provider-matches-'))
    sql = postgres(DATABASE_URL!, { max: 1, onnotice: () => {} })
    await sql`DROP TABLE IF EXISTS place_provider_matches`

    const replication = join(dir, 'feed')
    mkdirSync(join(replication, '000/000'), { recursive: true })
    writeFileSync(join(replication, 'state.txt'), `sequenceNumber=1\ntimestamp=2026-01-02T00\\:00\\:00Z\n`)
    writeFileSync(join(replication, '000/000/001.state.txt'), `sequenceNumber=1\ntimestamp=2026-01-02T00\\:00\\:00Z\n`)
    writeFileSync(
      join(replication, '000/000/001.osc.gz'),
      Bun.gzipSync(`<osmChange version="0.6"><modify>${elements(2, 'Fixture Coffee Roasters', 'Fixture Books and Maps')}</modify></osmChange>`),
    )
    feed = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch: (req) => new Response(Bun.file(join(replication, new URL(req.url).pathname))),
    })

    writeFileSync(join(dir, 'fixture.osm'), `<?xml version="1.0" encoding="UTF-8"?>\n<osm version="0.6">\n${elements(1, 'Fixture Coffee', 'Fixture Books')}\n</osm>\n`)
    await run([
      'osmium', 'cat', '-O', '-o', pbf(), join(dir, 'fixture.osm'),
      `--output-header=osmosis_replication_base_url=http://127.0.0.1:${feed.port}`,
      '--output-header=osmosis_replication_sequence_number=0',
      `--output-header=osmosis_replication_timestamp=${T0}`,
    ])
    await sql`DROP TABLE IF EXISTS osm_replication_state`

    await run(['bash', 'scripts/import-osm.sh'], env())
    expect(await placeName('N', 1001)).toBe('Fixture Coffee')

    service = await import('../src/services/provider-matches.service')
    await service.setProviderMatch({ osmType: 'node', osmId: '1001' }, 'foursquare', { providerPlaceId: '4cd3fixture', confirmed: true })
    await service.setProviderMatch({ osmType: 'node', osmId: '1001' }, 'opentable', { providerPlaceId: null, confirmed: true })
    await service.setProviderMatch({ osmType: 'way', osmId: '2001' }, 'google', { providerPlaceId: 'ChIJfixture', confirmed: false, label: 'Fixture Books' })
  }, 600_000)

  afterAll(async () => {
    feed?.stop(true)
    await sql?.end()
    if (dir) rmSync(dir, { recursive: true, force: true })
  })

  test('a replication cycle without middle tables swaps the rows and keeps the matches', async () => {
    const out = await run(['bash', 'scripts/replicate-extract.sh'], env())
    expect(out).toContain('REPLICATION_APPLIED=1')
    expect(await placeName('N', 1001)).toBe('Fixture Coffee Roasters')
    expect(await placeName('W', 2001)).toBe('Fixture Books and Maps')
    expect(await matches()).toEqual(expected)
  }, 600_000)

  test('a diff applied through the middle tables keeps the matches', async () => {
    const osc = join(dir, 'append.osc')
    writeFileSync(osc, `<osmChange version="0.6"><modify>${elements(3, 'Fixture Espresso', 'Fixture Books and Maps')}</modify></osmChange>`)
    await run([
      'osm2pgsql', '--append', '--slim', '--output=flex',
      `--style=${join(PROJECT, 'import/osm2pgsql-flex.lua')}`, '-d', DATABASE_URL!, osc,
    ])
    expect(await placeName('N', 1001)).toBe('Fixture Espresso')
    expect(await matches()).toEqual(expected)
  }, 600_000)

  test('a full re-import recreates geo_places and keeps the matches', async () => {
    const [before] = await sql`SELECT 'geo_places'::regclass::oid AS oid`
    await run(['bash', 'scripts/import-osm.sh'], env())
    const [after] = await sql`SELECT 'geo_places'::regclass::oid AS oid`
    expect(after.oid).not.toBe(before.oid)
    expect(await placeName('N', 1001)).toBe('Fixture Coffee Roasters')
    expect(await matches()).toEqual(expected)
  }, 600_000)
})
