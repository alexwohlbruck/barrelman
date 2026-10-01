/**
 * Build the Pelias address index for the selected REGIONS, end to end:
 * config → schema → downloads → street extract → imports → API restart.
 *
 *   REGIONS=united-states bun run scripts/build-pelias.ts
 *   RESET_INDEX=1         bun run scripts/build-pelias.ts   # drop and rebuild
 *
 * barrelman-ops has the docker CLI but not compose, so every step is a
 * `docker run` of its service in pelias/docker-compose.yml — image, user and
 * mounts are read from that file rather than restated here.
 */
import { existsSync, mkdirSync, readFileSync, rmSync, statSync } from 'node:fs'
import { hostname } from 'node:os'
import { basename, dirname, isAbsolute, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { resolveRegions } from '../src/config/regions'
import { writePeliasConfig } from './generate-pelias-config'

interface ComposeService {
  image: string
  container_name?: string
  user?: string
  volumes?: string[]
}

const PELIAS_DIR = resolve(dirname(fileURLToPath(import.meta.url)), '../pelias')
// Elasticsearch owns the data dir as uid 1000; importers must write as the same user.
const DEFAULT_USER = '1000:1000'
const INDEX = 'pelias'
const OA_BATCH = 50
// The highway types `pbf streets` turns into street documents.
const STREET_HIGHWAYS = 'motorway,primary,residential,road,secondary,service,tertiary,trunk'
// `pbf streets` needs roughly ten times a file's size in memory.
const STREET_CHUNK_BYTES = 150 * 1024 ** 2
const STREET_MEMORY = '4g'

type Bbox = [west: number, south: number, east: number, north: number]

const log = (msg: string) => console.log(`[${new Date().toTimeString().slice(0, 8)}] [pelias] ${msg}`)

async function docker(args: string[], { quiet = false } = {}): Promise<string> {
  const proc = Bun.spawn(['docker', ...args], {
    stdout: quiet ? 'pipe' : 'inherit',
    stderr: quiet ? 'pipe' : 'inherit',
  })
  const out = quiet ? await new Response(proc.stdout).text() : ''
  if ((await proc.exited) !== 0) {
    const err = quiet ? await new Response(proc.stderr).text() : ''
    throw new Error(`docker ${args.slice(0, 2).join(' ')} failed${err ? `: ${err.trim()}` : ''}`)
  }
  return out.trim()
}

function readEnvFile(path: string): Record<string, string> {
  if (!existsSync(path)) return {}
  return Object.fromEntries(
    readFileSync(path, 'utf8')
      .split('\n')
      .map((line) => /^\s*([A-Z_][A-Z0-9_]*)=(.*)$/.exec(line))
      .filter((m): m is RegExpExecArray => !!m)
      .map((m) => [m[1], m[2].replace(/^(['"])(.*)\1$/, '$2')]),
  )
}

const env: Record<string, string | undefined> = { ...readEnvFile(`${PELIAS_DIR}/.env`), ...process.env }

/** Compose-style `${VAR}` / `${VAR:-default}` substitution. */
const interpolate = (s: string) =>
  s.replace(/\$\{(\w+)(?::-([^}]*))?\}/g, (_, name: string, fallback = '') => env[name] || fallback)

/**
 * Containers started through the socket resolve bind mounts on the host, so
 * relative paths need the host location of pelias/, not this container's.
 */
async function hostPeliasDir(): Promise<string> {
  if (!existsSync('/.dockerenv')) return PELIAS_DIR
  const mounts = JSON.parse(await docker(['inspect', hostname(), '--format', '{{json .Mounts}}'], { quiet: true })) as {
    Source: string
    Destination: string
  }[]
  const mount = mounts.find((m) => m.Destination === PELIAS_DIR)
  if (!mount) throw new Error(`${PELIAS_DIR} is not bind-mounted into this container, so its host path is unknown`)
  return mount.Source
}

const services = (Bun.YAML.parse(readFileSync(`${PELIAS_DIR}/docker-compose.yml`, 'utf8')) as {
  services: Record<string, ComposeService>
}).services
const hostDir = await hostPeliasDir()

function volumeArgs(service: ComposeService): string[] {
  return (service.volumes ?? []).flatMap((spec) => {
    const [src, ...rest] = interpolate(spec).split(':')
    const source = isAbsolute(src) ? src : resolve(hostDir, src)
    return ['-v', [source, ...rest].join(':')]
  })
}

const containerName = (name: string) => services[name].container_name ?? name

async function containerState(name: string): Promise<string | null> {
  try {
    return await docker(['inspect', containerName(name), '--format', '{{.State.Status}}'], { quiet: true })
  } catch {
    return null
  }
}

/** Start Elasticsearch if it is stopped, and return the network the importers must join to reach it. */
async function startElastic(): Promise<string> {
  const state = await containerState('elasticsearch')
  if (!state) {
    throw new Error(
      'No Elasticsearch container. Create it once from the barrelman directory: docker compose --profile pelias up -d elasticsearch',
    )
  }
  if (state !== 'running') await docker(['start', containerName('elasticsearch')])
  const networks = await docker(
    ['inspect', containerName('elasticsearch'), '--format', '{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}'],
    { quiet: true },
  )
  return networks.split(' ')[0]
}

async function run(
  name: string,
  command: string[],
  { user, memory }: { user?: string; memory?: string } = {},
): Promise<void> {
  const service = services[name]
  log(`${name}: ${command.join(' ')}`)
  await docker([
    'run', '--rm',
    '--network', network,
    '--user', user ?? (interpolate(service.user ?? '') || DEFAULT_USER),
    ...(memory ? ['--memory', memory, '--memory-swap', memory] : []),
    ...volumeArgs(service),
    service.image,
    ...command,
  ])
}

/** ops reaches Elasticsearch by container name; a host shell reaches its loopback port. */
async function elasticUrl(): Promise<string> {
  for (const url of [`http://${containerName('elasticsearch')}:9200`, 'http://127.0.0.1:9200']) {
    if (await fetch(url, { signal: AbortSignal.timeout(3000) }).then(() => true, () => false)) return url
  }
  throw new Error('Elasticsearch is running but unreachable from here')
}

async function withRetries(attempts: number, fn: () => Promise<void>): Promise<void> {
  for (let i = 1; ; i++) {
    try {
      return await fn()
    } catch (err) {
      if (i === attempts) throw err
      log(`attempt ${i} of ${attempts} failed, retrying in ${i * 30}s`)
      await Bun.sleep(i * 30_000)
    }
  }
}

/**
 * The downloader resolves every file against the OpenAddresses API at once, and
 * a single 5xx among thousands aborts the run, so fetch in batches and skip
 * files already on disk.
 */
/** The data dir as seen from here, or null when it lives outside the mounted pelias/ directory. */
const localDataDir = () => (isAbsolute(dataDir) ? null : resolve(PELIAS_DIR, dataDir))

async function downloadOpenAddresses(files: string[]): Promise<void> {
  const localData = localDataDir()
  const missing = files.filter(
    (f) => !localData || !existsSync(`${localData}/openaddresses/${f.replace(/\.csv$/, '')}.geojson`),
  )
  log(`openaddresses: ${files.length - missing.length} of ${files.length} files already downloaded`)
  for (let i = 0; i < missing.length; i += OA_BATCH) {
    writePeliasConfig({ ...regions, peliasOpenaddresses: missing.slice(i, i + OA_BATCH) })
    await withRetries(3, () => run('openaddresses', ['./bin/download']))
  }
  writePeliasConfig(regions)
}

async function downloadOsm(extracts: string[]): Promise<void> {
  const localData = localDataDir()
  if (localData && extracts.every((url) => existsSync(`${localData}/openstreetmap/${basename(url)}`))) {
    log('openstreetmap: extracts already downloaded — delete them to fetch fresh copies')
    return
  }
  await withRetries(3, () => run('openstreetmap', ['./bin/download']))
}

async function osmium(args: string[]): Promise<void> {
  const proc = Bun.spawn(['osmium', ...args], { stdout: 'inherit', stderr: 'inherit' })
  if ((await proc.exited) !== 0) throw new Error(`osmium ${args[0]} failed`)
}

async function osmiumBbox(pbf: string): Promise<Bbox> {
  const proc = Bun.spawn(['osmium', 'fileinfo', '-e', '-g', 'data.bbox', pbf], { stdout: 'pipe', stderr: 'inherit' })
  const out = await new Response(proc.stdout).text()
  if ((await proc.exited) !== 0) throw new Error(`osmium fileinfo failed on ${basename(pbf)}`)
  const box = out.match(/-?[\d.]+/g)?.map(Number)
  if (box?.length !== 4) throw new Error(`no bounding box in ${basename(pbf)}`)
  return box as Bbox
}

/** Halve a PBF along its longer side until every piece fits `pbf streets`, deleting the parents. */
async function splitForStreets(pbf: string, box: Bbox, pieces: string[], depth = 0): Promise<void> {
  if (statSync(pbf).size <= STREET_CHUNK_BYTES || depth >= 12) {
    pieces.push(pbf)
    return
  }
  const [w, s, e, n] = box
  const halves: Bbox[] =
    e - w >= n - s
      ? [[w, s, (w + e) / 2, n], [(w + e) / 2, s, e, n]]
      : [[w, s, e, (s + n) / 2], [w, (s + n) / 2, e, n]]
  for (const [i, half] of halves.entries()) {
    const part = pbf.replace(/\.osm\.pbf$/, `${i}.osm.pbf`)
    await osmium(['extract', '--overwrite', '--strategy', 'complete_ways', '-b', half.join(','), pbf, '-o', part])
    await splitForStreets(part, half, pieces, depth + 1)
  }
  rmSync(pbf)
}

/**
 * Build the street-name extract. Pelias's docker_extract.sh refuses any PBF over
 * 1 GB because `pbf streets` holds it in memory, so cut each extract down to the
 * named highways it reads, split that into small pieces, and convert them one at
 * a time under a memory cap.
 */
async function preparePolylines(extracts: string[]): Promise<void> {
  const localData = localDataDir()
  if (!localData) return run('polylines', ['bash', './docker_extract.sh'])

  const dir = `${localData}/polylines`
  const extract = `${dir}/extract.0sv`
  const pbfs = extracts.map((url) => `${localData}/openstreetmap/${basename(url)}`)
  if (existsSync(extract) && statSync(extract).size > 1 && pbfs.every((p) => statSync(p).mtimeMs < statSync(extract).mtimeMs)) {
    log('polylines: street extract is newer than the OSM data — skipping')
    return
  }

  mkdirSync(dir, { recursive: true })
  const pieces: string[] = []
  for (const [i, pbf] of pbfs.entries()) {
    const typed = `${dir}/streets-${i}-typed.osm.pbf`
    const named = `${dir}/streets-${i}-.osm.pbf`
    log(`polylines: filtering ${basename(pbf)} to named streets`)
    await osmium(['tags-filter', '--overwrite', pbf, `w/highway=${STREET_HIGHWAYS}`, '-o', typed])
    await osmium(['tags-filter', '--overwrite', typed, 'w/name', '-o', named])
    rmSync(typed)
    await splitForStreets(named, await osmiumBbox(named), pieces)
  }
  log(`polylines: converting ${pieces.length} pieces`)

  const tmp = '/data/polylines/extract.0sv.tmp'
  await run('polylines', ['sh', '-c', `: > ${tmp}`], { user: '0' })
  for (const piece of pieces) {
    await run('polylines', ['sh', '-c', `pbf streets /data/polylines/${basename(piece)} >> ${tmp}`], {
      user: '0',
      memory: STREET_MEMORY,
    })
    rmSync(piece)
  }
  await run('polylines', ['mv', tmp, '/data/polylines/extract.0sv'], { user: '0' })
}

async function waitForElastic(url: string): Promise<void> {
  for (let i = 0; i < 60; i++) {
    const res = await fetch(`${url}/_cluster/health`).catch(() => null)
    const status = res?.ok ? ((await res.json()) as { status: string }).status : null
    if (status === 'green' || status === 'yellow') return
    await Bun.sleep(5000)
  }
  throw new Error('Elasticsearch did not become healthy within 5 minutes')
}

// ── Build ────────────────────────────────────────────────────────────────────

const regions = await resolveRegions()
writePeliasConfig(regions)

const network = await startElastic()
const es = await elasticUrl()
await waitForElastic(es)

const dataDir = interpolate('${DATA_DIR:-./data}')
await run('whosonfirst', ['sh', '-c', `chown ${DEFAULT_USER} /data`], { user: '0' })
log(`data dir: ${isAbsolute(dataDir) ? dataDir : resolve(hostDir, dataDir)}`)

if (env.RESET_INDEX === '1' || env.RESET_INDEX === 'true') {
  log(`dropping index "${INDEX}"`)
  await fetch(`${es}/${INDEX}`, { method: 'DELETE' })
}
if ((await fetch(`${es}/${INDEX}`, { method: 'HEAD' })).status === 404) await run('schema', ['./bin/create_index'])

// With no files listed, the OpenAddresses downloader fetches the whole planet.
const withAddresses = regions.peliasOpenaddresses.length > 0
if (!withAddresses) {
  log('no OpenAddresses files in the selected regions — skipping them; only OSM addresses will be indexed')
}

await withRetries(3, () => run('whosonfirst', ['./bin/download']))
if (withAddresses) await downloadOpenAddresses(regions.peliasOpenaddresses)
await downloadOsm(regions.osmExtracts)
// Street names come only from this extract; skip it and street search is empty.
// Only the street import needs it, so it builds alongside the others.
const streetExtract = preparePolylines(regions.osmExtracts).then(
  () => null,
  (err: unknown) => err,
)

await run('whosonfirst', ['./bin/start'])
if (withAddresses) await run('openaddresses', ['./bin/parallel', env.OPENADDRESSES_PARALLELISM || '1'])
await run('openstreetmap', ['./bin/start'])
const streetError = await streetExtract
if (streetError) throw streetError
await run('polylines', ['./bin/start'])

const docs = (await (await fetch(`${es}/${INDEX}/_count`)).json()) as { count: number }
log(`index holds ${docs.count.toLocaleString()} documents`)

// The API only sees newly imported layers after a restart.
if (await containerState('api')) {
  await docker(['restart', containerName('api')])
  log('restarted the Pelias API')
} else {
  log('no Pelias API container yet. Start it from the barrelman directory: docker compose --profile pelias up -d api')
}

process.exit(0)
