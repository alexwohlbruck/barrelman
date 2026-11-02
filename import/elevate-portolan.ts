/**
 * Write the heights of transit ribbons, stations and stop marks on decks
 * and in tunnels into portolan's tile pyramids.
 *
 *   bun run import/elevate-portolan.ts --dir <pyramid>    one pyramid, e.g. portolan's --tile-hook $PORTOLAN_TILES
 *   bun run import/elevate-portolan.ts [--feeds a,b]      the live pyramids, each tile replaced whole
 *
 * Run after Build Bridge Decks and Build Tunnel Cuts, which it reads.
 */
import { existsSync, readdirSync } from 'fs'
import { join } from 'path'
import { resolvePortolanTilesDir } from '../src/config/portolan'
import { connection } from '../src/db'
import { argValue } from '../src/lib/cli-args'
import { cachedLineSources, readLineSources } from '../src/lib/line-elevation/lookup'
import { elevatePyramid } from '../src/lib/line-elevation/pyramid'

const args = process.argv.slice(2)
const dir = argValue(args, 'dir')

function livePyramids(): string[] {
  const workspace = process.env.PORTOLAN_WORKSPACE
  const tilesDir = resolvePortolanTilesDir(process.env.PORTOLAN_TILES_DIR ? undefined : workspace && join(workspace, 'build/tiles'))
  const feeds = argValue(args, 'feeds')?.split(',').filter(Boolean)
    ?? readdirSync(tilesDir, { withFileTypes: true }).filter(e => e.isDirectory() && !e.name.startsWith('.')).map(e => e.name).sort()
  return feeds.map(f => join(tilesDir, f)).filter(existsSync)
}

const pyramids = dir ? [dir] : livePyramids()

const sources = cachedLineSources(readLineSources, { max: 256, ttl: 3_600_000 })
const started = Date.now()
let failed = 0
for (const [i, pyramid] of pyramids.entries()) {
  const tally = await elevatePyramid(pyramid, sources, line => console.warn(`  ${pyramid}: ${line}`))
  failed += tally.failed
  if (tally.written || tally.failed || dir) console.log(`[${i + 1}/${pyramids.length}] ${pyramid}: ${tally.written} of ${tally.tiles} tiles given heights${tally.failed ? `, ${tally.failed} failed` : ''} (${Math.round((Date.now() - started) / 1000)} s)`)
}
await connection.end({ timeout: 5 })
process.exit(failed ? 1 : 0)
