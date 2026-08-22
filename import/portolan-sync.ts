#!/usr/bin/env bun
/**
 * Portolan Sync Wrapper
 *
 * Drives the `portolan` binary (corrected transit geometry: MVT tile pyramids
 * + corrected GTFS exports) and lands its output in barrelman:
 *
 *   1. Run `portolan sync <global|patch|check>` against the workspace,
 *      streaming its progress output.
 *   2. Parse the final `RESULT {…}` line (the machine-readable contract —
 *      see portolan's docs/SYNC.md).
 *   3. For every corrected GTFS zip portolan exported, map the portolan feed
 *      key → barrelman feed id via the transitland onestop id (portolan.json
 *      carries `onestop` per feed; gtfs_feeds.onestop_id is the join key),
 *      copy the zip over data/gtfs/<feed-id>.zip (atomic temp+rename), and
 *      re-run the SAME per-zip pipeline the GTFS importer uses: sanitize
 *      (strip GTFS-Flex), import into PostGIS, carry the previously computed
 *      walking transfers.txt into the new zip, inject Fares v2.
 *   4. Regenerate the MOTIS config and, unless --skip-motis, run
 *      scripts/rebuild-motis.sh. MOTIS has NO incremental import — `motis
 *      server` only serves the pre-built dataset, so even a one-feed patch
 *      requires a full dataset rebuild for the corrected geometry to reach
 *      riders.
 *
 * Usage:
 *   bun run import/portolan-sync.ts --global
 *   bun run import/portolan-sync.ts --patch=mta-subway,marc
 *   bun run import/portolan-sync.ts --check          # transitland sha diff → download → patch
 *
 * Flags (--flag=value form — see scripts/download-gtfs.sh for why):
 *   --workspace     portolan workspace root (default $PORTOLAN_WORKSPACE or ./data/portolan)
 *   --portolan-bin  portolan binary (default $PORTOLAN_BIN or `portolan` on PATH)
 *   --gtfs-dir      barrelman GTFS zip dir (default $GTFS_DATA_DIR or ./data/gtfs)
 *   --motis-config  MOTIS config output (default ./motis/config.yml)
 *   --jobs          parallel feed builds (passed through to portolan)
 *   --skip-motis    skip the MOTIS dataset rebuild
 *   --dry-run       plan only — portolan prints what would rebuild, nothing is imported
 *
 * TRANSITLAND_API_KEY is read from the environment by `portolan sync check`.
 */

import { parseArgs } from 'util'
import { existsSync, renameSync, unlinkSync, writeFileSync } from 'fs'
import { join, resolve } from 'path'
import JSZip from 'jszip'

// ── The RESULT contract ─────────────────────────────────────────────
//
// With --json, the LAST stdout line portolan prints is:
//
//   RESULT {"changed":[…],"affected":[…],"rebuilt":[…],"groups_rewritten":false,
//           "tiles":{"written":N,"unchanged":N,"removed":N},
//           "exported":["<feedkey>.zip",…],"errors":[…]}
//
// Exit 0 on success, 1 if any feed errored (with everything else completed —
// a single feed's failure lands in `errors` and does not abort the run).

export interface PortolanSyncResult {
  changed: string[]
  affected: string[]
  rebuilt: string[]
  groups_rewritten?: boolean
  tiles?: { written: number; unchanged: number; removed: number }
  /** Corrected GTFS zips written this run, named `<portolan feed key>.zip`. */
  exported: string[]
  errors: unknown[]
}

/**
 * Extract the machine-readable result from portolan's stdout.
 *
 * Scans for the last line beginning with `RESULT ` — everything above it is
 * human progress output. Returns null when no parseable RESULT line exists
 * (a contract violation on a completed run, but expected when the process
 * died early).
 */
export function parseResultLine(stdout: string): PortolanSyncResult | null {
  const lines = stdout.split('\n')
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i].trim()
    if (!line.startsWith('RESULT ')) continue
    try {
      const parsed = JSON.parse(line.slice('RESULT '.length))
      return {
        changed: parsed.changed ?? [],
        affected: parsed.affected ?? [],
        rebuilt: parsed.rebuilt ?? [],
        groups_rewritten: parsed.groups_rewritten,
        tiles: parsed.tiles,
        exported: parsed.exported ?? [],
        errors: parsed.errors ?? [],
      }
    } catch {
      return null
    }
  }
  return null
}

/** The three sync modes, mapped to portolan subcommands. */
export type SyncMode = 'global' | 'patch' | 'check'

/**
 * Build the argv for `portolan sync <mode>`. Every path is derived from the
 * workspace root, matching the layout `portolan sync` documents:
 * portolan.json + style/ + data/gtfs/ + build/{tiles,export,sync-state.json}.
 * --flag=value form throughout.
 */
export function buildSyncArgs(
  mode: SyncMode,
  workspace: string,
  opts: { feeds?: string; jobs?: string; dryRun?: boolean } = {},
): string[] {
  const args = [
    'sync',
    mode,
    `--config=${join(workspace, 'portolan.json')}`,
    `--data=${join(workspace, 'data/gtfs')}`,
    `--build=${join(workspace, 'build')}`,
    `--tiles=${join(workspace, 'build/tiles')}`,
    `--export-gtfs=${join(workspace, 'build/export')}`,
    `--state=${join(workspace, 'build/sync-state.json')}`,
    `--style-dir=${join(workspace, 'style')}`,
    '--json',
  ]
  if (mode === 'patch') args.push(`--feeds=${opts.feeds ?? ''}`)
  if (opts.jobs) args.push(`--jobs=${opts.jobs}`)
  if (opts.dryRun) args.push('--dry-run')
  return args
}

/**
 * Read the portolan feed key → onestop id map out of a parsed portolan.json.
 * Feed entries without an `onestop` id cannot be joined to barrelman's world
 * and are simply absent from the map (reported downstream as unmapped).
 */
export function keyToOnestopFromConfig(config: unknown): Map<string, string> {
  const map = new Map<string, string>()
  const feeds = (config as { feeds?: Record<string, { onestop?: string }> })?.feeds
  if (!feeds || typeof feeds !== 'object') return map
  for (const [key, entry] of Object.entries(feeds)) {
    if (entry && typeof entry.onestop === 'string' && entry.onestop) {
      map.set(key, entry.onestop)
    }
  }
  return map
}

export interface ExportMapping {
  zip: string
  key: string
  onestop: string
  feedId: string
}

export interface UnmappedExport {
  zip: string
  reason: string
}

/**
 * Map portolan's exported zip names to barrelman feed ids.
 *
 * `<key>.zip` → key → onestop (portolan.json) → feed_id (gtfs_feeds.onestop_id).
 * Rows written before onestop ids were recorded may hold transitland's numeric
 * feed id in onestop_id instead — those simply never match a real `f-…` id, so
 * such feeds surface as unmapped rather than mismapped.
 */
export function mapExportedZips(
  exported: string[],
  keyToOnestop: Map<string, string>,
  onestopToFeedId: Map<string, string>,
): { mapped: ExportMapping[]; unmapped: UnmappedExport[] } {
  const mapped: ExportMapping[] = []
  const unmapped: UnmappedExport[] = []
  for (const zip of exported) {
    const key = zip.replace(/\.zip$/, '')
    const onestop = keyToOnestop.get(key)
    if (!onestop) {
      unmapped.push({ zip, reason: `no onestop id for feed key "${key}" in portolan.json` })
      continue
    }
    const feedId = onestopToFeedId.get(onestop)
    if (!feedId) {
      unmapped.push({ zip, reason: `no barrelman feed with onestop_id "${onestop}" in gtfs_feeds` })
      continue
    }
    mapped.push({ zip, key, onestop, feedId })
  }
  return { mapped, unmapped }
}

// ── CLI ─────────────────────────────────────────────────────────────

if (import.meta.main) {
  const { values: args } = parseArgs({
    options: {
      global: { type: 'boolean', default: false },
      patch: { type: 'string' },
      check: { type: 'boolean', default: false },
      workspace: { type: 'string' },
      'portolan-bin': { type: 'string' },
      'gtfs-dir': { type: 'string' },
      'motis-config': { type: 'string', default: './motis/config.yml' },
      jobs: { type: 'string' },
      'skip-motis': { type: 'boolean', default: false },
      'dry-run': { type: 'boolean', default: false },
    },
  })

  const modes: SyncMode[] = []
  if (args.global) modes.push('global')
  if (args.patch !== undefined) modes.push('patch')
  if (args.check) modes.push('check')
  if (modes.length !== 1) {
    console.error('Error: exactly one of --global | --patch=<feed keys> | --check is required')
    process.exit(1)
  }
  const mode = modes[0]
  if (mode === 'patch' && !args.patch) {
    console.error('Error: --patch requires a comma-separated list of portolan feed keys')
    process.exit(1)
  }

  const workspace = resolve(args.workspace || process.env.PORTOLAN_WORKSPACE || './data/portolan')
  const portolanBin = args['portolan-bin'] || process.env.PORTOLAN_BIN || 'portolan'
  const gtfsDir = args['gtfs-dir'] || process.env.GTFS_DATA_DIR || './data/gtfs'
  const dryRun = args['dry-run']!

  main().catch((err) => {
    console.error('Fatal error:', err)
    process.exit(1)
  })

  async function main() {
    console.log(`\n=== Portolan Sync (${mode}) ===`)
    console.log(`Workspace: ${workspace}`)
    console.log(`Binary:    ${portolanBin}`)
    console.log(`GTFS dir:  ${gtfsDir}`)
    if (dryRun) console.log('DRY RUN — plan only')
    console.log('')

    const configPath = join(workspace, 'portolan.json')
    if (!existsSync(configPath)) {
      console.error(`Error: ${configPath} not found — is the portolan workspace set up?`)
      process.exit(1)
    }

    // ── 1. Run portolan sync, streaming its output ──────────────────
    const syncArgs = buildSyncArgs(mode, workspace, {
      feeds: args.patch,
      jobs: args.jobs,
      dryRun,
    })
    console.log(`$ ${portolanBin} ${syncArgs.join(' ')}\n`)

    let proc
    try {
      proc = Bun.spawn([portolanBin, ...syncArgs], {
        stdout: 'pipe',
        stderr: 'inherit',
        env: process.env as Record<string, string>,
      })
    } catch (err) {
      console.error(`Error: could not exec "${portolanBin}": ${err instanceof Error ? err.message : err}`)
      process.exit(1)
    }

    // Stream stdout through while capturing it — the last line is the RESULT
    // contract, everything above it is live progress the operator should see.
    let captured = ''
    const decoder = new TextDecoder()
    for await (const chunk of proc.stdout) {
      const text = decoder.decode(chunk, { stream: true })
      captured += text
      process.stdout.write(text)
    }
    const portolanExit = await proc.exited

    const result = parseResultLine(captured)
    if (!result) {
      console.error(`\n✗ portolan exited ${portolanExit} without a parseable RESULT line`)
      process.exit(portolanExit || 1)
    }

    console.log(`\n─ Sync result ────────────────────────────────────────`)
    console.log(`  changed:  ${result.changed.join(', ') || '(none)'}`)
    console.log(`  affected: ${result.affected.join(', ') || '(none)'}`)
    console.log(`  rebuilt:  ${result.rebuilt.join(', ') || '(none)'}`)
    if (result.tiles) {
      console.log(`  tiles:    ${result.tiles.written} written, ${result.tiles.unchanged} unchanged, ${result.tiles.removed} removed`)
    }
    console.log(`  exported: ${result.exported.join(', ') || '(none)'}`)
    for (const err of result.errors) {
      console.error(`  ✗ error: ${typeof err === 'string' ? err : JSON.stringify(err)}`)
    }

    if (dryRun) {
      console.log('\nDry run — nothing imported.')
      process.exit(result.errors.length > 0 || portolanExit !== 0 ? 1 : 0)
    }

    // ── 2. Land the corrected zips in barrelman ─────────────────────
    let imported = 0
    if (result.exported.length > 0) {
      // Deferred so `--check` runs that found nothing stay DB-free and cheap.
      const { ensureGtfsSchema } = await import('../src/db')
      const { db } = await import('../src/db')
      const { gtfsFeeds } = await import('../src/schema/gtfs')
      const { sanitizeGtfsZip } = await import('../src/services/gtfs.service')
      const { importFeedFile, injectTransfersTxt, readZipEntry } = await import('./feed-import')
      const { injectFaresV2 } = await import('./inject-fares-v2')

      await ensureGtfsSchema()

      const config = await Bun.file(configPath).json()
      const keyToOnestop = keyToOnestopFromConfig(config)

      const rows = await db.select().from(gtfsFeeds)
      const onestopToFeedId = new Map<string, string>()
      const rowByFeedId = new Map(rows.map(r => [r.feedId, r]))
      for (const row of rows) {
        if (row.onestopId) onestopToFeedId.set(row.onestopId, row.feedId)
      }

      const { mapped, unmapped } = mapExportedZips(result.exported, keyToOnestop, onestopToFeedId)
      for (const u of unmapped) {
        console.log(`  ⚠ skipping ${u.zip}: ${u.reason}`)
      }

      for (const m of mapped) {
        const src = join(workspace, 'build/export', m.zip)
        const dest = join(gtfsDir, `${m.feedId}.zip`)
        console.log(`\n── ${m.key} → ${m.feedId} ──`)

        if (!existsSync(src)) {
          console.error(`  ✗ exported zip missing on disk: ${src}`)
          continue
        }

        // Keep the previously injected walking transfers: portolan exports
        // from the raw feed, so its zip lacks the computed transfers.txt the
        // GTFS importer wrote into the old zip. Stop locations are untouched
        // by geometry correction, so the old transfers remain valid — carry
        // them over instead of recomputing every pair through GraphHopper.
        let carriedTransfers: string | null = null
        if (existsSync(dest)) {
          try {
            const oldZip = await JSZip.loadAsync(await Bun.file(dest).arrayBuffer())
            carriedTransfers = await readZipEntry(oldZip, 'transfers.txt')
          } catch (err) {
            console.error(`  ⚠ could not read previous zip for transfer carryover: ${err}`)
          }
        }

        // Sanitize (strip GTFS-Flex files that crash MOTIS), then land the
        // zip atomically — temp file + rename, so a crash mid-copy never
        // leaves a torn zip where MOTIS or a re-import will read it.
        const raw = await Bun.file(src).arrayBuffer()
        const { buffer, removedFiles } = await sanitizeGtfsZip(raw)
        if (removedFiles.length > 0) {
          console.log(`  ⚠ Stripped ${removedFiles.length} GTFS-Flex files: ${removedFiles.join(', ')}`)
        }
        const tmp = `${dest}.tmp-${process.pid}`
        try {
          writeFileSync(tmp, Buffer.from(buffer))
          renameSync(tmp, dest)
        } catch (err) {
          try { unlinkSync(tmp) } catch {}
          console.error(`  ✗ failed to write ${dest}: ${err}`)
          continue
        }

        // Re-import into PostGIS through the same path the GTFS importer
        // uses. Identity fields come from the existing gtfs_feeds row so
        // recordFeed's upsert preserves name/url/region/rt_urls instead of
        // blanking them.
        const existing = rowByFeedId.get(m.feedId)
        await importFeedFile(dest, {
          feedId: m.feedId,
          onestopId: existing?.onestopId || m.onestop,
          name: existing?.name || m.key,
          url: existing?.url || '',
          region: existing?.region || undefined,
          rtUrls: existing?.rtUrls || undefined,
        })

        // Post-import zip surgery, in the importer's order: transfers
        // injection AFTER the PostGIS import (so gtfs_transfers holds the
        // agency's own transfers, not our computed ones), then Fares v2.
        if (carriedTransfers) {
          try {
            await injectTransfersTxt(dest, carriedTransfers)
            console.log(`  ✓ Carried computed transfers.txt into the corrected zip`)
          } catch (err) {
            console.error(`  ✗ transfer carryover failed: ${err}`)
          }
        } else {
          console.log(`  ⚠ no previous transfers.txt to carry over — run the GTFS importer to compute walking transfers`)
        }
        try {
          const status = await injectFaresV2(dest)
          if (status.startsWith('converted')) console.log(`  ✓ Fares v2: ${status}`)
        } catch (err) {
          console.error(`  ✗ Fares v2 injection failed: ${err}`)
        }

        imported++
      }
    } else {
      console.log('\nNothing exported — no zips to import.')
    }

    // ── 3. MOTIS ────────────────────────────────────────────────────
    let motisFailed = false
    if (imported > 0) {
      // Same as the GTFS importer: regenerate the config from gtfs_feeds…
      const { generateMotisConfig } = await import('../src/services/gtfs.service')
      try {
        const configYaml = await generateMotisConfig()
        const { mkdirSync } = await import('fs')
        mkdirSync(join(args['motis-config']!, '..'), { recursive: true })
        writeFileSync(args['motis-config']!, configYaml)
        console.log(`\n✓ Wrote MOTIS config to ${args['motis-config']}`)
      } catch (err) {
        console.error(`\n✗ Failed to generate MOTIS config: ${err instanceof Error ? err.message : err}`)
      }

      // …then rebuild the dataset. MOTIS has no incremental import — the
      // server only serves the pre-built dataset, so this is a full rebuild
      // even for a single-feed patch (rebuild-motis.sh regenerates the
      // authoritative config itself, with street routing enabled).
      if (args['skip-motis']) {
        console.log('⚠ --skip-motis: corrected schedules will not reach riders until scripts/rebuild-motis.sh runs')
      } else {
        console.log('\n=== Rebuilding MOTIS dataset ===')
        const motis = Bun.spawn(['bash', 'scripts/rebuild-motis.sh'], {
          stdout: 'inherit',
          stderr: 'inherit',
          env: process.env as Record<string, string>,
        })
        const motisExit = await motis.exited
        if (motisExit !== 0) {
          console.error(`✗ MOTIS rebuild exited ${motisExit}`)
          motisFailed = true
        }
      }
    }

    console.log(`\n=== Portolan sync complete: ${imported} feed(s) re-imported ===`)
    const failed = result.errors.length > 0 || portolanExit !== 0 || motisFailed
    process.exit(failed ? 1 : 0)
  }
}
