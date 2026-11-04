import Elysia, { t } from 'elysia'
import { adminAuthHandler } from '../middleware/auth'
import {
  runPostImport as _runPostImport,
  runResolveParentContext as _runResolveParentContext,
  runResolveParentContextIncremental as _runResolveParentContextIncremental,
  runRebuildTsvectors as _runRebuildTsvectors,
  runGenerateCodes as _runGenerateCodes,
  runGenerateAbbreviations as _runGenerateAbbreviations,
  runFullMigration as _runFullMigration,
  getMigrationStatus as _getMigrationStatus,
} from '../services/admin.service'
import { discoverRtUrls as _discoverRtUrls } from '../services/gtfs.service'
import { serverError } from '../lib/errors'

const defaultDeps = {
  runPostImport: _runPostImport,
  runResolveParentContext: _runResolveParentContext,
  runResolveParentContextIncremental: _runResolveParentContextIncremental,
  runRebuildTsvectors: _runRebuildTsvectors,
  runGenerateCodes: _runGenerateCodes,
  runGenerateAbbreviations: _runGenerateAbbreviations,
  runFullMigration: _runFullMigration,
  getMigrationStatus: _getMigrationStatus,
  discoverRtUrls: _discoverRtUrls,
  /** Injectable for tests only, as a plain function: see admin.test.ts. */
  guard: adminAuthHandler as typeof adminAuthHandler,
}

export function createAdminRoutes(overrides: Partial<typeof defaultDeps> = {}) {
  const deps = { ...defaultDeps, ...overrides }
  // Guard attached directly (not via `.use(plugin)`): Elysia scopes a plugin's
  // lifecycle hooks to that plugin instance, so `.use(authMiddleware)` does NOT
  // protect the sibling routes declared below — they'd be publicly reachable.
  //
  // These run hours of destructive DDL over ~20M rows, so they take the ADMIN
  // guard, not the read-API one. They previously used `authHandler`, which
  // accepts the shared BARRELMAN_API_KEY — the service credential Parchment
  // also holds — while rejecting signed-in administrators outright. Exactly
  // backwards, and contrary to the rule in CLAUDE.md.
  return new Elysia({ prefix: '/admin' })
    .onBeforeHandle(deps.guard)
    .get('/migration/status', () => deps.getMigrationStatus(), {
      detail: {
        summary: 'Migration status',
        description: 'Check if parent_context is populated and coverage stats.',
        tags: ['Admin'],
      },
    })
    .post('/migration/run', () => deps.runFullMigration(), {
      detail: {
        summary: 'Run full migration',
        description: 'Run post-import → codes → abbreviations → parent context → tsvectors. This may take several minutes on large datasets.',
        tags: ['Admin'],
      },
    })
    .post('/migration/post-import', () => deps.runPostImport(), {
      detail: {
        summary: 'Run post-import SQL',
        description: 'Add columns, extract structured fields, build indexes.',
        tags: ['Admin'],
      },
    })
    .post('/migration/generate-codes', () => deps.runGenerateCodes(), {
      detail: {
        summary: 'Generate codes',
        description: 'Extract IATA, ICAO, ref, short_name, abbreviation, alt_name codes from OSM tags.',
        tags: ['Admin'],
      },
    })
    .post('/migration/generate-abbreviations', () => deps.runGenerateAbbreviations(), {
      detail: {
        summary: 'Generate abbreviations',
        description: 'Generate first-letter abbreviations for multi-word Latin-script names.',
        tags: ['Admin'],
      },
    })
    .post('/migration/resolve-parent-context', () => deps.runResolveParentContext(), {
      detail: {
        summary: 'Resolve parent context (full)',
        description: 'Spatial join to populate parent_context for all named places.',
        tags: ['Admin'],
      },
    })
    .post('/migration/resolve-parent-context-incremental', () => deps.runResolveParentContextIncremental(), {
      detail: {
        summary: 'Resolve parent context (incremental)',
        description: 'Resolve parent_context for new/changed POIs and cascade for changed boundaries.',
        tags: ['Admin'],
      },
    })
    .post('/migration/rebuild-tsvectors', () => deps.runRebuildTsvectors(), {
      detail: {
        summary: 'Rebuild tsvectors',
        description: 'Rebuild full-text search vectors for all named places (includes parent_context).',
        tags: ['Admin'],
      },
    })
    // Admin, not the transit group: it spends the operator's
    // TRANSITLAND_API_KEY and writes gtfs_feeds. The "Backfill GTFS-RT URLs"
    // script does the same from the console.
    .post('/transit/discover-rt-urls', async ({ body, request, set }) => {
      try {
        const { checked, updated, errors } = await deps.discoverRtUrls(body.feedId || undefined)
        return { checked, updated, errors }
      } catch (err) {
        return serverError(request, set, err, 'Failed to discover RT URLs')
      }
    }, {
      body: t.Object({ feedId: t.Optional(t.String({ maxLength: 200 })) }),
      detail: {
        summary: 'Discover GTFS-RT feed URLs',
        description:
          'Queries Transitland to discover GTFS-RT feed URLs (vehicle positions, ' +
          'trip updates, alerts) for existing feeds. If feedId is provided, only ' +
          'discovers for that feed; otherwise discovers for all feeds missing RT URLs.',
        tags: ['Admin'],
      },
    })
}

export const adminRoutes = createAdminRoutes()
