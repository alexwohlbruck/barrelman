import Elysia from 'elysia'
import { authMiddleware } from '../middleware/auth'
import {
  runPostImport as _runPostImport,
  runResolveParentContext as _runResolveParentContext,
  runResolveParentContextIncremental as _runResolveParentContextIncremental,
  runRebuildTsvectors as _runRebuildTsvectors,
  runFullMigration as _runFullMigration,
  getMigrationStatus as _getMigrationStatus,
} from '../services/admin.service'

export function createAdminRoutes(deps = {
  runPostImport: _runPostImport,
  runResolveParentContext: _runResolveParentContext,
  runResolveParentContextIncremental: _runResolveParentContextIncremental,
  runRebuildTsvectors: _runRebuildTsvectors,
  runFullMigration: _runFullMigration,
  getMigrationStatus: _getMigrationStatus,
}) {
  return new Elysia({ prefix: '/admin' })
    .use(authMiddleware)
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
        description: 'Run post-import → resolve parent context → rebuild tsvectors. This may take several minutes on large datasets.',
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
}

export const adminRoutes = createAdminRoutes()
