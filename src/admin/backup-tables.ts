/**
 * The tables `scripts/backup-db.sh` dumps: everything an import cannot bring
 * back.
 *
 * OSM, GTFS, GBFS and everything derived from them (geo_places, planet_osm_*,
 * gtfs_*, gbfs_*, road markings, bridge decks, the search lexicon, stop areas,
 * Portolan links) are rebuilt by re-running the importers, and on a country
 * import they are tens of gigabytes. The tables below are small and cannot be
 * regenerated from anything: lose them and every account, key, credit balance
 * and schedule is gone.
 *
 * Also not backed up, though they are not import outputs:
 *   - boundary_catalog: re-fetched from Geofabrik by "Fetch Boundary Catalog".
 *   - brand_logos: a Wikidata cache that refills itself in the background.
 *   - search_enrichment_state, schema_migrations: markers describing the data
 *     currently imported. Restored next to a different import they would claim
 *     work was done that was not.
 *
 * Landmarks are re-importable from their source repositories, but they are
 * kept because `landmark_sources` records which release each source last
 * brought in, and restoring that pointer without the rows it describes would
 * make the next import skip them as unchanged. They are small.
 *
 * backup-tables.test.ts checks this list against the CREATE TABLE
 * statements in src/, so a new accounts_*, ops_* or import_* table that is
 * missing here fails the build unless it is in NOT_BACKED_UP below.
 *
 * Run as a CLI to print one name per line; that is how the backup script reads
 * it, so this file stays the only copy of the list:
 *
 *   bun run src/admin/backup-tables.ts
 */
export const BACKUP_TABLES = [
  // Accounts, auth and billing (src/services/accounts.service.ts). Terms
  // acceptance and the Polar customer link are columns on accounts_users.
  'accounts_users',
  'accounts_sessions',
  'accounts_auth_tokens',
  'accounts_passkeys',
  'accounts_oauth',
  'accounts_api_keys',
  'accounts_usage',
  'accounts_request_metrics',
  'accounts_credit_ledger',
  'accounts_overage_reports',
  'accounts_moderation_log',
  'accounts_abuse_signals',
  'accounts_signup_attempts',
  // Webhook ids already applied; without them a restore re-applies retried deliveries.
  'accounts_billing_webhook_events',

  // Operator state: jobs, their logs, schedules and runtime history.
  'ops_jobs',
  'ops_job_logs',
  'ops_schedules',
  'script_runs',

  // Import configuration and cursors.
  'import_regions',
  'osm_replication_state',

  // Landmark placements and which release each source last imported.
  'landmark_models',
  'landmarks',
  'landmark_sources',
] as const

/**
 * Tables under a stateful prefix that are deliberately left out, with why. The
 * test treats these as covered; anything else under the prefix must be backed up.
 */
export const NOT_BACKED_UP: Record<string, string> = {
  // Penalty-box state lives at most half an hour. A restore would re-box
  // callers for offences long past.
  accounts_throttle_penalties: 'ephemeral throttle state',
}

if (import.meta.main) {
  console.log(BACKUP_TABLES.join('\n'))
}
