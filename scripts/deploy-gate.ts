#!/usr/bin/env bun
/**
 * Is it safe to restart this container right now?
 *
 * A console job is an import: OSM extracts, a GTFS refresh, a MOTIS rebuild,
 * a portolan sync. They run for minutes to hours, and restarting the
 * container mid-run does not pause the work — it abandons it, usually
 * leaving a half-written dataset the next run has to repair. An updater
 * that pulls a new image the moment CI publishes one will eventually do
 * exactly that, and the only reason it has not yet is luck about when
 * releases land.
 *
 * So the updater asks first. Two callers, one answer:
 *
 *   - Watchtower runs this as a pre-update lifecycle hook (label
 *     `com.centurylinklabs.watchtower.lifecycle.pre-update`) and reads the
 *     EXIT CODE: 0 proceeds, 75 (EX_TEMPFAIL) skips this container and
 *     retries on the next poll. Any other code aborts the whole update, so
 *     failures here must never be loud — see the catch.
 *   - The release workflow's deploy job runs it over SSH in a wait loop.
 *
 * Not "safe to stop the process" in general: it is specifically about the
 * ops job queue, which is the only work here that cannot be resumed.
 *
 * The API is gated as well as the worker because it is not merely a
 * bystander during a job: `kind:'internal'` jobs run in this process, and
 * scripts/rebuild-motis.sh reaches into this container (`docker exec
 * barrelman`) to generate the MOTIS config mid-run.
 */
import { jobStats } from '../src/services/ops-job-store'
import { connection } from '../src/db'

/** sysexits.h EX_TEMPFAIL — watchtower's "skip this one, ask again later". */
const EX_TEMPFAIL = 75

async function main(): Promise<number> {
  try {
    const { running } = await jobStats()
    if (running > 0) {
      console.error(`deploy-gate: ${running} job(s) running — deferring the update`)
      return EX_TEMPFAIL
    }
    console.log('deploy-gate: no jobs running — clear to update')
    return 0
  } catch (err) {
    // Fails CLOSED, like the psql gates on barrelman-db and barrelman-ops:
    // an unanswerable question is not a "no jobs running". 75 defers to the
    // next poll rather than aborting Watchtower's whole run, and a gate
    // stuck answering 75 shows up as a failed deploy job rather than as
    // silence.
    console.error(
      `deploy-gate: could not read the job queue (${err instanceof Error ? err.message : err}) — deferring the update`,
    )
    return EX_TEMPFAIL
  }
}

const code = await main()
await connection.end({ timeout: 5 }).catch(() => {})
process.exit(code)
