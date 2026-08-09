/**
 * Integration tests for the schedule store against the real database.
 *
 * The claim protocol is the part worth guarding. A schedule that fires twice
 * starts two concurrent imports, and the only thing preventing that is a
 * compare-and-set on `next_run_at` — a guarantee no unit test with a mocked
 * `sql` can actually demonstrate.
 *
 * Rows are created and deleted here, but only ones this file made: every
 * schedule it touches is created by the test and removed in afterAll.
 *
 * Run: bun run test:integration
 */

import { describe, test, expect, afterAll } from 'bun:test'

const DATABASE_URL = process.env.BARRELMAN_INTEGRATION_TESTS ? process.env.DATABASE_URL : undefined

let svc: typeof import('./schedules.service') | null = null
let sql: typeof import('../db').connection | null = null

if (DATABASE_URL) {
  const dbMod = await import('../db')
  sql = dbMod.connection
  svc = await import('./schedules.service')
  await svc.ensureSchedulesSchema()
}

const created: string[] = []

/** A schedule whose next fire is already in the past, so it reads as due. */
async function makeDue(scriptId = 'gbfs-import') {
  const s = await svc!.createSchedule({ scriptId, cron: '0 3 * * *', timezone: 'UTC', enabled: true })
  created.push(s.id)
  await sql!`UPDATE ops_schedules SET next_run_at = ${Date.now() - 1000} WHERE id = ${s.id}`
  return (await svc!.getSchedule(s.id))!
}

afterAll(async () => {
  if (!sql || !created.length) return
  await sql`DELETE FROM ops_schedules WHERE id = ANY(${created})`
})

describe.skipIf(!DATABASE_URL)('schedules.service', () => {
  test('rejects a script that is not in the manifest', async () => {
    await expect(svc!.createSchedule({ scriptId: 'no-such-script', cron: '0 3 * * *' })).rejects.toThrow(
      /Unknown script/,
    )
  })

  test('rejects a malformed cron expression', async () => {
    await expect(svc!.createSchedule({ scriptId: 'gbfs-import', cron: '0 99 * * *' })).rejects.toThrow(/Invalid cron/)
  })

  test('an enabled schedule gets a future next_run_at; a disabled one does not', async () => {
    const on = await svc!.createSchedule({ scriptId: 'gbfs-import', cron: '0 3 * * *', enabled: true })
    created.push(on.id)
    expect(on.nextRunAt).toBeGreaterThan(Date.now())

    const off = await svc!.createSchedule({ scriptId: 'gbfs-import', cron: '0 3 * * *', enabled: false })
    created.push(off.id)
    expect(off.nextRunAt).toBeUndefined()
  })

  test('toggling enabled off clears the next fire time, and back on restores it', async () => {
    const s = await svc!.createSchedule({ scriptId: 'gbfs-import', cron: '0 3 * * *', enabled: true })
    created.push(s.id)

    expect((await svc!.setScheduleEnabled(s.id, false))?.nextRunAt).toBeUndefined()
    expect((await svc!.setScheduleEnabled(s.id, true))?.nextRunAt).toBeGreaterThan(Date.now())
  })

  test('a due schedule is claimed exactly once', async () => {
    const due = await makeDue()
    expect(await svc!.dueSchedules()).toContainEqual(expect.objectContaining({ id: due.id }))

    // Both callers read the same row — only the first compare-and-set wins.
    expect(await svc!.claimSchedule(due)).toBe(true)
    expect(await svc!.claimSchedule(due)).toBe(false)
  })

  test('claiming rolls the next fire into the future, not onto the missed slot', async () => {
    // The stored time is a week stale, as it would be after an outage. The next
    // fire must be the next occurrence from now — replaying seven missed nights
    // would mean seven imports on restart.
    const s = await svc!.createSchedule({ scriptId: 'gbfs-import', cron: '0 3 * * *', timezone: 'UTC', enabled: true })
    created.push(s.id)
    const stale = Date.now() - 7 * 24 * 60 * 60 * 1000
    await sql!`UPDATE ops_schedules SET next_run_at = ${stale} WHERE id = ${s.id}`

    const due = (await svc!.getSchedule(s.id))!
    expect(await svc!.claimSchedule(due)).toBe(true)

    const after = (await svc!.getSchedule(s.id))!
    expect(after.nextRunAt).toBeGreaterThan(Date.now())
    expect(after.nextRunAt! - Date.now()).toBeLessThan(25 * 60 * 60 * 1000)
  })

  test('a disabled schedule is never due, however stale its stored time', async () => {
    const s = await svc!.createSchedule({ scriptId: 'gbfs-import', cron: '0 3 * * *', enabled: false })
    created.push(s.id)
    await sql!`UPDATE ops_schedules SET next_run_at = ${Date.now() - 1000} WHERE id = ${s.id}`

    const due = await svc!.dueSchedules()
    expect(due.find((d) => d.id === s.id)).toBeUndefined()
  })

  test('recordFire stores the job it produced, or why it produced none', async () => {
    const s = await svc!.createSchedule({ scriptId: 'gbfs-import', cron: '0 3 * * *', enabled: true })
    created.push(s.id)

    await svc!.recordFire(s.id, { skipReason: 'Previous run still in progress — skipped' })
    expect((await svc!.getSchedule(s.id))?.lastSkipReason).toMatch(/still in progress/)

    // A later successful fire must clear the stale reason, or the console would
    // keep warning about a skip that has since resolved.
    const jobId = crypto.randomUUID()
    await svc!.recordFire(s.id, { jobId })
    const after = await svc!.getSchedule(s.id)
    expect(after?.lastJobId).toBe(jobId)
    expect(after?.lastSkipReason).toBeUndefined()
  })
})
