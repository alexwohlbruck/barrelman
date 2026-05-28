import { db } from '../db'
import { sql } from 'drizzle-orm'
import {
  checkMotisHealth as _checkMotisHealth,
  type FetchFn,
} from './transit.service'

export interface HealthResult {
  status: 'ok' | 'degraded' | 'error'
  database: 'connected' | 'disconnected'
  motis?: 'ok' | 'unavailable'
}

export async function checkHealth(
  deps: { checkMotisHealth?: typeof _checkMotisHealth } = {},
): Promise<HealthResult> {
  const checkMotisHealth = deps.checkMotisHealth || _checkMotisHealth

  let database: HealthResult['database'] = 'disconnected'
  try {
    await db.execute(sql`SELECT 1`)
    database = 'connected'
  } catch {
    // database stays disconnected
  }

  const motisResult = await checkMotisHealth()
  const motis = motisResult.status

  // Overall status: error if DB down, degraded if MOTIS down, ok if both up
  let status: HealthResult['status'] = 'ok'
  if (database === 'disconnected') {
    status = 'error'
  } else if (motis === 'unavailable') {
    status = 'degraded'
  }

  return { status, database, motis }
}
