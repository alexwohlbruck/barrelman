import { db } from '../db'
import { sql } from 'drizzle-orm'

export interface HealthResult {
  status: 'ok' | 'error'
  database: 'connected' | 'disconnected'
}

export async function checkHealth(): Promise<HealthResult> {
  try {
    await db.execute(sql`SELECT 1`)
    return { status: 'ok', database: 'connected' }
  } catch {
    return { status: 'error', database: 'disconnected' }
  }
}
