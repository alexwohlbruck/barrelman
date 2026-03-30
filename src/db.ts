import postgres from 'postgres'
import { drizzle } from 'drizzle-orm/postgres-js'

export const dbUrl = process.env.DATABASE_URL || 'postgresql://barrelman:barrelman@localhost:5434/barrelman'

export const connection = postgres(dbUrl)
export const db = drizzle(connection)
