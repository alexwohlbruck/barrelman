import { customType, index } from 'drizzle-orm/pg-core'
import { sql } from 'drizzle-orm'

export function spatialColumn<T extends string>(
  name: T,
  geometryType:
    | 'POINT'
    | 'LINESTRING'
    | 'POLYGON'
    | 'MULTIPOINT'
    | 'MULTILINESTRING'
    | 'MULTIPOLYGON'
    | 'GEOMETRY' = 'POINT',
  srid: number = 4326,
) {
  return customType<{ data: string; driverData: string }>({
    dataType() {
      return `geometry(${geometryType}, ${srid})`
    },
  })(name)
}

export function vectorColumn<T extends string>(name: T, dimensions: number = 512) {
  return customType<{ data: number[]; driverData: string }>({
    dataType() {
      return `vector(${dimensions})`
    },
    toDriver(value: number[]) {
      return `[${value.join(',')}]`
    },
    fromDriver(value: string) {
      return value
        .replace(/[\[\]]/g, '')
        .split(',')
        .map(Number)
    },
  })(name)
}

export function spatialIndex(indexName: string, column: any) {
  return index(indexName).using('gist', column)
}

export function trigramIndex(indexName: string, column: any) {
  return index(indexName).using('gin', sql`${column} gin_trgm_ops`)
}

export function ginIndex(indexName: string, column: any) {
  return index(indexName).using('gin', column)
}
