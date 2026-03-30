import {
  pgTable,
  text,
  integer,
  bigint,
  jsonb,
  real,
  timestamp,
  index,
} from 'drizzle-orm/pg-core'
import { sql } from 'drizzle-orm'
import { spatialColumn, vectorColumn, spatialIndex, trigramIndex, ginIndex } from './spatial-helpers'

export const geoPlaces = pgTable(
  'geo_places',
  {
    // OSM identity — "node/123456", "way/789", "relation/42"
    id: text('id').primaryKey(),
    osmType: text('osm_type').notNull(),     // 'node', 'way', 'relation'
    osmId: bigint('osm_id', { mode: 'number' }).notNull(),

    // Core attributes
    name: text('name'),
    nameAbbrev: text('name_abbrev'),
    names: text('names').array(),
    tags: jsonb('tags').notNull(),            // ALL raw OSM tags
    categories: text('categories').array(),   // derived: ["amenity/restaurant"]

    // Geometry — two columns
    centroid: spatialColumn('centroid', 'POINT'),   // always a Point
    geom: spatialColumn('geom', 'GEOMETRY'),        // real shape (point, polygon, linestring, multi*)
    geomType: text('geom_type').notNull(),           // 'point', 'line', 'area'

    // Admin boundary info
    adminLevel: integer('admin_level'),
    areaM2: real('area_m2'),

    // Structured data extracted from OSM tags
    address: jsonb('address'),
    hours: text('hours'),
    phones: text('phones').array(),
    websites: text('websites').array(),

    // Search
    embedding: vectorColumn('embedding', 512),
    // ts column is managed via post-import SQL, not directly by Drizzle

    updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow(),
  },
  (table) => [
    // Universal indexes
    index('geo_places_osm_lookup_idx').using('btree', table.osmType, table.osmId),
    spatialIndex('geo_places_centroid_idx', table.centroid),
    spatialIndex('geo_places_geom_idx', table.geom),
    ginIndex('geo_places_tags_idx', sql`${table.tags} jsonb_path_ops`),
    index('geo_places_geom_type_idx').on(table.geomType),
    // Partial indexes for search (only named POIs)
    trigramIndex('geo_places_name_trgm_idx', table.name),
    ginIndex('geo_places_categories_idx', table.categories),
  ],
)

export type GeoPlace = typeof geoPlaces.$inferSelect
export type NewGeoPlace = typeof geoPlaces.$inferInsert
