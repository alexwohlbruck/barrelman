/**
 * Drizzle definitions for the GTFS/transit tables.
 *
 * These are NOT the source of truth for the schema — `ensureGtfsSchema()` in
 * `src/db.ts` runs the DDL, and the import scripts add a few columns and
 * materialized views on top. What these buy is a typed, *parametrized* query
 * surface: every value placed through the builder (or through a `sql` tagged
 * template) becomes a bind parameter, which puts the query on Postgres's
 * extended protocol. That protocol refuses multiple statements outright, so an
 * injected `'; UPDATE …` cannot execute even if a call site is written wrong.
 * The `sql.raw` string-building they replace had no such floor.
 *
 * Keep this file in step with `ensureGtfsSchema()` when a column is added — a
 * definition that has drifted is worse than none, because the builder will
 * happily generate SQL for a column that is not there.
 */
import {
  pgTable,
  text,
  char,
  bigint,
  integer,
  doublePrecision,
  jsonb,
  serial,
  timestamp,
  index,
  uniqueIndex,
} from 'drizzle-orm/pg-core'
import { spatialColumn, spatialIndex } from './spatial-helpers'

/**
 * Which kind of GTFS-RT feed a URL points at.
 *
 * Recorded rather than inferred from the URL. An agency's realtime feeds are
 * discovered through its operator, which lists *all* of them — MTA New York
 * City Transit publishes ten, covering subway line groups and buses — so the
 * subway's feed row carries the bus URLs too. Guessing the kind from the URL
 * string picked the bus vehicle feed for subway trains, and MTA's subway URLs
 * (`nyct%2Fgtfs-ace`) name no kind at all.
 */
export type RtUrlType = 'tripUpdates' | 'vehiclePositions' | 'alerts'

/** One entry of a feed's GTFS-RT URL list, as stored in `gtfs_feeds.rt_urls`. */
export interface RtUrlEntry {
  url: string
  headers?: Record<string, string>
  /** Absent on rows written before discovery recorded it. */
  type?: RtUrlType
}

// ── GTFS Feeds ──────────────────────────────────────────────────────

export const gtfsFeeds = pgTable(
  'gtfs_feeds',
  {
    id: serial('id').primaryKey(),
    feedId: text('feed_id').notNull().unique(),
    onestopId: text('onestop_id'),
    name: text('name'),
    url: text('url'),
    region: text('region'),
    stopCount: integer('stop_count').default(0),
    routeCount: integer('route_count').default(0),
    rtUrls: jsonb('rt_urls').$type<RtUrlEntry[] | null>(),
    importedAt: timestamp('imported_at', { withTimezone: true }).defaultNow(),
  },
)

export type GtfsFeed = typeof gtfsFeeds.$inferSelect
export type NewGtfsFeed = typeof gtfsFeeds.$inferInsert

// ── GTFS Stops ──────────────────────────────────────────────────────

export const gtfsStops = pgTable(
  'gtfs_stops',
  {
    id: serial('id').primaryKey(),
    stopId: text('stop_id').notNull(),
    feedId: text('feed_id').notNull(),
    stopName: text('stop_name'),
    stopCode: text('stop_code'),
    stopLat: doublePrecision('stop_lat').notNull(),
    stopLon: doublePrecision('stop_lon').notNull(),
    locationType: integer('location_type').default(0),
    parentStation: text('parent_station'),
    wheelchairBoarding: integer('wheelchair_boarding').default(0),
    platformCode: text('platform_code'),
    geom: spatialColumn('geom', 'POINT'),
  },
  (table) => [
    uniqueIndex('gtfs_stops_feed_stop_idx').on(table.feedId, table.stopId),
    spatialIndex('gtfs_stops_geom_idx', table.geom),
    index('gtfs_stops_feed_id_idx').on(table.feedId),
    index('gtfs_stops_parent_idx').on(table.parentStation),
    index('gtfs_stops_name_idx').on(table.stopName),
  ],
)

export type GtfsStop = typeof gtfsStops.$inferSelect
export type NewGtfsStop = typeof gtfsStops.$inferInsert

// ── GTFS Routes ─────────────────────────────────────────────────────

export const gtfsRoutes = pgTable(
  'gtfs_routes',
  {
    id: serial('id').primaryKey(),
    routeId: text('route_id').notNull(),
    feedId: text('feed_id').notNull(),
    agencyId: text('agency_id'),
    agencyName: text('agency_name'),
    routeShortName: text('route_short_name'),
    routeLongName: text('route_long_name'),
    routeType: integer('route_type').notNull(),
    routeColor: text('route_color'),
    routeTextColor: text('route_text_color'),
    routeUrl: text('route_url'),
    /** Most common shape for the route, derived from trips.txt during import. */
    shapeId: text('shape_id'),
    /** 0=unknown, 1=at least one bike-allowed trip, 2=all trips allow bikes. */
    bikesAllowed: integer('bikes_allowed').default(0),
  },
  (table) => [
    uniqueIndex('gtfs_routes_feed_route_idx').on(table.feedId, table.routeId),
    index('gtfs_routes_feed_id_idx').on(table.feedId),
  ],
)

export type GtfsRoute = typeof gtfsRoutes.$inferSelect
export type NewGtfsRoute = typeof gtfsRoutes.$inferInsert

// ── GTFS Stop–Route join ────────────────────────────────────────────

export const gtfsStopRoutes = pgTable(
  'gtfs_stop_routes',
  {
    id: serial('id').primaryKey(),
    feedId: text('feed_id').notNull(),
    stopId: text('stop_id').notNull(),
    routeId: text('route_id').notNull(),
  },
  (table) => [
    uniqueIndex('gtfs_stop_routes_uniq_idx').on(table.feedId, table.stopId, table.routeId),
    index('gtfs_stop_routes_stop_idx').on(table.feedId, table.stopId),
    index('gtfs_stop_routes_route_idx').on(table.feedId, table.routeId),
  ],
)

export type GtfsStopRoute = typeof gtfsStopRoutes.$inferSelect
export type NewGtfsStopRoute = typeof gtfsStopRoutes.$inferInsert

// ── GTFS Trip patterns ──────────────────────────────────────────────
// One row per distinct (route, direction, ordered station sequence). Stop ids
// are normalised to the parent station and the sequence stored comma-bounded so
// a leg's board→alight run matches as a substring (see db.ts for the rationale).

export const gtfsTripPatterns = pgTable(
  'gtfs_trip_patterns',
  {
    id: serial('id').primaryKey(),
    feedId: text('feed_id').notNull(),
    routeId: text('route_id').notNull(),
    directionId: integer('direction_id'),
    stopSeq: text('stop_seq').notNull(),
  },
  (table) => [index('gtfs_trip_patterns_feed_idx').on(table.feedId)],
)

export type GtfsTripPattern = typeof gtfsTripPatterns.$inferSelect
export type NewGtfsTripPattern = typeof gtfsTripPatterns.$inferInsert

// ── GTFS Transfers ──────────────────────────────────────────────────
// transfers.txt: the agency's own definition of which stations form one
// complex (Times Sq 1/2/3 <-> N/Q/R/W) and the minimum connection times.

export const gtfsTransfers = pgTable(
  'gtfs_transfers',
  {
    id: serial('id').primaryKey(),
    feedId: text('feed_id').notNull(),
    fromStopId: text('from_stop_id').notNull(),
    toStopId: text('to_stop_id').notNull(),
    transferType: integer('transfer_type').default(0),
    minTransferTime: integer('min_transfer_time'),
  },
  (table) => [
    uniqueIndex('gtfs_transfers_uniq_idx').on(table.feedId, table.fromStopId, table.toStopId),
    index('gtfs_transfers_from_idx').on(table.feedId, table.fromStopId),
    index('gtfs_transfers_to_idx').on(table.feedId, table.toStopId),
  ],
)

export type GtfsTransfer = typeof gtfsTransfers.$inferSelect
export type NewGtfsTransfer = typeof gtfsTransfers.$inferInsert

// ── GTFS Shapes ─────────────────────────────────────────────────────
//
// Deliberately omits the `geom` column that exists on long-lived databases.
// `ensureGtfsSchema()` does not create it, nothing in this repo populates or
// reads it, and it appears to be left over from an ad-hoc migration. Listing it
// here is not free: drizzle names every column of the table in an INSERT, so a
// definition claiming `geom` makes importShapes fail outright on any instance
// that has never had it — which is every fresh install.

export const gtfsShapes = pgTable(
  'gtfs_shapes',
  {
    id: serial('id').primaryKey(),
    feedId: text('feed_id').notNull(),
    shapeId: text('shape_id').notNull(),
    coordinates: jsonb('coordinates').$type<[number, number][]>().notNull(),
  },
  (table) => [
    uniqueIndex('gtfs_shapes_feed_shape_idx').on(table.feedId, table.shapeId),
    index('gtfs_shapes_feed_id_idx').on(table.feedId),
  ],
)

export type GtfsShape = typeof gtfsShapes.$inferSelect
export type NewGtfsShape = typeof gtfsShapes.$inferInsert

// ── OSM-linked station infrastructure ───────────────────────────────
// The next two are MATERIALIZED VIEWS, built by the station-linking import,
// not tables. Modelled as pgTable because the only thing barrelman ever does
// with them is SELECT — treat them as read-only and never insert.

export const stationEntrances = pgTable(
  'station_entrances',
  {
    feedId: text('feed_id'),
    stopId: text('stop_id'),
    osmEntranceId: text('osm_entrance_id'),
    entranceName: text('entrance_name'),
    entranceDescription: text('entrance_description'),
    entranceWheelchair: text('entrance_wheelchair'),
    entranceLevel: text('entrance_level'),
    railwayType: text('railway_type'),
    entranceGeom: spatialColumn('entrance_geom', 'POINT'),
    distanceM: doublePrecision('distance_m'),
  },
  (table) => [index('station_entrances_stop_idx').on(table.feedId, table.stopId)],
)

export const stationBuildings = pgTable(
  'station_buildings',
  {
    feedId: text('feed_id'),
    stopId: text('stop_id'),
    osmBuildingId: text('osm_building_id'),
    buildingName: text('building_name'),
    stationType: text('station_type'),
    buildingGeom: spatialColumn('building_geom', 'GEOMETRY'),
  },
  (table) => [index('station_buildings_stop_idx').on(table.feedId, table.stopId)],
)

// ── OSM stop_area relations ─────────────────────────────────────────
// Loaded by scripts/import-stop-areas.sh; absent on instances that have not
// run it, which is why station.service.ts probes for it before use.

export const stopAreaMembers = pgTable(
  'stop_area_members',
  {
    relationId: bigint('relation_id', { mode: 'number' }).notNull(),
    relationName: text('relation_name'),
    /** OSM element type: 'N', 'W' or 'R'. */
    memberType: char('member_type', { length: 1 }).notNull(),
    memberRef: bigint('member_ref', { mode: 'number' }).notNull(),
    memberRole: text('member_role'),
  },
  (table) => [
    index('stop_area_members_ref_idx').on(table.memberType, table.memberRef),
    index('stop_area_members_rel_idx').on(table.relationId),
  ],
)
