import {
  pgTable,
  text,
  integer,
  doublePrecision,
  serial,
  timestamp,
  index,
  uniqueIndex,
} from 'drizzle-orm/pg-core'
import { spatialColumn, spatialIndex } from './spatial-helpers'

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
    // Regular weekday-daytime trip count for this stop×route (see db.ts).
    // Back-compat alias of tripsWeekdayDay.
    weekdayTrips: integer('weekday_trips'),
    // Representative-day service counts (see resolveServiceCalendar in
    // gtfs.service.ts and the column comments in db.ts).
    tripsWeekdayDay: integer('trips_weekday_day'),
    tripsWeekdayAny: integer('trips_weekday_any'),
    tripsWeekendDay: integer('trips_weekend_day'),
    tripsAny: integer('trips_any'),
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
