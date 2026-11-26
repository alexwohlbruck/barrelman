/**
 * The listing a place resolved to at an external provider (a Google place id,
 * a Foursquare fsq_place_id), so Parchment can fetch it directly instead of
 * searching by name, and so a person can correct a bad match for everyone.
 *
 * Keyed by the OSM element (type + id), never by a geo_places row and with no
 * foreign key to it: every OSM import path rewrites geo_places (a full import
 * drops and recreates it, replication deletes and reinserts the touched rows),
 * and matches have to outlive all of them. See
 * import/provider-matches-reimport.integration.test.ts.
 */
import { connection as sql } from '../db'

export const OSM_TYPES = ['node', 'way', 'relation'] as const
export type OsmType = (typeof OSM_TYPES)[number]

export interface ProviderMatch {
  provider: string
  /** Null records that the place has no listing at this provider. */
  providerPlaceId: string | null
  /** Set by a person. Automatic matching leaves a confirmed match alone. */
  confirmed: boolean
  /** The listing's display name, for providers whose ids can't be looked up for free. */
  label: string | null
  updatedAt: string
}

export interface PlaceRef {
  osmType: OsmType
  osmId: string
}

let schemaReady: Promise<void> | null = null

export function ensureProviderMatchesSchema(): Promise<void> {
  schemaReady ??= sql`
    CREATE TABLE IF NOT EXISTS place_provider_matches (
      osm_type          text NOT NULL CHECK (osm_type IN ('node', 'way', 'relation')),
      osm_id            bigint NOT NULL,
      provider          text NOT NULL,
      provider_place_id text,
      confirmed         boolean NOT NULL DEFAULT false,
      updated_at        timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY (osm_type, osm_id, provider)
    )`
    .then(() => sql`ALTER TABLE place_provider_matches ADD COLUMN IF NOT EXISTS label text`)
    .then(() => undefined)
  return schemaReady
}

interface MatchRow {
  provider: string
  provider_place_id: string | null
  confirmed: boolean
  label: string | null
  updated_at: Date | string
}

function toMatch(row: MatchRow): ProviderMatch {
  return {
    provider: row.provider,
    providerPlaceId: row.provider_place_id,
    confirmed: row.confirmed,
    label: row.label ?? null,
    updatedAt: new Date(row.updated_at).toISOString(),
  }
}

export async function getProviderMatches({ osmType, osmId }: PlaceRef): Promise<ProviderMatch[]> {
  await ensureProviderMatchesSchema()
  const rows = await sql<MatchRow[]>`
    SELECT provider, provider_place_id, confirmed, label, updated_at
    FROM place_provider_matches
    WHERE osm_type = ${osmType} AND osm_id = ${osmId}
    ORDER BY provider`
  return rows.map(toMatch)
}

/**
 * Saves a match. An omitted `label` keeps the stored one while the listing
 * stays the same and clears it when the listing changes; `null` clears it.
 */
export async function setProviderMatch(
  { osmType, osmId }: PlaceRef,
  provider: string,
  match: { providerPlaceId: string | null; confirmed: boolean; label?: string | null },
): Promise<ProviderMatch> {
  await ensureProviderMatchesSchema()
  const labelGiven = match.label !== undefined
  const label = match.label?.trim() || null
  const [row] = await sql<MatchRow[]>`
    INSERT INTO place_provider_matches (osm_type, osm_id, provider, provider_place_id, confirmed, label)
    VALUES (${osmType}, ${osmId}, ${provider}, ${match.providerPlaceId}, ${match.confirmed}, ${label})
    ON CONFLICT (osm_type, osm_id, provider) DO UPDATE
      SET provider_place_id = EXCLUDED.provider_place_id,
          confirmed = EXCLUDED.confirmed,
          label = CASE
            WHEN ${labelGiven} THEN EXCLUDED.label
            WHEN place_provider_matches.provider_place_id IS NOT DISTINCT FROM EXCLUDED.provider_place_id
              THEN place_provider_matches.label
            ELSE NULL
          END,
          updated_at = now()
    RETURNING provider, provider_place_id, confirmed, label, updated_at`
  return toMatch(row)
}

export async function deleteProviderMatch({ osmType, osmId }: PlaceRef, provider: string): Promise<boolean> {
  await ensureProviderMatchesSchema()
  const rows = await sql`
    DELETE FROM place_provider_matches
    WHERE osm_type = ${osmType} AND osm_id = ${osmId} AND provider = ${provider}
    RETURNING 1`
  return rows.length > 0
}
