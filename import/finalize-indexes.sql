-- Build the query-serving indexes over settled data.
--
-- Run as the LAST step of a full import (import-osm.sh), after every
-- enrichment UPDATE has finished, and by update-osm.sh after post-import.sql
-- (where each is an IF NOT EXISTS no-op on an already-indexed database).
--
-- These deliberately do NOT live in post-import.sql. The enrichment steps
-- rewrite most named rows several times; building these first meant every one
-- of those rewrites paid maintenance on every GIN/GiST here, and the ts index
-- was populated through tens of millions of incremental inserts instead of
-- one bulk build. On a fresh import the table is only queryable by the API
-- once the tsvector exists anyway, so nothing user-facing is lost by
-- indexing at the end.
--
-- Session-local resources for the builds; harmless when run by update-osm.sh
-- where everything already exists. maintenance workers deliberately NOT
-- forced higher here — the server-level setting governs (see
-- BARRELMAN_DB_MAINT_WORKERS in docker-compose.yml).

CREATE INDEX IF NOT EXISTS geo_places_tags_idx ON geo_places USING GIN(tags jsonb_path_ops);
CREATE INDEX IF NOT EXISTS geo_places_geom_type_idx ON geo_places(geom_type);

CREATE INDEX IF NOT EXISTS geo_places_name_trgm_gist_idx ON geo_places USING GIST(name gist_trgm_ops) WHERE name IS NOT NULL;
CREATE INDEX IF NOT EXISTS geo_places_categories_idx ON geo_places USING GIN(categories) WHERE categories != '{}';
CREATE INDEX IF NOT EXISTS geo_places_ts_idx ON geo_places USING GIN(ts) WHERE ts IS NOT NULL;
CREATE INDEX IF NOT EXISTS geo_places_admin_level_idx ON geo_places(admin_level) WHERE admin_level IS NOT NULL;

-- Search layer (codes and abbreviation lookups)
CREATE INDEX IF NOT EXISTS geo_places_codes_idx ON geo_places USING GIN(codes) WHERE codes IS NOT NULL;
CREATE INDEX IF NOT EXISTS geo_places_name_abbrev_idx ON geo_places(name_abbrev) WHERE name_abbrev IS NOT NULL;
CREATE INDEX IF NOT EXISTS geo_places_osm_type_idx ON geo_places(osm_type);

-- Semantic search (HNSW, approximate nearest-neighbor). Partial on a column
-- the optional embed-places step fills later, so it is empty and instant here.
CREATE INDEX IF NOT EXISTS geo_places_embedding_hnsw_idx ON geo_places USING hnsw (embedding vector_cosine_ops) WHERE embedding IS NOT NULL;

-- ── Bicycle infrastructure ──────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS bicycle_ways_geom_idx ON bicycle_ways USING GIST(geom);
CREATE INDEX IF NOT EXISTS bicycle_ways_infra_type_idx ON bicycle_ways(infra_type);
CREATE INDEX IF NOT EXISTS bicycle_routes_geom_idx ON bicycle_routes USING GIST(geom);
CREATE INDEX IF NOT EXISTS bicycle_routes_network_idx ON bicycle_routes(network);
