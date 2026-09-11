#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Full OSM Import
# =============================================================================
#
# Complete import pipeline: downloads PBF, imports with osm2pgsql, and runs ALL
# post-processing steps. After this script finishes, the database is fully
# ready for search (tsvectors, codes, abbreviations, parent context, indexes).
#
# Runs inside the barrelman-db container (needs osm2pgsql + psql).
# Called from the host via scripts/run-import.sh, or directly:
#
#   docker exec -e DATABASE_URL=... barrelman-db bash /app/scripts/import-osm.sh
#
# Environment variables:
#   DATABASE_URL          - PostgreSQL connection string (required)
#   GEOFABRIK_URL         - PBF download URL (default: NC extract)
#   IMPORT_PBF            - Path to local PBF file, overrides download
#   BARRELMAN_DATA_DIR    - Data directory path (default: /data)
#   IMPORT_PHASE          - all (default) | download | osm2pgsql | post.
#                           run-import.sh drives the phases separately so the
#                           engine builds that need only the PBF can overlap
#                           the database work; "all" keeps this script usable
#                           standalone exactly as before.
#   OSM2PGSQL_FLAT_NODES  - Path to a flat node file. Unset (default) keeps node
#                           storage in Postgres. Set it for continent/planet
#                           imports — see step 2. Must match update-osm.sh.
#   OSM2PGSQL_CACHE_MB    - Node cache in MB when NOT using a flat node file
#   OSM2PGSQL_PROCESSES   - Parallel workers for the middle tables
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DATA_DIR="${BARRELMAN_DATA_DIR:-$PROJECT_DIR/data}"

GEOFABRIK_URL="${GEOFABRIK_URL:-https://download.geofabrik.de/north-america/us/north-carolina-latest.osm.pbf}"
PBF_FILE="${IMPORT_PBF:-${1:-$DATA_DIR/region.osm.pbf}}"
DATABASE_URL="${DATABASE_URL:?DATABASE_URL is required}"
PHASE="${IMPORT_PHASE:-all}"

case "$PHASE" in
  all|download|osm2pgsql|post) ;;
  *) echo "error: IMPORT_PHASE must be all, download, osm2pgsql or post (got '$PHASE')" >&2; exit 1 ;;
esac

mkdir -p "$DATA_DIR"

# ── Step 1: Download PBF(s) ──────────────────────────────────────────────────
# OSM_EXTRACTS (space/newline-separated URLs, resolved from the unified REGIONS
# config by run-import.sh) takes precedence over the single legacy GEOFABRIK_URL.
# Multiple extracts are downloaded and merged (deduped by OSM id) with osmium
# into one region.osm.pbf, so dev can combine e.g. NC + NY + NJ + CT.
if [[ "$PHASE" == "all" || "$PHASE" == "download" ]]; then
read -ra EXTRACTS <<< "${OSM_EXTRACTS:-$GEOFABRIK_URL}"

if [ -f "$PBF_FILE" ] && [ "${FORCE_DOWNLOAD:-0}" != "1" ]; then
    echo "[$(date '+%H:%M:%S')] [1/8] Using existing PBF: $PBF_FILE ($(du -h "$PBF_FILE" | cut -f1))"
elif [ "${#EXTRACTS[@]}" -le 1 ]; then
    echo "[$(date '+%H:%M:%S')] [1/8] Downloading PBF from ${EXTRACTS[0]}..."
    wget -q --show-progress -O "$PBF_FILE" "${EXTRACTS[0]}"
    echo "  Downloaded: $(du -h "$PBF_FILE" | cut -f1)"
else
    echo "[$(date '+%H:%M:%S')] [1/8] Downloading ${#EXTRACTS[@]} extracts and merging with osmium..."
    PARTS=()
    idx=0
    for url in "${EXTRACTS[@]}"; do
        part="$DATA_DIR/extract-$idx.osm.pbf"
        echo "  - $url"
        wget -q --show-progress -O "$part" "$url"
        PARTS+=("$part")
        idx=$((idx + 1))
    done
    osmium merge --overwrite -o "$PBF_FILE" "${PARTS[@]}"
    rm -f "${PARTS[@]}"
    echo "  Merged: $(du -h "$PBF_FILE" | cut -f1)"
fi
fi # download phase

# ── Step 2: osm2pgsql import ─────────────────────────────────────────────────
# --slim keeps the middle tables so osm2pgsql-replication can apply diffs later.
# Those tables are also where every node's coordinates live, so resolving a way
# means one index lookup per node against a table that, past a country or so,
# is far larger than RAM. --flat-nodes moves node storage into a flat file
# addressed by node ID — an 8-byte seek instead of a B-tree descent — and drops
# planet_osm_nodes entirely. On a continent-sized extract that is the difference
# between hours and days.
#
# It is opt-in because the file is sized by the highest node ID in the input,
# not by how many nodes are kept: any extract carrying present-day OSM IDs
# yields a ~100 GB file (measured: 106 GB for North America — osm2pgsql writes
# it dense, so budget real disk, not sparse-file hope). Below roughly a country
# the middle tables are both faster and far smaller, so leave this unset.
#
# Whatever is set here MUST also be set when diffs are applied — update-osm.sh
# reads the same variable. osm2pgsql 1.8, which barrelman-db ships, does not
# record the flat-nodes path in the database (1.9+ does), so an update that
# omits it cannot resolve any node the file holds.
if [[ "$PHASE" == "all" || "$PHASE" == "osm2pgsql" ]]; then
if [ ! -f "$PBF_FILE" ]; then
    echo "error: $PBF_FILE not found — run the download phase first" >&2
    exit 1
fi

OSM2PGSQL_ARGS=()
if [ -n "${OSM2PGSQL_FLAT_NODES:-}" ]; then
    mkdir -p "$(dirname "$OSM2PGSQL_FLAT_NODES")"
    # --create rebuilds from scratch, so a file left by an earlier import is
    # stale by definition. Reusing it would resolve ways against another
    # extract's node IDs.
    rm -f "$OSM2PGSQL_FLAT_NODES"
    echo "  Node storage: flat file at $OSM2PGSQL_FLAT_NODES"
    # --cache only ever holds nodes, which now live in the flat file, so any
    # cache is dead weight that osm2pgsql warns about and ignores.
    OSM2PGSQL_ARGS+=(--flat-nodes="$OSM2PGSQL_FLAT_NODES" --cache=0)
elif [ -n "${OSM2PGSQL_CACHE_MB:-}" ]; then
    OSM2PGSQL_ARGS+=(--cache="$OSM2PGSQL_CACHE_MB")
fi
if [ -n "${OSM2PGSQL_PROCESSES:-}" ]; then
    OSM2PGSQL_ARGS+=(--number-processes="$OSM2PGSQL_PROCESSES")
fi

echo "[$(date '+%H:%M:%S')] [2/8] Running osm2pgsql import..."
osm2pgsql \
    --create \
    --slim \
    --output=flex \
    --style="$PROJECT_DIR/import/osm2pgsql-flex.lua" \
    ${OSM2PGSQL_ARGS[@]+"${OSM2PGSQL_ARGS[@]}"} \
    -d "$DATABASE_URL" \
    "$PBF_FILE"
echo "  osm2pgsql complete."
fi # osm2pgsql phase

if [[ "$PHASE" == "all" || "$PHASE" == "post" ]]; then

# ── Step 3: Post-import SQL (columns, structured fields, core indexes) ───────
# Only the four indexes the pipeline itself needs are built here; the
# query-serving GIN/GiST set is deferred to step 7 so the enrichment UPDATEs
# below do not pay index maintenance on every rewritten row. See the note in
# post-import.sql.
echo "[$(date '+%H:%M:%S')] [3/8] Running post-import SQL..."
psql "$DATABASE_URL" -f "$PROJECT_DIR/import/post-import.sql"

# Transit tile views. Named "transit" but derived purely from geo_places — no
# GTFS table is involved — so they belong to the OSM import, not the optional
# transit pipeline.
#
# Martin treats an unresolvable source as fatal and exits, so while these views
# were created only by the transit steps, an OSM-only install (the documented
# minimum) had no vector tiles at all: martin crash-looped on
# "Source transit_platforms: Unavailable".
psql "$DATABASE_URL" -f "$PROJECT_DIR/import/create-transit-views.sql"

# Map detail tile views (trees, parking surfaces, street furniture). Like the
# transit views these are derived purely from geo_places, so they belong to the
# OSM import. The API also creates them at startup, which is what covers an
# instance upgrading without a re-import.
psql "$DATABASE_URL" -f "$PROJECT_DIR/import/create-detail-views.sql"

# The 3D buildings view holds rows rather than being a plain view, so creating
# it is not enough — it comes into existence empty. Filled here, where the
# building data it joins over has just changed. Minutes on a large extract.
echo "[$(date '+%H:%M:%S')] Building the 3D buildings view (spatial join, this takes a while)..."
psql "$DATABASE_URL" -c "REFRESH MATERIALIZED VIEW buildings_3d;"

# buildings_3d was the last consumer of geo_places' spatial indexes until the
# API takes over: the intersections and parent-context joins below both probe
# their own indexed temp tables, never these. Dropping them here means every
# enrichment UPDATE maintains one btree instead of three GiSTs — measured on
# the US import, spatial-index maintenance was the difference between ~1,800
# and several thousand rows/s on non-HOT rewrites. finalize-indexes.sql
# rebuilds them in bulk over the settled table.
echo "[$(date '+%H:%M:%S')] Dropping spatial indexes for the enrichment passes (rebuilt at step 7)..."
psql "$DATABASE_URL" -c "
DROP INDEX IF EXISTS geo_places_geom_idx;
DROP INDEX IF EXISTS geo_places_centroid_idx;
DROP INDEX IF EXISTS geo_places_admin_geom_idx;"

# ── Step 4: Codes and abbreviations, one pass ────────────────────────────────
# Codes (IATA, ICAO, ref, short_name, alt_name) and multi-word-name
# abbreviations touch heavily overlapping row sets, so they used to rewrite
# many named rows twice. One statement, one row version. The guards keep each
# column stable unless its own derived value moved — an abbreviation-only
# change does not clobber codes with NULL, and vice versa.
echo "[$(date '+%H:%M:%S')] [4/8] Extracting codes + generating abbreviations..."
psql "$DATABASE_URL" -c "
UPDATE geo_places g
SET codes = CASE WHEN s.codes IS NOT NULL THEN s.codes ELSE g.codes END,
    name_abbrev = CASE WHEN s.abbrev IS NOT NULL THEN s.abbrev ELSE g.name_abbrev END
FROM (
  SELECT coalesce(c.id, a.id) AS id, c.codes, a.abbrev
  FROM (
    SELECT id,
      array_agg(DISTINCT lower(trim(code))) FILTER (WHERE trim(code) <> '') AS codes
    FROM geo_places,
    LATERAL unnest(
      string_to_array(coalesce(tags->>'iata', ''), ';') ||
      string_to_array(coalesce(tags->>'icao', ''), ';') ||
      string_to_array(coalesce(tags->>'ref', ''), ';') ||
      string_to_array(coalesce(tags->>'short_name', ''), ';') ||
      string_to_array(coalesce(tags->>'abbreviation', ''), ';') ||
      string_to_array(coalesce(tags->>'alt_name', ''), ';')
    ) AS code
    WHERE tags IS NOT NULL
      AND (
        tags->>'iata' IS NOT NULL OR
        tags->>'icao' IS NOT NULL OR
        tags->>'ref' IS NOT NULL OR
        tags->>'short_name' IS NOT NULL OR
        tags->>'abbreviation' IS NOT NULL OR
        tags->>'alt_name' IS NOT NULL
      )
    GROUP BY id
  ) c
  FULL OUTER JOIN (
    SELECT id,
      lower(string_agg(left(word, 1), '' ORDER BY ord)) AS abbrev
    FROM (
      SELECT id, word, ord
      FROM geo_places,
      LATERAL unnest(regexp_split_to_array(name, '\s+')) WITH ORDINALITY AS t(word, ord)
      WHERE name IS NOT NULL
        AND name ~ '^[\w\s\d\-''\.&]+\$'
    ) words
    WHERE lower(word) NOT IN (
      'of','the','and','at','in','for','a','an',
      'de','la','le','les','du','des','et','au',
      'der','die','das','von','und','im','am',
      'del','los','las','el','dos','e',
      'di','della','dei','degli'
    )
    AND length(word) > 0
    GROUP BY id
    HAVING count(*) >= 2
  ) a ON a.id = c.id
) s
WHERE g.id = s.id
  AND ((s.codes IS NOT NULL AND g.codes IS DISTINCT FROM s.codes)
    OR (s.abbrev IS NOT NULL AND g.name_abbrev IS DISTINCT FROM s.abbrev));
"

# ── Step 5: Generate road intersections ──────────────────────────────────────
echo "[$(date '+%H:%M:%S')] [5/8] Generating road intersections..."
psql "$DATABASE_URL" -f "$PROJECT_DIR/import/generate-intersections.sql"

# ── Step 6: Parent context + full-text index (one pass over named rows) ──────
# resolve-parent-context.sql writes ts alongside parent_context, so the
# separate whole-table tsvector rewrite this pipeline used to end with is now
# a no-op verification and is skipped here. rebuild-tsvectors.sql still exists
# for the daily-update path.
echo "[$(date '+%H:%M:%S')] [6/8] Resolving parent boundary context + building tsvectors..."
psql "$DATABASE_URL" -f "$PROJECT_DIR/import/resolve-parent-context.sql"

# ── Step 7: Query-serving indexes, over settled data ─────────────────────────
echo "[$(date '+%H:%M:%S')] [7/8] Building search indexes (bulk, deferred until data settled)..."
psql "$DATABASE_URL" -f "$PROJECT_DIR/import/finalize-indexes.sql"

# ── Step 8: ANALYZE ──────────────────────────────────────────────────────────
echo "[$(date '+%H:%M:%S')] [8/8] Running ANALYZE..."
psql "$DATABASE_URL" -c "ANALYZE geo_places; ANALYZE bicycle_ways; ANALYZE bicycle_routes;"

echo ""
echo "[$(date '+%H:%M:%S')] ✓ Import complete!"
echo ""
echo "  Optional: generate semantic embeddings with:"
echo "  docker exec -e DATABASE_URL=\$DB_URL barrelman bun run import/embed-places.ts"

fi # post phase
