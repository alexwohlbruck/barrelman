#!/bin/bash
set -euo pipefail

# =============================================================================
# Apply a replication diff to the OSM extract on disk
# =============================================================================
#
# An `osm2pgsql-replication --post-processing` hook, run inside barrelman-db by
# scripts/update-osm.sh. It fires once per downloaded chunk, after that chunk
# has been appended to Postgres and *before* the replication cursor advances.
#
# Why this exists: osm2pgsql only updates Postgres, but GraphHopper builds its
# routing graph from /data/region.osm.pbf and MOTIS mounts the same volume.
# Without this, replication mode rebuilt the graph from whatever the last full
# import downloaded — a 20-minute no-op that produced a byte-identical graph and
# left street routing frozen while search and tiles moved on.
#
# Failure semantics: a non-zero exit here fails the whole update run and leaves
# the cursor unmoved, so the chunk is re-downloaded and re-applied next time.
# `osmium apply-changes` is idempotent for a given diff, so the extract side of
# that retry converges; re-appending a diff to Postgres is what pyosmium itself
# documents as "usually safe" and is the behaviour it relies on already.
#
# Arguments (supplied by osm2pgsql-replication): <sequence> <timestamp>
#
# Environment:
#   OSM_DIFF_FILE - the chunk just applied; must match update-osm.sh's
#                   --diff-file, since the hook is handed the sequence number
#                   rather than the path
#   OSM_PBF_FILE  - extract to patch (default: /data/region.osm.pbf)
# =============================================================================

SEQUENCE="${1:-unknown}"

DIFF_FILE="${OSM_DIFF_FILE:?OSM_DIFF_FILE is required}"
PBF_FILE="${OSM_PBF_FILE:-/data/region.osm.pbf}"

if [ ! -f "$PBF_FILE" ]; then
  # An install that imported from a PBF it no longer keeps has nothing to patch.
  # Stalling the Postgres update over that would be worse than carrying on.
  echo "  [pbf] $PBF_FILE not present — skipping (Postgres updated, routing graph left alone)"
  exit 0
fi

if [ ! -f "$DIFF_FILE" ]; then
  echo "  [pbf] diff $DIFF_FILE is missing — cannot patch the extract" >&2
  exit 1
fi

TMP_FILE="${PBF_FILE}.patching"
trap 'rm -f "$TMP_FILE"' EXIT

# -f pbf is not optional: osmium infers the output format from the filename, and
# the .patching suffix it cannot parse aborts the run. GraphHopper and MOTIS
# both require PBF, so there is nothing else this could sensibly be.
#
# On a merged multi-region extract this patches only the region whose feed the
# database follows; the others are left at their last full import, which is what
# update-osm.sh warns about. One inaccuracy comes with that: Geofabrik clips its
# extract diffs to the region polygon, so an object that moves out of the region
# arrives as a delete, and if a neighbouring extract in the merge also held that
# object it is dropped here too. It reappears on the next UPDATE_MODE=full, and
# the alternative — leaving the routing graph months stale — is worse.
echo "  [pbf] applying sequence ${SEQUENCE} to $(basename "$PBF_FILE")..."
osmium apply-changes --overwrite -f pbf -o "$TMP_FILE" "$PBF_FILE" "$DIFF_FILE"

# Same filesystem, so the swap is atomic. GraphHopper and MOTIS mount this
# volume and must never observe a half-written extract.
mv -f "$TMP_FILE" "$PBF_FILE"
trap - EXIT

echo "  [pbf] extract patched through sequence ${SEQUENCE}"
