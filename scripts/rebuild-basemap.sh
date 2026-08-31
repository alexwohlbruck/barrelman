#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman Basemap Rebuild (planetiler → PMTiles)
# =============================================================================
#
# Re-render basemap.pmtiles from the OSM extract so the basemap tracks the
# database instead of freezing at whenever it was last built by hand.
#
# Why this is a full re-render and not a patch: PMTiles is a write-once archive.
# Tiles are clustered in Hilbert tile-id order behind absolute directory offsets
# and deduplicated by content, so re-rendering one tile shifts every byte after
# it and forces the directories to be rewritten. planetiler is a batch renderer
# with no concept of "what changed" — it does not read .osc diffs at all. True
# incremental tiling would need a mutable store (MBTiles), osm2pgsql's
# --expire-tiles dirty list (flex output only gained expiry after 1.8, which is
# what barrelman-db ships), and a renderer able to draw one tile from Postgres
# in the OpenMapTiles schema — which barrelman's DB does not hold, since
# osm2pgsql-flex.lua writes barrelman's own schema. A full render of a couple of
# US states takes minutes, which is cheaper than any of that.
#
# Called automatically from:
#   - scripts/run-import.sh   (after a full import)
#   - scripts/update-osm.sh   (only when the extract actually moved)
#
# Or run manually / from the console ("Rebuild Basemap"):
#   ./scripts/rebuild-basemap.sh
#
# Skips silently when there is nothing to do — no martin container, no extract,
# or no existing basemap (see BASEMAP_FORCE). A skip is always exit 0 so an
# optional basemap can never fail the import that called it. A render that
# genuinely fails exits 1, matching rebuild-graphhopper.sh.
#
# Environment:
#   BASEMAP_FORCE           build even when no basemap.pmtiles exists yet.
#                           Off by default: an install that never opted into a
#                           basemap should not silently acquire a nightly
#                           multi-minute render.
#   PLANETILER_MEMORY       JVM heap for the render (default: 4g)
#   PLANETILER_IMAGE        override the planetiler image
#   BASEMAP_RESTART_MARTIN  restart martin after the swap (default: 1)
# =============================================================================

CONTAINER="barrelman-martin"
DB_CONTAINER="${DB_CONTAINER:-barrelman-db}"
PLANETILER_IMAGE="${PLANETILER_IMAGE:-ghcr.io/onthegomap/planetiler:latest}"
PLANETILER_MEMORY="${PLANETILER_MEMORY:-4g}"
# Same throwaway image rebuild-graphhopper.sh and rebuild-motis.sh already use
# for volume surgery.
HELPER_IMAGE="${BASEMAP_HELPER_IMAGE:-alpine}"

PBF_NAME="region.osm.pbf"
BASEMAP_NAME="basemap.pmtiles"
# planetiler infers the archive format from the output file's *last* extension,
# so the staging file has to keep .pmtiles at the end. Naming it
# basemap.pmtiles.next fails argument parsing with "Unsupported format next"
# before a single OSM block is read.
STAGING_NAME="${BASEMAP_NAME%.pmtiles}.next.pmtiles"
LOCK_NAME=".basemap-rebuild.lock"

log() { echo "[$(date '+%H:%M:%S')] [basemap] $*"; }

if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER}\$"; then
  log "container '${CONTAINER}' not found — skipping rebuild"
  exit 0
fi

# Resolve the two /data mounts separately rather than trusting this container's
# own /data.
#
# They are the same named volume in production, but docker-compose.dev.yml
# overrides martin (and the API) to bind ./data while barrelman-ops and
# barrelman-db keep barrelman-osm-data. So in dev the archive martin serves and
# the extract replication patches live in different places, and rendering into
# ops' /data would produce a basemap martin never reads — silently reproducing
# the very bug this script exists to fix.
#
# .Source (not .Name) because it resolves both cases: a bind gives the host
# path, a named volume gives /var/lib/docker/volumes/<name>/_data. Either can be
# handed straight back to `docker run -v`.
mount_source() {
  docker inspect "$1" \
    --format "{{range .Mounts}}{{if eq .Destination \"$2\"}}{{.Source}}{{end}}{{end}}" 2>/dev/null
}

OUT_DIR="$(mount_source "$CONTAINER" /data)"
# The extract is written by osm2pgsql and apply-osm-diff.sh inside barrelman-db,
# so that container is authoritative for it. Fall back to martin's mount for an
# install where the DB keeps no extract of its own.
IN_DIR="$(mount_source "$DB_CONTAINER" /data)"
IN_DIR="${IN_DIR:-$OUT_DIR}"

if [ -z "$OUT_DIR" ]; then
  log "could not resolve the /data mount for ${CONTAINER} — skipping" >&2
  exit 0
fi

# Every filesystem operation goes through a throwaway container holding the
# resolved directories. Those paths are meaningful to the DOCKER DAEMON, not to
# this container's namespace — in dev OUT_DIR is a host path that does not exist
# inside barrelman-ops at all, so a plain `mkdir`/`stat`/`mv` here would fail (or
# worse, succeed against the wrong directory).
#
# Mounting the same source twice when IN_DIR == OUT_DIR is fine; docker just
# creates two mounts, and the read-only one keeps the render honest about which
# side it is allowed to touch.
io_sh() {
  docker run --rm \
    -v "${IN_DIR}:/in:ro" \
    -v "${OUT_DIR}:/out" \
    "$HELPER_IMAGE" sh -c "$1"
}

# Prove the helper works before using its exit status to mean anything. Every
# check below reads a failed `io_sh` as a fact about the filesystem, so a helper
# that cannot run at all — no alpine image, daemon refusing the mount — would
# otherwise be reported as "another rebuild is already running", sending an
# operator to hunt a lock that was never taken.
if ! HELPER_ERR="$(io_sh 'true' 2>&1)"; then
  log "cannot run the ${HELPER_IMAGE} helper container — skipping" >&2
  log "  ${HELPER_ERR}" >&2
  exit 0
fi

# Single-flight. update-osm.sh calls this in-process so it inherits that job's
# exclusive lock, but a console-triggered "Rebuild Basemap" does not go through
# that lock and could land on top of a scheduled update's render — two planetiler
# runs writing the same temp file. mkdir is atomic on every filesystem here and
# needs no util-linux, unlike flock.
if ! io_sh "mkdir /out/${LOCK_NAME}" >/dev/null 2>&1; then
  log "another basemap rebuild is already running — skipping"
  log "(clear a stale lock with: rm -rf <data>/${LOCK_NAME})"
  exit 0
fi
trap 'io_sh "rmdir /out/'"${LOCK_NAME}"'" >/dev/null 2>&1 || true' EXIT

if ! io_sh "test -f /in/${PBF_NAME}" >/dev/null 2>&1; then
  log "${PBF_NAME} not present in ${IN_DIR} — nothing to render from, skipping"
  exit 0
fi

# An install with no basemap has martin's pmtiles source commented out, so
# rendering one would produce a multi-hundred-megabyte file nothing serves.
# Opting in is explicit.
if ! io_sh "test -f /out/${BASEMAP_NAME}" >/dev/null 2>&1 && [ "${BASEMAP_FORCE:-0}" != "1" ]; then
  log "no existing ${BASEMAP_NAME} — skipping (set BASEMAP_FORCE=1 to build the first one)"
  exit 0
fi

OLD_SIZE=$(io_sh "stat -c %s /out/${BASEMAP_NAME} 2>/dev/null || echo 0" | tr -cd '0-9')
io_sh "rm -f /out/${STAGING_NAME}" >/dev/null 2>&1 || true

log "[1/3] Rendering from ${PBF_NAME} (heap ${PLANETILER_MEMORY})..."
log "      input:  ${IN_DIR}"
log "      output: ${OUT_DIR}"

# --download fetches the non-OSM sources planetiler's OpenMapTiles profile needs
# (water polygons, Natural Earth, lake centerlines). --download-dir keeps them in
# the volume so that happens once rather than on every nightly run.
if ! docker run --rm \
  -e JAVA_TOOL_OPTIONS="-Xmx${PLANETILER_MEMORY}" \
  -v "${IN_DIR}:/in:ro" \
  -v "${OUT_DIR}:/out" \
  "$PLANETILER_IMAGE" \
  --osm-path="/in/${PBF_NAME}" \
  --output="/out/${STAGING_NAME}" \
  --download \
  --download-dir=/out/sources \
  --tmpdir=/out/tmp \
  --force; then
  log "ERROR: planetiler failed — leaving the current basemap in place" >&2
  io_sh "rm -f /out/${STAGING_NAME}" >/dev/null 2>&1 || true
  exit 1
fi

NEW_SIZE=$(io_sh "stat -c %s /out/${STAGING_NAME} 2>/dev/null || echo 0" | tr -cd '0-9')
if [ "${NEW_SIZE:-0}" -eq 0 ]; then
  log "ERROR: planetiler exited 0 but produced no archive — keeping the current basemap" >&2
  io_sh "rm -f /out/${STAGING_NAME}" >/dev/null 2>&1 || true
  exit 1
fi

# A render that collapses to a fraction of the previous archive usually means the
# extract shrank — a REGIONS change, or a full import that downloaded one region
# instead of four. Worth saying out loud, but not worth refusing: the operator
# may have meant it, and .prev is kept either way.
if [ "${OLD_SIZE:-0}" -gt 0 ] && [ "$((NEW_SIZE * 2))" -lt "$OLD_SIZE" ]; then
  log "WARNING: new archive is ${NEW_SIZE} bytes vs ${OLD_SIZE} before —"
  log "         check the extract still covers every configured region"
fi

log "[2/3] Swapping in the new archive (${NEW_SIZE} bytes)..."
# Same directory, so the rename is atomic and no client can observe a
# half-written archive. The previous one is kept for a cycle, like MOTIS's
# data.prev, so a bad render can be rolled back by hand.
io_sh "set -e
if [ -f /out/${BASEMAP_NAME} ]; then mv -f /out/${BASEMAP_NAME} /out/${BASEMAP_NAME}.prev; fi
mv -f /out/${STAGING_NAME} /out/${BASEMAP_NAME}"

# The rename swaps the directory entry, not the inode martin already has open.
# rebuild-motis.sh documents the same behaviour for MOTIS ("it keeps serving via
# the memory-mapped inodes"), which is the desired property there and exactly the
# wrong one here: without a restart martin would go on serving the old, now
# unlinked archive indefinitely — a rebuild that changes nothing a client can
# see. Martin renders its postgres sources per request and persists nothing of
# its own (see docker-compose.yml), so a restart costs only in-flight tiles.
if [ "${BASEMAP_RESTART_MARTIN:-1}" = "1" ]; then
  log "[3/3] Restarting ${CONTAINER} to pick up the new archive..."
  docker restart "$CONTAINER" >/dev/null

  sleep 3
  # Probe by container name: this runs inside barrelman-ops, where :3000 would be
  # ops itself. /health is the same endpoint src/services/health.service.ts uses.
  # `-w` already prints 000 on a connection error.
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "http://${CONTAINER}:3000/health" 2>/dev/null) || true
  if [ "${code:-000}" = "000" ]; then
    log "WARNING: ${CONTAINER} is not answering after the restart — check 'docker logs ${CONTAINER}'"
  else
    log "martin responding: HTTP ${code}"
  fi
else
  log "[3/3] Skipping martin restart (BASEMAP_RESTART_MARTIN=0)"
  log "      NOTE: martin holds the old archive open until it restarts — tiles will not change until then."
fi

log "Rebuild complete. Previous archive kept as ${BASEMAP_NAME}.prev"
