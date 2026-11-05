#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman state restore
# =============================================================================
#
# Restores a dump made by scripts/backup-db.sh. By default it goes into a
# scratch database next to the live one, for restore drills: the live
# database is never touched.
#
#   bash scripts/restore-db.sh [source] [--target-db NAME] [--drop-after]
#   BACKUP_RESTORE_CONFIRM=<live db name> bash scripts/restore-db.sh <source> --into-live
#
# source is one of
#   latest (the default)    newest dump in BACKUP_DIR, else the newest in the bucket
#   barrelman-….dump        a file name in BACKUP_DIR
#   /path/to/file.dump      a file anywhere this script can read
#   barrelman/barrelman-….dump  an object key in BACKUP_S3_BUCKET
#
#   --target-db NAME   scratch database to restore into. Must start with
#                      barrelman_restore (default barrelman_restore_<UTC time>).
#                      An existing one is dropped first, but only if this script
#                      created it: it marks each one with a database comment.
#                      Any other database is refused.
#   --drop-after       drop the scratch database once the row counts are printed
#   --into-live        replace the backed-up tables in the live database. Refused
#                      unless BACKUP_RESTORE_CONFIRM is set to the live database's
#                      name. Runs in one transaction, so it applies fully or not at
#                      all. Restart the API afterwards: it caches API keys and
#                      balances in memory.
# =============================================================================

source "$(dirname "${BASH_SOURCE[0]}")/backup-lib.sh"

SOURCE=""
TARGET="barrelman_restore_$(date -u +%Y%m%dt%H%M%Sz)"
# Set on every scratch database this script creates, and required before it
# drops one, so a name typed by hand cannot take out some other database.
SCRATCH_MARKER="barrelman restore-db.sh scratch database"
INTO_LIVE=0
DROP_AFTER=0
while [ $# -gt 0 ]; do
  case "$1" in
    --target-db) TARGET="${2:-}"; shift 2 ;;
    --target-db=*) TARGET="${1#*=}"; shift ;;
    --into-live) INTO_LIVE=1; shift ;;
    --drop-after) DROP_AFTER=1; shift ;;
    -*) fail "unknown option: $1" ;;
    *) [ -z "$SOURCE" ] || fail "more than one source given: $SOURCE, $1"; SOURCE="$1"; shift ;;
  esac
done
# No source means the newest backup, so a scheduled drill needs no parameters.
SOURCE="${SOURCE:-latest}"

LIVE="$(live_db_name)"

if [ "$INTO_LIVE" = 1 ]; then
  [ "${BACKUP_RESTORE_CONFIRM:-}" = "$LIVE" ] || fail "--into-live overwrites accounts, keys and credits in \"$LIVE\".
  Set BACKUP_RESTORE_CONFIRM=$LIVE to confirm."
  TARGET="$LIVE"
else
  [[ "$TARGET" =~ ^barrelman_restore[a-z0-9_]*$ ]] \
    || fail "--target-db must start with barrelman_restore, got \"$TARGET\". Use --into-live to restore over the live database."
  [ "$TARGET" != "$LIVE" ] || fail "\"$TARGET\" is the live database. Use --into-live to restore over it."
  EXISTING="$(db_psql -d postgres -c "SELECT coalesce(shobj_description(oid, 'pg_database'), '') FROM pg_database WHERE datname = '$TARGET'")"
  EXISTS="$(db_psql -d postgres -c "SELECT count(*) FROM pg_database WHERE datname = '$TARGET'")"
  if [ "$EXISTS" != 0 ] && [ "$EXISTING" != "$SCRATCH_MARKER" ]; then
    fail "\"$TARGET\" exists and was not created by this script, so it will not be dropped. Pick another --target-db."
  fi
fi

# ── Find the dump ───────────────────────────────────────────────────────────

DOWNLOADED=""
trap 'if [ -n "$DOWNLOADED" ]; then rm -f "$DOWNLOADED"; fi' EXIT

fetch() {
  mkdir -p "$BACKUP_DIR"
  DOWNLOADED="$BACKUP_DIR/restore-$(basename "$1")"
  log "Downloading s3://$BACKUP_S3_BUCKET/$1"
  s3 download "$1" "$DOWNLOADED"
  DUMP="$DOWNLOADED"
}

if [ "$SOURCE" = latest ]; then
  DUMP="$(ls -1 "$BACKUP_DIR"/barrelman-*.dump 2>/dev/null | sort | tail -n1 || true)"
  if [ -z "$DUMP" ]; then
    [ -n "${BACKUP_S3_BUCKET:-}" ] || fail "no dumps in $BACKUP_DIR and no BACKUP_S3_BUCKET to look in"
    KEY="$(s3 latest)" || fail "could not find the newest backup in s3://$BACKUP_S3_BUCKET/$BACKUP_S3_PREFIX"
    fetch "$KEY"
  fi
elif [ -f "$SOURCE" ]; then
  DUMP="$SOURCE"
elif [ -f "$BACKUP_DIR/$SOURCE" ]; then
  DUMP="$BACKUP_DIR/$SOURCE"
elif [ -n "${BACKUP_S3_BUCKET:-}" ]; then
  fetch "$SOURCE"
else
  fail "\"$SOURCE\" is not a file here or in $BACKUP_DIR, and BACKUP_S3_BUCKET is not set"
fi

TABLES="$(db_pg pg_restore --list < "$DUMP" | sed -n 's/^.* TABLE DATA public \([a-z0-9_]*\) .*$/\1/p')"
[ -n "$TABLES" ] || fail "$DUMP holds no table data. Is it a backup-db.sh dump?"
log "Restoring $(echo "$TABLES" | wc -l) tables from $DUMP into $TARGET"

# ── Restore ─────────────────────────────────────────────────────────────────

if [ "$INTO_LIVE" = 1 ]; then
  # --clean drops each table in the dump before recreating it. Tables not in
  # the dump (all of the imported data) are left alone.
  db_pg pg_restore -d "$TARGET" --no-owner --no-privileges --clean --if-exists \
    --single-transaction --exit-on-error < "$DUMP"
else
  db_psql -d postgres -c "SET client_min_messages = warning" -c "DROP DATABASE IF EXISTS \"$TARGET\" WITH (FORCE)" >/dev/null
  db_psql -d postgres -c "CREATE DATABASE \"$TARGET\"" >/dev/null
  db_psql -d postgres -c "COMMENT ON DATABASE \"$TARGET\" IS '$SCRATCH_MARKER'" >/dev/null
  # pg_dump -t leaves extensions out; landmarks need PostGIS for their columns.
  db_psql -d "$TARGET" -c "CREATE EXTENSION IF NOT EXISTS postgis;" >/dev/null
  db_pg pg_restore -d "$TARGET" --no-owner --no-privileges --exit-on-error < "$DUMP"
fi

# ── Report ──────────────────────────────────────────────────────────────────

log "Row counts in $TARGET:"
COUNTS_SQL="$(echo "$TABLES" | sed "s/.*/SELECT '&' AS t, count(*) FROM public.& UNION ALL/")"
db_psql -d "$TARGET" -F $'\t' -c "${COUNTS_SQL% UNION ALL} ORDER BY 1" | while IFS=$'\t' read -r t n; do
  printf '  %-28s %s\n' "$t" "$n"
done

if [ "$INTO_LIVE" = 1 ]; then
  log "Restored into the live database. Restart the API so it drops cached keys and balances:"
  log "  docker restart barrelman"
elif [ "$DROP_AFTER" = 1 ]; then
  db_psql -d postgres -c "DROP DATABASE \"$TARGET\" WITH (FORCE)" >/dev/null
  log "Dropped $TARGET."
else
  log "Left $TARGET in place for inspection. Drop it when done:"
  log "  docker exec barrelman-db psql -U barrelman -d postgres -c 'DROP DATABASE $TARGET'"
fi
