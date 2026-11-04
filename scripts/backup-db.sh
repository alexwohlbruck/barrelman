#!/bin/bash
set -euo pipefail

# =============================================================================
# Barrelman state backup
# =============================================================================
#
# Dumps the tables an import cannot rebuild (accounts, keys, credits, usage,
# schedules, job history, regions, landmark state; the list is
# src/admin/backup-tables.ts) to
#
#   $BACKUP_DIR/barrelman-<UTC timestamp>.dump      (pg_dump custom format)
#
# checks the dump with `pg_restore --list`, uploads it to S3-compatible object
# storage when BACKUP_S3_BUCKET is set, and prunes old copies in both places.
# Any failure exits non-zero, so a scheduled run shows up as a failed job.
#
# Run from the console (Scripts → Back Up Database) or:
#   docker compose exec barrelman-ops bash scripts/backup-db.sh [--local-only]
#
#   --local-only   skip the upload and the remote prune for this run
#
# Environment:
#   BACKUP_DIR                default /data/backups
#   BACKUP_KEEP_LOCAL_DAYS    default 7. The dump just written is always kept
#   BACKUP_KEEP_REMOTE_DAYS   default 30. The newest remote dump is always kept
#   BACKUP_S3_*               see scripts/backup-upload.py
# =============================================================================

source "$(dirname "${BASH_SOURCE[0]}")/backup-lib.sh"

LOCAL_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --local-only) LOCAL_ONLY=1 ;;
    *) fail "unknown argument: $arg" ;;
  esac
done

KEEP_LOCAL_DAYS="$(whole_number BACKUP_KEEP_LOCAL_DAYS "${BACKUP_KEEP_LOCAL_DAYS:-}" 7)"
KEEP_REMOTE_DAYS="$(whole_number BACKUP_KEEP_REMOTE_DAYS "${BACKUP_KEEP_REMOTE_DAYS:-}" 30)"

mkdir -p "$BACKUP_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DUMP="$BACKUP_DIR/barrelman-$STAMP.dump"
PARTIAL="$DUMP.partial"
trap 'rm -f "$PARTIAL"' EXIT

# ── Tables ──────────────────────────────────────────────────────────────────
# A table that has never been created (no landmarks imported yet, a fresh box
# that has not replicated) is skipped with a note; pg_dump would skip it too,
# but silently.

WANTED="$(backup_tables)"
DB_NAME="$(live_db_name)"
PRESENT="$(db_psql -d "$DB_NAME" -c "
  SELECT t FROM unnest(string_to_array('$(echo $WANTED)', ' ')) AS t
  WHERE to_regclass('public.' || t) IS NOT NULL ORDER BY t")"
MISSING="$(comm -23 <(echo "$WANTED" | sort) <(echo "$PRESENT" | sort) | tr '\n' ' ')"
[ -n "$PRESENT" ] || fail "none of the backup tables exist in $DB_NAME. Is this the right database?"
if [ -n "${MISSING// /}" ]; then log "Not created on this instance, skipped: $MISSING"; fi

TABLE_ARGS=()
COUNT=0
while read -r t; do
  TABLE_ARGS+=(-t "public.$t")
  COUNT=$((COUNT + 1))
done <<< "$PRESENT"

# ── Dump ────────────────────────────────────────────────────────────────────

log "Dumping $COUNT tables from $DB_NAME to $DUMP"
START=$SECONDS
db_pg pg_dump -d "$DB_NAME" -Fc --no-owner --no-privileges "${TABLE_ARGS[@]}" > "$PARTIAL"

# ── Verify ──────────────────────────────────────────────────────────────────
# Two checks. `pg_restore --list` reads only the table of contents, which sits
# at the front of the file, so counting its TABLE DATA entries catches a dump
# that is short of tables but passes one cut off halfway through the data.
# Converting the whole archive to SQL (thrown away) reads every byte, so a
# truncated or corrupt file fails there.

LISTED="$(db_pg pg_restore --list < "$PARTIAL" | grep -c ' TABLE DATA public ' || true)"
[ "$LISTED" = "$COUNT" ] || fail "dump lists $LISTED tables' data, expected $COUNT"
db_pg pg_restore -f /dev/null < "$PARTIAL" || fail "dump does not read back in full"
mv "$PARTIAL" "$DUMP"
log "Verified: $COUNT tables, $(du -h "$DUMP" | cut -f1), $((SECONDS - START))s"

# ── Upload ──────────────────────────────────────────────────────────────────

if [ -z "${BACKUP_S3_BUCKET:-}" ]; then
  log "BACKUP_S3_BUCKET is not set: this backup is on this machine only."
elif [ "$LOCAL_ONLY" = 1 ]; then
  log "--local-only: not uploading."
else
  log "Uploading to s3://$BACKUP_S3_BUCKET/$BACKUP_S3_PREFIX"
  KEY="$(s3 upload "$DUMP")"
  log "Uploaded $KEY"
  log "Pruning remote backups older than $KEEP_REMOTE_DAYS days"
  s3 prune "$KEEP_REMOTE_DAYS"
fi

# ── Prune local ─────────────────────────────────────────────────────────────

log "Pruning local backups older than $KEEP_LOCAL_DAYS days"
find "$BACKUP_DIR" -maxdepth 1 -type f -name 'barrelman-*.dump' ! -name "$(basename "$DUMP")" \
  -mmin +$((KEEP_LOCAL_DAYS * 1440)) -print -delete
# Leftovers from runs that died mid-dump.
find "$BACKUP_DIR" -maxdepth 1 -type f -name 'barrelman-*.dump.partial' -mmin +1440 -print -delete

log "Done: $DUMP"
