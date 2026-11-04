# Shared by scripts/backup-db.sh and scripts/restore-db.sh. Sourced, not run.
#
# Both scripts run in barrelman-ops, which has no Postgres client: every
# pg_dump, pg_restore and psql call goes through `docker exec` into
# barrelman-db, the way import-stop-areas.sh reaches it. Dumps stream over the
# exec's stdin and stdout, so the backup directory only has to exist where the
# script runs, and the same commands work from a host checkout.

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_CONTAINER="${DB_CONTAINER:-barrelman-db}"

# /data is the barrelman-osm-data volume in barrelman-ops, the one with room
# for extracts. A blank value (Compose's ${VAR:-}) means the default.
BACKUP_DIR="${BACKUP_DIR:-/data/backups}"
BACKUP_S3_PREFIX="${BACKUP_S3_PREFIX:-barrelman/}"

log() { echo "[$(date -u +%H:%M:%S)] $*"; }
fail() { echo "ERROR: $*" >&2; exit 1; }

# A whole number, or the default when blank. Anything else fails the job
# rather than silently pruning on a value nobody meant.
whole_number() {
  local name="$1" value="$2" fallback="$3"
  [ -z "$value" ] && value="$fallback"
  [[ "$value" =~ ^[0-9]+$ ]] || fail "$name must be a whole number of days, got \"$value\""
  echo "$value"
}

# psql inside barrelman-db. Extra arguments are passed through to psql.
db_psql() {
  docker exec -i "$DB_CONTAINER" sh -c \
    'PGPASSWORD="$POSTGRES_PASSWORD" exec psql -h 127.0.0.1 -U "$POSTGRES_USER" -v ON_ERROR_STOP=1 -X -tA "$@"' \
    sh "$@"
}

# pg_dump / pg_restore inside barrelman-db, same credentials.
db_pg() {
  local tool="$1"; shift
  docker exec -i "$DB_CONTAINER" sh -c \
    'tool="$1"; shift; PGPASSWORD="$POSTGRES_PASSWORD" exec "$tool" -h 127.0.0.1 -U "$POSTGRES_USER" "$@"' \
    sh "$tool" "$@"
}

live_db_name() { docker exec "$DB_CONTAINER" printenv POSTGRES_DB; }

# The table list lives in src/admin/backup-tables.ts so the code and its tests
# share one copy. Names are checked here because they end up in SQL.
backup_tables() {
  local t
  (cd "$PROJECT_DIR" && bun run src/admin/backup-tables.ts) | while read -r t; do
    [[ "$t" =~ ^[a-z_][a-z0-9_]*$ ]] || fail "unexpected table name \"$t\" in backup-tables.ts"
    echo "$t"
  done
}

# Object storage, through boto3. uv fetches boto3 on first use and caches it,
# the same way import-stop-areas.sh gets osmium, so the image carries nothing
# extra.
s3() {
  [ -n "${BACKUP_S3_BUCKET:-}" ] || fail "BACKUP_S3_BUCKET is not set"
  if python3 -c 'import boto3' 2>/dev/null; then
    python3 "$PROJECT_DIR/scripts/backup-upload.py" "$@"
  elif command -v uv >/dev/null 2>&1; then
    uv run --quiet --no-project --with boto3 python3 "$PROJECT_DIR/scripts/backup-upload.py" "$@"
  else
    fail "uploading needs python3 with boto3, or uv to fetch it"
  fi
}
