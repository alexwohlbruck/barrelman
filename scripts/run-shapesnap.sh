#!/usr/bin/env bash
#
# Run shapesnap (the GTFS→OSM shape rewrite) standalone on one processed feed.
#
# The import pipeline runs this automatically for feeds enabled in
# config/shapesnap.json (applyShapeRewrite in import/import-gtfs.ts); this
# script is the manual/debug entry point for a single feed. It matches every
# pattern of the feed's configured modes onto the cached OSM mode graph and
# rewrites data/gtfs-processed/<feedId>.zip IN PLACE (shapes.txt, trips.txt
# shape_id remap, shape_dist_traveled), writing matched_shapes/shapesnap_runs
# metadata to PostGIS. See docs/shapesnap.md.
#
# Reference reseed: because the rewrite is in place, a rerun would otherwise
# match against the PREVIOUS run's snapped output instead of the feed's own
# shapes — the dense-regime reference drifts (verified: same pattern, a
# different snap id, Fréchet 29.5 m vs 49.7 m). So before matching, this
# script restores shapes.txt / trips.txt / stop_times.txt into the processed
# zip from the pristine raw zip (data/gtfs/<feedId>.zip) — exactly the state
# the import pipeline hands shapesnap, since the shape rewrite is the FIRST
# transform step. Members baked in after shapesnap (display overrides,
# transfers.txt, fares) are kept. Set SHAPESNAP_RESEED=0 to skip; --dry-run
# never touches the zip, so it matches against it as-is.
#
# After a rewrite, hand the zip to MOTIS with scripts/rebuild-motis.sh.
#
# Usage:
#   scripts/run-shapesnap.sh <feedId> [extra shapesnap.run args]
#   scripts/run-shapesnap.sh 29
#   scripts/run-shapesnap.sh 29 --dry-run
#   scripts/run-shapesnap.sh 29 --modes rail --routes Brn,Blue
#
# Environment:
#   SHAPESNAP_ZIP    - zip to rewrite (default: ./data/gtfs-processed/<feedId>.zip)
#   SHAPESNAP_RESEED - 0 to skip the raw-zip reference reseed (default: 1)
#   DATABASE_URL     - PostGIS DSN for metadata (default: the dev DB)
#
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
FEED="${1:?feedId required (e.g. 29)}"
shift || true
ZIP="${SHAPESNAP_ZIP:-$PROJECT_DIR/data/gtfs-processed/$FEED.zip}"

# ── Preflight ────────────────────────────────────────────────────────
command -v uv >/dev/null 2>&1 || {
  echo "✗ uv not found — install it first (brew install uv)"
  exit 1
}
[ -f "$PROJECT_DIR/config/shapesnap.json" ] || {
  echo "✗ Missing config/shapesnap.json"
  exit 1
}
[ -f "$ZIP" ] || {
  echo "✗ Missing $ZIP — produce it with the import pipeline first:"
  echo "    bun run import/import-gtfs.ts --skip-download …"
  echo "  or seed it from the sanitized raw zip:"
  echo "    cp data/gtfs/$FEED.zip data/gtfs-processed/$FEED.zip"
  exit 1
}

cd "$PROJECT_DIR"

# ── Reference reseed (see header + docs/shapesnap.md) ────────────────
# Restore the members shapesnap rewrites from the pristine raw zip so the
# matcher never sees a previous run's snapped output as its reference.
RAW="$PROJECT_DIR/data/gtfs/$FEED.zip"
if [ "${SHAPESNAP_RESEED:-1}" = "0" ]; then
  echo "⚠ SHAPESNAP_RESEED=0 — matching against $ZIP as-is (reference may be a previous run's output)"
elif printf ' %s ' "$@" | grep -q ' --dry-run '; then
  echo "· --dry-run — zip untouched; matching against $ZIP as-is"
elif [ -f "$RAW" ]; then
  uv run python - "$RAW" "$ZIP" <<'PY'
import os, sys, zipfile

raw_path, zip_path = sys.argv[1], sys.argv[2]
tmp_path = zip_path + ".reseed.tmp"
MEMBERS = ("shapes.txt", "trips.txt", "stop_times.txt")
with zipfile.ZipFile(raw_path) as zraw, zipfile.ZipFile(zip_path) as zin, \
     zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zout:
    raw_names = set(zraw.namelist())
    for item in zin.infolist():
        if item.filename in MEMBERS:
            continue  # replaced from raw (or dropped if raw lacks it)
        zout.writestr(item, zin.read(item.filename))
    restored = [m for m in MEMBERS if m in raw_names]
    for name in restored:
        zout.writestr(name, zraw.read(name))
os.replace(tmp_path, zip_path)
print(f"[run-shapesnap] reseeded {', '.join(restored)} from {raw_path}")
PY
else
  echo "⚠ $RAW missing — cannot reseed the matching reference; matching against"
  echo "  the processed zip as-is (a rerun matches the previous run's snapped"
  echo "  output — expect reference drift)"
fi

exec uv run --with-requirements shapesnap/requirements.txt \
  python -m shapesnap.run --feed "$FEED" --zip "$ZIP" "$@"
