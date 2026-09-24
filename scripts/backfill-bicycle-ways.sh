#!/bin/sh
# Brings an existing bicycle_ways up to the current import rules without a
# reimport. Route membership comes from the PBF the database was imported from,
# since a non-slim import keeps no relation members.
#
# Runs inside barrelman-db:
#   docker exec barrelman-db sh /app/scripts/backfill-bicycle-ways.sh [pbf]
set -eu

PBF="${1:-/data/region.osm.pbf}"
SQL="$(cd "$(dirname "$0")/../import" && pwd)/backfill-bicycle-ways.sql"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

osmium tags-filter "$PBF" r/route=bicycle --omit-referenced -f opl -o "$TMP/routes.opl"

# OPL escapes separators inside values, so splitting on `,` and `=` is safe.
# The unbuilt-route rule mirrors `is_unbuilt_route` in osm2pgsql-flex.lua.
awk '{
  tags = ""; members = ""
  for (i = 1; i <= NF; i++) {
    if (substr($i, 1, 1) == "T") tags = substr($i, 2)
    else if (substr($i, 1, 1) == "M") members = substr($i, 2)
  }
  split("", t)
  n = split(tags, kv, ",")
  for (j = 1; j <= n; j++) {
    p = index(kv[j], "=")
    if (p) t[substr(kv[j], 1, p - 1)] = substr(kv[j], p + 1)
  }
  if (t["type"] != "route" || t["route"] != "bicycle") next
  if (t["state"] == "proposed" || t["state"] == "construction") next
  if (tolower(t["name"]) ~ /future|proposed|planned|construction/) next
  m = split(members, ms, ",")
  for (j = 1; j <= m; j++) {
    if (substr(ms[j], 1, 1) != "w") continue
    id = substr(ms[j], 2)
    sub(/@.*/, "", id)
    print id
  }
}' "$TMP/routes.opl" | sort -u > "$TMP/route-ways.txt"

echo "$(wc -l < "$TMP/route-ways.txt") way members of built bicycle routes"

psql -v ON_ERROR_STOP=1 -U "${POSTGRES_USER:-barrelman}" -d "${POSTGRES_DB:-barrelman}" <<SQL
CREATE TEMP TABLE route_ways (osm_id bigint PRIMARY KEY);
\copy route_ways FROM '$TMP/route-ways.txt'
\i $SQL
SQL
