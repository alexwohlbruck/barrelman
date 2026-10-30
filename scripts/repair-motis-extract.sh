#!/bin/bash
set -euo pipefail

# =============================================================================
# Repair an OSM extract so MOTIS's street router (osr) can load it
# =============================================================================
#
# Usage: repair-motis-extract.sh IN.osm.pbf OUT.osm.pbf EXCESS.ids [--recount]
#
# osr aborts the whole MOTIS import on either of two things an otherwise usable
# extract can carry, and the import then falls back to a timetable-only dataset
# that answers no bikeshare, bike or park-and-ride trip:
#
#   invalid location — a way references a node absent from the file. A merged
#     multi-region extract collects these from replication: Geofabrik clips each
#     region's diffs to its polygon, so a node deleted from one region's view is
#     dropped while a neighbouring region's way still uses it.
#   "node … has N ways, maximum is 16" — osr stores a node's ways in a fixed
#     array. The US has junctions past that, e.g. 24 truck-gate lanes fanning
#     out of one node at the Port of Los Angeles.
#
# Both are fixed by leaving ways out: every way with a missing node, and at an
# over-full node, the least important ways past 16 (service and track first).
# The dropped ways cost MOTIS nothing a rider would route over — broken ways are
# unroutable everywhere, and the 17th lane of a truck gate is not a walk to a
# bike dock. IN is never modified; GraphHopper, osm2pgsql and tiles keep it.
#
# Finding over-full nodes means counting every node reference in the extract,
# about 40 minutes on the United States, while the nodes themselves almost never
# change. So that pass runs only with --recount (rebuild-motis.sh passes it after
# an import hits the cap) and its result is kept in EXCESS.ids for later runs.
# Missing nodes are checked every time; that pass is quick and replication
# creates new ones nightly.
#
# Prints the number of ways dropped on stdout, and what it found on stderr. When that is 0,
# OUT is not written and IN can be used as is.
#
# Needs osmium and coreutils (barrelman-ops has both). Sort space comes from
# TMPDIR — on a continent-scale extract that is several GB, so keep it off the
# volume that holds the extracts.
# =============================================================================

IN="$1"
OUT="$2"
EXCESS="$3"
RECOUNT="${4:-}"
OSR_MAX_WAYS=16

tmp="$(mktemp -d "${TMPDIR:-/tmp}/motis-repair.XXXXXX")"
trap 'rm -rf "$tmp"' EXIT
export LC_ALL=C

# The ways osr can load, as a superset. Over-counting only lets an already
# crowded node keep a way it could have dropped; under-counting would leave the
# import to fail on a node this missed.
ROUTABLE=(w/highway w/public_transport=platform w/railway=platform w/route=ferry w/man_made=pier)
routable_re='(^|,)(highway=|public_transport=platform|railway=platform|route=ferry|man_made=pier)'

# 1. Ways referencing a node that is not in the file. check-refs prints
#    "n<node> in w<way>"; its exit status is non-zero whenever anything is
#    missing, so only the output decides.
{ osmium check-refs -i "$IN" 2>/dev/null || true; } \
  | sed -n 's/^n[0-9]* in \(w[0-9]*\)$/\1/p' | sort -u > "$tmp/broken.ids"

# 2. Nodes on more than OSR_MAX_WAYS routable ways. OPL puts a way's node list
#    last ("N" field), so this is a count of node references across ways.
if [ "$RECOUNT" = --recount ]; then
  osmium tags-filter -f opl -o - --omit-referenced "$IN" "${ROUTABLE[@]}" \
    | sed -n 's/^w[0-9]* .* N\(n[^ ]*\)$/\1/p' | tr ',' '\n' \
    | sort -T "$tmp" -S 1G --parallel=4 | uniq -c \
    | awk -v max="$OSR_MAX_WAYS" '$1 > max { print $2 }' > "$tmp/crowded.ids"

  # 3. At each crowded node, keep the OSR_MAX_WAYS most important routable ways
  #    and drop the rest. Rank 1 beats rank 0; among equals, older (lower) ids stay.
  : > "$tmp/excess.ids"
  if [ -s "$tmp/crowded.ids" ]; then
    osmium getparents -f opl -o - "$IN" -i "$tmp/crowded.ids" \
      | awk -v re="$routable_re" '
          NR == FNR { crowded[$1] = 1; next }
          /^w/ {
            tags = ""; nodes = ""
            for (i = 2; i <= NF; i++) {
              c = substr($i, 1, 1)
              if (c == "T") tags = substr($i, 2)
              else if (c == "N") nodes = substr($i, 2)
            }
            if (tags !~ re) next
            minor = "(^|,)highway=(service|track|construction|proposed|raceway|escape|bus_guideway)(,|$)"
            rank = (tags ~ minor) ? 0 : 1
            n = split(nodes, ref, ",")
            for (j = 1; j <= n; j++) if (ref[j] in crowded) print ref[j], rank, substr($1, 2)
          }' "$tmp/crowded.ids" - \
      | sort -u | sort -k1,1 -k2,2nr -k3,3n \
      | awk -v max="$OSR_MAX_WAYS" '{ if ($1 != node) { node = $1; kept = 0 } if (++kept > max) print "w" $3 }' \
      | sort -u > "$tmp/excess.ids"
  fi
  mv -f "$tmp/excess.ids" "$EXCESS"
fi
touch "$EXCESS"

sort -u "$tmp/broken.ids" "$EXCESS" > "$tmp/drop.ids"
echo "  $(wc -l < "$tmp/broken.ids" | tr -d ' ') way(s) reference missing nodes;" \
  "$(wc -l < "$EXCESS" | tr -d ' ') way(s) dropped at nodes past ${OSR_MAX_WAYS} ways${RECOUNT:+ (recounted: $(wc -l < "$tmp/crowded.ids" | tr -d ' ') node(s))}" >&2

if [ -s "$tmp/drop.ids" ]; then
  osmium removeid --overwrite -f pbf -i "$tmp/drop.ids" -o "$OUT.partial" "$IN"
  mv -f "$OUT.partial" "$OUT"
fi
wc -l < "$tmp/drop.ids" | tr -d ' '
