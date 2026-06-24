#!/usr/bin/env python3
"""
Extract public_transport=stop_area relations from an OSM extract as TSV.

A stop_area relation is the mapper's authoritative statement of which
entrances, platforms, and stop positions belong to one station. The
nearest-entrance search uses these as Tier 0 — an entrance that shares a
stop_area with the platform being boarded beats any purely geometric
candidate (Rule: relation first, proximity fallback).

Output columns (tab-separated, no header):
  relation_id  relation_name  member_type(N|W|R)  member_ref  member_role

Usage:
  python3 import/import-stop-areas.py region.osm.pbf > stop_areas.tsv
  # then load via scripts/import-stop-areas.sh

Requires: pyosmium
"""
import sys

import osmium

TYPE_CODE = {"n": "N", "w": "W", "r": "R"}


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2

    count = 0
    for obj in osmium.FileProcessor(sys.argv[1], osmium.osm.RELATION):
        tags = obj.tags
        if tags.get("public_transport") != "stop_area":
            continue
        name = (tags.get("name") or "").replace("\t", " ").replace("\n", " ")
        for m in obj.members:
            code = TYPE_CODE.get(m.type)
            if not code:
                continue
            role = (m.role or "").replace("\t", " ")
            print(f"{obj.id}\t{name}\t{code}\t{m.ref}\t{role}")
        count += 1
    print(f"{count} stop_area relations", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
