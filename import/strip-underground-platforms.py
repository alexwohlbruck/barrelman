#!/usr/bin/env python3
"""
Strip underground transit-platform ways from an OSM extract for MOTIS.

MOTIS matches each GTFS stop to the nearest walkable OSM way, level-aware.
Underground subway platforms (railway/public_transport=platform tagged
level=-1/-2 etc.) become an isolated subgraph wherever OSM lacks the
connecting steps/elevator ways — common in NYC outside major hubs. The stop
then matches that island and is UNREACHABLE for access/egress walks, so the
router can only enter the subway via a nearby bus, producing absurd
bus-first itineraries (observed: 139 Rogers Ave -> 43 E Broadway routed
B49 bus -> A instead of the obvious 3 train).

Removing the underground platform ways makes those stops fall back to
street-level matching (a footway/road is typically within meters), which is
reachable. Nothing of value is lost for routing:
  - Stop-to-stop transfers come from GTFS transfers.txt (barrelman
    pre-computes them via GraphHopper), not MOTIS platform footpaths.
  - Final access/egress walk geometry is re-snapped to the real station
    entrance downstream (parchment enrichIntermodalWalks).
  - Surface / elevated / at-grade platforms (level >= 0) are kept untouched.

This writes a MOTIS-only extract; region.osm.pbf is left intact for
GraphHopper, osm2pgsql, and the tile layers.

Usage:
  python3 import/strip-underground-platforms.py <input.osm.pbf> <output.osm.pbf>

Requires: pyosmium  (pip install osmium)
"""
import sys
import re
import osmium


def has_negative(value: str | None) -> bool:
    """True if any component of an OSM level/layer value is negative
    (handles '-1', '-2;-1', '0;-1', '-1--3' ranges)."""
    if not value:
        return False
    for token in re.split(r"[;,]", value):
        token = token.strip()
        if not token:
            continue
        # range like "-2--1": negative if it starts with '-'
        if token.startswith("-"):
            return True
    return False


def is_underground_platform(obj) -> bool:
    t = obj.tags
    is_platform = (
        t.get("public_transport") == "platform" or t.get("railway") == "platform"
    )
    if not is_platform:
        return False
    return has_negative(t.get("level")) or has_negative(t.get("layer"))


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    src, dst = sys.argv[1], sys.argv[2]

    removed = 0
    writer = osmium.SimpleWriter(dst, overwrite=True)
    try:
        for obj in osmium.FileProcessor(src):
            if obj.is_way() and is_underground_platform(obj):
                removed += 1
                continue
            writer.add(obj)
    finally:
        writer.close()

    print(f"Removed {removed} underground platform ways -> {dst}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
