#!/usr/bin/env python3
"""
Synthesize vertical connector ways for isolated underground platforms.

MOTIS's street router (OSR) is topological and level-aware: a pedestrian can
only reach a `railway/public_transport=platform` way through an unbroken
chain of ways sharing exact vertices. Underground platforms (level/layer < 0)
whose mappers never drew the connecting steps/elevators form isolated
subgraphs — the GTFS stop matches the island and becomes unreachable, so the
router produces absurd bus-first itineraries instead of boarding the subway.

The old fix (strip-underground-platforms.py) DELETED those platforms so the
stop fell back to street-level matching — effective, but it reintroduces the
2D "coordinate bleed" a level-aware router exists to prevent. This script
repairs the topology instead:

  1. A platform counts as CONNECTED if any of its vertices is shared with a
     pedestrian/vertical-connector way (steps, ELEVATOR, escalator, corridor,
     footway, path, ramp) or is itself an elevator node. Elevators are
     first-class transportation pathways — they connect levels exactly like
     steps and are the accessible option.
  2. For each isolated platform, pick the nearest anchor within 250 m —
     a `railway=subway_entrance` / `train_station_entrance` node or a
     `highway=elevator` node — and emit a synthetic connector way between
     the anchor node and the platform's nearest vertex (shared nodes at both
     ends; no new geometry). The way is tagged `highway=elevator` when the
     anchor is an elevator (wheelchair=yes), else `highway=steps`, with a
     Simple-Indoor-Tagging level span (e.g. `level=0;-1`).
  3. Platforms with no anchor in range fall back to being stripped — the
     old behavior — so reachability never regresses.

Output is the MOTIS-only extract; region.osm.pbf stays untouched for
GraphHopper, osm2pgsql, and the tile layers.

Usage:
  python3 import/synthesize-platform-connectors.py <input.osm.pbf> <output.osm.pbf>

Requires: pyosmium  (pip install osmium)
"""
import math
import re
import sys

import osmium

# Ways that carry pedestrians between or onto platforms. Sharing a vertex
# with any of these means the platform is reachable.
CONNECTOR_HIGHWAYS = {
    "steps",
    "elevator",
    "escalator",
    "corridor",
    "footway",
    "path",
    "pedestrian",
    "ramp",
}

# Anchor nodes a synthetic connector may start from, in preference order
# when distances tie (elevator first: accessible + explicit vertical link).
ANCHOR_KINDS = ("elevator", "subway_entrance", "train_station_entrance")

MAX_ANCHOR_DIST_M = 250.0
SYNTHETIC_WAY_ID_BASE = 10**12  # far above real OSM way ids


def has_negative(value: str | None) -> bool:
    """True if any component of an OSM level/layer value is negative
    (handles '-1', '-2;-1', '0;-1', '-1--3' ranges)."""
    if not value:
        return False
    for token in re.split(r"[;,]", value):
        token = token.strip()
        if not token:
            continue
        if token.startswith("-"):
            return True
    return False


def min_level(value: str | None) -> str:
    """Lowest numeric component of a level/layer value, as a string."""
    best = None
    for token in re.split(r"[;,]", value or ""):
        token = token.strip()
        try:
            n = float(token)
        except ValueError:
            continue
        if best is None or n < best:
            best = n
    if best is None:
        return "-1"
    return str(int(best)) if best == int(best) else str(best)


def is_underground_platform(tags) -> bool:
    is_platform = (
        tags.get("public_transport") == "platform" or tags.get("railway") == "platform"
    )
    if not is_platform:
        return False
    return has_negative(tags.get("level")) or has_negative(tags.get("layer"))


def haversine_m(a, b) -> float:
    lon1, lat1 = a
    lon2, lat2 = b
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = rlat2 - rlat1
    dlon = math.radians(lon2 - lon1)
    h = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * 6371000.0 * math.asin(math.sqrt(h))


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2
    src, dst = sys.argv[1], sys.argv[2]

    # ── Pass 1: ways — platforms + the set of pedestrian-connected nodes ──
    platforms: dict[int, dict] = {}  # way_id -> {nodes: [ids], level: str}
    connector_nodes: set[int] = set()
    for obj in osmium.FileProcessor(src, osmium.osm.WAY):
        t = obj.tags
        if is_underground_platform(t):
            platforms[obj.id] = {
                "nodes": [n.ref for n in obj.nodes],
                "level": min_level(t.get("level") or t.get("layer")),
            }
        elif t.get("highway") in CONNECTOR_HIGHWAYS or "conveying" in t:
            connector_nodes.update(n.ref for n in obj.nodes)

    platform_node_ids = {n for p in platforms.values() for n in p["nodes"]}

    # ── Pass 2: nodes — platform vertex locations + anchor candidates ──
    locations: dict[int, tuple[float, float]] = {}
    anchors: list[dict] = []
    elevator_node_ids: set[int] = set()
    for obj in osmium.FileProcessor(src, osmium.osm.NODE):
        if obj.id in platform_node_ids:
            locations[obj.id] = (obj.location.lon, obj.location.lat)
        t = obj.tags
        kind = None
        if t.get("highway") == "elevator":
            kind = "elevator"
            elevator_node_ids.add(obj.id)
        elif t.get("railway") in ("subway_entrance", "train_station_entrance"):
            kind = t.get("railway")
        if kind:
            anchors.append(
                {
                    "id": obj.id,
                    "loc": (obj.location.lon, obj.location.lat),
                    "kind": kind,
                    "level": t.get("level"),
                    "wheelchair": t.get("wheelchair"),
                }
            )

    # ── Decide per platform: connected / synthesize / strip ──
    synthetic_ways: list[dict] = []
    strip_ids: set[int] = set()
    connected = 0
    for way_id, p in platforms.items():
        nodes = p["nodes"]
        if any(n in connector_nodes or n in elevator_node_ids for n in nodes):
            connected += 1
            continue

        located = [(n, locations[n]) for n in nodes if n in locations]
        if not located:
            strip_ids.add(way_id)
            continue
        cx = sum(loc[0] for _, loc in located) / len(located)
        cy = sum(loc[1] for _, loc in located) / len(located)

        best = None
        for a in anchors:
            d = haversine_m((cx, cy), a["loc"])
            if d > MAX_ANCHOR_DIST_M:
                continue
            rank = (d, ANCHOR_KINDS.index(a["kind"]))
            if best is None or rank < best[0]:
                best = (rank, a)
        if best is None:
            strip_ids.add(way_id)
            continue

        anchor = best[1]
        # Platform vertex nearest to the anchor — the shared node the
        # connector lands on.
        join_node = min(located, key=lambda nl: haversine_m(nl[1], anchor["loc"]))[0]
        is_elevator = anchor["kind"] == "elevator"
        tags = {
            "highway": "elevator" if is_elevator else "steps",
            "level": f"{anchor['level'] or '0'};{p['level']}",
            "indoor": "yes",
            "foot": "yes",
            "synthetic": "platform_connector",
        }
        if is_elevator or anchor["wheelchair"] == "yes":
            tags["wheelchair"] = "yes"
        synthetic_ways.append({"from": anchor["id"], "to": join_node, "tags": tags})

    # ── Pass 3: write — copy everything, drop stripped, append synthetic ──
    writer = osmium.SimpleWriter(dst, overwrite=True)
    try:
        for obj in osmium.FileProcessor(src):
            if obj.is_way() and obj.id in strip_ids:
                continue
            writer.add(obj)
        for i, w in enumerate(synthetic_ways):
            writer.add_way(
                osmium.osm.mutable.Way(
                    id=SYNTHETIC_WAY_ID_BASE + i,
                    version=1,
                    nodes=[w["from"], w["to"]],
                    tags=list(w["tags"].items()),
                )
            )
    finally:
        writer.close()

    print(
        f"Platforms: {len(platforms)} underground | {connected} already connected | "
        f"{len(synthetic_ways)} connectors synthesized "
        f"({sum(1 for w in synthetic_ways if w['tags']['highway'] == 'elevator')} via elevators) | "
        f"{len(strip_ids)} stripped (no anchor in {int(MAX_ANCHOR_DIST_M)} m) -> {dst}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
