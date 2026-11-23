#!/usr/bin/env python3
"""
Straight paths across pedestrian areas, for GraphHopper.

GraphHopper reads a `highway=pedestrian` + `area=yes` polygon as a ring of
ways around its outline, so a walk across a plaza zigzags from corner to
corner of the outline. This adds the paths a person would actually walk:
between every way into the area and around the corners of whatever stands in
it (holes, buildings, water, walls), wherever the straight line stays inside.
Touching areas are joined first, so a plaza mapped as a mosaic is one space.

The new ways reuse the OSM nodes they join, carry the area's tags, and get
ids from SYNTHETIC_WAY_ID_BASE up. GraphHopper times them like any other
pedestrian way, so ETAs follow from their real length.

Two steps, with an `osmium extract` between them (see
scripts/prepare-graphhopper-osm.sh):

  plan   <areas.osm.pbf> <regions.geojson>   boxes around every walkable area
  paths  <nearby.osm.pbf> <paths.osm.pbf> <spaces.ndjson>
                                             the new nodes and ways, and the
                                             open space each run of ways crosses

The API rounds a walk's bends inside each space (src/lib/area-paths), checking
every curve against that space. spaces.ndjson has one line per space,
{"ways": [first id, last id], "rings": [[[lon, lat], ...], ...]}, outer ring
first; <spaces>.index.json lists [first id, last id, byte offset, byte length]
per line so the API can read one space without loading the rest.

Requires: pyosmium, shapely
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field

import osmium
from shapely import LineString, MultiPolygon, Point, Polygon, STRtree, box, transform, unary_union

from area_paths import lines, without_planters

WALKABLE = {"pedestrian", "footway", "path"}
# Linear ways mapped across an area that a walk there should follow.
FOOTWAYS = {"pedestrian", "footway", "path", "steps"}
OBSTACLE_TAGS = {"natural": {"water"}, "amenity": {"fountain"}, "leisure": {"swimming_pool"}}
NOT_OBSTACLE_BUILDINGS = {"no", "roof", "canopy", "carport", "construction"}
WALLS = {"wall", "fence", "hedge", "retaining_wall", "city_wall", "guard_rail", "jersey_barrier"}
COPIED_TAGS = ("highway", "name", "surface", "smoothness", "lit", "access", "foot", "bicycle",
               "wheelchair", "layer", "level", "bridge", "tunnel", "covered", "indoor")
REGION_MARGIN_DEG = 0.0005
# Below 2^31: GraphHopper stores osm_way_id in 31 bits.
SYNTHETIC_WAY_ID_BASE = 2_100_000_000
SYNTHETIC_NODE_ID_BASE = 10**12
M_PER_DEG = 111_320.0


def is_walkable(tags, from_way: bool) -> bool:
    """A closed way needs area=yes to be a plaza rather than a loop; a multipolygon is always one."""
    return tags.get("highway") in WALKABLE and (tags.get("area") == "yes" or not from_way)


def level_of(tags) -> tuple[str, str]:
    return (tags.get("layer") or "0", tags.get("level") or "0")


def is_raised(tags) -> bool:
    """Off the ground, where what is mapped below it is not in the way."""
    return level_of(tags) != ("0", "0") or tags.get("bridge", "no") != "no" or tags.get("indoor", "no") != "no"


def is_obstacle(tags) -> bool:
    building = tags.get("building")
    if building and building not in NOT_OBSTACLE_BUILDINGS and tags.get("location") != "underground" and not tags.get("layer", "0").startswith("-"):
        return True
    return "water" in tags or any(tags.get(k) in v for k, v in OBSTACLE_TAGS.items())


def rings_of(area):
    """(outer, holes) per polygon, each ring a list of (node id, lon, lat)."""
    for outer in area.outer_rings():
        yield ([(n.ref, n.lon, n.lat) for n in outer],
               [[(n.ref, n.lon, n.lat) for n in inner] for inner in area.inner_rings(outer)])


def polygon(outer, holes) -> Polygon:
    return Polygon([(lon, lat) for _, lon, lat in outer], [[(lon, lat) for _, lon, lat in h] for h in holes])


def walkable_areas(path: str):
    """Yield (tags, [(outer, holes)]) for each walkable area in the file."""
    for area in osmium.FileProcessor(path).with_locations().with_areas():
        if area.is_area() and is_walkable(area.tags, area.from_way()):
            yield dict(area.tags), list(rings_of(area))


def plan(src: str, dst: str) -> int:
    boxes = []
    for _, parts in walkable_areas(src):
        for outer, holes in parts:
            minx, miny, maxx, maxy = polygon(outer, holes).bounds
            boxes.append(box(minx, miny, maxx, maxy).buffer(REGION_MARGIN_DEG, join_style="mitre"))
    merged = unary_union(boxes) if boxes else MultiPolygon()
    with open(dst, "w") as f:
        json.dump({"type": "Feature", "properties": {}, "geometry": merged.__geo_interface__}, f)
    print(f"[area-paths] {len(boxes)} walkable areas -> {len(getattr(merged, 'geoms', [merged])) if boxes else 0} regions")
    return 0


@dataclass
class Space:
    """One open space: touching walkable areas of the same level, joined."""
    level: tuple[str, str]
    areas: list[tuple[dict, Polygon]] = field(default_factory=list)
    ring_nodes: dict[int, tuple[float, float]] = field(default_factory=dict)
    ring_edges: set[frozenset[int]] = field(default_factory=set)


class Projection:
    """Local metres around a point; exact enough across one plaza."""

    def __init__(self, lon0: float, lat0: float):
        self.lon0, self.lat0, self.kx = lon0, lat0, math.cos(math.radians(lat0)) * M_PER_DEG

    def xy(self, lon: float, lat: float) -> tuple[float, float]:
        return (round((lon - self.lon0) * self.kx, 3), round((lat - self.lat0) * M_PER_DEG, 3))

    def lonlat(self, x: float, y: float) -> tuple[float, float]:
        return (self.lon0 + x / self.kx, self.lat0 + y / M_PER_DEG)

    def shape(self, geom):
        return transform(geom, lambda c: [self.xy(lon, lat) for lon, lat in c])


@dataclass
class Found:
    walkables: list = field(default_factory=list)
    obstacles: list[tuple[Polygon, list]] = field(default_factory=list)
    walls: list[tuple[LineString, list]] = field(default_factory=list)
    way_nodes: set[int] = field(default_factory=set)
    way_ends: dict[int, tuple[float, float]] = field(default_factory=dict)
    footways: list[LineString] = field(default_factory=list)


def collect(src: str) -> Found:
    found = Found()
    for obj in osmium.FileProcessor(src).with_locations().with_areas():
        tags = obj.tags
        if obj.is_area():
            if is_walkable(tags, obj.from_way()):
                found.walkables.append((dict(tags), list(rings_of(obj))))
            elif is_obstacle(tags) or (obj.from_way() and tags.get("barrier") in WALLS):
                found.obstacles.extend((polygon(o, h), [o, *h]) for o, h in rings_of(obj))
        elif obj.is_way() and len(obj.nodes) > 1:
            if tags.get("barrier") in WALLS and not obj.is_closed():
                refs = [(n.ref, n.lon, n.lat) for n in obj.nodes if n.location.valid()]
                if len(refs) > 1:
                    found.walls.append((LineString([(lon, lat) for _, lon, lat in refs]), refs))
            elif "highway" in tags and not (obj.is_closed() and is_walkable(tags, True)):
                found.way_nodes.update(n.ref for n in obj.nodes)
                if tags.get("highway") in FOOTWAYS and tags.get("area") != "yes":
                    coords = [(n.lon, n.lat) for n in obj.nodes if n.location.valid()]
                    if len(coords) > 1:
                        found.footways.append(LineString(coords))
                for n in (obj.nodes[0], obj.nodes[-1]):
                    if n.location.valid():
                        found.way_ends[n.ref] = (n.lon, n.lat)
    return found


def spaces(walkables) -> list[Space]:
    by_level: dict[tuple[str, str], list] = defaultdict(list)
    for tags, parts in walkables:
        for outer, holes in parts:
            poly = polygon(outer, holes)
            if poly.is_valid and poly.area > 0:
                by_level[level_of(tags)].append((tags, poly, [outer, *holes]))
    found = []
    for level, items in by_level.items():
        joined = unary_union([p for _, p, _ in items])
        tree = STRtree([p for _, p, _ in items])
        for piece in getattr(joined, "geoms", [joined]):
            space = Space(level)
            for i in tree.query(piece, predicate="intersects"):
                tags, poly, rings = items[i]
                if piece.intersection(poly).area < poly.area * 0.5:
                    continue
                space.areas.append((tags, poly))
                for ring in rings:
                    for (a, *_), (b, *_) in zip(ring, ring[1:]):
                        space.ring_edges.add(frozenset((a, b)))
                    for ref, lon, lat in ring:
                        space.ring_nodes[ref] = (lon, lat)
            if space.areas:
                found.append(space)
    return found


def write_spaces(dst: str, spaces_out: list[tuple[int, int, list]]) -> None:
    index = []
    with open(dst, "wb") as f:
        for first, last, rings in spaces_out:
            line = json.dumps({"ways": [first, last], "rings": rings}, separators=(",", ":")).encode() + b"\n"
            index.append([first, last, f.tell(), len(line)])
            f.write(line)
    with open(dst.removesuffix(".ndjson") + ".index.json", "w") as f:
        json.dump(index, f, separators=(",", ":"))


def paths(src: str, dst: str, spaces_dst: str) -> int:
    found = collect(src)
    obstacle_tree = STRtree([p for p, _ in found.obstacles]) if found.obstacles else None
    wall_tree = STRtree([w for w, _ in found.walls]) if found.walls else None
    end_refs = list(found.way_ends)
    end_tree = STRtree([Point(found.way_ends[r]) for r in end_refs]) if end_refs else None
    footway_tree = STRtree(found.footways) if found.footways else None

    new_nodes: dict[tuple[float, float], int] = {}
    new_ways: list[tuple[list[int], dict]] = []
    spaces_out: list[tuple[int, int, list]] = []
    skipped = 0
    all_spaces = spaces(found.walkables)

    def node_for(xy, node_at, proj):
        ref = node_at.get(xy)
        if ref is None:
            ref = new_nodes.setdefault(proj.lonlat(*xy), SYNTHETIC_NODE_ID_BASE + len(new_nodes))
        return ref

    for space in all_spaces:
        outline = unary_union([p for _, p in space.areas])
        proj = Projection(*outline.centroid.coords[0])
        node_at = {proj.xy(lon, lat): ref for ref, (lon, lat) in space.ring_nodes.items()}
        anchors = {proj.xy(lon, lat) for ref, (lon, lat) in space.ring_nodes.items() if ref in found.way_nodes}

        open_space = outline
        if obstacle_tree is not None and not any(is_raised(t) for t, _ in space.areas):
            for i in obstacle_tree.query(outline, predicate="intersects"):
                block, rings = found.obstacles[i]
                if not block.is_valid or outline.intersection(block).area > outline.area * 0.5:
                    continue
                open_space = open_space.difference(block)
                for ring in rings:
                    node_at.update((proj.xy(lon, lat), ref) for ref, lon, lat in ring)

        if end_tree is not None:
            for i in end_tree.query(outline, predicate="contains"):
                lon, lat = found.way_ends[end_refs[i]]
                node_at[proj.xy(lon, lat)] = end_refs[i]
                anchors.add(proj.xy(lon, lat))

        footways = []
        if footway_tree is not None and not any(is_raised(t) for t, _ in space.areas):
            for i in footway_tree.query(outline, predicate="intersects"):
                footways.append(proj.shape(found.footways[i]))

        blocking = []
        if wall_tree is not None:
            for i in wall_tree.query(outline, predicate="intersects"):
                wall, refs = found.walls[i]
                blocking.append(proj.shape(wall))
                for ref, lon, lat in refs:
                    if outline.contains(Point(lon, lat)):
                        node_at[proj.xy(lon, lat)] = ref
                        anchors.add(proj.xy(lon, lat))

        tags_tree = STRtree([proj.shape(p) for _, p in space.areas])
        for piece in getattr(open_space, "geoms", [open_space]):
            if not isinstance(piece, Polygon) or piece.is_empty:
                continue
            metres = proj.shape(piece)
            reach = metres.buffer(0.05)
            inside = [w for w in (f.intersection(metres) for f in footways) if not w.is_empty]
            joined = lines(metres, [xy for xy in anchors if reach.covers(Point(xy))], blocking,
                           footways=[part for w in inside for part in getattr(w, "geoms", [w]) if isinstance(part, LineString)])
            if joined is None:
                skipped += 1
                continue
            first = len(new_ways)
            for a, b in joined:
                ref_a, ref_b = node_for(a, node_at, proj), node_for(b, node_at, proj)
                if frozenset((ref_a, ref_b)) in space.ring_edges:
                    continue
                source = space.areas[int(tags_tree.nearest(Point((a[0] + b[0]) / 2, (a[1] + b[1]) / 2)))][0]
                tags = {k: source[k] for k in COPIED_TAGS if k in source}
                tags["area:path"] = "yes"
                new_ways.append(([ref_a, ref_b], tags))
            if len(new_ways) > first:
                walk = without_planters(metres)
                rings = [[[round(c, 7) for c in proj.lonlat(x, y)] for x, y in ring.coords] for ring in (walk.exterior, *walk.interiors)]
                spaces_out.append((SYNTHETIC_WAY_ID_BASE + first, SYNTHETIC_WAY_ID_BASE + len(new_ways) - 1, rings))

    writer = osmium.SimpleWriter(dst, overwrite=True)
    try:
        for (lon, lat), ref in sorted(new_nodes.items(), key=lambda kv: kv[1]):
            writer.add_node(osmium.osm.mutable.Node(id=ref, version=1, location=osmium.osm.Location(lon, lat)))
        for i, (nodes, tags) in enumerate(new_ways):
            writer.add_way(osmium.osm.mutable.Way(id=SYNTHETIC_WAY_ID_BASE + i, version=1, nodes=nodes, tags=list(tags.items())))
    finally:
        writer.close()
    write_spaces(spaces_dst, spaces_out)
    print(f"[area-paths] {len(all_spaces)} spaces, {len(new_ways)} paths, {len(new_nodes)} new nodes, {skipped} too large to join")
    return 0


if __name__ == "__main__":
    commands = {"plan": (plan, 2), "paths": (paths, 3)}
    if len(sys.argv) < 2 or sys.argv[1] not in commands or len(sys.argv) != commands[sys.argv[1]][1] + 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    sys.exit(commands[sys.argv[1]][0](*sys.argv[2:]))
