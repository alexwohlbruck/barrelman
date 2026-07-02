#!/usr/bin/env python3
"""shapesnap.candidates — projected match graph, candidate lookup, bonuses.

Wraps a Phase A ModeGraph (shapesnap.graph) with everything the matcher
needs in a local metric plane:

  - a UTM projection picked from the graph's node centroid (pyproj),
  - per-edge projected LineStrings + an STRtree over them (candidate
    lookup = k-nearest edges within a radius, point projected onto edge),
  - directed-edge helpers (offsets are measured along the DIRECTED edge),
  - outgoing-edge adjacency honoring oneway, node degrees (stub detection),
  - turn restrictions grouped by via node,
  - RouteMatcher: GTFS route ↔ OSM route-relation matching (ref ==
    short_name, relation name contains long/short name, colour == color;
    case/whitespace-insensitive),
  - StationIndex + load_stations: optional OSM station/platform metadata
    for regime B emission bonuses (token-subset / cheap-TED name match,
    platform ref match) and stations-passed-without-stopping counting.

All coordinates handed to/returned from this module are projected (x, y)
meters unless a name says lonlat.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

from pyproj import Transformer
from shapely import STRtree
from shapely.geometry import LineString, Point
from shapely.ops import substring

from shapesnap.graph import ModeGraph

__all__ = [
    "Candidate",
    "MatchGraph",
    "RouteMatcher",
    "Station",
    "StationIndex",
    "load_stations",
    "utm_epsg_for",
]


def utm_epsg_for(lon: float, lat: float) -> int:
    """EPSG code of the UTM zone containing (lon, lat)."""
    zone = min(60, max(1, int((lon + 180.0) // 6.0) + 1))
    return (32600 if lat >= 0 else 32700) + zone


@dataclass(slots=True)
class Candidate:
    """A directed-edge position hypothesis for one observation."""

    edge: int          # index into MatchGraph.graph.edges
    dir: int           # +1 = from_node->to_node, -1 = reverse
    offset: float      # meters along the DIRECTED edge geometry
    x: float           # snap point (projected)
    y: float
    dist: float        # observation -> snap point distance (m)
    bonus: float = 0.0  # emission cost subtraction (regime B name/ref bonuses)


class MatchGraph:
    """Projected, indexed view of a ModeGraph for matching."""

    def __init__(self, graph: ModeGraph, epsg: int | None = None):
        self.graph = graph
        if not graph.edges:
            raise ValueError("empty graph")
        if epsg is None:
            lons = [c[0] for c in graph.nodes.values()]
            lats = [c[1] for c in graph.nodes.values()]
            epsg = utm_epsg_for(sum(lons) / len(lons), sum(lats) / len(lats))
        self.epsg = epsg
        self.to_utm = Transformer.from_crs(4326, epsg, always_xy=True)
        self.to_wgs = Transformer.from_crs(epsg, 4326, always_xy=True)

        # project every edge geometry once (forward orientation)
        self.lines: list[LineString] = []
        self.lengths: list[float] = []
        for e in graph.edges:
            xs, ys = self.to_utm.transform(
                [c[0] for c in e.geometry], [c[1] for c in e.geometry]
            )
            line = LineString(list(zip(xs, ys)))
            self.lines.append(line)
            self.lengths.append(line.length)
        self.tree = STRtree(self.lines)

        # adjacency: node -> [(edge_idx, dir), ...] of DEPARTING directed edges
        self.out_edges: dict[int, list[tuple[int, int]]] = {}
        self.degree: dict[int, int] = {}
        for i, e in enumerate(graph.edges):
            self.degree[e.from_node] = self.degree.get(e.from_node, 0) + 1
            self.degree[e.to_node] = self.degree.get(e.to_node, 0) + 1
            if e.oneway in (0, 1):
                self.out_edges.setdefault(e.from_node, []).append((i, 1))
            if e.oneway in (0, -1):
                self.out_edges.setdefault(e.to_node, []).append((i, -1))

        # turn restrictions by via node
        self.restrictions_at: dict[int, list] = {}
        for r in graph.restrictions:
            self.restrictions_at.setdefault(r.via_node, []).append(r)

    # ── directed-edge helpers ────────────────────────────────────────────────

    def start_node(self, e: int, d: int) -> int:
        edge = self.graph.edges[e]
        return edge.from_node if d == 1 else edge.to_node

    def end_node(self, e: int, d: int) -> int:
        edge = self.graph.edges[e]
        return edge.to_node if d == 1 else edge.from_node

    def fwd_offset(self, e: int, d: int, off: float) -> float:
        """Directed offset -> offset along the stored (forward) geometry."""
        return off if d == 1 else self.lengths[e] - off

    def dir_substring(self, e: int, d: int, a: float, b: float) -> list:
        """Coords of the directed edge between directed offsets a..b (a<=b)."""
        seg = substring(self.lines[e], self.fwd_offset(e, d, a), self.fwd_offset(e, d, b))
        if seg.is_empty:
            return []
        if seg.geom_type == "Point":
            return [(seg.x, seg.y)]
        return list(seg.coords)

    def dir_coords(self, e: int, d: int) -> list:
        coords = list(self.lines[e].coords)
        return coords if d == 1 else coords[::-1]

    def entry_vec(self, e: int, d: int) -> tuple[float, float]:
        """Unit vector of the first segment of the directed geometry."""
        c = self.dir_coords(e, d)
        return _unit(c[0], c[1] if len(c) > 1 else c[0])

    def exit_vec(self, e: int, d: int) -> tuple[float, float]:
        """Unit vector of the last segment of the directed geometry."""
        c = self.dir_coords(e, d)
        return _unit(c[-2] if len(c) > 1 else c[-1], c[-1])

    def is_stub(self, node: int) -> bool:
        """True when the node is a dead end (a genuine reversal point)."""
        return self.degree.get(node, 0) <= 1

    # ── candidate lookup ─────────────────────────────────────────────────────

    def candidates(self, x: float, y: float, radius: float, k: int) -> list[Candidate]:
        """k-nearest edges within radius, expanded to directed candidates."""
        pt = Point(x, y)
        idxs = self.tree.query(pt, predicate="dwithin", distance=radius)
        scored = []
        for i in idxs:
            i = int(i)
            line = self.lines[i]
            fwd = line.project(pt)
            snap = line.interpolate(fwd)
            dist = snap.distance(pt)
            if dist <= radius:
                scored.append((dist, i, fwd, snap))
        scored.sort(key=lambda t: t[0])
        out: list[Candidate] = []
        for dist, i, fwd, snap in scored[:k]:
            edge = self.graph.edges[i]
            dirs = (1,) if edge.oneway == 1 else (-1,) if edge.oneway == -1 else (1, -1)
            for d in dirs:
                off = fwd if d == 1 else self.lengths[i] - fwd
                out.append(Candidate(i, d, off, snap.x, snap.y, dist))
        return out

    def project_lonlat(self, coords) -> list:
        xs, ys = self.to_utm.transform([c[0] for c in coords], [c[1] for c in coords])
        return list(zip(xs, ys))

    def unproject(self, coords) -> list:
        lons, lats = self.to_wgs.transform([c[0] for c in coords], [c[1] for c in coords])
        return [(round(lon, 7), round(lat, 7)) for lon, lat in zip(lons, lats)]


def _unit(a, b) -> tuple[float, float]:
    dx, dy = b[0] - a[0], b[1] - a[1]
    n = math.hypot(dx, dy)
    return (dx / n, dy / n) if n > 0 else (0.0, 0.0)


# ── GTFS route ↔ OSM route relation matching ─────────────────────────────────


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().casefold())


def _norm_color(s: str | None) -> str:
    return _norm(s).lstrip("#")


class RouteMatcher:
    """Does an edge's OSM route relations match this GTFS route?

    Rules (docs/transit-pipeline-v3.md stage 3, all case/whitespace-
    insensitive; empty GTFS values never match):
      1. relation ref == route_short_name
      2. relation name contains route_long_name, or contains
         route_short_name as a whole word (word-bounded so route "2"
         doesn't match "22 Clark")
      3. relation colour == route_color (leading '#' stripped)
    """

    def __init__(self, short_name: str | None, long_name: str | None, color: str | None):
        self.short = _norm(short_name)
        self.long = _norm(long_name)
        self.color = _norm_color(color)
        self._short_re = (
            re.compile(r"(?<!\w)" + re.escape(self.short) + r"(?!\w)")
            if self.short
            else None
        )
        self._memo: dict[int, bool] = {}

    def matches_edge(self, edge_idx: int, edge) -> bool:
        hit = self._memo.get(edge_idx)
        if hit is None:
            hit = any(self._match_ref(r) for r in edge.route_refs)
            self._memo[edge_idx] = hit
        return hit

    def _match_ref(self, r: dict) -> bool:
        ref, name, colour = _norm(r.get("ref")), _norm(r.get("name")), _norm_color(r.get("colour"))
        if self.short and ref == self.short:
            return True
        if name:
            if self.long and self.long in name:
                return True
            if self._short_re is not None and self._short_re.search(name):
                return True
        if self.color and colour and colour == self.color:
            return True
        return False


# ── OSM stations (regime B bonuses) ──────────────────────────────────────────

STATION_RAIL = {"station", "halt", "stop", "tram_stop", "subway_entrance"}
STATION_PT = {"station", "stop_position", "platform"}


@dataclass(slots=True)
class Station:
    lon: float
    lat: float
    name: str
    ref: str  # platform / stop ref, may be ""


def load_stations(pbf_path, mode: str) -> list[Station]:
    """Single node pass over the pbf: named stations/stops for the mode."""
    import osmium

    keep: list[Station] = []
    fp = osmium.FileProcessor(str(pbf_path), osmium.osm.NODE).with_filter(
        osmium.filter.KeyFilter("railway", "public_transport", "highway")
    )
    for n in fp:
        tags = n.tags
        if mode == "rail":
            hit = tags.get("railway") in STATION_RAIL or tags.get("public_transport") in STATION_PT
        elif mode == "bus":
            hit = tags.get("highway") == "bus_stop" or tags.get("public_transport") in STATION_PT
        else:  # ferry
            hit = tags.get("amenity") == "ferry_terminal"
        if not hit:
            continue
        name = tags.get("name") or ""
        ref = tags.get("ref") or tags.get("local_ref") or ""
        if not name and not ref:
            continue
        loc = n.location
        if loc.valid():
            keep.append(Station(loc.lon, loc.lat, name, ref))
    return keep


_TOKEN_RE = re.compile(r"[a-z0-9]+")
_STOPWORDS = {"station", "stop", "platform", "cta", "metra", "amtrak", "and", "the"}


def name_tokens(name: str) -> frozenset:
    return frozenset(t for t in _TOKEN_RE.findall(_norm(name)) if t not in _STOPWORDS)


def cheap_ted(a: str, b: str, cap: int = 3) -> int:
    """Banded Levenshtein distance, capped at `cap` (returns cap+1 on miss)."""
    if abs(len(a) - len(b)) > cap:
        return cap + 1
    if a == b:
        return 0
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i] + [0] * len(b)
        lo = cap + 1
        for j, cb in enumerate(b, 1):
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb))
            lo = min(lo, cur[j])
        if lo > cap:
            return cap + 1
        prev = cur
    return min(prev[-1], cap + 1)


def name_similarity(gtfs_name: str, osm_name: str) -> float:
    """1.0 on token-subset match, else scaled cheap-TED similarity, else 0."""
    ta, tb = name_tokens(gtfs_name), name_tokens(osm_name)
    if not ta or not tb:
        return 0.0
    if ta <= tb or tb <= ta:
        return 1.0
    a, b = " ".join(sorted(ta)), " ".join(sorted(tb))
    d = cheap_ted(a, b)
    if d <= 3:
        return max(0.0, 1.0 - d / max(len(a), len(b)))
    return 0.0


class StationIndex:
    """Projected STRtree over stations for bonuses + passed-station counts."""

    def __init__(self, stations: list[Station], mg: MatchGraph):
        self.stations = stations
        self.points = [
            Point(xy) for xy in mg.project_lonlat([(s.lon, s.lat) for s in stations])
        ] if stations else []
        self.tree = STRtree(self.points) if self.points else None

    def near(self, geom, radius: float) -> list[int]:
        if self.tree is None:
            return []
        return [int(i) for i in self.tree.query(geom, predicate="dwithin", distance=radius)]

    def best_name_bonus(
        self, x: float, y: float, stop_name: str, platform: str, radius: float
    ) -> float:
        """Max(name similarity, platform-ref match) over stations near (x, y)."""
        best = 0.0
        pt = Point(x, y)
        for i in self.near(pt, radius):
            s = self.stations[i]
            if stop_name and s.name:
                best = max(best, name_similarity(stop_name, s.name))
            if platform and s.ref and _norm(platform) == _norm(s.ref):
                best = max(best, 1.0)
        return best

    def count_passed(self, path_line: LineString, radius: float, exclude_xy: list) -> int:
        """Stations within `radius` of the path, minus ones near endpoints."""
        if self.tree is None or path_line.is_empty:
            return 0
        n = 0
        for i in self.near(path_line, radius):
            p = self.points[i]
            if any(p.distance(Point(xy)) < 2 * radius for xy in exclude_xy):
                continue
            n += 1
        return n
