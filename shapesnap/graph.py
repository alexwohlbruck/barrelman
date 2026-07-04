#!/usr/bin/env python3
"""shapesnap.graph — per-mode OSM matching graph extraction.

Builds a mode-class (rail | bus | ferry) matching graph from an OSM .pbf.
Graph nodes are way endpoints plus OSM nodes used by >=2 kept way vertices
(junctions); each edge is the way segment between two consecutive graph
nodes and carries the FULL intermediate vertex geometry.

The load-bearing property: topology comes exclusively from shared OSM node
ids — never from geometry. Elevated (bridge / layer>=1) and subway
(tunnel / layer<0) ways never share node ids except at real portals, so
vertical separation (the Chicago Loop exam) is structural, not heuristic.
No geometric noding, no geo_places linestrings.

Mode tag policy (docs/transit-pipeline-v3.md, stage 3):
  rail  — railway in {rail, subway, light_rail, tram, narrow_gauge,
          funicular} or embedded_rails=*; service crossover/siding/spur
          penalty 1.75, yard 4.0, other service 2.0; usage
          industrial/military/tourism x2.0; EXCLUDES railway in
          {abandoned, razed, disused, proposed, construction, platform}
          and any razed:railway / construction:railway / proposed:railway.
  bus   — highway in the drivable set + all *_link; service
          parking_aisle/driveway dropped unless bus/psv open; access
          no|private re-opened by bus|psv|public_transport in
          {yes, designated}; oneway honored unless oneway:bus/psv=no.
  ferry — ways tagged route=ferry + member ways of type=route +
          route=ferry relations.

Route relations (type=route, route matching the mode class) contribute
route_refs {ref, name, colour} to every member track way's edges.
Turn restrictions (type=restriction) are stored as
(via_node, from_way, to_way, kind, applies_to_psv) with except=psv/bus
honored.

Cache: gzipped pickle of the ModeGraph dataclass, stamped with the source
pbf's mtime+size for staleness checks.

CLI (repo convention — uv, never system python):
  uv run --with osmium python -m shapesnap.graph \
      --pbf data/region.osm.pbf --mode rail
  # optional: --bbox minlon,minlat,maxlon,maxlat   (feed bbox + margin)
  #           --out <cache-path>                    (default data/shapesnap/)
  #           --postgis [DSN]                       (QA dump into mm_edges)
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import pickle
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

# osmium is imported lazily inside the graph-BUILDING functions
# (scan_relations / _way_processor) so that lightweight consumers — the
# display-geometry regular-service predicate (is_regular_service_track) and
# the tag-policy constants — can import this module without the heavy osmium
# dependency (segments/lineorder requirements omit it).

FORMAT_VERSION = 1
MODE_CLASSES = ("rail", "bus", "ferry")

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PBF = REPO_ROOT / "data" / "region.osm.pbf"
DEFAULT_DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)

# ── tag policy ───────────────────────────────────────────────────────────────

RAIL_INCLUDE = {"rail", "subway", "light_rail", "tram", "narrow_gauge", "funicular"}
RAIL_EXCLUDE = {"abandoned", "razed", "disused", "proposed", "construction", "platform"}
RAIL_LIFECYCLE_KEYS = ("razed:railway", "construction:railway", "proposed:railway")
RAIL_SERVICE_PENALTY = {"crossover": 1.75, "siding": 1.75, "spur": 1.75, "yard": 4.0}
RAIL_SERVICE_DEFAULT = 2.0  # unknown service=* values: conservative
RAIL_USAGE_PENALTY = {"industrial": 2.0, "military": 2.0, "tourism": 2.0}

# DISPLAY-GEOMETRY track set (regular-service predicate). A "regular-service"
# track is one a train in revenue service actually runs on for through
# travel — the mainline. Non-running tracks (yards, sidings, spurs,
# crossovers; industrial/military/tourism usage) are PENALIZED-BUT-AVAILABLE
# in matching (trains do use crossovers to reverse at terminals — see
# RAIL_SERVICE_PENALTY), but they must NOT be ground truth for DISPLAY
# geometry: a yard's fan of parallel tracks pulls a pair/family midline and
# a reconciliation snap toward track no service rides, and real transit
# diagrams ignore them. is_regular_service_track() is the single predicate
# for that filter (reconcile_offtrack_corridors, the pair/platform midline,
# and the track-fidelity ground truth). This is a display filter, NOT a
# matching change — the classify_rail penalties above are untouched.
NON_REGULAR_SERVICE_VALUES = ("yard", "siding", "spur", "crossover")
NON_REGULAR_USAGE_VALUES = ("industrial", "military", "tourism")

BUS_HIGHWAY = {
    "motorway", "trunk", "primary", "secondary", "tertiary", "unclassified",
    "residential", "living_street", "service", "busway", "bus_guideway",
}
BUS_SERVICE_DROP = {"parking_aisle", "driveway"}
OPEN_ACCESS = {"yes", "designated"}

ROUTE_RELATION_MODES = {
    "rail": {"train", "subway", "light_rail", "tram", "railway"},
    "bus": {"bus", "trolleybus"},
    "ferry": {"ferry"},
}
PSV_EXCEPT = {"psv", "bus", "minibus", "coach"}
# route-relation member roles that are not part of the travelled path
NON_TRACK_ROLE_PREFIXES = ("platform", "stop")

# minimal tags carried onto edges (QA + matcher costs)
KEEP_TAGS = (
    "layer", "bridge", "tunnel", "service", "usage", "ref",
    "name", "highway", "railway", "route", "embedded_rails",
)

# ── data model ───────────────────────────────────────────────────────────────


@dataclass(slots=True)
class Edge:
    edge_id: int
    from_node: int          # OSM node id (graph node)
    to_node: int            # OSM node id (graph node)
    way_id: int
    mode: str
    geometry: list          # [(lon, lat), ...] full intermediate vertices
    length_m: float
    oneway: int             # 0 = both, 1 = from->to only, -1 = to->from only
    class_penalty: float
    tags: dict              # minimal tag subset (KEEP_TAGS)
    route_refs: list = field(default_factory=list)  # [{ref, name, colour}]


@dataclass(slots=True)
class TurnRestriction:
    via_node: int
    from_way: int
    to_way: int
    kind: str               # e.g. no_left_turn, only_straight_on
    applies_to_psv: bool    # False when except=psv/bus/... exempts buses


@dataclass(slots=True)
class ModeGraph:
    format_version: int
    mode: str
    source_path: str
    source_mtime: float
    source_size: int
    bbox: tuple | None      # (minlon, minlat, maxlon, maxlat) crop or None
    build_seconds: float
    nodes: dict             # graph node id -> (lon, lat)
    edges: list             # [Edge]
    restrictions: list      # [TurnRestriction]

    def adjacency(self) -> dict:
        """node id -> list of edge indices touching it (both directions)."""
        adj: dict = {}
        for i, e in enumerate(self.edges):
            adj.setdefault(e.from_node, []).append(i)
            adj.setdefault(e.to_node, []).append(i)
        return adj


# ── geometry helpers ─────────────────────────────────────────────────────────

_EARTH_R = 6371008.8


def haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * _EARTH_R * math.asin(math.sqrt(a))


def line_length_m(coords) -> float:
    return sum(
        haversine_m(a[0], a[1], b[0], b[1]) for a, b in zip(coords, coords[1:])
    )


# ── per-mode way classification ──────────────────────────────────────────────


def classify_rail(tags) -> dict | None:
    for k in RAIL_LIFECYCLE_KEYS:
        if k in tags:
            return None
    rw = tags.get("railway")
    if rw in RAIL_EXCLUDE:
        return None
    embedded = tags.get("embedded_rails")
    if rw not in RAIL_INCLUDE and not (embedded and embedded != "no"):
        return None
    penalty = 1.0
    service = tags.get("service")
    if service:
        penalty *= RAIL_SERVICE_PENALTY.get(service, RAIL_SERVICE_DEFAULT)
    usage = tags.get("usage")
    if usage in RAIL_USAGE_PENALTY:
        penalty *= RAIL_USAGE_PENALTY[usage]
    # rail is traversed both ways for matching purposes
    return {"penalty": penalty, "oneway": 0}


def is_regular_service_track(tags) -> bool:
    """True when a rail way is REGULAR-SERVICE track (mainline a train runs
    on in revenue service) rather than a yard/siding/spur/crossover or an
    industrial/military/tourism way.

    Display-geometry predicate ONLY (reconciliation snap target, pair/platform
    midline, track-fidelity ground truth) — matching still SEES service tracks
    (penalized). `tags` is any dict-like with OSM keys (an Edge.tags subset,
    a geo_places tags jsonb, an mm_edges row): a missing key means the way
    carries no such qualifier, i.e. it is regular service.
    """
    if tags.get("service") in NON_REGULAR_SERVICE_VALUES:
        return False
    if tags.get("usage") in NON_REGULAR_USAGE_VALUES:
        return False
    return True


def _bus_open(tags) -> bool:
    return (
        tags.get("bus") in OPEN_ACCESS
        or tags.get("psv") in OPEN_ACCESS
        or tags.get("public_transport") in OPEN_ACCESS
    )


def classify_bus(tags) -> dict | None:
    hw = tags.get("highway")
    if not hw:
        return None
    if hw not in BUS_HIGHWAY and not hw.endswith("_link"):
        return None
    if tags.get("service") in BUS_SERVICE_DROP and not _bus_open(tags):
        return None
    if tags.get("access") in ("no", "private") and not _bus_open(tags):
        return None
    # directionality: oneway:bus / oneway:psv override plain oneway
    override = tags.get("oneway:bus") or tags.get("oneway:psv")
    ow = tags.get("oneway")
    if override == "no":
        oneway = 0
    elif override in ("yes", "true", "1"):
        oneway = 1
    elif override == "-1":
        oneway = -1  # reversed contraflow: buses ride against the way
    elif ow in ("yes", "true", "1"):
        oneway = 1
    elif ow == "-1":
        oneway = -1
    elif ow in ("no", "0", "false"):
        oneway = 0
    elif tags.get("junction") in ("roundabout", "circular"):
        oneway = 1
    else:
        oneway = 0
    return {"penalty": 1.0, "oneway": oneway}


def classify_ferry(tags, way_id: int, ferry_member_ways: set) -> dict | None:
    if tags.get("route") == "ferry" or way_id in ferry_member_ways:
        return {"penalty": 1.0, "oneway": 0}
    return None


# ── pass 1: relations (route refs, ferry members, turn restrictions) ─────────


def _is_track_role(role: str) -> bool:
    role = role or ""
    return not role.startswith(NON_TRACK_ROLE_PREFIXES)


def scan_relations(pbf_path: str, mode: str):
    """One pass over relations.

    Returns (way_routes, ferry_member_ways, raw_restrictions):
      way_routes:        way id -> [{ref, name, colour}, ...]
      ferry_member_ways: way ids belonging to type=route+route=ferry relations
      raw_restrictions:  [(via_node, from_way, to_way, kind, applies_to_psv)]
    """
    import osmium

    route_values = ROUTE_RELATION_MODES[mode]
    way_routes: dict = {}
    ferry_member_ways: set = set()
    raw_restrictions: list = []

    for rel in osmium.FileProcessor(str(pbf_path), osmium.osm.RELATION):
        tags = rel.tags
        rel_type = tags.get("type")
        if rel_type == "route":
            route = tags.get("route")
            if route not in route_values:
                continue
            info = {
                "ref": tags.get("ref"),
                "name": tags.get("name"),
                "colour": tags.get("colour") or tags.get("color"),
            }
            for m in rel.members:
                if m.type != "w" or not _is_track_role(m.role):
                    continue
                way_routes.setdefault(m.ref, []).append(info)
                if mode == "ferry":
                    ferry_member_ways.add(m.ref)
        elif rel_type == "restriction":
            kind = (
                tags.get("restriction")
                or tags.get("restriction:bus")
                or tags.get("restriction:psv")
            )
            if not kind:
                continue
            except_modes = {
                v.strip() for v in (tags.get("except") or "").split(";") if v.strip()
            }
            applies_to_psv = not (PSV_EXCEPT & except_modes)
            from_way = to_way = via_node = None
            for m in rel.members:
                if m.role == "from" and m.type == "w" and from_way is None:
                    from_way = m.ref
                elif m.role == "to" and m.type == "w" and to_way is None:
                    to_way = m.ref
                elif m.role == "via" and m.type == "n" and via_node is None:
                    via_node = m.ref
            # via-way restrictions are skipped (rare; matcher handles via-node only)
            if from_way is not None and to_way is not None and via_node is not None:
                raw_restrictions.append(
                    (via_node, from_way, to_way, kind, applies_to_psv)
                )

    return way_routes, ferry_member_ways, raw_restrictions


# ── pass 2: ways (tag filter + locations) ────────────────────────────────────

# C++-side prefilters so the python loop only sees candidate ways
_MODE_KEY_FILTER = {
    "rail": ("railway", "embedded_rails"),
    "bus": ("highway",),
    "ferry": ("route",),
}


def _way_processor(pbf_path: str, keys=None, ids=None):
    import osmium

    # NODE must be in the entity mask for the location cache; the cache handler
    # runs BEFORE the filter chain (see pyosmium FileProcessor.__iter__), so an
    # EntityFilter then drops nodes at C++ speed and python only sees ways.
    fp = osmium.FileProcessor(
        str(pbf_path), osmium.osm.WAY | osmium.osm.NODE
    ).with_locations()
    fp = fp.with_filter(osmium.filter.EntityFilter(osmium.osm.WAY))
    if keys:
        fp = fp.with_filter(osmium.filter.KeyFilter(*keys))
    if ids is not None:
        fp = fp.with_filter(osmium.filter.IdFilter(ids))
    return fp


def _collect_way(way, meta, bbox, kept, bad_loc_counter):
    refs, coords = [], []
    for n in way.nodes:
        loc = n.location
        if not loc.valid():
            bad_loc_counter[0] += 1
            return
        refs.append(n.ref)
        coords.append((round(loc.lon, 7), round(loc.lat, 7)))
    if len(refs) < 2:
        return
    if bbox is not None:
        minlon, minlat, maxlon, maxlat = bbox
        if not any(
            minlon <= lon <= maxlon and minlat <= lat <= maxlat
            for lon, lat in coords
        ):
            return
    tags = way.tags
    minimal = {k: tags[k] for k in KEEP_TAGS if k in tags}
    kept.append((way.id, refs, coords, meta, minimal))


def scan_ways(pbf_path: str, mode: str, ferry_member_ways: set, bbox):
    """Collect kept ways: [(way_id, node_refs, coords, meta, minimal_tags)]."""
    kept: list = []
    bad_loc = [0]
    seen: set = set()

    if mode == "rail":
        for w in _way_processor(pbf_path, keys=_MODE_KEY_FILTER["rail"]):
            meta = classify_rail(w.tags)
            if meta is not None:
                _collect_way(w, meta, bbox, kept, bad_loc)
    elif mode == "bus":
        for w in _way_processor(pbf_path, keys=_MODE_KEY_FILTER["bus"]):
            meta = classify_bus(w.tags)
            if meta is not None:
                _collect_way(w, meta, bbox, kept, bad_loc)
    elif mode == "ferry":
        for w in _way_processor(pbf_path, keys=_MODE_KEY_FILTER["ferry"]):
            meta = classify_ferry(w.tags, w.id, ferry_member_ways)
            if meta is not None:
                seen.add(w.id)
                _collect_way(w, meta, bbox, kept, bad_loc)
        remaining = sorted(ferry_member_ways - seen)
        if remaining:
            for w in _way_processor(pbf_path, ids=remaining):
                meta = classify_ferry(w.tags, w.id, ferry_member_ways)
                if meta is not None:
                    _collect_way(w, meta, bbox, kept, bad_loc)
    else:
        raise ValueError(f"unknown mode {mode!r}")

    return kept, bad_loc[0]


# ── graph assembly ───────────────────────────────────────────────────────────


def build_graph(pbf_path, mode: str, bbox=None) -> ModeGraph:
    """Build the per-mode matching graph from an OSM pbf (or .osm XML)."""
    if mode not in MODE_CLASSES:
        raise ValueError(f"mode must be one of {MODE_CLASSES}, got {mode!r}")
    if bbox is not None:
        # mirror the CLI's _parse_bbox: a degenerate/inverted bbox would
        # silently drop every way and return an empty graph
        try:
            minlon, minlat, maxlon, maxlat = (float(v) for v in bbox)
        except (TypeError, ValueError):
            raise ValueError(
                f"bbox must be 4 numbers (minlon, minlat, maxlon, maxlat), got {bbox!r}"
            ) from None
        if not (minlon < maxlon and minlat < maxlat):
            raise ValueError(f"bbox min must be < max, got {bbox!r}")
        bbox = (minlon, minlat, maxlon, maxlat)
    pbf_path = Path(pbf_path)
    if not pbf_path.exists():
        raise FileNotFoundError(pbf_path)
    t0 = time.perf_counter()

    way_routes, ferry_member_ways, raw_restrictions = scan_relations(pbf_path, mode)
    kept, bad_loc = scan_ways(pbf_path, mode, ferry_member_ways, bbox)

    # graph nodes = way endpoints + nodes used >= 2 times across kept ways
    # (occurrence counting also catches self-intersections within one way)
    use_count: Counter = Counter()
    for _, refs, _, _, _ in kept:
        use_count.update(refs)

    nodes: dict = {}
    edges: list = []
    kept_way_ids: set = set()
    edge_id = 0
    for way_id, refs, coords, meta, minimal in kept:
        kept_way_ids.add(way_id)
        routes = way_routes.get(way_id)
        if routes:
            dedup, route_refs = set(), []
            for r in routes:
                key = (r["ref"], r["name"], r["colour"])
                if key not in dedup:
                    dedup.add(key)
                    route_refs.append(dict(r))
        else:
            route_refs = []
        last = 0
        end = len(refs) - 1
        for i in range(1, end + 1):
            if i == end or use_count[refs[i]] >= 2:
                geom = coords[last : i + 1]
                a, b = refs[last], refs[i]
                nodes[a] = coords[last]
                nodes[b] = coords[i]
                edges.append(
                    Edge(
                        edge_id=edge_id,
                        from_node=a,
                        to_node=b,
                        way_id=way_id,
                        mode=mode,
                        geometry=geom,
                        length_m=line_length_m(geom),
                        oneway=meta["oneway"],
                        class_penalty=meta["penalty"],
                        tags=minimal,
                        # own copy per edge: a consumer mutating one edge's
                        # list must not mutate its way siblings'
                        route_refs=list(route_refs),
                    )
                )
                edge_id += 1
                last = i

    restrictions = [
        TurnRestriction(via, frm, to, kind, psv)
        for (via, frm, to, kind, psv) in raw_restrictions
        if frm in kept_way_ids and to in kept_way_ids and via in use_count
    ]

    stat = pbf_path.stat()
    graph = ModeGraph(
        format_version=FORMAT_VERSION,
        mode=mode,
        source_path=str(pbf_path),
        source_mtime=stat.st_mtime,
        source_size=stat.st_size,
        bbox=tuple(bbox) if bbox else None,
        build_seconds=time.perf_counter() - t0,
        nodes=nodes,
        edges=edges,
        restrictions=restrictions,
    )
    if bad_loc:
        print(
            f"[shapesnap.graph] warning: dropped {bad_loc} ways with missing "
            "node locations (extract boundary)",
            file=sys.stderr,
        )
    return graph


def connected_components(edges) -> list:
    """Union-find over edge endpoints. Returns list of node-id sets."""
    parent: dict = {}

    def find(x):
        root = x
        while parent.setdefault(root, root) != root:
            root = parent[root]
        while parent[x] != root:  # path compression
            parent[x], x = root, parent[x]
        return root

    for e in edges:
        ra, rb = find(e.from_node), find(e.to_node)
        if ra != rb:
            parent[rb] = ra

    groups: dict = {}
    for node in parent:
        groups.setdefault(find(node), set()).add(node)
    return list(groups.values())


# ── cache io ─────────────────────────────────────────────────────────────────


def save_graph(graph: ModeGraph, out_path) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with gzip.open(tmp, "wb", compresslevel=6) as f:
        pickle.dump(graph, f, protocol=pickle.HIGHEST_PROTOCOL)
    tmp.replace(out_path)


def load_graph(cache_path, expect_pbf=None) -> ModeGraph:
    """Load a cached graph. If expect_pbf is given, raise on staleness.

    Unreadable caches (corrupt gzip, truncated pickle, dataclasses pickled
    under __main__.* by the pre-f0933b8 CLI) raise ValueError, never an
    opaque unpickling error — callers treat ValueError as "rebuild".
    """
    try:
        with gzip.open(cache_path, "rb") as f:
            graph = pickle.load(f)
    except FileNotFoundError:
        raise
    except Exception as err:
        raise ValueError(
            f"cache {cache_path} unreadable ({type(err).__name__}: {err}); rebuild"
        ) from err
    version = getattr(graph, "format_version", None)
    if version != FORMAT_VERSION:
        raise ValueError(
            f"cache format {version} != current {FORMAT_VERSION}; rebuild"
        )
    if expect_pbf is not None and is_stale(graph, expect_pbf):
        raise ValueError(f"cache {cache_path} is stale relative to {expect_pbf}; rebuild")
    return graph


def is_stale(graph: ModeGraph, pbf_path=None) -> bool:
    path = Path(pbf_path or graph.source_path)
    if not path.exists():
        return True
    stat = path.stat()
    return stat.st_size != graph.source_size or stat.st_mtime != graph.source_mtime


def default_cache_path(pbf_path, mode: str) -> Path:
    pbf_path = Path(pbf_path)
    stem = pbf_path.name
    for suffix in (".osm.pbf", ".osm", ".pbf"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break
    return pbf_path.parent / "shapesnap" / f"{stem}.{mode}.graph.pkl.gz"


# ── optional PostGIS QA dump ─────────────────────────────────────────────────


def dump_postgis(graph: ModeGraph, dsn: str, table: str = "mm_edges") -> int:
    """Write edges into an additive QA table for visual inspection."""
    import psycopg  # optional dep: uv run --with "psycopg[binary]"

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {table} (
                    edge_id bigint NOT NULL,
                    mode text NOT NULL,
                    geom geometry(LineString, 4326),
                    tags jsonb,
                    PRIMARY KEY (mode, edge_id)
                )"""
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS {table}_geom_idx ON {table} USING gist (geom)"
        )
        cur.execute(f"DELETE FROM {table} WHERE mode = %s", (graph.mode,))
        with cur.copy(
            f"COPY {table} (edge_id, mode, geom, tags) FROM STDIN"
        ) as copy:
            for e in graph.edges:
                ewkt = (
                    "SRID=4326;LINESTRING("
                    + ",".join(f"{lon} {lat}" for lon, lat in e.geometry)
                    + ")"
                )
                tags = {
                    **e.tags,
                    "way_id": e.way_id,
                    "from_node": e.from_node,
                    "to_node": e.to_node,
                    "oneway": e.oneway,
                    "class_penalty": e.class_penalty,
                    "length_m": round(e.length_m, 1),
                    "route_refs": e.route_refs,
                }
                copy.write_row((e.edge_id, e.mode, ewkt, json.dumps(tags)))
        conn.commit()
    return len(graph.edges)


# ── cli ──────────────────────────────────────────────────────────────────────


def _parse_bbox(text: str):
    parts = [float(p) for p in text.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("bbox must be minlon,minlat,maxlon,maxlat")
    minlon, minlat, maxlon, maxlat = parts
    if minlon >= maxlon or minlat >= maxlat:
        raise argparse.ArgumentTypeError("bbox min must be < max")
    return (minlon, minlat, maxlon, maxlat)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m shapesnap.graph",
        description="Extract a per-mode OSM matching graph and cache it.",
    )
    ap.add_argument(
        "--pbf", type=Path, default=DEFAULT_PBF,
        help=f"OSM extract (default: {DEFAULT_PBF})",
    )
    ap.add_argument("--mode", required=True, choices=MODE_CLASSES)
    ap.add_argument(
        "--out", type=Path, default=None,
        help="cache path (default: <pbf dir>/shapesnap/<region>.<mode>.graph.pkl.gz)",
    )
    ap.add_argument(
        "--bbox", type=_parse_bbox, default=None,
        help="crop: minlon,minlat,maxlon,maxlat (keep ways with any vertex inside)",
    )
    ap.add_argument(
        "--postgis", nargs="?", const=DEFAULT_DSN, default=None, metavar="DSN",
        help="also dump edges to the mm_edges QA table (default DSN from "
             "DATABASE_URL or the barrelman dev DB)",
    )
    args = ap.parse_args(argv)

    if not args.pbf.exists():
        ap.error(f"pbf not found: {args.pbf}")
    out = args.out or default_cache_path(args.pbf, args.mode)

    print(f"[shapesnap.graph] building {args.mode} graph from {args.pbf}", flush=True)
    graph = build_graph(args.pbf, args.mode, bbox=args.bbox)
    save_graph(graph, out)

    n_routes = sum(1 for e in graph.edges if e.route_refs)
    total_km = sum(e.length_m for e in graph.edges) / 1000.0
    print(
        f"[shapesnap.graph] {args.mode}: {len(graph.nodes)} nodes, "
        f"{len(graph.edges)} edges ({total_km:.1f} km), "
        f"{n_routes} edges with route refs, "
        f"{len(graph.restrictions)} turn restrictions "
        f"in {graph.build_seconds:.1f}s"
    )
    print(f"[shapesnap.graph] cache: {out} ({out.stat().st_size / 1e6:.1f} MB)")

    if args.postgis:
        n = dump_postgis(graph, args.postgis)
        print(f"[shapesnap.graph] postgis: {n} rows into mm_edges (mode={args.mode})")
    return 0


if __name__ == "__main__":
    # Re-import ourselves under the canonical module name so pickled
    # dataclasses reference shapesnap.graph.* (not __main__.*) and caches
    # written by the CLI stay loadable from any entry point.
    from shapesnap.graph import main as _main

    sys.exit(_main())
