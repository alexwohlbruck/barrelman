"""lineorder.model — optimization-graph model + PostGIS loader.

The ordering graph ("line graph" in the TSAS 2019 paper, "optimization
graph" once reduced) is deliberately geometry-free: an edge is an
unordered set of line ids between two nodes, a node carries its incident
edges in CLOCKWISE angular order (derived from edge geometry bearings at
load time, or from planar coordinates in synthetic tests), and every
node points at its ORIGINAL line-graph node v* — crossing/separation
weights are always evaluated on v* (paper section 4.1).

A *solution* for a graph is {edge_id: tuple(line ids)} — the left-to-right
order of the lines for a traveler moving along the edge's stored
geometric direction u -> v.

Loader: `load_build(build_key, dsn)` reads transit_graph_nodes/edges/
edge_lines and recovers topology by exact endpoint/node coordinate match
(linegraph.emit writes both with %.7f, verified exact for chicago:l-v3).
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field, replace

DEFAULT_DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)


# ---------------------------------------------------------------- lines

@dataclass(frozen=True)
class Line:
    """A transit line: (feed_id, route_id) plus display attributes."""
    uid: int
    feed_id: str
    route_id: str
    short_name: str = ""
    color: str = ""
    text_color: str = ""
    route_type: int | None = None


@dataclass(frozen=True)
class PseudoLine:
    """P2 partner collapse product: a block of lines with a fixed internal
    order (Lemma 4.1: any relative order is globally optimal)."""
    uid: int
    members: tuple[int, ...]  # real line uids, canonical block order


class LineRegistry:
    def __init__(self):
        self._by_uid: dict[int, Line | PseudoLine] = {}
        self._next = 0

    def add_line(self, **kw) -> Line:
        ln = Line(uid=self._next, **kw)
        self._by_uid[ln.uid] = ln
        self._next += 1
        return ln

    def add_pseudo(self, members: tuple[int, ...]) -> PseudoLine:
        pl = PseudoLine(uid=self._next, members=members)
        self._by_uid[pl.uid] = pl
        self._next += 1
        return pl

    def get(self, uid: int) -> Line | PseudoLine:
        return self._by_uid[uid]

    def mult(self, uid: int) -> int:
        """Multiplicity: pseudo-lines count their members (paper: 'weight
        crossings with K by the number of lines |B| it combines')."""
        ln = self._by_uid[uid]
        return len(ln.members) if isinstance(ln, PseudoLine) else 1

    def expand(self, uid: int) -> tuple[int, ...]:
        ln = self._by_uid[uid]
        return ln.members if isinstance(ln, PseudoLine) else (uid,)

    def is_pseudo(self, uid: int) -> bool:
        return isinstance(self._by_uid[uid], PseudoLine)

    def __iter__(self):
        return iter(self._by_uid.values())


# ---------------------------------------------------------------- graph

@dataclass(frozen=True)
class OrigNode:
    """v* — the original line-graph node a reduced node descends from.
    Weights only ever read degree/station of these. Synthetic nodes
    (C1/C2 stubs, U6 dummy target) get weight 0 via degree=0."""
    oid: int
    degree: int
    station: bool
    label: str | None = None
    synthetic: bool = False


@dataclass
class Node:
    nid: int
    orig: int  # OrigNode.oid (v*)
    x: float = 0.0
    y: float = 0.0


@dataclass
class Edge:
    eid: int
    u: int
    v: int
    lines: tuple[int, ...]  # SET semantics; kept sorted for determinism
    dummy: bool = False     # U6 dummy edge: transparent to scoring
    stub: bool = False      # C1 cut product: never re-cut


class OptGraph:
    """Nodes + edges + per-node clockwise incident-edge order."""

    def __init__(self):
        self.nodes: dict[int, Node] = {}
        self.edges: dict[int, Edge] = {}
        self.order: dict[int, list[int]] = {}  # nid -> eids, clockwise
        self.orig_nodes: dict[int, OrigNode] = {}
        self._next_node = 0
        self._next_edge = 0

    # -- construction -------------------------------------------------

    def add_orig(self, degree: int, station: bool, label: str | None = None,
                 synthetic: bool = False) -> int:
        oid = len(self.orig_nodes)
        self.orig_nodes[oid] = OrigNode(oid, degree, station, label, synthetic)
        return oid

    def add_node(self, orig: int, x: float = 0.0, y: float = 0.0) -> int:
        nid = self._next_node
        self._next_node += 1
        self.nodes[nid] = Node(nid, orig, x, y)
        self.order[nid] = []
        return nid

    def add_edge(self, u: int, v: int, lines, dummy=False, stub=False) -> int:
        eid = self._next_edge
        self._next_edge += 1
        self.edges[eid] = Edge(eid, u, v, tuple(sorted(lines)), dummy, stub)
        return eid

    def copy(self) -> "OptGraph":
        g = OptGraph()
        g.nodes = {k: replace(v) for k, v in self.nodes.items()}
        g.edges = {k: replace(v) for k, v in self.edges.items()}
        g.order = {k: list(v) for k, v in self.order.items()}
        g.orig_nodes = dict(self.orig_nodes)
        g._next_node = self._next_node
        g._next_edge = self._next_edge
        return g

    # -- queries ------------------------------------------------------

    def degree(self, nid: int) -> int:
        return len(self.order[nid])

    def incident(self, nid: int) -> list[int]:
        return self.order[nid]

    def other(self, eid: int, nid: int) -> int:
        e = self.edges[eid]
        return e.v if e.u == nid else e.u

    def line_edges_at(self, nid: int, uid: int) -> list[int]:
        """Incident edges at nid carrying line uid (membership-based
        continuation — CTA Loop corners have lines on 3 incident edges)."""
        return [eid for eid in self.order[nid]
                if uid in self.edges[eid].lines]

    def terminates(self, uid: int, eid: int, nid: int) -> bool:
        """Line uid on edge eid terminates at nid (no other incident edge
        carries it)."""
        return all(f == eid or uid not in self.edges[f].lines
                   for f in self.order[nid])

    def clockwise_from(self, nid: int, eid: int) -> list[int]:
        """Other incident edges of nid in clockwise order starting at eid
        (pi^v_e enumeration of Lemma 4.4)."""
        lst = self.order[nid]
        i = lst.index(eid)
        return [lst[(i + k) % len(lst)] for k in range(1, len(lst))]

    def max_cardinality(self) -> int:
        return max((len(e.lines) for e in self.edges.values()), default=0)

    def max_degree(self) -> int:
        return max((len(v) for v in self.order.values()), default=0)

    # -- mutation helpers ---------------------------------------------

    def attach(self, nid: int, eid: int):
        self.order[nid].append(eid)

    def detach_edge(self, nid: int, eid: int):
        self.order[nid].remove(eid)

    def replace_slot(self, nid: int, old_eid: int, new_eids: list[int]):
        """Replace old_eid in nid's clockwise list with new_eids read in
        clockwise order at the same angular slot."""
        lst = self.order[nid]
        i = lst.index(old_eid)
        self.order[nid] = lst[:i] + list(new_eids) + lst[i + 1:]

    def remove_edge(self, eid: int, *, drop_isolated=True):
        e = self.edges.pop(eid)
        for nid in {e.u, e.v}:
            if eid in self.order[nid]:
                self.order[nid].remove(eid)
            if drop_isolated and not self.order[nid]:
                del self.order[nid]
                del self.nodes[nid]


# --------------------------------------------------- solution frames

def ord_arrive(g: OptGraph, sol: dict, eid: int, nid: int) -> tuple:
    """Left-to-right line order for a traveler ARRIVING at nid along
    edge eid. Storage frame is left-to-right in direction u -> v."""
    p = tuple(sol[eid])
    return p if g.edges[eid].v == nid else tuple(reversed(p))


def ord_leave(g: OptGraph, sol: dict, eid: int, nid: int) -> tuple:
    """Left-to-right order for a traveler LEAVING nid along eid."""
    p = tuple(sol[eid])
    return p if g.edges[eid].u == nid else tuple(reversed(p))


# ---------------------------------------------------------------- instance

@dataclass
class Instance:
    graph: OptGraph
    registry: LineRegistry
    build_key: str | None = None
    node_loom: dict[int, str] = field(default_factory=dict)
    edge_loom: dict[int, str] = field(default_factory=dict)
    edge_db_id: dict[int, int] = field(default_factory=dict)
    # provisional per-edge line order as stored in the DB slot column
    # (route_id order after linegraph emit) — the "before" baseline
    provisional: dict[int, tuple[int, ...]] = field(default_factory=dict)


# ---------------------------------------------------------------- builders

def build_graph(node_xy: dict, edge_spec: list, stations=()) -> tuple:
    """Synthetic-test builder. node_xy: {name: (x, y)} planar coords
    (x east, y north); edge_spec: [(u_name, v_name, [line names])].
    Returns (Instance, {line name: uid}, {node name: nid}).
    Clockwise order derives from planar bearings, same convention as the
    PostGIS loader (azimuth clockwise from north)."""
    reg = LineRegistry()
    g = OptGraph()
    line_ids: dict[str, int] = {}
    node_ids: dict[str, int] = {}
    inc: dict[str, list] = {n: [] for n in node_xy}

    for u, v, lines in edge_spec:
        inc[u].append((u, v, lines))
        inc[v].append((u, v, lines))

    for name, (x, y) in node_xy.items():
        oid = g.add_orig(degree=len(inc[name]), station=name in stations,
                         label=name)
        node_ids[name] = g.add_node(oid, x, y)

    eids = []
    for u, v, lines in edge_spec:
        uids = []
        for ln in lines:
            if ln not in line_ids:
                line_ids[ln] = reg.add_line(feed_id="t", route_id=str(ln),
                                            short_name=str(ln)).uid
            uids.append(line_ids[ln])
        eids.append(g.add_edge(node_ids[u], node_ids[v], uids))

    for name, nid in node_ids.items():
        x0, y0 = node_xy[name]
        def az(eid):
            e = g.edges[eid]
            oname = [k for k, val in node_ids.items()
                     if val == (e.v if e.u == nid else e.u)][0]
            x1, y1 = node_xy[oname]
            return math.atan2(x1 - x0, y1 - y0) % (2 * math.pi)
        incident = [eid for eid in eids
                    if g.edges[eid].u == nid or g.edges[eid].v == nid]
        # parallel edges to the same neighbor keep spec order (stable sort)
        g.order[nid] = sorted(incident, key=az)

    return Instance(graph=g, registry=reg), line_ids, node_ids


# ---------------------------------------------------------------- loader

def _bearing(coords, at_start: bool) -> float:
    """Azimuth (clockwise from north, radians) of the edge geometry at one
    end, from the first geometrically distinct vertex."""
    pts = coords if at_start else list(reversed(coords))
    lon0, lat0 = pts[0]
    for lon1, lat1 in pts[1:]:
        if (lon1, lat1) != (lon0, lat0):
            dx = (lon1 - lon0) * math.cos(math.radians((lat0 + lat1) / 2))
            dy = lat1 - lat0
            return math.atan2(dx, dy) % (2 * math.pi)
    return 0.0


def load_build(build_key: str, dsn: str = DEFAULT_DSN) -> Instance:
    """Load a transit_graph_* build into an ordering Instance."""
    import json

    import psycopg

    g = OptGraph()
    reg = LineRegistry()
    inst = Instance(graph=g, registry=reg, build_key=build_key)

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT id, loom_id, station_id, station_label,
                      ST_X(geom), ST_Y(geom)
               FROM transit_graph_nodes WHERE build_key = %s""",
            (build_key,))
        node_rows = cur.fetchall()
        cur.execute(
            """SELECT id, loom_id, ST_AsGeoJSON(geom)
               FROM transit_graph_edges WHERE build_key = %s ORDER BY id""",
            (build_key,))
        edge_rows = cur.fetchall()
        cur.execute(
            """SELECT l.edge_id, l.slot, l.feed_id, l.route_id,
                      l.route_short_name, l.route_type, l.route_color,
                      l.route_text_color
               FROM transit_graph_edge_lines l
               JOIN transit_graph_edges e ON e.id = l.edge_id
               WHERE e.build_key = %s ORDER BY l.edge_id, l.slot""",
            (build_key,))
        line_rows = cur.fetchall()

    if not edge_rows:
        raise ValueError(f"no transit_graph rows for build_key {build_key!r}")

    # lines: identity is (feed_id, route_id)
    by_key: dict[tuple, int] = {}
    edge_lines: dict[int, list[int]] = {}
    for db_eid, _slot, feed_id, route_id, short, rtype, color, tcolor in line_rows:
        key = (feed_id, route_id)
        if key not in by_key:
            by_key[key] = reg.add_line(
                feed_id=feed_id, route_id=route_id, short_name=short or route_id,
                color=color or "", text_color=tcolor or "", route_type=rtype,
            ).uid
        edge_lines.setdefault(db_eid, []).append(by_key[key])

    # nodes keyed by exact rounded coordinate (emit writes %.7f)
    def ckey(lon, lat):
        return (round(lon, 7), round(lat, 7))

    coord_node: dict[tuple, int] = {}
    incident_count: dict[int, int] = {}
    geo: dict[int, list] = {}
    for db_eid, _loom, gj in edge_rows:
        coords = json.loads(gj)["coordinates"]
        geo[db_eid] = coords

    node_of_coord: dict[tuple, tuple] = {}
    for row in node_rows:
        db_nid, _loom, _sid, _label, lon, lat = row
        k = ckey(lon, lat)
        if k in node_of_coord:
            raise ValueError(
                f"nodes {node_of_coord[k][0]} and {db_nid} share rounded "
                f"coordinate {k} (build {build_key})")
        node_of_coord[k] = row
    for db_eid, coords in geo.items():
        for end in (coords[0], coords[-1]):
            k = ckey(*end)
            if k not in node_of_coord:
                raise ValueError(
                    f"edge {db_eid} endpoint {end} matches no node "
                    f"(build {build_key})")
            incident_count[k] = incident_count.get(k, 0) + 1

    for k, (db_nid, loom, sid, label, lon, lat) in node_of_coord.items():
        deg = incident_count.get(k, 0)
        oid = g.add_orig(degree=deg, station=sid is not None, label=label)
        nid = g.add_node(oid, lon, lat)
        coord_node[k] = nid
        inst.node_loom[nid] = loom

    bearings: dict[int, dict[int, float]] = {}  # nid -> {eid: azimuth}
    for db_eid, loom, _gj in edge_rows:
        coords = geo[db_eid]
        u = coord_node[ckey(*coords[0])]
        v = coord_node[ckey(*coords[-1])]
        if u == v:
            # a self-loop would need TWO angular slots at the node for
            # ord_arrive/ord_leave frames to stay well-defined; reject
            # loudly instead of silently undercounting the degree
            raise ValueError(
                f"edge {db_eid} is a self-loop at node "
                f"{node_of_coord[ckey(*coords[0])][0]} (build {build_key})")
        lines = edge_lines.get(db_eid, [])
        if not lines:
            continue  # line-less edge carries no ordering information
        eid = g.add_edge(u, v, lines)
        inst.edge_loom[eid] = loom
        inst.edge_db_id[eid] = db_eid
        inst.provisional[eid] = tuple(lines)  # slot order (rows sorted)
        bearings.setdefault(u, {})[eid] = _bearing(coords, True)
        bearings.setdefault(v, {})[eid] = _bearing(coords, False)

    for nid, az in bearings.items():
        g.order[nid] = sorted(az, key=lambda eid: (az[eid], eid))
    for nid in [n for n in g.nodes if not g.order[n]]:
        del g.order[nid]
        del g.nodes[nid]

    return inst
