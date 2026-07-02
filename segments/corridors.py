"""segments.corridors — maximal corridors + colour-collapsed ribbons.

A corridor is a maximal chain of graph edges joined through degree-2
nodes whose two incident edges carry IDENTICAL line sets (the same walk
as lineorder/exam/stability_exam.py, but ordered and with geometry).
Stage 5 guarantees each line's slot is constant along a corridor, so a
corridor is the natural steady-segment unit.

Ribbons mirror the edge_colors/ranked CTEs of
import/create-transit-lines-runtime.sql: lines sharing a colour collapse
into one ribbon (color_key = COALESCE(NULLIF(route_color,''),
'rid:'||route_id)); ribbon_slot = dense rank ordered by (min line slot,
color_key); offset_px = (ribbon_slot - (ribbon_count-1)/2) * gap_px.

Slots are stored left-to-right in each edge's u->v storage direction.
The walk normalizes every edge into the corridor's TRAVEL frame
(node_a -> node_b) — an edge traversed against storage direction has its
slots mirrored (n-1-slot) — and asserts constancy across the chain.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field

DEFAULT_DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)

GAP_PX = 4.4  # on-screen ribbon spacing (handoff §2b)


# ----------------------------------------------------------------- graph

@dataclass(frozen=True)
class LineAttr:
    feed_id: str
    route_id: str
    short_name: str = ""
    route_type: int | None = None
    color: str = ""
    text_color: str = ""

    @property
    def key(self) -> tuple[str, str]:
        return (self.feed_id, self.route_id)

    @property
    def color_key(self) -> str:
        return self.color if self.color else f"rid:{self.route_id}"


@dataclass
class GNode:
    nid: int
    lon: float
    lat: float
    station_id: str | None = None
    label: str | None = None


@dataclass
class GEdge:
    eid: int
    u: int
    v: int
    coords: list  # [(lon, lat)], coords[0] at u, coords[-1] at v
    lines: list   # [LineAttr] in stored slot order (slot 0 first)

    @property
    def line_keys(self) -> frozenset:
        return frozenset(ln.key for ln in self.lines)


@dataclass
class Graph:
    nodes: dict            # nid -> GNode
    edges: dict            # eid -> GEdge
    incident: dict         # nid -> [eids]
    build_key: str | None = None

    def degree(self, nid: int) -> int:
        return len(self.incident.get(nid, ()))


def graph_from_spec(node_xy: dict, edge_spec: list, *, meters: bool = True,
                    feed_id: str = "t") -> tuple[Graph, dict]:
    """Synthetic-test builder. node_xy: {name: (x, y)} planar coords;
    edge_spec: [(u_name, v_name, [(route_id, color), ...])] with lines in
    SLOT ORDER for the edge's u->v direction. meters=True places the graph
    near (0, 0) lon/lat at ~1 m/unit. Returns (Graph, {name: nid})."""
    sx = 1.0 / 111319.4908 if meters else 1.0  # equator metres per deg lon
    sy = 1.0 / 110574.2727 if meters else 1.0  # metres per deg lat
    nodes, incident, node_ids = {}, {}, {}
    for i, (name, (x, y)) in enumerate(node_xy.items()):
        nodes[i] = GNode(i, x * sx, y * sy, label=name)
        node_ids[name] = i
        incident[i] = []
    edges = {}
    for i, (u, v, lines) in enumerate(edge_spec):
        uid, vid = node_ids[u], node_ids[v]
        attrs = [LineAttr(feed_id=feed_id, route_id=r, short_name=r, color=c)
                 for r, c in lines]
        nu, nv = nodes[uid], nodes[vid]
        edges[i] = GEdge(i, uid, vid,
                         [(nu.lon, nu.lat), (nv.lon, nv.lat)], attrs)
        incident[uid].append(i)
        incident[vid].append(i)
    return Graph(nodes, edges, incident), node_ids


def load_graph(build_key: str, dsn: str = DEFAULT_DSN) -> Graph:
    """Load transit_graph_* rows; topology by exact %.7f coordinate match
    (same contract as lineorder.model.load_build)."""
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT id, station_id, station_label, ST_X(geom), ST_Y(geom)
               FROM transit_graph_nodes WHERE build_key = %s ORDER BY id""",
            (build_key,))
        node_rows = cur.fetchall()
        cur.execute(
            """SELECT id, ST_AsGeoJSON(geom)
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

    def ckey(lon, lat):
        return (round(lon, 7), round(lat, 7))

    nodes, node_of_coord = {}, {}
    for nid, sid, label, lon, lat in node_rows:
        nodes[nid] = GNode(nid, lon, lat, sid, label)
        node_of_coord[ckey(lon, lat)] = nid

    edge_lines: dict[int, list[LineAttr]] = {}
    for eid, _slot, feed, rid, short, rtype, color, tcolor in line_rows:
        edge_lines.setdefault(eid, []).append(LineAttr(
            feed_id=feed, route_id=rid, short_name=short or rid,
            route_type=rtype, color=color or "", text_color=tcolor or ""))

    edges, incident = {}, {n: [] for n in nodes}
    for eid, gj in edge_rows:
        lines = edge_lines.get(eid)
        if not lines:
            continue  # line-less edge carries no display information
        coords = json.loads(gj)["coordinates"]
        try:
            u = node_of_coord[ckey(*coords[0])]
            v = node_of_coord[ckey(*coords[-1])]
        except KeyError as err:
            raise ValueError(f"edge {eid} endpoint matches no node "
                             f"(build {build_key})") from err
        edges[eid] = GEdge(eid, u, v, [tuple(c) for c in coords], lines)
        incident[u].append(eid)
        incident[v].append(eid)
    incident = {n: sorted(inc) for n, inc in incident.items() if inc}
    nodes = {n: nd for n, nd in nodes.items() if n in incident}
    return Graph(nodes, edges, incident, build_key)


# -------------------------------------------------------------- corridors

@dataclass
class Ribbon:
    color_key: str
    slot: int              # dense rank within the corridor bundle
    count: int             # ribbon count of the corridor bundle
    offset_px: float       # (slot - (count-1)/2) * gap_px, travel frame
    members: tuple         # LineAttr, travel-slot order
    route_ids: str         # comma list, sorted distinct
    route_short_names: str  # comma list, sorted distinct
    feed_id: str
    route_type: int | None
    route_color: str
    route_text_color: str

    @property
    def member_keys(self) -> frozenset:
        return frozenset(m.key for m in self.members)


@dataclass
class Corridor:
    cid: int
    node_a: int
    node_b: int
    steps: list            # [(eid, forward)] node_a -> node_b
    coords: list           # [(lon, lat)] node_a -> node_b
    ribbons: list          # [Ribbon]
    line_keys: frozenset
    ring: bool = False

    def ribbon(self, color_key: str) -> Ribbon | None:
        for r in self.ribbons:
            if r.color_key == color_key:
                return r
        return None


def is_interior(g: Graph, nid: int) -> bool:
    inc = g.incident.get(nid, ())
    return (len(inc) == 2
            and g.edges[inc[0]].line_keys == g.edges[inc[1]].line_keys)


def make_ribbons(travel_lines: list, gap_px: float = GAP_PX) -> list:
    """Colour-collapse travel-slot-ordered lines into ribbons, mirroring
    the display SQL's edge_colors/ranked CTEs."""
    groups: dict[str, dict] = {}
    for i, ln in enumerate(travel_lines):
        grp = groups.setdefault(ln.color_key, {"first": i, "members": []})
        grp["members"].append(ln)
    ranked = sorted(groups.items(), key=lambda kv: (kv[1]["first"], kv[0]))
    count = len(ranked)
    ribbons = []
    for rank, (ck, grp) in enumerate(ranked):
        members = grp["members"]
        color = max((m.color for m in members), default="")
        tcolor = next((m.text_color for m in members if m.color == color),
                      members[0].text_color)
        rtypes = [m.route_type for m in members if m.route_type is not None]
        ribbons.append(Ribbon(
            color_key=ck, slot=rank, count=count,
            offset_px=(rank - (count - 1) / 2.0) * gap_px,
            members=tuple(members),
            route_ids=",".join(sorted({m.route_id for m in members})),
            route_short_names=",".join(sorted({m.short_name for m in members})),
            feed_id=min(m.feed_id for m in members),
            route_type=min(rtypes) if rtypes else None,
            route_color=color, route_text_color=tcolor))
    return ribbons


def _travel_slots(edge: GEdge, forward: bool) -> dict:
    n = len(edge.lines)
    return {ln.key: (i if forward else n - 1 - i)
            for i, ln in enumerate(edge.lines)}


def walk_corridors(g: Graph, gap_px: float = GAP_PX) -> list:
    """All maximal corridors (single-edge ones included), deterministic."""
    visited: set[int] = set()
    out: list[Corridor] = []
    for seed in sorted(g.edges):
        if seed in visited:
            continue
        steps = [(seed, True)]
        visited.add(seed)
        ring = False
        # extend forward past interior nodes
        end = g.edges[seed].v
        while is_interior(g, end):
            nxt = [e for e in g.incident[end] if e != steps[-1][0]]
            e = nxt[0]
            if e in visited:
                ring = True
                break
            fwd = g.edges[e].u == end
            steps.append((e, fwd))
            visited.add(e)
            end = g.edges[e].v if fwd else g.edges[e].u
        start = g.edges[seed].u
        if not ring:
            while is_interior(g, start):
                nxt = [e for e in g.incident[start] if e != steps[0][0]]
                e = nxt[0]
                if e in visited:
                    ring = True
                    break
                fwd = g.edges[e].v == start
                steps.insert(0, (e, fwd))
                visited.add(e)
                start = g.edges[e].u if fwd else g.edges[e].v

        # assemble geometry node_a -> node_b
        coords: list = []
        for eid, fwd in steps:
            part = g.edges[eid].coords if fwd else list(
                reversed(g.edges[eid].coords))
            if coords:
                j0, j1 = coords[-1], part[0]
                if (abs(j0[0] - j1[0]) > 1e-6 or abs(j0[1] - j1[1]) > 1e-6):
                    raise ValueError(
                        f"corridor discontinuity at edge {eid}: {j0} != {j1}")
                part = part[1:]
            coords.extend(part)

        # travel-frame slot constancy (stage-5 invariant, asserted)
        ref = _travel_slots(g.edges[steps[0][0]], steps[0][1])
        for eid, fwd in steps[1:]:
            if _travel_slots(g.edges[eid], fwd) != ref:
                raise ValueError(
                    f"corridor through edge {eid} is not slot-stable in the "
                    f"travel frame (stage-5 invariant violated)")
        travel_lines = sorted(g.edges[steps[0][0]].lines,
                              key=lambda ln: ref[ln.key])

        e0, fwd0 = steps[0]
        el, fwdl = steps[-1]
        node_a = g.edges[e0].u if fwd0 else g.edges[e0].v
        node_b = g.edges[el].v if fwdl else g.edges[el].u
        out.append(Corridor(
            cid=len(out), node_a=node_a, node_b=node_b, steps=steps,
            coords=coords, ribbons=make_ribbons(travel_lines, gap_px),
            line_keys=g.edges[seed].line_keys, ring=ring))
    return out
