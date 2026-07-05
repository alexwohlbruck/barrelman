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
    offset_px: float       # AUTHORITATIVE perpendicular offset, travel frame.
    #                        Starts symmetric (slot - (count-1)/2) * gap_px and
    #                        is then re-anchored by assign_stable_offsets so a
    #                        through-line holds a constant offset across the
    #                        junctions where its bundle merely gains/loses a
    #                        neighbour (no re-center curve on straight track).
    #                        slot/count stay the TRUE bundle rank/size (fillet,
    #                        pairing); the emitted legacy (slot,line_count) pair
    #                        is re-derived from offset_px at emit time.
    base_offset_px: float  # the symmetric offset before re-anchoring (diag)
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
        base = (rank - (count - 1) / 2.0) * gap_px
        ribbons.append(Ribbon(
            color_key=ck, slot=rank, count=count,
            offset_px=base, base_offset_px=base,
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


# ------------------------------------------------ stable-offset re-anchoring

# How far (in px) a bundle's re-anchored offset frame may drift off its own
# corridor centerline before we stop propagating the hold across a junction.
# One gap keeps bundles "slightly off centre" (the Apple tradeoff for straight
# through-lines) without letting a long chain of joins march a ribbon far from
# its track. Above this, the junction legitimately eases (a real fork).
DEFAULT_DRIFT_CAP_PX = GAP_PX * 1.0


# A junction end whose corridors' bearings differ by less than this (undirected)
# is a STRAIGHT through-connection — a line crossing it should not curve, so a
# nonzero offset delta there is a pure artifact worth removing. Above it the
# track genuinely bends and a constant-offset ribbon follows the bend anyway.
STRAIGHT_JOINT_DEG = 8.0
# Arc length over which a corridor's bearing at an end is measured.
_END_BEARING_M = 40.0


def _corridor_len_m(coords) -> float:
    if len(coords) < 2:
        return 0.0
    lat0 = coords[0][1]
    m_lon = 111320.0 * math.cos(math.radians(lat0))
    m_lat = 111320.0
    return sum(math.dist((coords[i][0] * m_lon, coords[i][1] * m_lat),
                         (coords[i + 1][0] * m_lon, coords[i + 1][1] * m_lat))
               for i in range(len(coords) - 1))


def _end_bearing(coords, at_start: bool, span_m: float = _END_BEARING_M) -> float:
    """Undirected-frame bearing of a corridor near one end (projected)."""
    lat0 = coords[0][1]
    m_lon = 111320.0 * math.cos(math.radians(lat0))
    m_lat = 111320.0
    xy = [(x * m_lon, y * m_lat) for x, y in coords]
    if at_start:
        p0 = xy[0]
        p1 = xy[-1]
        acc = 0.0
        for i in range(1, len(xy)):
            acc += math.dist(xy[i - 1], xy[i])
            if acc >= span_m:
                p1 = xy[i]
                break
    else:
        p0 = xy[-1]
        p1 = xy[0]
        acc = 0.0
        for i in range(len(xy) - 1, 0, -1):
            acc += math.dist(xy[i], xy[i - 1])
            if acc >= span_m:
                p1 = xy[i - 1]
                break
    return math.atan2(p1[1] - p0[1], p1[0] - p0[0])


def _bearing_diff_deg(b1: float, b2: float) -> float:
    """0 = parallel/antiparallel, 90 = perpendicular."""
    return math.degrees(abs((b1 - b2 + math.pi / 2) % math.pi - math.pi / 2))


def assign_stable_offsets(corridors: list, g: Graph, gap_px: float = GAP_PX,
                          drift_cap_px: float = DEFAULT_DRIFT_CAP_PX) -> dict:
    """Re-anchor each corridor's ribbon offsets so a line that runs STRAIGHT
    through a junction (its bundle merely gains or loses a neighbour) keeps a
    constant perpendicular offset — instead of the symmetric
    ``(slot - (count-1)/2)`` centering, which re-centers the whole bundle at
    every composition change and forces a curve onto dead-straight track.

    Model — asymmetric bundle growth. Each corridor carries one additive SHIFT
    applied to all its ribbons, so within-corridor slot ORDER and spacing are
    untouched (the no-braiding / anti-kiss invariants hold by construction — the
    bundle only slides bodily, never re-orders). The shifts are chosen to
    minimise the perpendicular offset CHANGE that continuing colours suffer at
    STRAIGHT junction ends (bearings within ``STRAIGHT_JOINT_DEG``): a straight
    join/leave at the edge of a bundle then holds every through-line's offset
    constant, so the through-line renders straight and only the joining/leaving
    line eases to/from its slot. Where the join is mid-bundle (a genuine
    express/local reorder) or the tracks actually fork, no single body-shift can
    hold the crossing line and the transition legitimately eases.

    Solved EXACTLY per connected component of the straight-joint coupling graph.
    Bent joints never couple corridors (a constant-offset ribbon just follows the
    bend), so coupling only through STRAIGHT joints shatters the network into tiny
    components (<= 11 corridors here); each is solved by backtracking over the
    5-value half-gap grid (``+/- drift_cap_px``, bundles stay slightly off-centre,
    the accepted Apple tradeoff). Objective per component, lexicographic:
    (1) a HARD monotone guard — no straight joint's per-colour delta may exceed
    its symmetric baseline, so re-anchoring can only REMOVE straight-track
    curving, never introduce it; (2) minimise the count of straight joints that
    still curve (nonzero delta); (3) minimise total |shift| (stay centred). This
    is the global optimum over the component under the guard. Deterministic
    (fixed value order, cid-sorted). Rewrites ``ribbon.offset_px`` in place;
    returns {cid: shift_px} for diagnostics."""
    half = gap_px / 2.0
    base = {c.cid: {r.color_key: r.base_offset_px for r in c.ribbons}
            for c in corridors}

    ends: dict[int, list] = {}
    for c in corridors:
        ends.setdefault(c.node_a, []).append((c.cid, "a"))
        ends.setdefault(c.node_b, []).append((c.cid, "b"))
    bear = {(c.cid, s): _end_bearing(c.coords, s == "a")
            for c in corridors for s in ("a", "b")}

    # Every shared-colour joint is a term (a, b, straight, [rel per colour]).
    # rel = base_a[ck] - base_b[ck]; the delta colour ck suffers there is
    # |S_a - S_b + rel|. Only STRAIGHT joints (bearings within
    # STRAIGHT_JOINT_DEG) COUPLE corridors into components — bent joints don't
    # curve a constant-offset ribbon, so coupling through them would needlessly
    # re-merge the whole network into one intractable component. The monotone
    # guard still applies to EVERY joint (straight or bent): a shift may never
    # worsen any joint's delta beyond its symmetric baseline, evaluated against
    # the neighbour's decided shift (components solved in cid order, undecided
    # neighbours held at their base S=0). Only straight joints enter the
    # curve-count objective.
    joints: list = []
    adj: dict[int, set] = {c.cid: set() for c in corridors}
    for lst in ends.values():
        for i in range(len(lst)):
            for j in range(i + 1, len(lst)):
                (a, sa), (b, sb) = lst[i], lst[j]
                if a == b:
                    continue
                common = set(base[a]) & set(base[b])
                if not common:
                    continue
                straight = (_bearing_diff_deg(bear[(a, sa)], bear[(b, sb)])
                            < STRAIGHT_JOINT_DEG)
                rels = [base[a][ck] - base[b][ck] for ck in sorted(common)]
                joints.append((a, b, straight, rels))
                if straight:
                    adj[a].add(b)
                    adj[b].add(a)

    grid = tuple(k * half for k in
                 range(-int(round(drift_cap_px / half)),
                       int(round(drift_cap_px / half)) + 1))
    shift = {c.cid: 0.0 for c in corridors}
    j_by_cid: dict[int, list] = {}
    for idx, (a, b, _st, _r) in enumerate(joints):
        j_by_cid.setdefault(a, []).append(idx)
        j_by_cid.setdefault(b, []).append(idx)

    def _component(seed, seen):
        stack, comp = [seed], []
        while stack:
            x = stack.pop()
            if x in seen:
                continue
            seen.add(x)
            comp.append(x)
            stack.extend(sorted(adj[x] - seen))
        return sorted(comp)

    def _delta(idx, S, pos):
        """(a's shift) - (b's shift) for joint idx, taking in-component shifts
        from S and out-of-component ones from the (already decided) `shift`."""
        a, b, _st, _r = joints[idx]
        sa = S[pos[a]] if a in pos else shift[a]
        sb = S[pos[b]] if b in pos else shift[b]
        return sa - sb

    seen: set = set()
    for c in sorted(cid for cid in shift):
        if c in seen or not adj[c]:
            continue
        comp = _component(c, seen)
        pos = {cid: i for i, cid in enumerate(comp)}
        # every joint touching a component corridor: internal ones plus joints
        # to fixed external corridors (guarded so re-anchoring this component
        # never worsens a joint to an already-decided / base neighbour).
        touching = sorted({idx for cid in comp for idx in j_by_cid.get(cid, [])})
        S = [0.0] * len(comp)
        best = {"key": None, "assign": None}

        def _cost():
            curve = 0
            for idx in touching:
                _a, _b, straight, rels = joints[idx]
                sd = _delta(idx, S, pos)
                for rel in rels:
                    d = abs(sd + rel)
                    if d > abs(rel) + 1e-6:
                        return None          # monotone guard: forbidden (all)
                    if straight and d > 0.3:
                        curve += 1           # visible curve counted on straight
            mag = sum(abs(v) for v in S)
            return (curve, round(mag, 6))

        def _dfs(k):
            if k == len(comp):
                key = _cost()
                if key is not None and (best["key"] is None
                                        or key < best["key"]):
                    best["key"] = key
                    best["assign"] = list(S)
                return
            for v in grid:
                S[k] = v
                # partial monotone prune: any joint all of whose endpoints are
                # decided (in-comp index <= k, or external) must satisfy the guard
                ok = True
                for idx in j_by_cid.get(comp[k], []):
                    a, b, _straight, rels = joints[idx]
                    if (a in pos and pos[a] > k) or (b in pos and pos[b] > k):
                        continue
                    sd = _delta(idx, S, pos)
                    if any(abs(sd + rel) > abs(rel) + 1e-6 for rel in rels):
                        ok = False
                        break
                if ok:
                    _dfs(k + 1)
            S[k] = 0.0

        _dfs(0)
        if best["assign"] is not None:
            for cid, v in zip(comp, best["assign"]):
                shift[cid] = v

    for c in corridors:
        d = shift[c.cid]
        if abs(d) < 1e-12:
            continue
        for r in c.ribbons:
            r.offset_px = r.base_offset_px + d
    return shift


def walk_corridors(g: Graph, gap_px: float = GAP_PX,
                   drift_cap_px: float = DEFAULT_DRIFT_CAP_PX) -> list:
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
    assign_stable_offsets(out, g, gap_px, drift_cap_px)
    return out
