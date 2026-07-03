#!/usr/bin/env python3
"""[DEPRECATED (way-graph era)] linegraph.unfuse — split raster-fused corridors of physically distinct
line families (phase B.0, between the skeleton and the refit).

The raster merges ANY strokes closer than the merge width, including
corridors that belong to physically separate systems: at Brooklyn
Bridge-City Hall the J/Z (Nassau St) tracks pass 7-8 m from the
Lexington locals — separated by a station wall, no track connection
exists or is possible — and the stroke fuses them into one edge whose
green and brown ribbons converge and split again on the map. The same
plan-view fusion glues level-separated CROSSINGS into short shared
edges (the 4/5 Joralemon tube deflecting onto the N/R/W Montague tube
at Whitehall; 1 x 4/5 at South Ferry; 1/2/3 x N/Q/R/W at Times Sq).

Genuine bundles must survive: same-trench corridors (Queens Blvd
E+F/M+R), stacked structures (Queensboro Plaza 7 over N/W, Chicago's
Lake St elevated over the Blue subway — user-approved bundling pinned
by the loop exam), tangent side-by-side tunnels under one street
(B/Q beside 2/3/4/5 under Flatbush Av), and real track sharing.

DISCRIMINATOR (measured on the matched-shape evidence, feed 5 + 29):
a family pair fused in a zone is SEPARABLE iff, walking each family's
own evidence from the point of closest approach to the other family,
the gap exceeds ESCAPE_GAP_MULT x merge width within ESCAPE_DIST_M on
BOTH sides, from BOTH families' perspectives ("both-sides escape").
Crossings and tangent kisses escape on both sides; forks, bundles and
shared track stay close on at least one side forever:

    site                        pair          closest  ±250 m   verdict
    Brooklyn Bridge (J/Z x 456) brown/green     6.6 m  34/63 m  split
    Whitehall (4/5 x N/R/W)     green/yellow   16.4 m  75/138   split
    South Ferry (1 x 4/5)       red/green       0.8 m  65/22*   split
    Times Sq (1/2/3 x NQRW)     red/yellow     11.3 m  59/46    split
    168 St (1 x A)              red/blue        1.3 m  81/104   split
    Flatbush Av (B/Q x 2345)    yellow/green    1.7 m  129/7.6  keep
    Queensboro Plaza (7 x N/W)  purple/yellow   3.4 m  9.0/104  keep
    Lake St (Blue x G/Pink)     blue/green      5.1 m  5.1/5.8  keep
    Queens Blvd (F/M x R)       orange/yellow   3.1 m  3.8/3.8  keep
    Eastern Pkwy (2/3 x 4/5)    red/green       0.0 m  0/0      keep
    (* South Ferry's +side reaches 22 m — above the 18 m gap.)

The originally proposed platform carving (erase railway=platform
polygons from the ink grid) is contradicted by the same data: the
platforms at Brooklyn Bridge / Chambers St measure 3.7-4.5 m inscribed
width and none of them lies in the 7 m J/Z-to-local gap (a wall does),
while carving the polygons that DO exist there would instead split the
green local/express groups around their island platforms. No polygon
erasure can separate what the data shows; the family evidence can.

SURGERY: a separable zone (maximal connected run of edges carrying the
families) is rebuilt per family GROUP (union-find over non-separable
pairs): each group gets one new edge between its two boundary
attachment nodes, geometry = the cluster-weighted average of the
group's own evidence passes; the first group keeps the original
boundary node ids, later groups get copies, and their external edges
are rewired and terminal-blended. Crossing groups end up as two edges
that cross in plan with NO shared node — matching the physical
level-separated structure. Zones that do not validate (a group with
other than two attachments, cross-group external edges, line-less
externals) are skipped and logged, never guessed at.

Runs BEFORE the refit: the refit's own coarse attribution then snaps
every family to its own (nearer) corridor and rebuilds final geometry;
station snapping later labels each complex on its own corridor
(Brooklyn Bridge-City Hall on the green, Chambers St on the brown).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from types import SimpleNamespace

import numpy as np
import shapely
from pyproj import Transformer
from shapely.geometry import LineString

from linegraph.attribute import EdgeSnapIndex, attribute_shape_xy
from linegraph.model import LGNode
from linegraph.refit import (PASS_DENSIFY_M, _blend_snap,
                             _cluster_weighted_mean, _edge_contributions,
                             sample_polyline_xy)

ESCAPE_GAP_MULT = 1.0    # x merge_width: the gap that counts as escaped
ESCAPE_DIST_M = 250.0    # escape must happen within this of closest approach
PROFILE_STEP_M = 12.5    # gap-profile sampling step along the anchor
EVIDENCE_RADIUS_M = 50.0  # family evidence counts as in-zone within this
MIN_GROUP_SHAPES = 1     # a group must have own evidence to be rebuilt


@dataclass(slots=True)
class UnfuseStats:
    n_zones: int = 0
    n_split: int = 0
    n_kept: int = 0
    n_skipped: int = 0
    split_zones: list = field(default_factory=list)    # (lon, lat, groups)
    kept_zones: list = field(default_factory=list)     # (lon, lat, families)
    skipped_zones: list = field(default_factory=list)  # (lon, lat, reason)
    n_edges_removed: int = 0
    n_edges_added: int = 0


def shape_families(patterns, shapes):
    """color_key family set per deduped shape (build.dedup_shapes order).

    color_key mirrors the display SQL: route_color, else 'rid:'+route_id.
    """
    from shapesnap.match import geometry_hash

    by_hash: dict = {}
    for p in patterns:
        if not p.shape or len(p.shape) < 2:
            continue
        key = (p.route_color or "").strip() or f"rid:{p.route_id}"
        by_hash.setdefault(geometry_hash(p.shape), set()).add(key)
    return [frozenset(by_hash[geometry_hash(s)]) for s in shapes]


class _UnionFind(dict):
    def find(self, a):
        while self.setdefault(a, a) != a:
            self[a] = self[self[a]]
            a = self[a]
        return a

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self[max(ra, rb)] = min(ra, rb)


def _pass_runs(dense_pts, zone_geom, radius):
    """Index windows of contiguous within-radius runs, extended by
    ESCAPE_DIST_M on both sides. Returns [(lo, hi, run_lo, run_hi)]."""
    d = shapely.distance(shapely.points(dense_pts[:, 0], dense_pts[:, 1]),
                         zone_geom)
    idx = np.flatnonzero(d <= radius)
    if idx.size == 0:
        return []
    ext = int(math.ceil(ESCAPE_DIST_M / PASS_DENSIFY_M))
    breaks = np.flatnonzero(np.diff(idx) > 1)
    out = []
    for run in np.split(idx, breaks + 1):
        out.append((max(0, run[0] - ext), min(len(dense_pts) - 1, run[-1] + ext),
                    int(run[0]), int(run[-1])))
    return out


def _escapes_both_sides(anchor_pts, run_lo, run_hi, other_geom,
                        escape_gap):
    """Both-sides escape test along one anchor pass.

    anchor_pts spans the extended window; [run_lo, run_hi] indexes the
    in-zone portion WITHIN that window. Truncated evidence that never
    reaches the gap = NOT escaped (conservative: keep fused).
    """
    step = max(1, int(round(PROFILE_STEP_M / PASS_DENSIFY_M)))
    sub = anchor_pts[::step]
    gaps = shapely.distance(shapely.points(sub[:, 0], sub[:, 1]), other_geom)
    in_zone = slice(run_lo // step, max(run_lo // step + 1, run_hi // step + 1))
    zone_gaps = gaps[in_zone]
    if zone_gaps.size == 0:
        return False, None
    i_star = in_zone.start + int(np.argmin(zone_gaps))
    sub_spacing = step * PASS_DENSIFY_M
    reach = int(math.ceil(ESCAPE_DIST_M / sub_spacing)) + 1
    closest = float(gaps[i_star])
    lo_ok = bool((gaps[max(0, i_star - reach):i_star + 1] >= escape_gap).any())
    hi_ok = bool((gaps[i_star:i_star + reach + 1] >= escape_gap).any())
    return lo_ok and hi_ok, closest


def _pair_separable(fam_a, fam_b, fam_shapes, dense, shape_lines, zone_geom,
                    merge_width):
    """True when both families' evidence escapes the other on both sides."""
    escape_gap = ESCAPE_GAP_MULT * merge_width
    for a, b in ((fam_a, fam_b), (fam_b, fam_a)):
        other = shapely.union_all([shape_lines[si] for si in fam_shapes[b]])
        shapely.prepare(other)
        best = None  # longest in-zone pass across a's shapes
        for si in fam_shapes[a]:
            for lo, hi, rlo, rhi in _pass_runs(dense[si], zone_geom,
                                               EVIDENCE_RADIUS_M):
                if best is None or (rhi - rlo) > best[0]:
                    best = (rhi - rlo, dense[si][lo:hi + 1], rlo - lo, rhi - lo)
        if best is None:
            return False  # no in-zone evidence: inconclusive, keep
        ok, _ = _escapes_both_sides(best[1], best[2], best[3], other,
                                    escape_gap)
        if not ok:
            return False
    return True


def _zone_components(cand, edge_nodes):
    """Connected components of candidate edge positions via shared nodes."""
    uf = _UnionFind()
    by_node: dict = {}
    for pos in cand:
        uf.find(pos)
        for nid in edge_nodes[pos]:
            by_node.setdefault(nid, []).append(pos)
    for nid, lst in by_node.items():
        for other in lst[1:]:
            uf.union(lst[0], other)
    comps: dict = {}
    for pos in cand:
        comps.setdefault(uf.find(pos), []).append(pos)
    return [sorted(v) for _, v in sorted(comps.items())]


def _zone_path(zone, edge_nodes, lg, start, goal):
    """Node path start->goal over zone edges (BFS, fewest edges), returned
    as concatenated coords_xy."""
    adj: dict = {}
    for pos in zone:
        a, b = edge_nodes[pos]
        adj.setdefault(a, []).append((b, pos))
        adj.setdefault(b, []).append((a, pos))
    prev = {start: None}
    queue = [start]
    while queue:
        cur = queue.pop(0)
        if cur == goal:
            break
        for nxt, pos in sorted(adj.get(cur, [])):
            if nxt not in prev:
                prev[nxt] = (cur, pos)
                queue.append(nxt)
    if goal not in prev:
        return None
    hops = []
    cur = goal
    while prev[cur] is not None:
        par, pos = prev[cur]
        hops.append((par, cur, pos))
        cur = par
    hops.reverse()
    coords: list = []
    for a, b, pos in hops:
        e = lg.edges[pos]
        seg = e.coords_xy if e.from_node == a else list(reversed(e.coords_xy))
        coords.extend(seg if not coords else seg[1:])
    return coords


def _normalize_chain_orientation(lg) -> int:
    """Orient every maximal degree-2 chain head-to-tail.

    Stage 5's raw-slot corridor stability reads slots in the storage
    direction, so a head-to-head edge pair mid-corridor mirrors slots.
    Pre-station graphs have no degree-2 nodes except the attachment
    nodes the surgery just created (vector.cleanup merges the rest), so
    flips here cannot disturb any other chain. Returns edges flipped.
    """
    inc: dict = {}
    for pos, e in enumerate(lg.edges):
        inc.setdefault(e.from_node, []).append(pos)
        if e.to_node != e.from_node:
            inc.setdefault(e.to_node, []).append(pos)
    deg2 = {nid for nid, lst in inc.items()
            if len(lst) == 2 and lst[0] != lst[1]}

    def flip(e):
        e.from_node, e.to_node = e.to_node, e.from_node
        e.coords = list(reversed(e.coords))
        e.coords_xy = list(reversed(e.coords_xy))

    visited: set = set()
    n_flipped = 0
    for seed in range(len(lg.edges)):
        if seed in visited:
            continue
        e0 = lg.edges[seed]
        if e0.from_node == e0.to_node:
            visited.add(seed)
            continue
        # walk to the chain head (or back to the seed on a pure cycle)
        pos, nid = seed, e0.from_node
        seen = {seed}
        while nid in deg2:
            prev = [p for p in inc[nid] if p != pos]
            if not prev or prev[0] in seen:
                break
            pos = prev[0]
            seen.add(pos)
            e = lg.edges[pos]
            nid = e.from_node if e.to_node == nid else e.to_node
        # orient the chain from the head onward
        cur_pos, cur_node = pos, nid
        while True:
            e = lg.edges[cur_pos]
            if e.from_node != cur_node:
                flip(e)
                n_flipped += 1
            visited.add(cur_pos)
            cur_node = e.to_node
            if cur_node not in deg2:
                break
            nxt = [p for p in inc[cur_node] if p != cur_pos]
            if not nxt or nxt[0] in visited:
                break
            cur_pos = nxt[0]
    return n_flipped


def unfuse_corridors(lg, shapes_lonlat, families, *, verbose=True):
    """Detect and split separable fused corridors. Mutates lg in place.

    shapes_lonlat: deduped shape list (build.dedup_shapes output);
    families: frozenset of color_keys per shape (shape_families).
    """
    stats = UnfuseStats()
    if not lg.edges or not shapes_lonlat:
        return stats

    def log(msg):
        if verbose:
            print(f"[unfuse] {msg}", flush=True)

    to_xy = Transformer.from_crs(4326, lg.epsg, always_xy=True)
    to_wgs = Transformer.from_crs(lg.epsg, 4326, always_xy=True)
    shapes_xy = []
    for coords in shapes_lonlat:
        xs, ys = to_xy.transform([c[0] for c in coords], [c[1] for c in coords])
        shapes_xy.append(list(zip(xs, ys)))

    # families sharing a shape can never separate — union them globally
    fam_uf = _UnionFind()
    for fams in families:
        fams = sorted(fams)
        for f in fams[1:]:
            fam_uf.union(fams[0], f)
    fam_of_shape = [fam_uf.find(sorted(f)[0]) if f else None
                    for f in families]
    fam_members: dict = {}  # root family -> every color_key united in it
    for fams in families:
        for f in fams:
            fam_members.setdefault(fam_uf.find(f), set()).add(f)

    # coarse attribution on the raw skeleton
    index = EdgeSnapIndex(lg)
    edge_shapes: dict = {}
    for si, sxy in enumerate(shapes_xy):
        if len(sxy) < 2:
            continue
        ridden, _, _, _ = attribute_shape_xy(index, sxy)
        for pos in ridden:
            edge_shapes.setdefault(pos, set()).add(si)

    edge_fams = {
        pos: sorted({fam_of_shape[si] for si in sis
                     if fam_of_shape[si] is not None})
        for pos, sis in edge_shapes.items()
    }
    cand = sorted(pos for pos, fams in edge_fams.items() if len(fams) >= 2)
    if not cand:
        return stats

    edge_nodes = [(e.from_node, e.to_node) for e in lg.edges]
    incidence: dict = {}
    for pos, e in enumerate(lg.edges):
        incidence.setdefault(e.from_node, []).append(pos)
        if e.to_node != e.from_node:
            incidence.setdefault(e.to_node, []).append(pos)

    dense = {}
    shape_lines = {}
    zones = _zone_components(cand, edge_nodes)
    stats.n_zones = len(zones)

    plans = []  # (zone set, [(group fams, group shapes, attach pair)], ...)
    for zone in zones:
        zone_set = set(zone)
        zfams = sorted({f for pos in zone for f in edge_fams.get(pos, ())})
        zone_geom = shapely.union_all(
            [LineString(lg.edges[pos].coords_xy) for pos in zone])
        shapely.prepare(zone_geom)
        c = zone_geom.centroid
        lon, lat = to_wgs.transform(c.x, c.y)
        site = (round(lon, 5), round(lat, 5))

        zone_shapes = sorted({si for pos in zone
                              for si in edge_shapes.get(pos, ())})
        fam_shapes: dict = {}
        for si in zone_shapes:
            f = fam_of_shape[si]
            if f is not None:
                fam_shapes.setdefault(f, []).append(si)
        for si in zone_shapes:
            if si not in dense:
                dense[si] = sample_polyline_xy(shapes_xy[si], PASS_DENSIFY_M)
                shape_lines[si] = LineString(shapes_xy[si])

        # pairwise separability over co-riding pairs; union non-separable
        guf = _UnionFind()
        for f in zfams:
            guf.find(f)
        co_pairs = sorted({
            (a, b)
            for pos in zone
            for i, a in enumerate(edge_fams.get(pos, ()))
            for b in edge_fams.get(pos, ())[i + 1:]
        })
        any_split = False
        for a, b in co_pairs:
            if guf.find(a) == guf.find(b):
                continue
            if _pair_separable(a, b, fam_shapes, dense, shape_lines,
                               zone_geom, lg.merge_width_m):
                any_split = True
            else:
                guf.union(a, b)
        groups: dict = {}
        for f in zfams:
            groups.setdefault(guf.find(f), []).append(f)
        groups = [sorted(v) for _, v in sorted(groups.items())]
        if len(groups) < 2 or not any_split:
            stats.n_kept += 1
            stats.kept_zones.append((site, zfams))
            continue

        # ── validate boundary structure ─────────────────────────────────
        znodes = sorted({n for pos in zone for n in edge_nodes[pos]})
        boundary: dict = {}   # node -> [external edge pos]
        for nid in znodes:
            ext = [p for p in incidence.get(nid, []) if p not in zone_set]
            if ext:
                boundary[nid] = ext
        group_of_fam = {f: gi for gi, fams in enumerate(groups) for f in fams}
        attach: dict = {gi: [] for gi in range(len(groups))}
        reason = None
        for nid in sorted(boundary):
            for p in boundary[nid]:
                efams = edge_fams.get(p, [])
                if not efams:
                    reason = f"line-less external edge {p} at node {nid}"
                    break
                gis = {group_of_fam.get(f) for f in efams}
                if len(gis) != 1 or None in gis:
                    reason = f"cross-group external edge {p} at node {nid}"
                    break
                gi = next(iter(gis))
                if nid not in [n for n, _ in attach[gi]]:
                    attach[gi].append((nid, p))
            if reason:
                break
        if reason is None:
            for gi, fams in enumerate(groups):
                if len(attach[gi]) != 2:
                    reason = (f"group {fams} has {len(attach[gi])} "
                              f"attachment(s), need 2")
                    break
                if len(fam_shapes_for_group(fam_shapes, fams)) < MIN_GROUP_SHAPES:
                    reason = f"group {fams} has no evidence"
                    break
        if reason is not None:
            stats.n_skipped += 1
            stats.skipped_zones.append((site, reason))
            log(f"zone at {site} families {zfams}: SKIPPED ({reason})")
            continue

        # ── build per-group replacement edges ───────────────────────────
        group_edges = []
        for gi, fams in enumerate(groups):
            (na, _), (nb, _) = attach[gi]
            virtual = _zone_path(zone, edge_nodes, lg, na, nb)
            if virtual is None or len(virtual) < 2:
                reason = f"no zone path between {na} and {nb}"
                break
            shim = SimpleNamespace(coords_xy=virtual)
            gshapes = fam_shapes_for_group(fam_shapes, fams)
            contribs = _edge_contributions(
                shim, gshapes, dense,
                {si: shapely.points(dense[si][:, 0], dense[si][:, 1])
                 for si in gshapes},
                EVIDENCE_RADIUS_M)
            if not contribs:
                reason = f"group {fams} evidence produced no contributions"
                break
            geom = _cluster_weighted_mean(contribs)
            # store the edge HEAD-TO-TAIL with its corridor neighbors —
            # stage 5's raw-slot corridor stability reads slots in the
            # storage direction, so a head-to-head edge mid-corridor
            # would mirror every slot (lineorder stability exam)
            gext = {
                nid: [p for p in boundary[nid]
                      if group_of_fam.get(
                          edge_fams.get(p, [None])[0]) == gi]
                for nid, _ in attach[gi]
            }
            fwd = (sum(1 for p in gext[na] if lg.edges[p].to_node == na)
                   + sum(1 for p in gext[nb] if lg.edges[p].from_node == nb))
            rev = (sum(1 for p in gext[na] if lg.edges[p].from_node == na)
                   + sum(1 for p in gext[nb] if lg.edges[p].to_node == nb))
            if rev > fwd:
                na, nb = nb, na
                geom = geom[::-1]
            group_edges.append((gi, fams, na, nb, geom))
        if reason is not None:
            stats.n_skipped += 1
            stats.skipped_zones.append((site, reason))
            log(f"zone at {site} families {zfams}: SKIPPED ({reason})")
            continue

        plans.append((zone_set, znodes, boundary, group_of_fam, attach,
                      group_edges, site, groups))
        stats.n_split += 1
        stats.split_zones.append((site, [tuple(g) for g in groups]))
        log(f"zone at {site}: SPLIT into {len(groups)} corridors "
            f"{[tuple(g) for g in groups]} "
            f"({len(zone)} fused edge(s), {sum(lg.edges[p].length_m for p in zone):.0f} m)")

    if not plans:
        return stats

    # ── apply all surgeries in one rebuild ──────────────────────────────
    from linegraph.model import LGEdge

    remove: set = set()
    new_edges: list = []
    next_node = max(n.node_id for n in lg.nodes) + 1
    node_moves: dict = {}
    rewires: dict = {}  # (edge pos, node id) -> new node id
    new_nodes: list = []

    for (zone_set, znodes, boundary, group_of_fam, attach, group_edges,
         site, groups) in plans:
        remove |= zone_set
        for gi, fams, na, nb, geom in group_edges:
            ends = {}
            for nid, pt in ((na, geom[0]), (nb, geom[-1])):
                if gi == 0:
                    node_moves[nid] = pt
                    ends[nid] = nid
                else:
                    lon, lat = to_wgs.transform(pt[0], pt[1])
                    new_nodes.append(LGNode(
                        node_id=next_node, lon=float(lon), lat=float(lat),
                        x=float(pt[0]), y=float(pt[1]), degree=0,
                        kind="junction"))
                    for p in boundary[nid]:
                        if group_of_fam.get(edge_fams.get(p, [None])[0]) == gi:
                            rewires[(p, nid)] = next_node
                    ends[nid] = next_node
                    next_node += 1
            sxy = [(float(x), float(y)) for x, y in geom]
            lons, lats = to_wgs.transform([p[0] for p in sxy],
                                          [p[1] for p in sxy])
            new_edges.append(LGEdge(
                edge_id=-1, from_node=ends[na], to_node=ends[nb],
                px_len=max(2, int(LineString(sxy).length / lg.resolution_m)),
                length_m=float(LineString(sxy).length),
                coords=list(zip(lons, lats)), coords_xy=sxy,
                # family lock: through the erstwhile blob both corridors
                # run within a merge width, so sample-level attribution
                # cannot tell them apart (the Q claiming the 6th Av
                # corridor through Herald Sq) — the surgery KNOWS whose
                # corridor this is
                families=frozenset().union(
                    *(fam_members[f] for f in fams)),
            ))

    lg.nodes = sorted(lg.nodes + new_nodes, key=lambda n: n.node_id)
    node_pos = {n.node_id: n for n in lg.nodes}
    for nid, pt in node_moves.items():
        n = node_pos[nid]
        n.x, n.y = float(pt[0]), float(pt[1])
        lon, lat = to_wgs.transform(n.x, n.y)
        n.lon, n.lat = float(lon), float(lat)

    kept_edges: list = []
    for pos, e in enumerate(lg.edges):
        if pos in remove:
            continue
        na = rewires.get((pos, e.from_node))
        nb = rewires.get((pos, e.to_node))
        touched = (na is not None or nb is not None
                   or e.from_node in node_moves or e.to_node in node_moves)
        if na is not None:
            e.from_node = na
        if nb is not None:
            e.to_node = nb
        if touched:
            pa = node_pos[e.from_node]
            pb = node_pos[e.to_node]
            xy = _blend_snap(np.asarray(e.coords_xy, dtype=float),
                             np.array([pa.x, pa.y]), np.array([pb.x, pb.y]),
                             25.0)
            e.coords_xy = [(float(x), float(y)) for x, y in xy]
            lons, lats = to_wgs.transform([p[0] for p in e.coords_xy],
                                          [p[1] for p in e.coords_xy])
            e.coords = list(zip(lons, lats))
            e.length_m = float(LineString(e.coords_xy).length)
        kept_edges.append(e)
    kept_edges.extend(new_edges)
    for k, e in enumerate(kept_edges):
        e.edge_id = k
    lg.edges = kept_edges
    stats.n_edges_removed = len(remove)
    stats.n_edges_added = len(new_edges)

    # degrees + orphaned interior nodes
    deg: dict = {}
    for e in lg.edges:
        deg[e.from_node] = deg.get(e.from_node, 0) + 1
        deg[e.to_node] = deg.get(e.to_node, 0) + 1
    lg.nodes = [n for n in lg.nodes if deg.get(n.node_id)]
    for n in lg.nodes:
        n.degree = deg[n.node_id]

    n_flipped = _normalize_chain_orientation(lg)
    if n_flipped:
        log(f"re-oriented {n_flipped} edge(s) head-to-tail along the new "
            f"corridors")
    return stats


def fam_shapes_for_group(fam_shapes, fams):
    return sorted({si for f in fams for si in fam_shapes.get(f, ())})
