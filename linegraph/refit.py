#!/usr/bin/env python3
"""linegraph.refit — shape-evidence geometry refit (phase B.1).

The raster skeleton is the right source of TOPOLOGY but a lossy source
of GEOMETRY: at X and T junctions the crossing/forking track's ink
bulges the stroked blob, so the medial axis of a through-corridor bows
toward the fork even though the underlying OSM-matched track is dead
straight (NYC receipts: the 7th Av 1/2/3 trunk bowing ~7 m where the L
crosses under it at 14 St; the 8th Av trunk wiggling where lines fork
away; the Nevins St 2/3 + 4/5 convergence). Chicago's Loop never showed
it because its junctions are genuine 90-degree corners.

After a COARSE attribution (the standard attribute.py core run on the
deduped input shapes, purely to learn which shapes ride which edge),
every edge's geometry is rebuilt from the OSM-matched shapes actually
attributed to it:

  1. per (edge, shape): the sub-polyline of the shape between the
     projections of the edge's two endpoints, extracted on a SINGLE
     PASS of the shape near the edge — a contiguous within-radius run,
     the same guard as segments' _shape_passes — because loop shapes
     (CTA Brn/Org/P/Pink) pass a junction twice and projecting onto the
     whole shape would splice the two passes. A pass qualifies when
     both endpoint projections land near the edge's endpoints and the
     spanned length is within COVER_RATIO of the edge length; a route
     merely CROSSING the edge projects both endpoints to nearly one
     point and is rejected here, so it never votes on the
     through-corridor's geometry.
  2. qualifying sub-polylines are resampled to a common arc-length
     parameterization and averaged pointwise: directional track pairs
     ~5-10 m apart average to the true corridor centerline; identical
     shapes average to themselves. The average is CLUSTER-WEIGHTED
     (_cluster_weighted_mean): contributions group into lateral
     clusters (tracks) and each cluster gets equal weight, so a
     corridor ridden by 18 pattern variants on one directional track
     and 5 on the other still centers between the TRACKS, not at the
     variant-count-weighted mean (Bowling Green receipt: the plain
     mean sat +2.2 m off the skeleton toward the busier track, hugging
     the island platform's east edge instead of the corridor center).
  3. node placement: a degree-1 node moves to its single refit
     endpoint; every other node moves to the Tikhonov-regularized
     least-squares intersection of its incident edges' terminal
     tangent LINES (regularized toward the mean refit endpoint, which
     also serves as the sanity fallback). The mean alone cannot fix
     shallow crossings: a ~30-degree X skeletonizes into an "H" (two
     deg-3 nodes + a bisector rung) whose nodes sit BETWEEN the tracks,
     and averaging endpoints leaves them there; the tangent-line
     intersection slides each node ALONG the through-tracks to the true
     crossing point — a pure-tangential move that bends nothing.
     Adjacent nodes that would collapse onto each other (the perfect-X
     limit of that "H") keep a NODE_FLOOR_M separation along their
     original axis so no edge degenerates to zero length.
     Y-fork exception (deg-3): when traversal evidence identifies a
     DOMINANT through-pair — the straightest arm pair some shape rides
     through, clearly straighter than any other evidenced pair — the
     node solves the LSQ over THAT PAIR's tangent lines only, so the
     diverging arm cannot tug the node off the through line (Lafayette
     Av receipt: the G merging into the A/C corridor bent both
     throughs ~5 m past their own evidence). The diverging arm then
     terminal-snaps to the result like any other arm. Deg-3 nodes with
     no evidenced pair or without a clearly straightest one (and all
     multi-through X nodes) keep the all-arms LSQ.
  4. every incident edge's terminal vertex then snaps EXACTLY to its
     node, the correction blended linearly over the terminal BLEND_M
     (whole-edge lerp when the edge is shorter than twice that) so no
     kink is introduced inside the edge.

Robustness: an edge with no usable shape evidence keeps its skeleton
geometry (line-less connector rungs, spurs); a refit that moves any
point farther than the merge width from the skeleton geometry indicates
mis-attribution, so that edge keeps its skeleton geometry too and is
logged. Topology — node ids, edge ids, incidence — is never touched.

Config-gated in build.enrich_graph (default ON, --no-refit to disable).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np
import shapely
from pyproj import Transformer
from shapely.geometry import LineString, Point
from shapely.ops import substring

from linegraph.attribute import (DEFAULT_SAMPLE_M, DEVIATION_GATE_M,
                                 EdgeSnapIndex, attribute_shape_xy,
                                 sample_polyline_xy)

RESAMPLE_STEP_M = 5.0     # common-parameterization target spacing
RESAMPLE_MIN_PTS = 16     # short edges still average on a dense frame
RESAMPLE_MAX_PTS = 512    # long edges stay bounded
PASS_DENSIFY_M = 5.0      # shape densification for pass extraction
COVER_RATIO_MIN = 0.5     # accepted sub-polyline span / edge length
COVER_RATIO_MAX = 2.0
BLEND_M = 25.0            # terminal snap-correction blend length
TANGENT_WINDOW_M = 20.0   # terminal tangent estimation window
NODE_MU = 0.005           # Tikhonov weight toward the mean endpoint —
                          # a numerical floor only: it must stay well
                          # below the tangent bundle's weak eigenvalue
                          # at a shallow crossing (~0.13 at 30 deg,
                          # sum of sin^2 to the bundle axis) or the
                          # node stalls short of the true crossing
                          # point along the corridor
NODE_SANITY_MULT = 2.5    # LSQ farther than this x merge_width from the
                          # mean endpoint falls back to the mean
NODE_FLOOR_M = 4.0        # min separation of adjacent nodes after refit
SIMPLIFY_M = 0.5          # post-refit Douglas-Peucker tolerance
LENGTH_OUTLIER_FRAC = 0.10
CLUSTER_GAP_M = 3.0       # lateral gap that separates contribution
                          # clusters (tracks) in the weighted average —
                          # above directional-pair jitter (~0.5 m),
                          # below real track spacing (~3.7 m+)
THROUGH_MARGIN_DEG = 5.0  # a deg-3 dominant through-pair must be this
                          # much straighter than the runner-up pair


@dataclass(slots=True)
class RefitStats:
    n_edges: int = 0
    n_refit: int = 0              # edges rebuilt from shape evidence
    n_no_evidence: int = 0        # kept skeleton: no usable sub-polyline
    n_capped: int = 0             # kept skeleton: refit beyond merge width
    n_contributions: int = 0      # accepted sub-polylines across all edges
    n_node_fallback: int = 0      # LSQ sanity fallbacks to the mean
    n_floor_pairs: int = 0        # adjacent-node separations enforced
    n_y_through: int = 0          # deg-3 nodes placed on a dominant
                                  # through-pair's tangents only
    max_point_move_m: float = 0.0  # refit vs skeleton, accepted edges
    max_node_move_m: float = 0.0
    length_outliers: list = field(default_factory=list)  # (edge_id, before, after)
    capped_edges: list = field(default_factory=list)     # (edge_id, stray_m)
    no_evidence_edges: list = field(default_factory=list)


def _n_pts(length_m: float) -> int:
    return int(np.clip(math.ceil(length_m / RESAMPLE_STEP_M) + 1,
                       RESAMPLE_MIN_PTS, RESAMPLE_MAX_PTS))


def _resample(coords, n: int) -> np.ndarray:
    """Polyline -> n points uniformly spaced in arc length."""
    pts = np.asarray(coords, dtype=float)
    if len(pts) < 2:
        return np.repeat(pts[:1], n, axis=0)
    seg = np.hypot(*(pts[1:] - pts[:-1]).T)
    cum = np.concatenate([[0.0], np.cumsum(seg)])
    if cum[-1] <= 0.0:
        return np.repeat(pts[:1], n, axis=0)
    t = np.linspace(0.0, cum[-1], n)
    return np.column_stack(
        [np.interp(t, cum, pts[:, 0]), np.interp(t, cum, pts[:, 1])]
    )


def _cluster_weighted_mean(contribs) -> np.ndarray:
    """Pointwise average with equal weight per lateral CLUSTER.

    Contributions (same-shape arrays, oriented alike) cluster by their
    mean signed offset from the plain pointwise mean, split at gaps >
    CLUSTER_GAP_M; each cluster (track) weighs the same regardless of
    how many pattern variants ride it. One cluster == the plain mean.
    """
    stack = np.asarray(contribs, dtype=float)
    if len(stack) < 3:
        return stack.mean(axis=0)
    ref = stack.mean(axis=0)
    d = np.gradient(ref, axis=0)
    norm = np.hypot(d[:, 0], d[:, 1])
    norm[norm < 1e-9] = 1.0
    nrm = np.column_stack([-d[:, 1] / norm, d[:, 0] / norm])
    offs = np.einsum("cij,ij->c", stack - ref[None, :, :], nrm) / len(ref)
    order = np.argsort(offs, kind="stable")
    weights = np.empty(len(stack))
    start = 0
    for k in range(1, len(order) + 1):
        if k == len(order) or offs[order[k]] - offs[order[k - 1]] > CLUSTER_GAP_M:
            weights[order[start:k]] = 1.0 / (k - start)
            start = k
    return np.average(stack, axis=0, weights=weights)


def _pass_coords(shape_pts: np.ndarray, within: np.ndarray) -> list:
    """Contiguous within-radius runs of a densified shape.

    Returns coordinate arrays, one per pass. A closed shape whose seam
    falls inside a run has its end+start runs rejoined (segments'
    _shape_passes seam rule), so one physical pass is never split in
    two at the shape's arbitrary start vertex.
    """
    idx = np.flatnonzero(within)
    if idx.size == 0:
        return []
    breaks = np.flatnonzero(np.diff(idx) > 1)
    runs = np.split(idx, breaks + 1)
    closed = bool(np.hypot(*(shape_pts[0] - shape_pts[-1])) < 1e-6)
    if (closed and len(runs) > 1 and runs[0][0] == 0
            and runs[-1][-1] == len(shape_pts) - 1):
        joined = np.vstack([shape_pts[runs[-1][:-1]], shape_pts[runs[0]]])
        return [joined] + [shape_pts[r] for r in runs[1:-1]]
    return [shape_pts[r] for r in runs]


def _terminal_tangent(xy: np.ndarray, terminal: str, skip_m: float = 0.0):
    """(anchor point, unit direction) over the terminal TANGENT_WINDOW_M.

    Sign is irrelevant to the caller (line projection is symmetric in
    +-t), so the direction simply points into the edge. skip_m anchors
    the window that far INTO the edge — the dominant through-pair node
    placement skips the terminal blend zone, where a fork's diverging
    evidence contaminates the averaged geometry, and reads the clean
    corridor line beyond it.
    """
    pts = xy if terminal == "a" else xy[::-1]
    seg = np.hypot(*(pts[1:] - pts[:-1]).T)
    cum = np.concatenate([[0.0], np.cumsum(seg)])
    if skip_m > 0.0 and cum[-1] > 2.0 * skip_m:
        i0 = int(np.searchsorted(cum, skip_m))
        i0 = min(max(i0, 0), len(pts) - 2)
    else:
        i0 = 0
    p0 = pts[i0]
    k = int(np.searchsorted(cum, cum[i0] + TANGENT_WINDOW_M))
    k = min(max(k, i0 + 1), len(pts) - 1)
    d = pts[k] - p0
    n = float(np.hypot(*d))
    if n < 1e-9:
        d = pts[-1] - p0
        n = float(np.hypot(*d))
        if n < 1e-9:
            return p0, np.array([1.0, 0.0])
    return p0, d / n


def _blend_snap(xy: np.ndarray, pa: np.ndarray, pb: np.ndarray,
                blend_m: float) -> np.ndarray:
    """Snap terminals to pa/pb, corrections blended over the terminal
    blend_m (whole-edge lerp below 2x blend_m). Terminal vertices land
    EXACTLY on the node coordinates."""
    xy = np.array(xy, dtype=float, copy=True)
    da = pa - xy[0]
    db = pb - xy[-1]
    seg = np.hypot(*(xy[1:] - xy[:-1]).T)
    cum = np.concatenate([[0.0], np.cumsum(seg)])
    total = float(cum[-1])
    if total <= 0.0:
        xy += da
    elif total < 2.0 * blend_m:
        t = cum / total
        xy += np.outer(1.0 - t, da) + np.outer(t, db)
    else:
        wa = np.clip(1.0 - cum / blend_m, 0.0, None)
        wb = np.clip(1.0 - (total - cum) / blend_m, 0.0, None)
        xy += np.outer(wa, da) + np.outer(wb, db)
    xy[0] = pa
    xy[-1] = pb
    return xy


def _edge_contributions(edge, shape_ids, dense, dense_geoms,
                        pass_radius_m: float):
    """Qualifying sub-polylines for one edge, resampled onto its common
    parameterization and oriented from the edge's start to its end."""
    exy = np.asarray(edge.coords_xy, dtype=float)
    if len(exy) < 2:
        return []
    edge_line = LineString(exy)
    shapely.prepare(edge_line)
    elen = float(edge_line.length)
    if elen <= 0.0:
        return []
    n = _n_pts(elen)
    A, B = exy[0], exy[-1]
    pt_a, pt_b = Point(A), Point(B)
    contribs = []
    for si in shape_ids:
        pts = dense[si]
        within = shapely.distance(dense_geoms[si], edge_line) <= pass_radius_m
        for run in _pass_coords(pts, within):
            if len(run) < 2:
                continue
            pass_line = LineString(run)
            sa = float(pass_line.project(pt_a))
            sb = float(pass_line.project(pt_b))
            qa, qb = pass_line.interpolate(sa), pass_line.interpolate(sb)
            if (math.hypot(qa.x - A[0], qa.y - A[1]) > pass_radius_m
                    or math.hypot(qb.x - B[0], qb.y - B[1]) > pass_radius_m):
                continue  # the pass never reaches this terminal
            span = abs(sb - sa)
            if not (COVER_RATIO_MIN * elen <= span <= COVER_RATIO_MAX * elen):
                continue  # crossing/partial evidence, not a traversal
            sub = substring(pass_line, min(sa, sb), max(sa, sb))
            sub_xy = list(sub.coords)
            if len(sub_xy) < 2:
                continue
            if sa > sb:
                sub_xy.reverse()
            contribs.append(_resample(sub_xy, n))
    return contribs


def _dominant_through_pair(ends, edge_shapes):
    """Dominant through-pair among a deg-3 node's arms, or None.

    ends: [(edge pos, endpoint, unit tangent into the edge), ...].
    Evidenced pair = some shape rides both arms (coarse attribution).
    Dominant = the straightest evidenced pair (tangents most nearly
    collinear), by THROUGH_MARGIN_DEG over the runner-up when several
    pairs are evidenced (a genuine multi-through node keeps the
    all-arms LSQ). Returns (i, j) indices into ends.
    """
    cands = []
    for i in range(len(ends)):
        for j in range(i + 1, len(ends)):
            pi, pj = ends[i][0], ends[j][0]
            if pi == pj:
                continue  # both terminals of a self-loop
            if not (set(edge_shapes.get(pi, ()))
                    & set(edge_shapes.get(pj, ()))):
                continue
            cos = abs(float(np.dot(ends[i][2], ends[j][2])))
            cands.append((math.degrees(math.acos(min(1.0, cos))), i, j))
    if not cands:
        return None
    cands.sort()
    if len(cands) > 1 and cands[1][0] - cands[0][0] < THROUGH_MARGIN_DEG:
        return None
    return cands[0][1], cands[0][2]


def refit_geometry(lg, shapes_lonlat, *,
                   sample_m: float = DEFAULT_SAMPLE_M,
                   snap_radius_m: float | None = None,
                   deviation_gate_m: float = DEVIATION_GATE_M,
                   pass_radius_m: float | None = None,
                   blend_m: float = BLEND_M) -> RefitStats:
    """Rebuild every edge's geometry from the shapes attributed to it.

    Mutates lg in place (edge coords/coords_xy/length_m and node
    positions). Topology is untouched; px_len keeps the skeleton trace
    count. shapes_lonlat is the deduped input shape list the raster was
    stamped from (build.dedup_shapes output).
    """
    stats = RefitStats(n_edges=len(lg.edges))
    if not lg.edges or not shapes_lonlat:
        return stats
    if pass_radius_m is None:
        pass_radius_m = deviation_gate_m

    to_xy = Transformer.from_crs(4326, lg.epsg, always_xy=True)
    shapes_xy = []
    for coords in shapes_lonlat:
        xs, ys = to_xy.transform([c[0] for c in coords], [c[1] for c in coords])
        shapes_xy.append(list(zip(xs, ys)))

    # ── coarse attribution: which shapes ride which edge ────────────────
    index = EdgeSnapIndex(lg)
    edge_shapes: dict = {}
    for si, sxy in enumerate(shapes_xy):
        if len(sxy) < 2:
            continue
        ridden, _, _, _ = attribute_shape_xy(
            index, sxy, sample_m=sample_m, snap_radius_m=snap_radius_m,
            deviation_gate_m=deviation_gate_m,
        )
        for eid in ridden:
            edge_shapes.setdefault(eid, []).append(si)

    used = sorted({si for lst in edge_shapes.values() for si in lst})
    dense = {si: sample_polyline_xy(shapes_xy[si], PASS_DENSIFY_M)
             for si in used}
    dense_geoms = {si: shapely.points(dense[si][:, 0], dense[si][:, 1])
                   for si in used}

    # ── per-edge averaged refit (dense; snapping + simplify follow) ─────
    refit_xy: dict = {}
    contribs_of: dict = {}  # pos -> {shape idx: [contribution arrays]}
    for pos, e in enumerate(lg.edges):
        shape_ids = edge_shapes.get(pos, [])
        if not shape_ids:
            stats.n_no_evidence += 1
            stats.no_evidence_edges.append(e.edge_id)
            continue
        by_shape = {
            si: _edge_contributions(e, [si], dense, dense_geoms,
                                    pass_radius_m)
            for si in shape_ids
        }
        contribs = [arr for arrs in by_shape.values() for arr in arrs]
        if not contribs:
            stats.n_no_evidence += 1
            stats.no_evidence_edges.append(e.edge_id)
            continue
        contribs_of[pos] = by_shape
        avg = _cluster_weighted_mean(contribs)
        edge_line = LineString(e.coords_xy)
        shapely.prepare(edge_line)
        dmax = float(
            shapely.distance(shapely.points(avg[:, 0], avg[:, 1]),
                             edge_line).max()
        )
        if dmax > lg.merge_width_m:
            stats.n_capped += 1
            stats.capped_edges.append((e.edge_id, round(dmax, 1)))
            continue  # mis-attribution guard: keep skeleton geometry
        stats.n_refit += 1
        stats.n_contributions += len(contribs)
        stats.max_point_move_m = max(stats.max_point_move_m, dmax)
        refit_xy[pos] = avg

    # ── node placement ───────────────────────────────────────────────────
    old_pos = {n.node_id: np.array([n.x, n.y]) for n in lg.nodes}
    incident: dict = {}
    for pos, e in enumerate(lg.edges):
        xy = refit_xy.get(pos)
        if xy is None:
            xy = np.asarray(e.coords_xy, dtype=float)
        if len(xy) < 2:
            continue
        for nid, terminal in ((e.from_node, "a"), (e.to_node, "b")):
            incident.setdefault(nid, []).append(
                (pos, *_terminal_tangent(xy, terminal), xy, terminal))

    new_pos = {}
    y_through: list = []  # (node id, (edge pos, edge pos), shared shapes)
    for nid in sorted(old_pos):
        ends = incident.get(nid)
        if not ends:
            new_pos[nid] = old_pos[nid]
            continue
        if len(ends) == 1:
            x = ends[0][1]  # endpoint node: exactly its refit terminal
        else:
            lines = [(p, t) for _, p, t, _, _ in ends]
            if len(ends) == 3:
                # Y fork: a dominant through-pair pins the node to ITS
                # tangent lines only — the diverging arm cannot tug the
                # node off the through line. Tangents re-read beyond
                # the terminal blend zone, where the fork's diverging
                # evidence contaminates the averaged geometry.
                pair = _dominant_through_pair(ends, edge_shapes)
                if pair is not None:
                    lines = [
                        _terminal_tangent(ends[k][3], ends[k][4],
                                          skip_m=blend_m)
                        for k in pair
                    ]
                    pa, pb = ends[pair[0]][0], ends[pair[1]][0]
                    y_through.append((
                        nid, (pa, pb),
                        set(edge_shapes.get(pa, ()))
                        & set(edge_shapes.get(pb, ())),
                    ))
                    stats.n_y_through += 1
            pbar = np.mean([p for p, _ in lines], axis=0)
            # Tikhonov-regularized LSQ intersection of the tangent lines
            a_mat = NODE_MU * np.eye(2)
            b_vec = NODE_MU * pbar
            for p, t in lines:
                proj = np.eye(2) - np.outer(t, t)
                a_mat += proj
                b_vec += proj @ p
            x = np.linalg.solve(a_mat, b_vec)
            if float(np.hypot(*(x - pbar))) > NODE_SANITY_MULT * lg.merge_width_m:
                x = pbar
                stats.n_node_fallback += 1
        new_pos[nid] = x
        stats.max_node_move_m = max(
            stats.max_node_move_m, float(np.hypot(*(x - old_pos[nid])))
        )

    # dominant-pair arms follow the THROUGH shapes' own average over the
    # terminal blend zone: the diverging arm's evidence rides the trunk
    # too and its rising tail would otherwise bend the through-bar. The
    # zone takes the through average's SHAPE plus a fading offset read
    # at the zone boundary — the corridor's lateral offset eases to
    # zero at the node while the contaminated tail is discarded whole.
    for nid, (pa, pb), shared in y_through:
        for pos in (pa, pb):
            xy = refit_xy.get(pos)
            by_shape = contribs_of.get(pos)
            if xy is None or not by_shape or not shared:
                continue
            thru = [arr for si in sorted(shared) for arr in by_shape.get(si, ())]
            if not thru or len(thru) == len(
                    [a for arrs in by_shape.values() for a in arrs]):
                continue  # no through subset, or nothing to exclude
            thru_avg = _cluster_weighted_mean(thru)
            e = lg.edges[pos]
            seg = np.hypot(*(xy[1:] - xy[:-1]).T)
            cum = np.concatenate([[0.0], np.cumsum(seg)])
            s = cum if e.from_node == nid else cum[-1] - cum
            w = np.clip(1.0 - s / blend_m, 0.0, 1.0)  # 1 at the node end
            inside = w > 0.0
            outside = np.flatnonzero(~inside)
            i_b = (int(outside[0]) if e.from_node == nid and outside.size
                   else int(outside[-1]) if outside.size
                   else int(np.argmin(w)))
            delta_b = xy[i_b] - thru_avg[i_b]
            blended = thru_avg + (1.0 - w)[:, None] * delta_b[None, :]
            refit_xy[pos] = np.where(inside[:, None], blended, xy)

    # adjacent nodes may not collapse onto each other (perfect-X limit):
    # push violating pairs apart along their ORIGINAL axis
    for _ in range(5):
        changed = False
        for e in lg.edges:
            if e.from_node == e.to_node:
                continue
            a, b = new_pos[e.from_node], new_pos[e.to_node]
            if float(np.hypot(*(b - a))) >= NODE_FLOOR_M:
                continue
            axis = old_pos[e.to_node] - old_pos[e.from_node]
            norm = float(np.hypot(*axis))
            axis = axis / norm if norm > 1e-9 else np.array([1.0, 0.0])
            mid = (a + b) / 2.0
            new_pos[e.from_node] = mid - 0.5 * NODE_FLOOR_M * axis
            new_pos[e.to_node] = mid + 0.5 * NODE_FLOOR_M * axis
            stats.n_floor_pairs += 1
            changed = True
        if not changed:
            break

    # ── snap terminals, simplify, write back ────────────────────────────
    to_wgs = Transformer.from_crs(lg.epsg, 4326, always_xy=True)
    for pos, e in enumerate(lg.edges):
        xy = refit_xy.get(pos)
        kept = xy is None
        if kept:
            da = new_pos[e.from_node] - np.asarray(e.coords_xy[0])
            db = new_pos[e.to_node] - np.asarray(e.coords_xy[-1])
            if float(np.hypot(*da)) < 1e-9 and float(np.hypot(*db)) < 1e-9:
                continue  # untouched: keep skeleton geometry verbatim
            xy = _resample(e.coords_xy, _n_pts(e.length_m))
        xy = _blend_snap(xy, new_pos[e.from_node], new_pos[e.to_node], blend_m)
        line = LineString(xy).simplify(SIMPLIFY_M, preserve_topology=False)
        sxy = [(float(px), float(py)) for px, py in line.coords]
        sxy[0] = (float(new_pos[e.from_node][0]), float(new_pos[e.from_node][1]))
        sxy[-1] = (float(new_pos[e.to_node][0]), float(new_pos[e.to_node][1]))
        before_len = e.length_m
        e.coords_xy = sxy
        e.length_m = float(LineString(sxy).length)
        lons, lats = to_wgs.transform([p[0] for p in sxy], [p[1] for p in sxy])
        e.coords = list(zip(lons, lats))
        if (not kept and before_len > 0.0
                and abs(e.length_m - before_len) / before_len
                > LENGTH_OUTLIER_FRAC):
            stats.length_outliers.append(
                (e.edge_id, round(before_len, 1), round(e.length_m, 1))
            )

    for n in lg.nodes:
        p = new_pos[n.node_id]
        n.x, n.y = float(p[0]), float(p[1])
        lon, lat = to_wgs.transform(n.x, n.y)
        n.lon, n.lat = float(lon), float(lat)
    return stats
