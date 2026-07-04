#!/usr/bin/env python3
"""linegraph.corridors — track-exact corridors on the OSM way graph.

Stage 4, way-graph era (replaces raster/skeleton/vector/refit/unfuse —
those modules remain only behind --legacy-raster). Guarantees:

  R1  the rendered centerline follows REAL track geometry: raw corridors
      are concatenated OSM way polylines VERBATIM (dedup of coincident
      vertices only); merged ribbons are weighted midlines of real
      tracks; forks happen at the real switch nodes.
  R2  routes bundle only when they share track, with exactly three
      principled merges:
        1. directional-pair merge  — corridors with IDENTICAL route sets
           sustained-parallel within pair_gap_m collapse to their
           equal-weight midline (this is what centers a ribbon between
           an island platform's flanking tracks — the Bowling Green
           lesson: tracks weigh equally, never per-route);
        2. same-family merge      — corridors whose colour-family sets
           are EQUAL but route sets differ (N/Q vs R/W) merge within
           family_gap_m into one ribbon carrying the union set (the
           approved single yellow Broadway ribbon; 4-track trunks);
        3. cross-family proximity bundle — corridors of DIFFERENT
           families merge only where they run tightly parallel
           (cross_family_gap_m) for a sustained stretch
           (cross_family_min_len_m) with low relative bearing
           (parallel, not crossing); the merged stretch covers
           exactly the qualifying window and its boundaries are
           composition-change junctions (Chicago's Lake-leg Blue under
           the elevated; the Loop legs' two one-way tracks).

Windows are flap-guarded (Schmitt trigger, all merge kinds): real
4-track spacing breathes across the gap threshold, so qualifying
windows separated by a dip shorter than window_dip_coalesce_m coalesce
(the dip inherits the bundled state), and a formed window only releases
where the gap exceeds release_gap_mult x gap sustained for at least
release_sustain_m — the Sea Beach directional pair used to tile into
five windows with jagged seams between them.

Window boundaries SNAP to corridor ends within merge_end_slack_m (the
seam lands on the real convergence junction, never a synthetic node
floating half a block from the switch), and every boundary is
C1-CONTINUOUS: where exactly one constituent continues past the window
end, the midline eases into the continuing track over ease_len_m
(smoothstep, clamped to the window) so the through track leaves the
bundle tangentially with ZERO step; where both continue (proximity
bundle onset), the cut tails ease onto the seam over the same length.
Eased geometry that would self-intersect falls back to the plain
midline (logged).

Sustained-parallel for merges 1-2 = gap under threshold for the full
overlap with no divergence beyond it: at each window end at least one
corridor must END within merge_end_slack_m — two corridors that both
continue while the gap grows are crossing/brushing and NEVER merge.

Ridden crossovers (a turnback diagonal lying between the tracks of the
bundle it connects — its routes a strict subset of a neighboring
corridor's) are ABSORBED: dropped without bending the through geometry,
their break nodes re-chained away.

Junctions are way-graph nodes where corridor route sets change — the
real switches. Merge-boundary nodes are the only synthetic nodes, placed
on the weighted midline within half a track gap of the physical switch.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field

import numpy as np
import shapely
from pyproj import Transformer
from shapely import STRtree
from shapely.geometry import LineString, Point
from shapely.ops import substring

from linegraph.model import FORMAT_VERSION, LGEdge, LGNode, LineGraph
from linegraph.waygraph import Usage, WaygraphConfig, WayIndex

SYNTH_BASE = 10 ** 15  # synthetic node ids start here (above OSM ids)


@dataclass(slots=True)
class Corr:
    cid: int
    routes: frozenset        # route_id strings
    families: frozenset      # color_key strings
    u: int
    v: int
    pts: np.ndarray          # (n, 2) projected meters, oriented u -> v
    tracks: int = 1          # physical tracks represented (midline weight)

    @property
    def length(self) -> float:
        d = np.hypot(*(self.pts[1:] - self.pts[:-1]).T)
        return float(d.sum())

    def line(self) -> LineString:
        return LineString(self.pts)


@dataclass(slots=True)
class MergeRecord:
    kind: str                # pair | family | cross
    routes_a: tuple
    routes_b: tuple
    window_len_m: float
    gap_mean_m: float
    gap_max_m: float
    bearing_mean_deg: float
    at_lonlat: tuple


@dataclass(slots=True)
class BuildNotes:
    merges: list = field(default_factory=list)      # [MergeRecord]
    rejects: list = field(default_factory=list)     # near-miss cross bundles
    n_absorbed: int = 0
    n_contracted: int = 0
    n_offgraph: int = 0
    n_raw_corridors: int = 0


# ── small geometry helpers ───────────────────────────────────────────────────


def _dedup(pts: np.ndarray) -> np.ndarray:
    if len(pts) < 2:
        return pts
    keep = np.ones(len(pts), dtype=bool)
    keep[1:] = np.hypot(*(pts[1:] - pts[:-1]).T) > 1e-6
    return pts[keep]


def _cum(pts: np.ndarray) -> np.ndarray:
    return np.concatenate([[0.0], np.cumsum(np.hypot(*(pts[1:] - pts[:-1]).T))])


def _interp(pts: np.ndarray, cum: np.ndarray, t: float) -> np.ndarray:
    return np.array([np.interp(t, cum, pts[:, 0]),
                     np.interp(t, cum, pts[:, 1])])


def _bearing(pts: np.ndarray, cum: np.ndarray, t: float, h: float = 8.0):
    a = _interp(pts, cum, max(0.0, t - h))
    b = _interp(pts, cum, min(cum[-1], t + h))
    d = b - a
    return math.atan2(d[1], d[0])


def _bearing_diff_deg(b1: float, b2: float) -> float:
    """Undirected: 0 = parallel or antiparallel, 90 = perpendicular."""
    return math.degrees(abs((b1 - b2 + math.pi / 2) % math.pi - math.pi / 2))


def _smoothstep(x):
    """C1 easing profile: 0 -> 0, 1 -> 1, zero slope at both ends."""
    x = np.clip(x, 0.0, 1.0)
    return x * x * (3.0 - 2.0 * x)


def _densify_end(pts: np.ndarray, at_start: bool, span_m: float,
                 step_m: float = 5.0) -> np.ndarray:
    """Insert vertices within span_m of one end (rest stays VERBATIM).

    A blend can only bend at vertices — a 2-vertex straight segment
    would shift as a rigid chord no matter the easing profile.
    """
    cum = _cum(pts)
    total = cum[-1]
    d = cum if at_start else (total - cum)
    out = [pts[0]]
    for i in range(1, len(pts)):
        a, b = cum[i - 1], cum[i]
        da, db = (d[i - 1], d[i])
        if max(min(da, db), 0.0) < span_m and b - a > 1.5 * step_m:
            for t in np.arange(a + step_m, b - step_m / 2, step_m):
                out.append(_interp(pts, cum, float(t)))
        out.append(pts[i])
    return np.asarray(out, dtype=float)


def _blend_to(pts: np.ndarray, at_start: bool, target: np.ndarray,
              blend_m: float) -> np.ndarray:
    """Move one endpoint onto `target`, easing the shift over blend_m.

    The decay is a smoothstep (C1 at both ends) so a blended tail leaves
    the seam tangentially instead of with the kink a linear ramp makes.
    """
    pts = np.asarray(pts, dtype=float).copy()
    if len(pts) < 2:
        return np.array([target, target], dtype=float)
    pts = _densify_end(pts, at_start, blend_m)
    cum = _cum(pts)
    total = cum[-1]
    if total <= 0:
        return np.array([target, target], dtype=float)
    d = cum if at_start else (total - cum)
    delta = np.asarray(target, dtype=float) - (pts[0] if at_start else pts[-1])
    w = _smoothstep(1.0 - d / max(min(blend_m, total), 1e-9))
    out = _dedup(pts + w[:, None] * delta[None, :])
    if len(out) >= 4 and not LineString(out).is_simple:
        # eased shift folded the geometry — fall back to the linear ramp
        w = np.clip(1.0 - d / max(min(blend_m, total), 1e-9), 0.0, 1.0)
        out = _dedup(pts + w[:, None] * delta[None, :])
    return out


# ── corridor graph state ─────────────────────────────────────────────────────


class _State:
    def __init__(self, cfg: WaygraphConfig, verbose: bool):
        self.cfg = cfg
        self.verbose = verbose
        self.nodes: dict = {}          # nid -> np.array([x, y])
        self.corrs: dict = {}          # cid -> Corr
        self._next_cid = 0
        self._next_nid = SYNTH_BASE
        self.notes = BuildNotes()

    def log(self, msg: str):
        if self.verbose:
            print(f"[corridors] {msg}", flush=True)

    def new_node(self, xy) -> int:
        nid = self._next_nid
        self._next_nid += 1
        self.nodes[nid] = np.asarray(xy, dtype=float)
        return nid

    def add_corr(self, routes, families, u, v, pts, tracks=1) -> int:
        cid = self._next_cid
        self._next_cid += 1
        pts = _dedup(np.asarray(pts, dtype=float))
        if len(pts) < 2:
            pts = np.array([self.nodes[u], self.nodes[v]])
        self.corrs[cid] = Corr(cid, frozenset(routes), frozenset(families),
                               u, v, pts, tracks)
        return cid

    def incidence(self) -> dict:
        inc: dict = {}
        for c in self.corrs.values():
            inc.setdefault(c.u, []).append(c.cid)
            inc.setdefault(c.v, []).append(c.cid)
        return inc

    def retarget_node(self, old: int, new: int):
        """Every corridor endpoint at `old` moves to `new` (blended)."""
        if old == new:
            return
        tgt = self.nodes[new]
        doomed = []
        for c in self.corrs.values():
            if c.u == old:
                c.pts = _blend_to(c.pts, True, tgt, self.cfg.blend_m)
                c.u = new
            if c.v == old:
                c.pts = _blend_to(c.pts, False, tgt, self.cfg.blend_m)
                c.v = new
            if c.u == new or c.v == new:
                if c.u == c.v and c.length < 1.0:
                    doomed.append(c.cid)  # fully collapsed into the node
                elif len(c.pts) < 2:
                    c.pts = np.array([self.nodes.get(c.u, tgt),
                                      self.nodes.get(c.v, tgt)], dtype=float)
        for cid in doomed:
            self.corrs.pop(cid, None)
        self.nodes.pop(old, None)

    def rechain(self):
        """Join corridor pairs at degree-2 equal-route-set nodes,
        keeping head-to-tail orientation (stage-5 slot frames)."""
        changed = True
        while changed:
            changed = False
            inc = self.incidence()
            for nid in sorted(inc):
                cids = inc[nid]
                if len(cids) != 2 or cids[0] == cids[1]:
                    continue
                a, b = self.corrs[cids[0]], self.corrs[cids[1]]
                if a.routes != b.routes:
                    continue
                ap = a.pts if a.v == nid else a.pts[::-1]
                au = a.u if a.v == nid else a.v
                bp = b.pts if b.u == nid else b.pts[::-1]
                bv = b.v if b.u == nid else b.u
                if au == bv and len(inc.get(au, ())) <= 2:
                    continue  # floating loop; leave as two edges
                merged = _dedup(np.vstack([ap, bp]))
                self.corrs.pop(a.cid)
                self.corrs.pop(b.cid)
                self.add_corr(a.routes, a.families | b.families,
                              au, bv, merged, max(a.tracks, b.tracks))
                self.nodes.pop(nid, None)
                changed = True
                break

    def drop_orphan_nodes(self):
        used = set()
        for c in self.corrs.values():
            used.add(c.u)
            used.add(c.v)
        for nid in [n for n in self.nodes if n not in used]:
            self.nodes.pop(nid)


# ── phase 1: raw corridors from the used way subgraph ───────────────────────


def _coverage_cuts(index: WayIndex, usage: Usage, eid: int,
                   cfg: WaygraphConfig):
    """Interior arc positions where the riding route set changes.

    A route whose coverage of a long way edge starts/ends mid-edge
    entered or left the track partway (the FX express rejoining the
    Culver local mid-way, a service short-turning between way nodes):
    the edge is cut at the coverage boundary instead of the route
    claiming track it never rides. Only cuts that actually CHANGE the
    piece route set survive; boundaries within cov_cut_margin_m of an
    edge end are snap noise.
    """
    rc = _filtered_cov(usage, eid, cfg)
    if not rc:
        return []
    elen = index.lines[eid].length
    margin = cfg.cov_cut_margin_m
    cand = set()
    for ivals in rc.values():
        if ivals is None:
            continue
        for lo, hi in ivals:
            if lo > margin:
                cand.add(lo)
            if hi < elen - margin:
                cand.add(hi)
    if not cand:
        return []
    cuts = []
    for p in sorted(cand):  # dedupe boundaries a sample step apart
        if cuts and p - cuts[-1] < 10.0:
            continue
        cuts.append(p)
    bounds = [0.0] + cuts + [elen]
    sets = [_piece_routes(rc, a, b) for a, b in zip(bounds, bounds[1:])]
    return [p for i, p in enumerate(cuts)
            if sets[i] and sets[i + 1] and sets[i] != sets[i + 1]]


def _filtered_cov(usage: Usage, eid: int, cfg: WaygraphConfig):
    """edge_route_cov with junction pass-through touches removed.

    A coverage interval shorter than edge_member_min_m is samples
    grabbing the edge for a moment where alignments graze (the FX
    touching the Culver local for 13 m at the bypass portal) — it must
    neither place a cut nor claim a piece. A route whose intervals ALL
    filter away keeps its raw union (old behavior — never silently drop
    a route the membership machinery accepted).
    """
    rc = usage.edge_route_cov.get(eid)
    if not rc:
        return rc
    bridge = 2.0 * cfg.cov_cut_margin_m
    out = {}
    for r, ivals in rc.items():
        if ivals is None:
            out[r] = None
            continue
        # bridge small holes first: samples flip-flopping between
        # near-coincident parallel edges pepper the union with meter-
        # scale gaps that are not service boundaries (a genuine mid-edge
        # entry/leave gap runs hundreds of meters)
        merged = []
        for lo, hi in ivals:
            if merged and lo - merged[-1][1] <= bridge:
                merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
            else:
                merged.append((lo, hi))
        kept = [iv for iv in merged if iv[1] - iv[0] >= cfg.edge_member_min_m]
        out[r] = kept if kept else merged
    return out


def _piece_routes(rc: dict, a: float, b: float) -> frozenset:
    """Routes riding the [a, b] slice: coverage overlap >= half the piece."""
    need = 0.5 * (b - a)
    out = set()
    for r, ivals in rc.items():
        if ivals is None:
            out.add(r)
            continue
        cov = sum(max(0.0, min(hi, b) - max(lo, a)) for lo, hi in ivals)
        if cov >= need:
            out.add(r)
    return frozenset(out)


def _split_edges_for_runs(index: WayIndex, usage: Usage, st: _State):
    """Work edges from used way edges, split at off-run anchors and at
    partial-coverage boundaries (mid-edge route-set changes).

    Returns [(u, v, pts, routes, cov_lo, cov_hi)] with cov_* relative to
    the piece; registers anchor/cut nodes and adds off-run corridors.
    """
    anchors: dict = {}
    run_specs: list = []
    for routes, run in usage.off_runs:
        ends = []
        for anchor, end_xy in ((run.start_anchor, run.coords_xy[0]),
                               (run.end_anchor, run.coords_xy[-1])):
            if anchor is not None and anchor[0] in usage.edge_routes:
                eid, pos = anchor
                nid = st.new_node(
                    np.asarray(index.lines[eid].interpolate(pos).coords[0]))
                anchors.setdefault(eid, []).append((pos, nid))
            else:
                nid = st.new_node(end_xy)
            ends.append(nid)
        run_specs.append((routes, run, ends[0], ends[1]))

    n_cov_cuts = 0
    work = []
    for eid in sorted(usage.edge_routes):
        routes = usage.edge_routes[eid]
        e = index.graph.edges[eid]
        line = index.lines[eid]
        cov = usage.edge_cover.get(eid, (0.0, line.length))
        cuts = sorted(anchors.get(eid, []))
        anchor_pos = [p for p, _ in cuts]
        for p in _coverage_cuts(index, usage, eid, st.cfg):
            if any(abs(p - q) < 10.0 for q in anchor_pos):
                continue  # an off-run anchor already splits here
            cuts.append((p, st.new_node(
                np.asarray(line.interpolate(p).coords[0]))))
            n_cov_cuts += 1
        cuts.sort()
        rc = _filtered_cov(usage, eid, st.cfg)
        bounds = [0.0] + [p for p, _ in cuts] + [line.length]
        hops = [e.from_node] + [nid for _, nid in cuts] + [e.to_node]
        for a, b, hu, hv in zip(bounds, bounds[1:], hops, hops[1:]):
            if b - a < 0.5:
                continue
            piece = np.asarray(substring(line, a, b).coords)
            piece_routes = (_piece_routes(rc, a, b) or routes) if rc \
                else routes
            work.append((hu, hv, piece, piece_routes,
                         max(cov[0] - a, 0.0), min(cov[1] - a, b - a)))
    if n_cov_cuts:
        st.log(f"{n_cov_cuts} partial-coverage cut(s): route sets change "
               "mid-edge at the covered extent, not at way nodes")
    for u, v, pts, *_rest in work:
        for nid, xy in ((u, pts[0]), (v, pts[-1])):
            if nid not in st.nodes:
                st.nodes[nid] = np.asarray(xy, dtype=float)
    for routes, run, nu, nv in run_specs:
        # the run's raw sample ends sit up to a couple of meters off the
        # anchor nodes (the anchor is the on-graph attachment on the
        # split edge) — connect them EXACTLY or the corridor endpoint
        # never coincides with its node downstream
        pts = np.vstack([st.nodes[nu][None, :], np.asarray(run.coords_xy),
                         st.nodes[nv][None, :]])
        st.add_corr(routes, {usage.route_color[r] for r in routes},
                    nu, nv, pts)
    st.notes.n_offgraph = len(run_specs)
    return work


def _build_chains(work, usage: Usage, st: _State):
    """Maximal constant-route-set runs through degree-2 connectivity."""
    cfg = st.cfg
    inc: dict = {}
    for i, (u, v, *_rest) in enumerate(work):
        inc.setdefault(u, []).append(i)
        inc.setdefault(v, []).append(i)

    def is_break(nid) -> bool:
        es = inc[nid]
        if len(es) != 2:
            return True
        return work[es[0]][3] != work[es[1]][3]

    visited = [False] * len(work)

    def walk(start_edge, from_node):
        chain = []
        i, nid = start_edge, from_node
        while True:
            visited[i] = True
            u, v, *_rest = work[i]
            fwd = (u == nid)
            chain.append((i, fwd))
            nid = v if fwd else u
            if is_break(nid):
                return chain, nid
            nxt = [j for j in inc[nid] if j != i]
            if not nxt or visited[nxt[0]]:
                return chain, nid
            i = nxt[0]

    def emit(chain, u_node, v_node):
        routes = work[chain[0][0]][3]
        parts = [work[i][2] if fwd else work[i][2][::-1] for i, fwd in chain]
        pts = _dedup(np.vstack(parts))
        # terminal trim: a chain end at a degree-1 node is a pattern
        # terminus — cut track beyond the covered extent (+ pad)
        if len(inc.get(u_node, ())) == 1:
            i, fwd = chain[0]
            elen = _cum(work[i][2])[-1]
            start = work[i][4] if fwd else (elen - work[i][5])
            start = max(0.0, start - cfg.terminal_trim_pad_m)
            line = LineString(pts)
            if 1.0 < start < line.length - 1.0:
                pts = np.asarray(substring(line, start, line.length).coords)
                st.nodes[u_node] = pts[0].copy()
        if len(inc.get(v_node, ())) == 1:
            i, fwd = chain[-1]
            elen = _cum(work[i][2])[-1]
            tail = (elen - work[i][5]) if fwd else work[i][4]
            tail = max(0.0, tail - cfg.terminal_trim_pad_m)
            line = LineString(pts)
            if 1.0 < tail < line.length - 1.0:
                pts = np.asarray(substring(line, 0.0, line.length - tail).coords)
                st.nodes[v_node] = pts[-1].copy()
        st.add_corr(routes, {usage.route_color[r] for r in routes},
                    u_node, v_node, pts)

    for nid in sorted(inc):
        if not is_break(nid):
            continue
        for i in sorted(inc[nid]):
            if not visited[i]:
                chain, end = walk(i, nid)
                emit(chain, nid, end)
    for i in range(len(work)):  # pure cycles
        if not visited[i]:
            chain, end = walk(i, work[i][0])
            emit(chain, work[i][0], end)
    st.notes.n_raw_corridors = len(st.corrs)


# ── phase 2: absorption of ridden crossovers ─────────────────────────────────


def _absorb_crossovers(st: _State):
    cfg = st.cfg
    changed = True
    while changed:
        changed = False
        cids = sorted(st.corrs)
        lines = {cid: st.corrs[cid].line() for cid in cids}
        tree = STRtree([lines[cid] for cid in cids])
        for cid in cids:
            c = st.corrs.get(cid)
            if c is None or c.length > cfg.absorb_max_len_m or c.u == c.v:
                continue
            probe = shapely.points(c.pts[:, 0], c.pts[:, 1])
            for j in tree.query(lines[cid], predicate="dwithin",
                                distance=cfg.pair_gap_m):
                p = st.corrs.get(cids[int(j)])
                if p is None or p.cid == cid:
                    continue
                if not (c.routes < p.routes):
                    continue
                # a true crossover runs BETWEEN the partner's tracks:
                # it never shares an endpoint node with the partner (a
                # shared node means c is SERIES track — its removal
                # would strand its routes) and both its endpoints
                # project strictly inside the partner's arc
                if {c.u, c.v} & {p.u, p.v}:
                    continue
                if float(shapely.distance(probe, lines[p.cid]).max()) \
                        > cfg.pair_gap_m:
                    continue
                pl = lines[p.cid]
                s_u = float(shapely.line_locate_point(
                    pl, shapely.points(*c.pts[0])))
                s_v = float(shapely.line_locate_point(
                    pl, shapely.points(*c.pts[-1])))
                if min(s_u, s_v) < 5.0 or max(s_u, s_v) > pl.length - 5.0:
                    continue
                st.corrs.pop(cid)
                st.notes.n_absorbed += 1
                changed = True
                break
            if changed:
                break
        if changed:
            st.rechain()
    st.drop_orphan_nodes()


# ── phase 3: the three merges ────────────────────────────────────────────────


def _windows(c1: Corr, c2: Corr, gap: float, step: float,
             cfg: WaygraphConfig):
    """Maximal arc windows along c1 where dist(c1, c2) <= gap.

    Flap guard (Schmitt trigger): real track spacing breathes across the
    threshold, so two qualifying windows separated by a dip coalesce —
    the dip inherits the bundled state — unless the dip both lasts at
    least window_dip_coalesce_m AND contains a contiguous stretch of at
    least release_sustain_m beyond release_gap_mult x gap (a genuine
    release, not a station throat or an interlocking bulge).
    """
    pts, cum = c1.pts, _cum(c1.pts)
    total = cum[-1]
    n = max(2, int(total / step) + 1)
    ts = np.linspace(0.0, total, n)
    dt = ts[1] - ts[0] if n > 1 else step
    probe = shapely.points(np.interp(ts, cum, pts[:, 0]),
                           np.interp(ts, cum, pts[:, 1]))
    d = shapely.distance(probe, c2.line())
    inside = d <= gap
    runs = []
    i = 0
    while i < n:
        if not inside[i]:
            i += 1
            continue
        j = i
        while j + 1 < n and inside[j + 1]:
            j += 1
        runs.append([i, j])
        i = j + 1
    if len(runs) > 1:
        release = cfg.release_gap_mult * gap
        merged = [runs[0]]
        for r in runs[1:]:
            lo, hi = merged[-1][1] + 1, r[0] - 1
            dip_len = ts[r[0]] - ts[merged[-1][1]]
            # longest contiguous stretch of the dip beyond the release gap
            sustain = run_len = 0
            for far in (d[lo:hi + 1] > release):
                run_len = run_len + 1 if far else 0
                sustain = max(sustain, run_len)
            if dip_len >= cfg.window_dip_coalesce_m \
                    and sustain * dt >= cfg.release_sustain_m:
                merged.append(r)
            else:
                merged[-1][1] = r[1]
        runs = merged
    return [(float(ts[i]), float(ts[j])) for i, j in runs], cum


def _window_metrics(c1: Corr, c2: Corr, a: float, b: float, step: float):
    """(gap_mean, gap_max, bearing_mean, s_a, s_b, monotonic) for [a, b]."""
    pts, cum = c1.pts, _cum(c1.pts)
    n = max(3, int((b - a) / step) + 1)
    ts = np.linspace(a, b, n)
    p = np.column_stack([np.interp(ts, cum, pts[:, 0]),
                         np.interp(ts, cum, pts[:, 1])])
    line2 = c2.line()
    probe = shapely.points(p[:, 0], p[:, 1])
    d = shapely.distance(probe, line2)
    s = shapely.line_locate_point(line2, probe)
    mono = bool(np.all(np.diff(s) > -10.0) or np.all(np.diff(s) < 10.0))
    pts2, cum2 = c2.pts, _cum(c2.pts)
    bdiffs = [_bearing_diff_deg(_bearing(pts, cum, float(t)),
                                _bearing(pts2, cum2, float(si)))
              for t, si in zip(ts, s)]
    # projected SPAN on c2 from the extreme s values, not the endpoint
    # samples: a window overhanging c2's end clamps its end projections
    # to c2's terminus, and treating those as the span corrupted the
    # tail bookkeeping (the 7 km straight N/R/W edge across the East
    # River). fwd = the projection TREND (half-mean comparison — robust
    # to endpoint clamping).
    half = max(1, len(s) // 2)
    fwd = bool(np.mean(s[half:]) >= np.mean(s[:half]))
    return (float(d.mean()), float(d.max()), float(np.mean(bdiffs)),
            float(s.min()), float(s.max()), fwd, mono)


def _kiss_gates(c1: Corr, c2: Corr, a: float, b: float, gap: float,
                step: float, cfg: WaygraphConfig):
    """Distinguish a BUNDLE (stable parallel co-run) from a KISS (transient
    convergence) by PROFILE, for the cross-family merge with its raised gap.

    Returns (ok, frac_below, gap_ratio, crosses):
      * frac_below — the fraction of a CONTEXT window (the merge window grown
                     by cross_family_min_len_m/2 on each side, clipped to c1)
                     that sits within `gap` of c2. The merge window itself is
                     below-threshold by construction, so it never discriminates
                     — a KISS is a below-threshold VALLEY inside a wider
                     above-threshold neighbourhood (Rector, Whitehall dip under
                     only near closest approach), a BUNDLE stays under across
                     the whole context (DeKalb, the Chicago Lake leg).
      * gap_ratio  — below-threshold gap_max / gap_mean. A loose safety valve
                     only: genuine bundles breathe a lot (Chicago P+Red 4.1,
                     the Lake leg 3.2 as their gap dips to 0 at shared switches
                     and rises to ~14 m between), so this must not reject them.
      * crosses    — the two geometries INTERSECT in the merge-window interior
                     (endpoint convergence at a shared switch is excluded by
                     trimming cross_family_cross_slack_m off each end). A
                     mid-span crossing is the definitive kiss signal.

    ok = sustained (frac_below high) AND flat-enough (gap_ratio) AND
    non-crossing.
    """
    pts, cum = c1.pts, _cum(c1.pts)
    total = cum[-1]

    # gap-stability + frac_below over the CONTEXT window (grown either side).
    # The neighbourhood must be WIDE — a kiss is a below-threshold valley
    # inside a wider above-threshold context — so grow by the SUSTAINED
    # length, not the (now short) min length, or the context shrinks with
    # the floor and a brief valley reads as sustained.
    ctx = cfg.cross_family_sustained_min_m / 2.0
    ca, cb = max(0.0, a - ctx), min(total, b + ctx)
    nc = max(3, int((cb - ca) / step) + 1)
    tc = np.linspace(ca, cb, nc)
    pc = np.column_stack([np.interp(tc, cum, pts[:, 0]),
                          np.interp(tc, cum, pts[:, 1])])
    line2 = c2.line()
    dc = shapely.distance(shapely.points(pc[:, 0], pc[:, 1]), line2)
    below = dc <= gap
    frac_below = float(below.mean())
    gb = dc[below]
    gap_ratio = float(gb.max() / max(gb.mean(), 1e-9)) if len(gb) else 999.0

    # NON-CROSSING: a mid-span crossing is the definitive kiss signal, but a
    # co-terminal parallel CONVERGES onto its partner at the shared switches
    # (gap -> 0 near the window ends) — that endpoint convergence is not a
    # crossing. So we ignore intersections in the endpoint zones (the greater
    # of an absolute slack and the outer 10% of the window — a switch ladder
    # can reach a good fraction of a block: Chicago P+Brn touch at 99% of a
    # 3.2 km window) AND intersections where the pair gap is ~0 there anyway.
    slack = max(cfg.cross_family_cross_slack_m, 0.10 * (b - a))
    lo, hi = a + slack, b - slack
    crosses = False
    if hi > lo:
        nw = max(2, int((b - a) / step) + 1)
        tw = np.linspace(a, b, nw)
        pw = np.column_stack([np.interp(tw, cum, pts[:, 0]),
                              np.interp(tw, cum, pts[:, 1])])
        mask = (tw >= lo) & (tw <= hi)
        if mask.sum() >= 2:
            inter = LineString(pw[mask]).intersection(line2)
            if (not inter.is_empty) and inter.geom_type in (
                    "Point", "MultiPoint", "GeometryCollection"):
                # A shared 4-track trunk WEAVES: express and local tracks
                # physically cross at every bypass/interlocking, but the pair
                # stays near-coincident there (gap -> 0 at the shared switch) —
                # that is not a KISS. A true kiss X DIVERGES away from the
                # crossing (the gap grows on both sides), so around the
                # crossing the corridors are genuinely SEPARATED. Count a
                # crossing only where the two geometries are meaningfully apart
                # in the crossing's neighbourhood; ignore weave-crossings where
                # they hug a shared centerline (Queens Blvd E x F/FX, DeKalb
                # N/Q x B/D, the near-coincident M x J/Z), which the sustained
                # frac_below already vouches for as a bundle.
                ipts = []
                for g in getattr(inter, "geoms", [inter]):
                    if g.geom_type == "Point":
                        ipts.append((g.x, g.y))
                # A kiss X DIVERGES: a short distance either side of the
                # crossing the corridors are far apart. A weave stays hugged.
                # Sample the pair gap a fixed offset (a bit over a track gap)
                # to each side of every crossing; a crossing is a real kiss
                # only where BOTH sides have pulled meaningfully apart.
                pwl = LineString(pw)
                off = max(cfg.pair_gap_m, 20.0)
                weave_tol = max(cfg.pair_gap_m, gap / 2.0)
                for ix, iy in ipts:
                    s_c = pwl.project(Point(ix, iy))
                    apart = []
                    for ds in (-off, off):
                        sp = min(max(s_c + ds, 0.0), pwl.length)
                        q = pwl.interpolate(sp)
                        apart.append(line2.distance(q))
                    if apart and min(apart) > weave_tol:
                        crosses = True
                        break
    # NEAR-COINCIDENT exemption: two corridors within a couple of meters are
    # the SAME physical track (a duplicate centerline the merges must fold
    # into one) — their gap dips to ~0, so gap_max/gap_mean explodes past the
    # ratio valve for a purely numerical reason (0.1 m mean vs 2 m max = 20x).
    # A ~0 m mean gap is the strongest possible bundle signal, never a kiss,
    # so the ratio valve does not apply there.
    near_coincident = len(gb) > 0 and float(gb.mean()) <= cfg.pair_gap_m / 3.0
    ratio_ok = near_coincident or gap_ratio <= cfg.cross_family_max_gap_ratio
    ok = (frac_below >= cfg.cross_family_min_frac_below
          and ratio_ok
          and not crosses)
    return ok, frac_below, gap_ratio, crosses


def _lonlat_of(xy, epsg: int):
    inv = Transformer.from_crs(epsg, 4326, always_xy=True)
    lon, lat = inv.transform(xy[0], xy[1])
    return (round(lon, 5), round(lat, 5))


def _try_merge(st: _State, kind: str, c1: Corr, c2: Corr, epsg: int):
    """Qualifying window between c1 and c2, or None.

    Returns (kind, cid_short, cid_long, a, b, wlen, gap_mean, gap_max,
    bearing) with [a, b] in the SHORTER corridor's arc space.
    """
    cfg = st.cfg
    if c1.u == c1.v or c2.u == c2.v:
        return None
    if c2.length < c1.length:
        c1, c2 = c2, c1
    if kind == "pair":
        gap, max_bear = cfg.pair_gap_m, cfg.merge_max_bearing_deg
    elif kind == "family":
        gap, max_bear = cfg.family_gap_m, cfg.merge_max_bearing_deg
    else:
        gap, max_bear = cfg.cross_family_gap_m, cfg.cross_family_max_bearing_deg

    # co-terminal twins: two tracks of ONE service between the same two
    # junction areas (the 5's directional tracks around the Mott Haven
    # wye) are a closed two-track loop and must render as one line even
    # where they bow apart beyond pair_gap mid-span — otherwise the
    # ribbon becomes a theta graph and stage 6's walk shears offsets.
    # Endpoints need not be the same node (each track has its own
    # switch); they must correspond within a switch-ladder's reach.
    if kind == "pair" and c1.u != c1.v and c2.u != c2.v:
        if max(c1.length, c2.length) <= 1.35 * min(c1.length, c2.length):
            e1a, e1b = c1.pts[0], c1.pts[-1]
            e2a, e2b = c2.pts[0], c2.pts[-1]
            straight = min(
                max(np.hypot(*(e1a - e2a)), np.hypot(*(e1b - e2b))),
                max(np.hypot(*(e1a - e2b)), np.hypot(*(e1b - e2a))))
            if straight <= 35.0:
                gm, gx, bear, s_lo, s_hi, fwd, mono = _window_metrics(
                    c1, c2, 0.0, c1.length, cfg.midline_step_m)
                # bearing guard: the wye legs bow to ~26 deg; PERPENDICULAR
                # switch-ladder crumbs at an interlocking (Tower 18) are
                # not twins — merging them regenerates pieces forever
                if mono and gx <= 4.0 * gap and bear <= 45.0:
                    return (kind, c1.cid, c2.cid, 0.0, c1.length, c1.length,
                            gm, gx, bear, True)

    wins, cum = _windows(c1, c2, gap, cfg.midline_step_m, cfg)
    if not wins:
        return None
    a, b = max(wins, key=lambda w: w[1] - w[0])
    len1, len2 = cum[-1], c2.length
    # boundary snap: a window ending within slack of a corridor end
    # extends TO the end — the merge boundary lands on the real
    # convergence junction instead of a synthetic seam floating half a
    # block short of the switch (and no sub-slack stub survives).
    # Guarded by proximity: only when that corridor end actually sits
    # within the merge gap of the partner — a converging RAMP's far end
    # (the 63 St FX ramp foot, 45 m off the F/M mainline) must keep its
    # own approach geometry, not get swallowed into the midline.
    slack = cfg.merge_end_slack_m
    line2 = c2.line()
    if a <= slack and line2.distance(
            shapely.points(*c1.pts[0])) <= gap:
        a = 0.0
    if len1 - b <= slack and line2.distance(
            shapely.points(*c1.pts[-1])) <= gap:
        b = len1
    wlen = b - a
    base_min = min(cfg.merge_min_len_m, 0.8 * min(len1, len2))
    if wlen < max(base_min, 1.0):
        return None
    gm, gx, bear, s_lo, s_hi, fwd, mono = _window_metrics(c1, c2, a, b,
                                                          cfg.midline_step_m)
    if not mono:
        return None
    # a CONNECTING RAMP joins the partner at one end and diverges from
    # it at the other (the 63 St FX ramp onto the Queens Blvd F/M) — it
    # must keep its own approach geometry and attach at the real
    # junction, never be midlined into the mainline. Switch-ladder
    # fragments (Tower 18) stay below the length floor and still merge.
    # A ramp genuinely CONVERGES onto its partner at one end (its foot
    # physically joins the mainline at a switch, gap -> ~0), so the
    # "joined" threshold is a FIXED small distance — half a directional
    # pair gap — NOT a fraction of the round-19 raised merge gap. At
    # 0.5 x 18 m an 8 m-parallel bundle ONSET (both tracks side-by-side,
    # never touching, then one curves away at a fork) was misread as a
    # ramp foot and rejected; half a pair gap (7.5 m, below the pair
    # spacing) keeps the real ramp foot caught while the parallel onset
    # merges.
    # A ramp foot converges onto the mainline at a switch (one end ~0 m off)
    # and angles away at the other (far off). A SUSTAINED parallel co-run
    # that converges at one junction and diverges at a real fork downstream
    # (E onto the F/FX trunk for 2.5 km, the 4/5 beside the 2/3 for 1.25 km,
    # F onto the N/Q/R 60th-St tunnel for 567 m, A/C onto F at Jay St for
    # ~400 m) has the SAME endpoint profile but is the transitive BUNDLE we
    # want, not a ramp. For pair/family merges (no profile gates) the endpoint
    # test is the only ramp discriminator, so it stands. For CROSS merges the
    # anti-kiss profile gates below already separate a ramp (which angles in —
    # a brief valley failing frac_below, or a mid-window crossing) from a
    # low-bearing sustained co-run, so the endpoint test is redundant and must
    # NOT veto a genuine parallel bundle that merely shares one junction.
    joined_tol = min(0.5 * gap, 0.5 * cfg.pair_gap_m)
    if kind != "cross" and wlen > 3.0 * cfg.merge_end_slack_m:
        d_u = float(line2.distance(shapely.points(*c1.pts[0])))
        d_v = float(line2.distance(shapely.points(*c1.pts[-1])))
        if min(d_u, d_v) <= joined_tol and max(d_u, d_v) > gap:
            return None
    # the window must map onto a comparable arc of c2 — a projected span
    # much shorter than the window means c1 merely brushes past c2's
    # terminus (projections clamp there), not that they co-run
    if (s_hi - s_lo) < 0.5 * wlen:
        return None
    if bear > max_bear:
        if kind == "cross" and wlen >= cfg.cross_family_min_len_m:
            st.notes.rejects.append(
                ("cross_bearing", tuple(sorted(c1.routes)),
                 tuple(sorted(c2.routes)), round(wlen, 1), round(bear, 1)))
        return None
    # ANTI-KISS PROFILE GATES (cross-family only): raising cross_family_gap
    # to 18 m would re-admit transient convergences on gap alone, so a
    # cross-family window must also look like a BUNDLE, not a KISS —
    # sustained (frac_below), flat (gap_ratio) and non-crossing. A window
    # that dips under the gap only briefly, spikes, or crosses mid-span
    # fails here even though its minimum gap qualified (Rector 1 x R/W,
    # Whitehall crossing tubes). Genuine parallels pass (DeKalb).
    if kind == "cross":
        ok, frac_below, gap_ratio, crosses = _kiss_gates(
            c1, c2, a, b, gap, cfg.midline_step_m, cfg)
        if not ok:
            if wlen >= 150.0:
                reason = ("cross_crosses" if crosses
                          else "cross_unstable_gap")
                st.notes.rejects.append(
                    (reason, tuple(sorted(c1.routes)),
                     tuple(sorted(c2.routes)), round(wlen, 1),
                     _lonlat_of(_interp(c1.pts, cum, (a + b) / 2), epsg)))
            return None
    # no divergence beyond the window: at each window end at least one
    # corridor ENDS within slack — two corridors that both continue
    # while the gap grows are crossing/brushing, never a merge
    head2 = s_lo if fwd else (len2 - s_hi)
    tail2 = (len2 - s_hi) if fwd else s_lo
    ends_ok = ((a <= slack or head2 <= slack)
               and ((len1 - b) <= slack or tail2 <= slack))
    if kind == "pair" and not ends_ok:
        return None
    if kind == "family" and not ends_ok \
            and wlen < cfg.family_sustained_min_m:
        # local/express of one family genuinely co-run for kilometers
        # and then BOTH continue past a real fork (7th Av: the 1 to
        # South Ferry, the 2/3 to Brooklyn) — a sustained window merges
        # anyway; kisses and crossings never last this long
        return None
    if kind == "cross" and wlen < cfg.cross_family_min_len_m:
        # under the sustained-bundle floor, a cross-family merge is
        # allowed only for CO-EXTENSIVE TWINS: the window is the full
        # mutual overlap and ends where a corridor ends (the Loop legs'
        # two junction-to-junction one-way tracks) — a mid-corridor
        # brush that diverges past the window (Brooklyn Bridge J/Z vs
        # the Lexington locals) keeps failing here
        if not ends_ok:
            if wlen >= 150.0:
                st.notes.rejects.append(
                    ("cross_too_short", tuple(sorted(c1.routes)),
                     tuple(sorted(c2.routes)), round(wlen, 1),
                     _lonlat_of(_interp(c1.pts, cum, (a + b) / 2), epsg)))
            return None
    return (kind, c1.cid, c2.cid, a, b, wlen, gm, gx, bear, False)


def _apply_merge(st: _State, cand, epsg: int):
    cfg = st.cfg
    kind, cid1, cid2, a, b, wlen, gm, gx, bear, twin = cand
    c1, c2 = st.corrs[cid1], st.corrs[cid2]
    pts1, cum1 = c1.pts, _cum(c1.pts)
    line2 = c2.line()

    n = max(3, int(wlen / cfg.midline_step_m) + 1)
    ts = np.linspace(a, b, n)
    p1 = np.column_stack([np.interp(ts, cum1, pts1[:, 0]),
                          np.interp(ts, cum1, pts1[:, 1])])
    s = shapely.line_locate_point(line2, shapely.points(p1[:, 0], p1[:, 1]))
    p2 = shapely.get_coordinates(shapely.line_interpolate_point(line2, s))

    # trim clamp zones off the window ends: near c2's termini the
    # projections CLAMP, so the pointwise pair distance blows past the
    # merge gap even though line-distance qualified the window — a seam
    # placed there sits on track that does not exist (the Culver portal
    # seam landed 25 m west of the express). Only END runs are trimmed;
    # coalesced dips in the middle keep their bundled state.
    gap = {"pair": cfg.pair_gap_m, "family": cfg.family_gap_m,
           "cross": cfg.cross_family_gap_m}[kind]
    pairdist = np.hypot(*(p1 - p2).T)
    lim = 2.0 * gap
    eps = 0.5 * cfg.midline_step_m
    i0, i1 = 0, n - 1
    if not twin:  # a co-terminal twin's window IS the full mutual
        #           extent — the balloon bow plateaus the projections
        #           and trimming would knot the wye
        while i0 < i1 - 1 and (pairdist[i0] > lim
                               or abs(s[i0 + 1] - s[i0]) < eps):
            i0 += 1
        while i1 > i0 + 1 and (pairdist[i1] > lim
                               or abs(s[i1 - 1] - s[i1]) < eps):
            i1 -= 1
    if i0 or i1 < n - 1:
        ts, p1, p2, s = ts[i0:i1 + 1], p1[i0:i1 + 1], p2[i0:i1 + 1], \
            s[i0:i1 + 1]
        a, b = float(ts[0]), float(ts[-1])
        wlen = b - a
        n = len(ts)
        if wlen < 2 * cfg.tail_collapse_m:
            # the window was almost ALL clamp zone — perpendicular
            # switch crumbs whose projections pile onto a shared node
            # (Tower 18) are not co-running track; applying the sliver
            # would just re-create the same twins around a new node
            return False
    w1, w2 = c1.tracks, c2.tracks

    # c2-side bookkeeping from the WINDOW-END projections, not the s
    # extremes: a single interior sample brushing c2's terminus drags
    # s.max() to the end and the "fully consumed" lie left a 107 m
    # straight fan-in chord at Queensboro Plaza. The plain-midline
    # endpoints project to clamp-free interior feet.
    plain0 = (w1 * p1[0] + w2 * p2[0]) / (w1 + w2)
    plain1 = (w1 * p1[-1] + w2 * p2[-1]) / (w1 + w2)
    h2a = float(shapely.line_locate_point(line2, shapely.points(*plain0)))
    h2b = float(shapely.line_locate_point(line2, shapely.points(*plain1)))
    half = max(1, len(s) // 2)
    fwd2 = bool(np.mean(s[half:]) >= np.mean(s[:half]))
    s_lo, s_hi = min(h2a, h2b), max(h2a, h2b)

    # C1 boundary easing: at a window end where exactly ONE constituent
    # continues past the boundary, the midline eases into the continuing
    # track over ease_len_m (smoothstep) — the through track leaves the
    # bundle tangentially with zero step. Where both end (a real
    # convergence) the seam sits between the switches as before; where
    # both continue (proximity-bundle onset) the cut tails ease instead.
    slack = cfg.merge_end_slack_m
    len1, len2 = cum1[-1], c2.length
    head1, tail1 = a, len1 - b
    head2 = s_lo if fwd2 else (len2 - s_hi)
    tail2 = (len2 - s_hi) if fwd2 else s_lo
    u1 = np.ones(n)
    u2 = np.ones(n)
    t_rel = ts - a
    ease = max(min(cfg.ease_len_m, 0.45 * wlen), 1.0)
    for at_start in (True, False):
        e1 = (head1 if at_start else tail1) <= slack   # c1 ends here
        e2 = (head2 if at_start else tail2) <= slack   # c2 ends here
        d_end = t_rel if at_start else (wlen - t_rel)
        # only ease when the ending track genuinely converges onto the
        # through track: identical route sets (the directional-pair ->
        # single handoff) with the window-end pair distance within the
        # merge gap. Family/cross seams keep the plain weighted midpoint
        # — at multi-line interlockings (36 St in Queens) easing the
        # seam fully onto one constituent strands every OTHER line's
        # fan-in a full track-gap away.
        pd_end = pairdist[0 if at_start else -1]
        if kind != "pair" or pd_end > gap:
            continue
        if e1 and not e2:
            u1 *= _smoothstep(d_end / ease)
        elif e2 and not e1:
            u2 *= _smoothstep(d_end / ease)
    denom = w1 * u1 + w2 * u2
    flat = denom <= 1e-9  # both eased to zero (degenerate tiny window)
    if np.any(flat):
        u1[flat] = u2[flat] = 1.0
        denom = w1 * u1 + w2 * u2
    mid = _dedup((w1 * u1[:, None] * p1 + w2 * u2[:, None] * p2)
                 / denom[:, None])
    if len(mid) >= 4 and not LineString(mid).is_simple:
        st.log(f"eased midline self-intersects ({sorted(c1.routes)} + "
               f"{sorted(c2.routes)}) — plain midline kept")
        mid = _dedup((w1 * p1 + w2 * p2) / (w1 + w2))
    if len(mid) < 2:
        return False

    routes = c1.routes if kind == "pair" else (c1.routes | c2.routes)
    families = c1.families | c2.families

    if wlen < 2 * cfg.tail_collapse_m:
        # degenerate window (switch-ladder fragments): a micro midline
        # corridor would only smear the interlocking — collapse the
        # whole window into ONE node instead
        n_c = st.new_node(mid[len(mid) // 2])
        sides = [
            (c1, a, c1.length - b, n_c, n_c),
            (c2, s_lo, c2.length - s_hi, n_c, n_c),
        ]
    else:
        n_a = st.new_node(mid[0])
        n_b = st.new_node(mid[-1])
        st.add_corr(routes, families, n_a, n_b, mid, c1.tracks + c2.tracks)
        # (corridor, head_len, tail_len, node at window start, node at end)
        sides = [
            (c1, a, c1.length - b, n_a, n_b),
            (c2, s_lo, c2.length - s_hi, n_a if fwd2 else n_b,
             n_b if fwd2 else n_a),
        ]
    n_a, n_b = sides[0][3], sides[0][4]
    st.corrs.pop(cid1)
    st.corrs.pop(cid2)
    for c, head_len, tail_len, n_head, n_tail in sides:
        cl = c.line()
        head_len = min(max(head_len, 0.0), cl.length)
        tail_len = min(max(tail_len, 0.0), cl.length)
        # a CONTINUING corridor's cut tail eases onto the seam over
        # ease_len_m (it is the same physical track the window midlined);
        # a diverging switch leg (sub-slack stub) keeps the short blend —
        # its real track leaves the bundle, and a long drag would
        # parallel it with the ribbon (the Culver portal lesson)
        head_ease = cfg.ease_len_m if head_len > slack else cfg.blend_m
        tail_ease = cfg.ease_len_m if tail_len > slack else cfg.blend_m
        def attach(end_node, seam):
            """Collapse the sub-collapse stub — retarget when the node
            sits on the seam's doorstep, otherwise keep an explicit
            fan-in connector: retargeting a node a full track-gap away
            drags EVERY corridor there sideways onto the seam (the
            Culver express dove 19 m west into the local seam).
            Degenerate single-node windows always retarget — a connector
            pair out of a collapsing switch ladder would just re-merge
            with itself forever (the Tower 18 P fragments)."""
            if n_head != n_tail and \
                    np.hypot(*(st.nodes[end_node] - st.nodes[seam])) > gap:
                st.add_corr(c.routes, c.families, end_node, seam,
                            np.array([st.nodes[end_node],
                                      st.nodes[seam]]), c.tracks)
            else:
                st.retarget_node(end_node, seam)

        piece = None
        if head_len >= cfg.tail_collapse_m:
            piece = np.asarray(substring(cl, 0.0, head_len).coords)
        if piece is not None and len(_dedup(piece)) >= 2:
            piece = _blend_to(piece, False, st.nodes[n_head], head_ease)
            st.add_corr(c.routes, c.families, c.u, n_head, piece, c.tracks)
        elif c.u not in (n_a, n_b) and c.u in st.nodes:
            attach(c.u, n_head)
        piece = None
        if tail_len >= cfg.tail_collapse_m:
            piece = np.asarray(substring(cl, cl.length - tail_len,
                                         cl.length).coords)
        if piece is not None and len(_dedup(piece)) >= 2:
            piece = _blend_to(piece, True, st.nodes[n_tail], tail_ease)
            st.add_corr(c.routes, c.families, n_tail, c.v, piece, c.tracks)
        elif c.v not in (n_a, n_b) and c.v in st.nodes:
            attach(c.v, n_tail)

    rec = MergeRecord(
        kind, tuple(sorted(c1.routes)), tuple(sorted(c2.routes)),
        round(wlen, 1), round(gm, 2), round(gx, 2), round(bear, 1),
        _lonlat_of(mid[len(mid) // 2], epsg))
    st.notes.merges.append(rec)
    st.log(f"merge[{kind}] {list(rec.routes_a)} + {list(rec.routes_b)} "
           f"len={wlen:.0f}m gap={gm:.1f}/{gx:.1f}m bear={bear:.1f}deg "
           f"@ {rec.at_lonlat}")
    return True


def _merge_pass(st: _State, kind: str, epsg: int):
    cfg = st.cfg
    gap = {"pair": cfg.pair_gap_m, "family": cfg.family_gap_m,
           "cross": cfg.cross_family_gap_m}[kind]
    guard = 0
    max_iter = 4 * max(len(st.corrs), 8)
    dead: set = set()  # candidate pairs whose application degenerated —
    #                    cids are never reused, so skip them forever
    while guard < max_iter:
        guard += 1
        cids = sorted(st.corrs)
        lines = [st.corrs[cid].line() for cid in cids]
        tree = STRtree(lines)
        best = None
        seen = set()
        for ii, cid in enumerate(cids):
            c1 = st.corrs[cid]
            if c1.u == c1.v:
                continue
            for j in tree.query(lines[ii], predicate="dwithin", distance=gap):
                cid2 = cids[int(j)]
                pair_key = frozenset((cid, cid2))
                if cid2 == cid or pair_key in seen or pair_key in dead:
                    continue
                seen.add(pair_key)
                c2 = st.corrs[cid2]
                if kind == "pair" and c1.routes != c2.routes:
                    continue
                if kind == "family" and not (c1.families == c2.families
                                             and c1.routes != c2.routes):
                    continue
                if kind == "cross" and c1.families == c2.families:
                    continue
                cand = _try_merge(st, kind, c1, c2, epsg)
                if cand is None:
                    continue
                key = (cand[5], -min(cand[1], cand[2]))
                if best is None or key > best[0]:
                    best = (key, cand)
        if best is None:
            return
        if not _apply_merge(st, best[1], epsg):
            dead.add(frozenset((best[1][1], best[1][2])))
            continue
        st.rechain()
        st.drop_orphan_nodes()
    raise RuntimeError(f"merge pass '{kind}' exceeded {max_iter} iterations")


# ── phase 4: switch-ladder contraction ──────────────────────────────────────


def _contract_ladders(st: _State):
    """Contract junction-to-junction micro corridors into single nodes.

    An interlocking (Tower 18) survives the merges as a cluster of
    junction nodes connected by switch-ladder fragments a few meters
    long; each fragment would anchor its own stage-6 transition site and
    the consumed-corridor merges balloon past the exam's ground-length
    bounds. A fragment under ladder_contract_m between two junctions
    collapses to one node at its midpoint (moving either junction by
    less than half a merge gap — forks stay on their switches).
    Terminal stubs and chainable deg-2 pieces are never contracted.
    """
    cfg = st.cfg
    # micro self-loops first: a sub-ladder loop at a junction is switch
    # residue (a coverage-cut or merge tail whose both ends retargeted
    # onto the same node) — it would emit as a degenerate edge
    for cid in [cid for cid, c in st.corrs.items()
                if c.u == c.v and c.length < cfg.ladder_contract_m]:
        st.corrs.pop(cid)
        st.notes.n_contracted += 1
    changed = True
    while changed:
        changed = False
        inc = st.incidence()
        for cid in sorted(st.corrs):
            c = st.corrs[cid]
            if c.u == c.v or c.length >= cfg.ladder_contract_m:
                continue
            if len(inc.get(c.u, ())) < 3 or len(inc.get(c.v, ())) < 3:
                continue
            mid = c.pts[len(c.pts) // 2]
            n_c = st.new_node(mid)
            u, v = c.u, c.v
            st.corrs.pop(cid)
            st.retarget_node(u, n_c)
            st.retarget_node(v, n_c)
            st.notes.n_contracted += 1
            changed = True
            break
        if changed:
            st.rechain()
    st.drop_orphan_nodes()


def _heal_tears(st: _State):
    """Reconnect dangling merge residue (the Mott Haven wye fragments).

    A cascade of tiny twin merges can leave a corridor's endpoint node
    dangling a few meters from the seam its neighbors were retargeted
    onto — the ribbon TEARS. A deg-1 node whose corridor shares a route
    with a node nearby is such residue: coalesce it within reach of a
    switch ladder (retarget), bridge it with a straight connector up to
    merge_end_slack_m. Pattern terminals are unaffected in practice:
    corridor-interior track has no nodes to heal onto, and co-located
    terminals of different routes share no route.
    """
    cfg = st.cfg
    changed = True
    while changed:
        changed = False
        inc = st.incidence()
        for nid in sorted(inc):
            if len(inc[nid]) != 1 or nid not in st.nodes:
                continue
            c = st.corrs[inc[nid][0]]
            if c.u == c.v:
                continue
            best = None
            for mid, xy in st.nodes.items():
                if mid == nid:
                    continue
                others = [st.corrs[k] for k in inc.get(mid, ())]
                if not others or not any(c.routes & o.routes for o in others):
                    continue
                d = float(np.hypot(*(st.nodes[nid] - xy)))
                if d <= cfg.merge_end_slack_m and (best is None or d < best[0]):
                    best = (d, mid)
            if best is None:
                continue
            d, mid = best
            if d <= 2.0 * cfg.tail_collapse_m:
                st.retarget_node(nid, mid)
                st.log(f"healed tear: dangling node coalesced ({d:.1f} m, "
                       f"routes {sorted(c.routes)})")
            else:
                st.add_corr(c.routes, c.families, nid, mid,
                            np.array([st.nodes[nid], st.nodes[mid]]))
                st.log(f"healed tear: {d:.1f} m connector "
                       f"(routes {sorted(c.routes)})")
            changed = True
            break
        if changed:
            st.rechain()
    st.drop_orphan_nodes()


# ── assembly ─────────────────────────────────────────────────────────────────


def build_corridor_linegraph(index: WayIndex, usage: Usage,
                             cfg: WaygraphConfig, *, build_key: str,
                             feed_id: str, mode: str, input_digest: str,
                             n_input_shapes: int,
                             verbose: bool = True) -> tuple:
    """Usage graph -> merged corridor LineGraph. Returns (lg, notes)."""
    t0 = time.perf_counter()
    st = _State(cfg, verbose)

    work = _split_edges_for_runs(index, usage, st)
    _build_chains(work, usage, st)
    st.log(f"raw: {len(st.corrs)} corridors ({st.notes.n_offgraph} off-graph "
           f"runs) over {len(work)} used way edges")
    st.rechain()
    _absorb_crossovers(st)
    st.log(f"absorbed {st.notes.n_absorbed} ridden crossovers -> "
           f"{len(st.corrs)} corridors")

    # the trailing second "pair" sweep catches directional twins that
    # only exist as TAILS of family/cross merges (the Mott Haven wye
    # legs materialize when the 4/5 trunk merge splits its tails)
    for kind in ("pair", "family", "cross", "pair"):
        n0 = len(st.corrs)
        _merge_pass(st, kind, index.epsg)
        _absorb_crossovers(st)
        st.rechain()
        st.drop_orphan_nodes()
        st.log(f"pass {kind}: {n0} -> {len(st.corrs)} corridors "
               f"({sum(1 for m in st.notes.merges if m.kind == kind)} merges)")

    _contract_ladders(st)
    st.log(f"ladder contraction: {st.notes.n_contracted} micro corridors "
           f"-> {len(st.corrs)} corridors")
    _heal_tears(st)

    inv = Transformer.from_crs(index.epsg, 4326, always_xy=True)
    inc = st.incidence()
    nid_order = sorted(st.nodes)
    nid_map = {nid: i for i, nid in enumerate(nid_order)}
    nodes = []
    for nid in nid_order:
        x, y = st.nodes[nid]
        lon, lat = inv.transform(x, y)
        deg = len(inc.get(nid, ()))
        nodes.append(LGNode(node_id=nid_map[nid], lon=float(lon),
                            lat=float(lat), x=float(x), y=float(y),
                            degree=deg,
                            kind="endpoint" if deg <= 1 else "junction"))
    edges = []
    for cid in sorted(st.corrs):
        c = st.corrs[cid]
        lons, lats = inv.transform(c.pts[:, 0], c.pts[:, 1])
        edges.append(LGEdge(
            edge_id=len(edges), from_node=nid_map[c.u], to_node=nid_map[c.v],
            px_len=max(2, int(round(c.length / 2.0))), length_m=c.length,
            coords=list(zip(map(float, lons), map(float, lats))),
            coords_xy=[(float(x), float(y)) for x, y in c.pts],
            families=c.families, routes=c.routes,
        ))

    lg = LineGraph(
        format_version=FORMAT_VERSION, build_key=build_key, feed_id=feed_id,
        mode=mode, merge_width_m=cfg.station_sliver_m, resolution_m=0.0,
        epsg=index.epsg, origin=(0.0, 0.0), grid_shape=(0, 0), grid_bytes=0,
        input_digest=input_digest, n_input_shapes=n_input_shapes,
        build_seconds=time.perf_counter() - t0, nodes=nodes, edges=edges,
    )
    st.log(f"assembled {len(nodes)} nodes, {len(edges)} edges, "
           f"{lg.total_length_m() / 1000:.1f} km in {lg.build_seconds:.1f}s")
    return lg, st.notes
