"""segments.segment — steady/transition segmentation (v3 contract C1/C3).

Per transition site (junction node deg>=3, or deg-2 composition change):
adjacent corridors are cut back transition_len_m/2 along the centerline;
each ribbon's connecting piece = incoming tail + node + outgoing head,
biarc-filleted at the corner (min radius >= ribbon_count_max * gap_px *
fillet_radius_factor metres, clamped to the available halves; both cut
seams take their tangent from the ACTUAL polyline, so the fillet is G1
at the seams and at the biarc junction — no single-vertex kinks) and
densified to <= densify_step_m vertex spacing. Steady pieces keep the
(trimmed) corridor geometry — long features, few boundaries.

Rules (docs/transit-pipeline-v3.md stage 6 + PAR-12 v3 handoff §4):
  * a continuing ribbon gets off_from_px/off_to_px in the transition
    feature's own travel frame (signs flip against a corridor stored in
    the opposite direction — the renderer only sees the feature frame);
  * the transition is skipped (plain constant-offset steady connector)
    only when |off_from - off_to| < offset_eps_px AND the corner is
    straight enough that the fillet is a no-op;
  * a ribbon terminating at the node keeps its offset constant to the
    end of geometry (a steady stub into the node — NO collapse to the
    centerline; designed decision, terminus polish is a client task);
  * short corridors shrink the transition halves to fit; a fully
    consumed corridor merges its two transitions into one feature;
  * ribbons on >= 3 corridor ends at a node (CTA Loop corners) pair by
    matched_shapes traversal evidence, falling back to straightest-
    continuation greedy pairing.
"""

from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field, replace

from shapely.geometry import LineString, Point
from shapely.ops import substring

from .corridors import GAP_PX, Corridor, Graph, walk_corridors


@dataclass(frozen=True)
class SegmentConfig:
    transition_len_m: float = 60.0    # fixed ground length centred on the node
    gap_px: float = GAP_PX            # on-screen ribbon spacing
    densify_step_m: float = 7.5       # max vertex spacing in transitions
    fillet_radius_factor: float = 2.5  # min radius = max ribbon count * gap_px * this (m)
    straight_skip_deg: float = 2.0    # corner turn below this => fillet no-op
    offset_eps_px: float = 0.01       # offset delta below this => unchanged
    probe_dist_m: float = 40.0        # shape-evidence probe distance from node
    evidence_tol_m: float = 30.0      # shape-to-probe distance tolerance


@dataclass
class Segment:
    seg_id: int
    kind: str                      # 'steady' | 'transition'
    color_key: str
    route_short_names: str
    route_ids: str
    feed_id: str
    route_type: int | None
    route_color: str
    route_text_color: str
    slot: int
    line_count: int
    offset_px: float | None
    off_from_px: float | None
    off_to_px: float | None
    len_m: float
    coords: list                   # [(lon, lat)]
    # provenance / diagnostics (not emitted)
    corridor_id: int | None = None
    sites: tuple = ()
    in_end: tuple | None = None    # (cid, side)
    out_end: tuple | None = None
    turn_deg: float = 0.0
    fillet_radius_m: float | None = None   # achieved min biarc radius
    fillet_clamped: bool = False           # target radius not reachable
    raw_min_radius_m: float | None = None  # pre-fillet floor, corner excl.


# ------------------------------------------------------------ projection

class LocalProj:
    """Azimuthal-equidistant local plane (true ground metres)."""

    def __init__(self, lon0: float, lat0: float):
        from pyproj import Transformer
        crs = (f"+proj=aeqd +lat_0={lat0} +lon_0={lon0} "
               f"+datum=WGS84 +units=m +no_defs")
        self._fwd = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
        self._inv = Transformer.from_crs(crs, "EPSG:4326", always_xy=True)

    def to_xy(self, coords):
        return [self._fwd.transform(lon, lat) for lon, lat in coords]

    def to_ll(self, coords):
        return [self._inv.transform(x, y) for x, y in coords]


# ------------------------------------------------------- geometry helpers

def _dedupe(coords, eps=1e-9):
    out = [coords[0]]
    for p in coords[1:]:
        if abs(p[0] - out[-1][0]) > eps or abs(p[1] - out[-1][1]) > eps:
            out.append(p)
    return out


def _length(coords) -> float:
    return sum(math.dist(coords[i], coords[i + 1])
               for i in range(len(coords) - 1))


def densify(coords, step: float):
    out = [coords[0]]
    for a, b in zip(coords, coords[1:]):
        d = math.dist(a, b)
        if d > step:
            n = int(math.ceil(d / step))
            for k in range(1, n):
                t = k / n
                out.append((a[0] + (b[0] - a[0]) * t,
                            a[1] + (b[1] - a[1]) * t))
        out.append(b)
    return out


def _substring_coords(line: LineString, a: float, b: float):
    seg = substring(line, a, b)
    return _dedupe(list(seg.coords))


def _cum_lengths(coords):
    cum = [0.0]
    for a, b in zip(coords, coords[1:]):
        cum.append(cum[-1] + math.dist(a, b))
    return cum


def _point_at(coords, cum, s: float):
    if s <= 0:
        return coords[0]
    if s >= cum[-1]:
        return coords[-1]
    for i in range(1, len(cum)):
        if cum[i] >= s:
            t = (s - cum[i - 1]) / (cum[i] - cum[i - 1])
            a, b = coords[i - 1], coords[i]
            return (a[0] + (b[0] - a[0]) * t, a[1] + (b[1] - a[1]) * t)
    return coords[-1]


def _circumradius(a, b, c) -> float:
    la, lb, lc = math.dist(b, c), math.dist(a, c), math.dist(a, b)
    area2 = abs((b[0] - a[0]) * (c[1] - a[1])
                - (c[0] - a[0]) * (b[1] - a[1]))
    if area2 < 1e-9:
        return float("inf")
    return la * lb * lc / (2.0 * area2)


def raw_min_radius(coords, corner_idx: int, step: float) -> float:
    """Min circumradius of the DENSIFIED raw piece, excluding the triple
    centred on the corner vertex itself (that kink is the fillet's job).
    Inherited track curvature sharper than the fillet target cannot be
    fixed by a corner fillet — it is recorded so the curvature exam can
    tell it apart from fillet-introduced kinks."""
    corner = coords[corner_idx]
    dense = densify(coords, step)
    best = float("inf")
    for a, b, c in zip(dense, dense[1:], dense[2:]):
        if b == corner:
            continue
        best = min(best, _circumradius(a, b, c))
    return best


def _tangent_at(coords, cum, s: float, *, before: bool):
    """Unit direction of the polyline segment containing arc-length s
    (the segment arriving at s when before=True, leaving it otherwise)."""
    s = max(0.0, min(s, cum[-1]))
    if before:
        i = bisect.bisect_left(cum, s - 1e-9)
    else:
        i = bisect.bisect_right(cum, s + 1e-9)
    i = max(1, min(i, len(cum) - 1))
    a, b = coords[i - 1], coords[i]
    d = (b[0] - a[0], b[1] - a[1])
    n = math.hypot(*d) or 1.0
    return (d[0] / n, d[1] / n)


def _arc_points(p, tan, q, step: float):
    """Sample the circular arc from p (unit tangent `tan`) to q.
    Returns (points incl. endpoints, |radius|); straight chord (radius
    inf) when collinear or degenerate."""
    chord = (q[0] - p[0], q[1] - p[1])
    cross = tan[0] * chord[1] - tan[1] * chord[0]
    if abs(cross) < 1e-9:
        return [p, q], float("inf")
    normal = (-tan[1], tan[0]) if cross > 0 else (tan[1], -tan[0])
    denom = 2.0 * (chord[0] * normal[0] + chord[1] * normal[1])
    if abs(denom) < 1e-12:
        return [p, q], float("inf")
    r = (chord[0] ** 2 + chord[1] ** 2) / denom
    if r <= 0:
        return [p, q], float("inf")
    center = (p[0] + normal[0] * r, p[1] + normal[1] * r)
    a1 = math.atan2(p[1] - center[1], p[0] - center[0])
    a2 = math.atan2(q[1] - center[1], q[0] - center[0])
    sweep = a2 - a1
    while sweep > math.pi:
        sweep -= 2 * math.pi
    while sweep < -math.pi:
        sweep += 2 * math.pi
    n_pts = max(2, int(math.ceil(abs(sweep) * r / (step / 2.0))))
    pts = [(center[0] + r * math.cos(a1 + sweep * k / n_pts),
            center[1] + r * math.sin(a1 + sweep * k / n_pts))
           for k in range(n_pts + 1)]
    return pts, r


def _biarc_radii(p1, t1, p2, t2, d1: float):
    """Tangent-polygon biarc P1 -> J -> P2 with tangent length d1 at P1:
    C1 = P1 + d1*t1, C2 = P2 - d2*t2 with |C1C2| = d1 + d2, J splitting
    C1C2 at ratio d1:d2 — both arcs are tangent to C1C2 at J, so the pair
    is G1 throughout. Returns (J, r1, r2) or None when invalid."""
    v = (p2[0] - p1[0], p2[1] - p1[1])
    w = (v[0] - d1 * t1[0], v[1] - d1 * t1[1])  # C1 -> P2
    den = 2.0 * (t2[0] * w[0] + t2[1] * w[1] + d1)
    if den <= 1e-9:
        return None
    d2 = (w[0] * w[0] + w[1] * w[1] - d1 * d1) / den
    if d2 <= 1e-9:
        return None
    c1 = (p1[0] + d1 * t1[0], p1[1] + d1 * t1[1])
    c2 = (p2[0] - d2 * t2[0], p2[1] - d2 * t2[1])
    f = d1 / (d1 + d2)
    j = (c1[0] + (c2[0] - c1[0]) * f, c1[1] + (c2[1] - c1[1]) * f)

    def _turn(u, wv):
        dot = max(-1.0, min(1.0, u[0] * wv[0] + u[1] * wv[1]))
        return math.acos(dot)

    u1 = ((j[0] - c1[0]) / d1, (j[1] - c1[1]) / d1)
    th1 = _turn(t1, u1)
    u2 = ((c2[0] - j[0]) / d2, (c2[1] - j[1]) / d2)
    th2 = _turn(u2, t2)
    if th1 > 2.8 or th2 > 2.8:  # near-reversal arcs: reject
        return None
    r1 = float("inf") if th1 < 1e-6 else d1 / math.tan(th1 / 2.0)
    r2 = float("inf") if th2 < 1e-6 else d2 / math.tan(th2 / 2.0)
    return j, r1, r2


def _best_biarc(p1, t1, p2, t2, step: float):
    """Maximise the biarc's min radius over the tangent length d1
    (coarse grid + one refinement pass; deterministic). Returns
    (points, min_radius) or None."""
    span = math.dist(p1, p2)
    if span < 1e-6:
        return None

    def _eval(d1):
        ba = _biarc_radii(p1, t1, p2, t2, d1)
        if ba is None:
            return None
        return min(ba[1], ba[2]), ba[0]

    best = None  # (score, d1, J)
    grid = [span * k / 20.0 for k in range(1, 41)]
    for d1 in grid:
        ev = _eval(d1)
        if ev and (best is None or ev[0] > best[0]):
            best = (ev[0], d1, ev[1])
    if best is None:
        return None
    lo, hi = best[1] / 1.5, best[1] * 1.5
    for k in range(21):
        d1 = lo + (hi - lo) * k / 20.0
        ev = _eval(d1)
        if ev and ev[0] > best[0]:
            best = (ev[0], d1, ev[1])
    _score, _d1, j = best
    arc1, r1 = _arc_points(p1, t1, j, step)
    arc2, r2 = _arc_points(p2, (-t2[0], -t2[1]), j, step)
    return _dedupe(arc1 + list(reversed(arc2))[1:]), min(r1, r2)


def fillet_corner(coords, corner_idx: int, radius: float, cfg: SegmentConfig):
    """Replace the corner at coords[corner_idx] with a tangent-continuous
    biarc of min radius >= target (setback grows until the target is met,
    capped at 90% of each side's available length). Seam tangents come
    from the ACTUAL polyline at the cut points — the accumulated turn of
    the removed lead-in/lead-out vertices is absorbed by the biarc, never
    concentrated into single-vertex seam kinks. Candidates are scored by
    the min circumradius of the densified RESULT inside the transition
    window, so the setback also grows to swallow raw track kinks sitting
    just outside the cut. Returns (coords, turn_deg, radius_m|None,
    clamped) — radius_m is the achieved window minimum."""
    cum = _cum_lengths(coords)
    s_c = cum[corner_idx]
    avail_in, avail_out = s_c, cum[-1] - s_c
    if avail_in <= 0 or avail_out <= 0:
        return coords, 0.0, None, False

    # corner turn (straight-skip rule + diagnostics)
    p_in = coords[corner_idx - 1]
    p_out = coords[corner_idx + 1]
    c = coords[corner_idx]
    u_in = (c[0] - p_in[0], c[1] - p_in[1])
    u_out = (p_out[0] - c[0], p_out[1] - c[1])
    n_in = math.hypot(*u_in) or 1.0
    n_out = math.hypot(*u_out) or 1.0
    u_in = (u_in[0] / n_in, u_in[1] / n_in)
    u_out = (u_out[0] / n_out, u_out[1] / n_out)
    dot = max(-1.0, min(1.0, u_in[0] * u_out[0] + u_in[1] * u_out[1]))
    turn = math.degrees(math.acos(dot))
    if turn < cfg.straight_skip_deg:
        return coords, turn, None, False

    phi = math.pi - math.radians(turn)  # interior angle between the legs
    t0 = radius / math.tan(phi / 2) if phi > 1e-6 else avail_in
    t_max = 0.9 * min(avail_in, avail_out)
    if t_max < 1e-6:
        return coords, turn, None, True
    ts, t = [], min(t0, t_max)
    while True:  # straight-leg estimate, grown geometrically to the cap
        ts.append(t)
        if t >= t_max - 1e-9:
            break
        t = min(t * 1.3, t_max)

    t_win = t_max + cfg.densify_step_m  # same window for every candidate

    def _window_min_radius(cand):
        dense = densify(cand, cfg.densify_step_m)
        best = float("inf")
        for a, b, cc in zip(dense, dense[1:], dense[2:]):
            if math.dist(b, c) <= t_win:
                best = min(best, _circumradius(a, b, cc))
        return best

    best = None  # (window_min_radius, points)
    for t in ts:
        p1 = _point_at(coords, cum, s_c - t)
        p2 = _point_at(coords, cum, s_c + t)
        tan1 = _tangent_at(coords, cum, s_c - t, before=True)
        tan2 = _tangent_at(coords, cum, s_c + t, before=False)
        ba = _best_biarc(p1, tan1, p2, tan2, cfg.densify_step_m)
        if ba is None:
            continue
        pts, _r_arc = ba
        head = [p for p, s in zip(coords, cum) if s < s_c - t - 1e-9]
        tail = [p for p, s in zip(coords, cum) if s > s_c + t + 1e-9]
        cand = _dedupe(head + pts + tail)
        r_min = _window_min_radius(cand)
        if best is None or r_min > best[0]:
            best = (r_min, cand)
        if r_min >= radius - 1e-6:  # smallest setback meeting the target
            break
    if best is None:
        return coords, turn, None, True
    r_min, cand = best
    clamped = r_min < radius - 1e-6
    return cand, turn, r_min, clamped


# ------------------------------------------------------------ sites/ends

def transition_sites(g: Graph) -> dict:
    """{node_id: 'junction' | 'composition'} — the only places offsets
    may transition (v3 contract C1)."""
    sites = {}
    for nid, inc in g.incident.items():
        if len(inc) >= 3:
            sites[nid] = "junction"
        elif len(inc) == 2:
            a, b = inc
            if g.edges[a].line_keys != g.edges[b].line_keys:
                sites[nid] = "composition"
    return sites


@dataclass
class CorridorGeom:
    corridor: Corridor
    line: LineString      # xy (metres)
    length: float
    trim_a: float
    trim_b: float

    @property
    def consumed(self) -> bool:
        return self.length - self.trim_a - self.trim_b <= 0.05

    def trim(self, side: str) -> float:
        return self.trim_a if side == "a" else self.trim_b

    def node(self, side: str) -> int:
        return self.corridor.node_a if side == "a" else self.corridor.node_b

    def tail_xy(self, side: str):
        """Trimmed piece travelling TOWARD the node at `side`."""
        if side == "b":
            return _substring_coords(self.line, self.length - self.trim_b,
                                     self.length)
        return list(reversed(_substring_coords(self.line, 0.0, self.trim_a)))

    def head_xy(self, side: str):
        """Trimmed piece travelling AWAY from the node at `side`."""
        return list(reversed(self.tail_xy(side)))

    def frame_sign(self, side: str, *, toward: bool) -> float:
        """+1 when travelling toward(/away from) the node at `side` runs
        WITH the corridor's stored travel frame (node_a -> node_b)."""
        if toward:
            return 1.0 if side == "b" else -1.0
        return 1.0 if side == "a" else -1.0

    def probe_xy(self, side: str, dist: float):
        d = min(dist, 0.9 * self.length)
        s = self.length - d if side == "b" else d
        p = self.line.interpolate(s)
        return (p.x, p.y)


# ---------------------------------------------------------------- pairing

def _end_dir_at_node(cg: CorridorGeom, side: str, *, toward: bool):
    """Unit direction of the corridor at the node (toward or away)."""
    tail = cg.tail_xy(side)
    if len(tail) < 2:
        return (0.0, 0.0)
    a, b = (tail[-2], tail[-1]) if toward else (tail[-1], tail[-2])
    d = (b[0] - a[0], b[1] - a[1])
    n = math.hypot(*d) or 1.0
    return (d[0] / n, d[1] / n)


def _shape_passes(shapes, node_xy, window: float) -> list:
    """Split shapes into single PASSES near the node: contiguous vertex
    runs within `window` of it. Loop routes (CTA Brn/Org/P/Pink) pass a
    corner junction more than once in one shape — projecting probes on
    the whole shape scrambles the ordering test, so each pass is tested
    on its own. A closed shape whose seam falls inside the window has its
    end+start runs rejoined."""
    passes = []
    for ls in shapes:
        # densify first: sparse vertices on long straights would otherwise
        # clip runs at vertex granularity and lose the pass entirely
        coords = densify(list(ls.coords), window / 10.0)
        runs, cur = [], []
        for p in coords:
            if math.dist(p, node_xy) < window:
                cur.append(p)
            elif cur:
                runs.append(cur)
                cur = []
        if cur:
            runs.append(cur)
        if (len(runs) > 1 and runs[0][0] == tuple(coords[0])
                and runs[-1][-1] == tuple(coords[-1])
                and math.dist(coords[0], coords[-1]) < window):
            runs = [runs[-1] + runs[0]] + runs[1:-1]
        passes.extend(LineString(r) for r in runs if len(r) >= 2)
    return passes


def _shape_evidence(passes, node_pt: Point, pi: Point, pj: Point,
                    tol: float) -> bool:
    for ls in passes:
        if (ls.distance(node_pt) < tol and ls.distance(pi) < tol
                and ls.distance(pj) < tol):
            fi, fn, fj = (ls.project(pi), ls.project(node_pt), ls.project(pj))
            if fi < fn < fj or fj < fn < fi:
                return True
    return False


def pair_entries(nid: int, entries: list, cgs: dict, shapes_xy: dict,
                 node_xy, cfg: SegmentConfig, info: dict):
    """entries: [(corridor, side, ribbon)] for ONE color_key at ONE site.
    Returns (pairs, stubs)."""
    if len(entries) == 1:
        return [], list(entries)

    node_pt = Point(node_xy)
    member_keys = set()
    for _c, _s, r in entries:
        member_keys |= r.member_keys
    shapes = [ls for k in sorted(member_keys) for ls in shapes_xy.get(k, [])]
    window = 2 * cfg.probe_dist_m + cfg.evidence_tol_m
    passes = _shape_passes(shapes, (node_pt.x, node_pt.y), window)
    probes = [Point(cgs[c.cid].probe_xy(s, cfg.probe_dist_m))
              for c, s, _r in entries]

    if len(entries) == 2:
        # two corridor ends still need matched_shapes support: two same-
        # colour routes terminating opposite each other at one node must
        # NOT fuse into a fictitious through-transition (they become two
        # steady stubs). A shared member ROUTE keeps the pairing even
        # when its shape stops short — GTFS shapes often end at the
        # terminal platform while the graph's track continues (CTA Red
        # tail north of Howard) — recorded as a shape gap. Without
        # shapes, pair as before.
        if passes and not _shape_evidence(passes, node_pt, probes[0],
                                          probes[1], cfg.evidence_tol_m):
            (_c1, _s1, r1), (_c2, _s2, r2) = entries
            if r1.member_keys & r2.member_keys:
                info.setdefault("two_end_shape_gap_sites", []).append(nid)
            else:
                info.setdefault("two_end_unsupported_sites", []).append(nid)
                return [], list(entries)
        return [(entries[0], entries[1])], []

    pairs, used = [], set()
    if passes:
        for i in range(len(entries)):
            for j in range(i + 1, len(entries)):
                if _shape_evidence(passes, node_pt, probes[i], probes[j],
                                   cfg.evidence_tol_m):
                    pairs.append((entries[i], entries[j]))
                    used.update((i, j))
    if not pairs:
        info.setdefault("greedy_paired_sites", []).append(nid)
        cand = []
        for i in range(len(entries)):
            for j in range(i + 1, len(entries)):
                ci, si, _ = entries[i]
                cj, sj, _ = entries[j]
                d_in = _end_dir_at_node(cgs[ci.cid], si, toward=True)
                d_out = _end_dir_at_node(cgs[cj.cid], sj, toward=False)
                dot = max(-1.0, min(1.0,
                                    d_in[0] * d_out[0] + d_in[1] * d_out[1]))
                cand.append((math.degrees(math.acos(dot)), i, j))
        for _turn, i, j in sorted(cand):
            if i not in used and j not in used:
                pairs.append((entries[i], entries[j]))
                used.update((i, j))
    stubs = [entries[k] for k in range(len(entries)) if k not in used]
    return pairs, stubs


# ------------------------------------------------------------ main builder

def _merge_ribbon_meta(r1, r2):
    ids = sorted(set(r1.route_ids.split(",")) | set(r2.route_ids.split(",")))
    shorts = sorted(set(r1.route_short_names.split(","))
                    | set(r2.route_short_names.split(",")))
    rtypes = [t for t in (r1.route_type, r2.route_type) if t is not None]
    return {
        "route_ids": ",".join(ids),
        "route_short_names": ",".join(shorts),
        "feed_id": min(r1.feed_id, r2.feed_id),
        "route_type": min(rtypes) if rtypes else None,
        "route_color": max(r1.route_color, r2.route_color),
        "route_text_color": r1.route_text_color or r2.route_text_color,
    }


def build_segments(g: Graph, cfg: SegmentConfig = SegmentConfig(),
                   shapes: dict | None = None):
    """Segment the graph. shapes: {(feed_id, route_id): [[(lon, lat)...]]}
    (matched_shapes geometries) for junction pairing evidence; optional.
    Returns (segments, info)."""
    corridors = walk_corridors(g, cfg.gap_px)
    sites = transition_sites(g)
    lon0 = sum(n.lon for n in g.nodes.values()) / len(g.nodes)
    lat0 = sum(n.lat for n in g.nodes.values()) / len(g.nodes)
    proj = LocalProj(lon0, lat0)
    info: dict = {"sites": sites, "corridors": len(corridors),
                  "site_transitions": {}, "merged": 0, "skipped": 0,
                  "stubs": 0, "fillet_clamped": 0}

    half = cfg.transition_len_m / 2.0
    cgs: dict[int, CorridorGeom] = {}
    segments: list[Segment] = []

    for c in corridors:
        line = LineString(proj.to_xy(c.coords))
        length = line.length
        t_a = half if (not c.ring and c.node_a in sites) else 0.0
        t_b = half if (not c.ring and c.node_b in sites) else 0.0
        if t_a + t_b > length:  # short corridor: shrink halves to fit
            if t_a > 0 and t_b > 0:
                t_a = t_b = length / 2.0
            elif t_a > 0:
                t_a = length
            else:
                t_b = length
        cg = CorridorGeom(c, line, length, t_a, t_b)
        cgs[c.cid] = cg
        if not cg.consumed:
            coords_ll = proj.to_ll(
                _substring_coords(line, t_a, length - t_b))
            seg_len = length - t_a - t_b
            for r in c.ribbons:
                segments.append(Segment(
                    seg_id=-1, kind="steady", color_key=r.color_key,
                    route_short_names=r.route_short_names,
                    route_ids=r.route_ids, feed_id=r.feed_id,
                    route_type=r.route_type, route_color=r.route_color,
                    route_text_color=r.route_text_color, slot=r.slot,
                    line_count=r.count, offset_px=r.offset_px,
                    off_from_px=None, off_to_px=None, len_m=seg_len,
                    coords=coords_ll, corridor_id=c.cid))

    shapes_xy: dict = {}
    for key, geoms in (shapes or {}).items():
        shapes_xy[key] = [LineString(proj.to_xy(cs)) for cs in geoms
                          if len(cs) >= 2]

    ends_by_site: dict[int, list] = {}
    for cg in cgs.values():
        for side in ("a", "b"):
            if cg.trim(side) > 0:
                ends_by_site.setdefault(cg.node(side), []).append(
                    (cg.corridor, side))

    transitions: list[Segment] = []
    for nid in sorted(ends_by_site):
        node = g.nodes[nid]
        node_xy = proj.to_xy([(node.lon, node.lat)])[0]
        by_ck: dict[str, list] = {}
        for c, side in ends_by_site[nid]:
            for r in c.ribbons:
                by_ck.setdefault(r.color_key, []).append((c, side, r))
        n_t = 0
        for ck in sorted(by_ck):
            pairs, stubs = pair_entries(nid, by_ck[ck], cgs, shapes_xy,
                                        node_xy, cfg, info)
            for (c1, s1, r1), (c2, s2, r2) in pairs:
                cg1, cg2 = cgs[c1.cid], cgs[c2.cid]
                tail = cg1.tail_xy(s1)   # deduped, ends at the node
                head = cg2.head_xy(s2)   # deduped, starts at the node
                joined = tail + head[1:]
                corner = len(tail) - 1
                radius = (max(r1.count, r2.count) * cfg.gap_px
                          * cfg.fillet_radius_factor)
                raw_min = raw_min_radius(joined, corner, cfg.densify_step_m)
                coords, turn, r_arc, clamped = fillet_corner(
                    joined, corner, radius, cfg)
                coords = densify(coords, cfg.densify_step_m)
                off_from = cg1.frame_sign(s1, toward=True) * r1.offset_px
                off_to = cg2.frame_sign(s2, toward=False) * r2.offset_px
                meta = _merge_ribbon_meta(r1, r2)
                big = r1 if r1.count >= r2.count else r2
                if clamped:
                    info["fillet_clamped"] += 1
                transitions.append(Segment(
                    seg_id=-1, kind="transition", color_key=ck,
                    slot=big.slot, line_count=big.count, offset_px=None,
                    off_from_px=off_from, off_to_px=off_to,
                    len_m=_length(coords), coords=coords,  # xy for now
                    sites=(nid,), in_end=(c1.cid, s1), out_end=(c2.cid, s2),
                    turn_deg=turn, fillet_radius_m=r_arc,
                    fillet_clamped=clamped, raw_min_radius_m=raw_min,
                    **meta))
                n_t += 1
            for c, side, r in stubs:
                cg = cgs[c.cid]
                coords = densify(cg.tail_xy(side), cfg.densify_step_m)
                off = cg.frame_sign(side, toward=True) * r.offset_px
                segments.append(Segment(
                    seg_id=-1, kind="steady", color_key=ck,
                    route_short_names=r.route_short_names,
                    route_ids=r.route_ids, feed_id=r.feed_id,
                    route_type=r.route_type, route_color=r.route_color,
                    route_text_color=r.route_text_color, slot=r.slot,
                    line_count=r.count, offset_px=off, off_from_px=None,
                    off_to_px=None, len_m=_length(coords),
                    coords=proj.to_ll(coords), corridor_id=c.cid,
                    sites=(nid,)))
                info["stubs"] += 1
        info["site_transitions"][nid] = n_t

    transitions = _merge_consumed(transitions, cgs, info)

    for t in transitions:  # classify skips, convert coords to lon/lat
        if (abs(t.off_from_px - t.off_to_px) < cfg.offset_eps_px
                and t.turn_deg < cfg.straight_skip_deg):
            t.kind = "steady"
            t.offset_px = t.off_from_px
            t.off_from_px = t.off_to_px = None
            info["skipped"] += 1
        t.coords = proj.to_ll(t.coords)
    segments.extend(transitions)

    segments.sort(key=lambda s: (s.kind, s.corridor_id if s.corridor_id
                                 is not None else -1, s.sites, s.color_key,
                                 s.slot))
    for i, s in enumerate(segments):
        s.seg_id = i
    return segments, info


def _reverse_transition(t: Segment) -> Segment:
    """Reverse a transition's travel frame in place: geometry flips, the
    offsets swap AND negate (right-of-travel becomes left-of-travel)."""
    t.coords = list(reversed(t.coords))
    t.off_from_px, t.off_to_px = -t.off_to_px, -t.off_from_px
    t.in_end, t.out_end = t.out_end, t.in_end
    return t


def _q_side(t: Segment, cid: int) -> str | None:
    """Which side of corridor cid this transition touches (None if both
    or neither — a hairpin through the corridor cannot merge)."""
    at_in = t.in_end and t.in_end[0] == cid
    at_out = t.out_end and t.out_end[0] == cid
    if at_in and at_out:
        return None
    if at_in:
        return t.in_end[1]
    if at_out:
        return t.out_end[1]
    return None


def _merge_consumed(transitions: list, cgs: dict, info: dict) -> list:
    """A fully consumed corridor (no steady piece) merges the two
    transitions that meet at its split point into one feature. Pair
    orientation out of pair_entries is arbitrary, so candidates are
    re-oriented (reversed) to run INTO then OUT OF the corridor."""
    out = list(transitions)
    changed = True
    while changed:
        changed = False
        for cid, cg in cgs.items():
            if not cg.consumed or cg.trim_a <= 0 or cg.trim_b <= 0:
                continue
            touching = [t for t in out if _q_side(t, cid) is not None]
            merged_pair = None
            for i, a in enumerate(touching):
                for b in touching[i + 1:]:
                    if b.color_key != a.color_key:
                        continue
                    if _q_side(a, cid) == _q_side(b, cid):
                        continue  # same split-point side: cannot chain
                    if a.in_end and a.in_end[0] == cid:
                        _reverse_transition(a)  # q becomes a's out end
                    if b.out_end and b.out_end[0] == cid:
                        _reverse_transition(b)  # q becomes b's in end
                    if abs(a.off_to_px - b.off_from_px) > 1e-6:
                        info.setdefault("merge_offset_mismatch",
                                        []).append(cid)
                    gap = math.dist(a.coords[-1], b.coords[0])
                    if gap > 0.5:
                        info.setdefault("merge_gap", []).append((cid, gap))
                        continue
                    merged_pair = (a, b)
                    break
                if merged_pair:
                    break
            if merged_pair:
                a, b = merged_pair
                radii = [r for r in (a.fillet_radius_m, b.fillet_radius_m)
                         if r is not None]
                raws = [r for r in (a.raw_min_radius_m, b.raw_min_radius_m)
                        if r is not None]
                merged = replace(
                    a, off_to_px=b.off_to_px,
                    coords=_dedupe(a.coords + b.coords),
                    len_m=a.len_m + b.len_m, sites=a.sites + b.sites,
                    out_end=b.out_end, turn_deg=max(a.turn_deg, b.turn_deg),
                    fillet_radius_m=min(radii) if radii else None,
                    fillet_clamped=a.fillet_clamped or b.fillet_clamped,
                    raw_min_radius_m=min(raws) if raws else None,
                    route_ids=",".join(sorted(
                        set(a.route_ids.split(","))
                        | set(b.route_ids.split(",")))),
                    route_short_names=",".join(sorted(
                        set(a.route_short_names.split(","))
                        | set(b.route_short_names.split(",")))))
                out.remove(a)
                out.remove(b)
                out.append(merged)
                info["merged"] += 1
                changed = True
    return out
