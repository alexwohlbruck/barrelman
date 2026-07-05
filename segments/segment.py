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

from shapely import STRtree
from shapely.geometry import LineString, Point
from shapely.ops import substring

from .corridors import GAP_PX, Corridor, Graph, walk_corridors


# Zoom-banded transition lengths: (min_zoom, transition_len_m) — the tileset
# half of the low-zoom density work (the client half squeezes the ribbon gap,
# half spacing at z11 -> full at z14). A fixed 60 m transition is ~34 px at
# z15 but ~4 px at z12 — junctions look mushy at city scale. Bands keep the
# transition roughly constant SCREEN length: each band down doubles the
# ground length as m/px doubles. The build emits the COMPLETE feature set
# (steady + transitions, trims consistent) once per band; transit_lines_rt2
# serves exactly one band per request zoom — the band whose min_zoom is the
# highest <= z.
DEFAULT_BANDS: tuple = ((15, 60.0), (14, 120.0), (13, 240.0), (0, 480.0))


def band_ranges(bands=DEFAULT_BANDS) -> list:
    """[(band_minzoom, band_maxzoom, transition_len_m)] with maxzoom
    partners derived so the bands PARTITION the zoom axis: each band's
    maxzoom = next band's minzoom - 1, top band capped at 99. Sorted by
    minzoom descending (the z15 band — the pre-band default — first)."""
    ordered = sorted(bands, key=lambda b: -b[0])
    if len({mz for mz, _ in ordered}) != len(ordered):
        raise ValueError(f"duplicate band min_zooms: {bands}")
    out, hi = [], 99
    for mz, length in ordered:
        out.append((int(mz), hi, float(length)))
        hi = int(mz) - 1
    return out


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
    bands: tuple = DEFAULT_BANDS      # ((min_zoom, transition_len_m), ...)
    cusp_turn_deg: float = 150.0      # vertex turn above this = reversal cusp
    cusp_window_m: float = 20.0       # max cusp cluster span worth excising
    cusp_pad_m: float = 7.5           # excision window padding past the cusp
    loop_window_m: float = 100.0      # max corridor self-intersection loop
    #                                   arc worth excising (micro-artifacts
    #                                   only; genuine balloon loops stay)
    fillet_len_budget: float = 1.1    # max emitted transition length as a
    #                                   factor of the site's transition
    #                                   length (mirrors the exam's C3 cap)
    track_snap_tol_m: float = 18.0    # a steady corridor whose centerline
    #                                   strays past this from the real OSM
    #                                   track is reconciled back onto it
    #                                   (off-graph agency-bridged runs) —
    #                                   well above any legit bundle-midline
    #                                   offset (NYC 4-track trunks measure
    #                                   <= ~12 m), so on-track geometry and
    #                                   chicago:l-v3 are never touched
    track_snap_vertex_m: float = 8.0  # per-vertex off-track threshold in the
    #                                   way-snap fallback (project only the
    #                                   vertices genuinely off the track)
    track_snap_max_dist_m: float = 120.0  # never snap onto a way farther
    #                                   than this (a corridor with no nearby
    #                                   track — off-map, ferry — is left as
    #                                   is rather than yanked onto a stranger)


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
    slot: int                      # bundle position in the FEATURE's
    line_count: int                # travel frame (matches the offsets)
    offset_px: float | None        # authoritative (steady)
    off_from_px: float | None      # authoritative (transition)
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
    fillet_target_m: float | None = None   # min radius the corner was BUILT
    # to (local bundle count x gap x factor at pair time; min across
    # merged corners) — a merged chain's line_count is the bigger end's,
    # so exams must not re-derive interior corners' targets from it


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


# ---------------------------------------------------- legacy slot encoding

def legacy_slot_count(offset_px: float, gap_px: float) -> tuple:
    """A (slot, line_count) pair whose v2 constant-offset formula
    ``(slot - (line_count-1)/2) * gap_px`` reproduces ``offset_px`` exactly,
    with slot >= 0 — the same half-gap k/h scheme the tile function already
    derives for transitions (import/create-transit-lines-rt2.sql). Stable
    re-anchoring decouples ``offset_px`` from the bundle's symmetric centering,
    so a steady row's emitted (slot, line_count) must be re-derived from the
    authoritative offset for stock-Mapbox degradation to keep matching it.
    (line_count here is a rendering encoding, NOT the true bundle size — the
    true count lives on the Ribbon and drives fillet radius / pairing.)"""
    half = gap_px / 2.0
    k = round(offset_px / half)          # offset in half-gap units
    h0 = max(-k / 2.0, 0.0)
    h = math.ceil(h0) if k % 2 == 0 else math.ceil(h0 - 0.5) + 0.5
    return int(round(k / 2.0 + h)), int(round(2 * h + 1))


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
    tell it apart from fillet-introduced kinks.

    Densified at step/2 — the FINEST vertex grid the emitted feature can
    contain (biarc arcs sample at step/2; densify halves any segment
    just over step): discrete circumradius at a fixed bend shrinks with
    its neighbouring segment lengths, so a floor probed on a coarser
    grid than the emitted rows overstates the inherited radius and fails
    the exam on a measurement artifact (GC 7/7X: the same 20-degree raw
    bend read 12.7 m on the 7.5 m grid vs 10.7 m on the emitted 5.7 m
    split). step/2 makes the recorded floor a true lower bound for any
    per-triple measurement of the served geometry."""
    corner = coords[corner_idx]
    dense = densify(coords, step / 2.0)
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


def fillet_corner(coords, corner_idx: int, radius: float, cfg: SegmentConfig,
                  budget: float | None = None):
    """Replace the corner at coords[corner_idx] with a tangent-continuous
    biarc of min radius >= target (setback grows until the target is met,
    capped at 90% of each side's available length). Seam tangents come
    from the ACTUAL polyline at the cut points — the accumulated turn of
    the removed lead-in/lead-out vertices is absorbed by the biarc, never
    concentrated into single-vertex seam kinks. Candidates are scored by
    the min circumradius of the densified RESULT inside the transition
    window, so the setback also grows to swallow raw track kinks sitting
    just outside the cut. `budget` caps the RESULT's length (C3: the
    emitted feature must stay within fillet_len_budget x the site's
    transition length) — a candidate over it only wins when nothing fits
    (Mott Haven wye: a bulging biarc bought +13 m of length for a sub-
    target +1 m of radius on 1.5 m inherited switch curvature). Returns
    (coords, turn_deg, radius_m|None, clamped) — radius_m is the
    achieved window minimum."""
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

    best = None      # (window_min_radius, points) within the length budget
    best_any = None  # unconstrained fallback when nothing fits the budget
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
        fits = budget is None or _length(cand) <= budget
        if best_any is None or r_min > best_any[0]:
            best_any = (r_min, cand)
        if fits and (best is None or r_min > best[0]):
            best = (r_min, cand)
        if fits and r_min >= radius - 1e-6:
            break  # smallest setback meeting target within budget
    if best is None:
        best = best_any
    if best is None:
        return coords, turn, None, True
    r_min, cand = best
    clamped = r_min < radius - 1e-6
    return cand, turn, r_min, clamped


def excise_reversal_cusps(coords, cfg: SegmentConfig):
    """Remove micro-hairpins from a transition centerline.

    A refit-collapsed plan-view X leaves a crossing rung a few metres
    long between two junction nodes; when the node placement overshoots
    along the through-direction, a through-transition must traverse the
    rung AGAINST travel — near-180-degree turns bounded within a couple
    of metres that self-intersect once densified (nyc:subway-v3 Borough
    Hall, segs 521-523: a ~1 m loop + vertex cluster at the rung). The
    corner fillet cannot absorb them: the biarc rejects near-reversal
    arcs by design.

    Each cluster of vertices whose turn exceeds cusp_turn_deg, spanning
    at most cusp_window_m of arc, is cut out (padded by cusp_pad_m,
    never touching the feature endpoints — they carry the offset handoff
    to the adjacent steady features) and re-joined with a biarc taking
    its tangents from the surviving polyline; a degenerate biarc falls
    back to the straight chord. Returns (coords, min_biarc_radius|None,
    n_excised) — coords are unchanged when no cusp exists.
    """
    def _reversal_window(coords, cum):
        """First (s_from, s_to) arc window where the path's heading
        reverses (>= cusp_turn_deg vs an upstream segment) within
        cusp_window_m — catches both a raw near-180 corner vertex AND
        the same reversal smeared over several vertices by a small
        fillet arc. None when the path never doubles back."""
        dirs = []
        for a, b in zip(coords, coords[1:]):
            d = (b[0] - a[0], b[1] - a[1])
            n = math.hypot(*d)
            dirs.append((d[0] / n, d[1] / n) if n > 1e-12 else (0.0, 0.0))
        thresh = math.cos(math.radians(cfg.cusp_turn_deg))
        for i in range(len(dirs)):
            last_rev = None
            j = i + 1
            while j < len(dirs) and cum[j] - cum[i + 1] <= cfg.cusp_window_m:
                if dirs[i][0] * dirs[j][0] + dirs[i][1] * dirs[j][1] < thresh:
                    last_rev = j
                j += 1
            if last_rev is not None:
                return cum[i + 1], cum[last_rev + 1]
        return None

    n_excised = 0
    min_r = None
    guard = 0
    while guard < 4:  # nested/adjacent clusters: re-scan after each fix
        guard += 1
        cum = _cum_lengths(coords)
        win = _reversal_window(coords, cum)
        if win is None:
            break
        s_lo = max(0.5, win[0] - cfg.cusp_pad_m)
        s_hi = min(cum[-1] - 0.5, win[1] + cfg.cusp_pad_m)
        if s_hi - s_lo < 1e-6 or s_hi - s_lo > 2 * cfg.cusp_window_m + 2 * cfg.cusp_pad_m:
            break  # not a rung-scale artifact: leave it alone
        p1 = _point_at(coords, cum, s_lo)
        p2 = _point_at(coords, cum, s_hi)
        t1 = _tangent_at(coords, cum, s_lo, before=True)
        t2 = _tangent_at(coords, cum, s_hi, before=False)
        head = [p for p, s in zip(coords, cum) if s < s_lo - 1e-9]
        tail = [p for p, s in zip(coords, cum) if s > s_hi + 1e-9]
        ba = _best_biarc(p1, t1, p2, t2, cfg.densify_step_m)
        if ba is not None:
            mid, r = ba
        else:
            mid, r = [p1, p2], None
        cand = _dedupe(head + mid + tail)
        if len(cand) < 2 or not LineString(cand).is_simple:
            cand = _dedupe(head + [p1, p2] + tail)  # chord fallback
            r = None
            if len(cand) < 2 or not LineString(cand).is_simple:
                break  # give up rather than emit something worse
        coords = cand
        n_excised += 1
        if r is not None:
            min_r = r if min_r is None else min(min_r, r)
    return coords, min_r, n_excised


def excise_corridor_loops(coords, cfg: SegmentConfig):
    """Remove micro self-intersection loops from a corridor centerline.

    The way-graph corridor walk can hand segments a centerline that
    crosses ITSELF at track-artifact scale: at the Mott Haven / E 149 St
    wye (nyc:subway-v3) the 5's merged corridor carries a ~55 m
    out-and-back excursion of the triangle's west leg, so every steady
    trim or transition tail long enough to contain it fails ST_IsSimple
    — at every band, before any transition machinery runs. The corridor
    is the authoritative geometry for ALL features cut from it, so the
    cleanup happens here, once, not per-feature.

    Walking the polyline, the first pair of non-adjacent segments that
    cross bounds a loop [s_i, s_j] (arc positions of the crossing point
    X on each branch). A loop no longer than loop_window_m is excised by
    rejoining the path THROUGH X — geometry outside the loop is
    untouched, the path stays continuous, and an out-and-back excursion
    (two crossings chained) resolves over the re-scan iterations.
    Anything larger is genuine topology (balloon loops) and is left
    alone — a non-simple corridor that survives here still fails the
    exam loudly rather than being silently rewritten. Closed rings skip
    the first/last segment adjacency. Returns (coords, n_excised,
    removed_m)."""
    if len(coords) < 4 or LineString(coords).is_simple:
        return coords, 0, 0.0

    def _first_crossing(coords, cum, closed):
        n = len(coords)
        for i in range(n - 2):
            seg_i = LineString(coords[i:i + 2])
            for j in range(i + 2, n - 1):
                if closed and i == 0 and j == n - 2:
                    continue
                seg_j = LineString(coords[j:j + 2])
                if not seg_i.intersects(seg_j):
                    continue
                inter = seg_i.intersection(seg_j)
                # transversal crossing -> Point; collinear overlap ->
                # LineString: take a representative point on both
                x = (inter.x, inter.y) if inter.geom_type == "Point" \
                    else (inter.representative_point().x,
                          inter.representative_point().y)
                s_i = cum[i] + math.dist(coords[i], x)
                s_j = cum[j] + math.dist(coords[j], x)
                return i, j, x, s_j - s_i
        return None

    n_excised = 0
    removed = 0.0
    guard = 0
    while guard < 8:
        guard += 1
        ls = LineString(coords)
        if ls.is_simple:
            break
        cum = _cum_lengths(coords)
        closed = math.dist(coords[0], coords[-1]) < 1e-9
        hit = _first_crossing(coords, cum, closed)
        if hit is None:
            break
        i, j, x, loop_len = hit
        if loop_len > cfg.loop_window_m:
            break  # not a micro artifact: leave it (and fail loudly)
        coords = _dedupe(coords[:i + 1] + [x] + coords[j + 1:])
        if len(coords) < 2:
            break
        n_excised += 1
        removed += loop_len
    return coords, n_excised, removed


# ------------------------------------------------ off-track reconciliation

def _nearest_way_indices(tree, geom) -> list:
    """STRtree.query_nearest result normalized to a plain int list (it
    returns a numpy array, occasionally with more than one equidistant
    hit)."""
    res = tree.query_nearest(geom)
    try:
        return [int(k) for k in res]
    except TypeError:
        return [int(res)]


def _max_stray_xy(line, tree, ways, step: float = 15.0):
    """(max stray, nearest-way distance sampled at the max) of an xy
    LineString from the real OSM track (STRtree of way LineStrings).
    Returns (None, None) when NO way lies near the line — an unmeasurable
    corridor (off-map / wrong mode) must never be snapped."""
    if line.length < 1e-6:
        return 0.0, 0.0
    n = max(2, int(line.length / step))
    best = 0.0
    any_near = False
    for i in range(n + 1):
        p = line.interpolate(i / n, normalized=True)
        ks = _nearest_way_indices(tree, p)
        ds = [ways[k].distance(p) for k in ks]
        if not ds:
            continue
        any_near = True
        best = max(best, min(ds))
    return (best, None) if any_near else (None, None)


def _snap_vertices_to_ways(coords_xy, tree, ways, cfg: SegmentConfig):
    """Project every vertex farther than track_snap_vertex_m from the real
    track onto the nearest way; keep on-track vertices verbatim; pin the
    endpoints (they carry the corridor's node handoff). Vertices whose
    nearest way is beyond track_snap_max_dist_m are left untouched (no
    stranger-track yanks). The polyline is densified first so a sparse
    off-run chord has interior vertices to pull down onto the curve."""
    coords_xy = densify(coords_xy, cfg.densify_step_m)
    out = []
    for p in coords_xy:
        pt = Point(p)
        ks = _nearest_way_indices(tree, pt)
        if not ks:
            out.append(p)
            continue
        k = min(ks, key=lambda k: ways[k].distance(pt))
        d = ways[k].distance(pt)
        if cfg.track_snap_vertex_m < d <= cfg.track_snap_max_dist_m:
            cp = ways[k].interpolate(ways[k].project(pt))
            out.append((cp.x, cp.y))
        else:
            out.append(p)
    out[0] = coords_xy[0]
    out[-1] = coords_xy[-1]
    return _dedupe(out)


def reconcile_offtrack_corridors(corridors, ways_xy, proj, cfg: SegmentConfig,
                                 info: dict):
    """Reconcile steady corridors whose centerline strays from the real
    OSM track back ONTO it, IN PLACE (rewrites corridor.coords).

    The way-graph corridor is normally real OSM way polyline verbatim
    (R1), but a route whose shapesnap match failed over a stretch carries
    an OFF-GRAPH agency-bridged run: a straight GTFS-shape chord the
    linegraph rides literally (the F-express `FX` cutting a 63 m chord
    across the Culver S-curve at 15 St-Prospect Park; the `5`'s off-run
    stub at the Nevins wye). The steady segments cut from such a corridor
    inherit the chord — a STEADY centerline must never leave its real
    track by more than a track's width.

    Two remedies, preferred first, both gated by track_snap_tol_m (well
    above any legitimate bundle-midline offset, so on-track corridors and
    chicago:l-v3 are byte-identical):

      (a) sibling adoption — a co-endpoint sibling corridor (the SAME node
          pair) that DOES follow the track donates its polyline (oriented
          to node_a -> node_b, endpoints pinned). This is the right fix
          for a route sharing physical track with a well-matched
          neighbour (`FX` adopts the on-graph `F` local's curve); the
          client offsets the two ribbons apart by slot as before.
      (b) way-snap — no faithful sibling (a solo off-run at a wye):
          project the off-track vertices onto the nearest real way,
          keeping the on-track vertices and the endpoints verbatim.
          Accepted ONLY when the projection stays simple (after a
          micro-loop cleanup) — a wye/turnback fold that projects onto
          itself is left as the honest off-track chord rather than a
          tangled snap.

    Corridors with no nearby track of the mode are never touched. Records
    `track_reconciled` {cid: (method, before_m, after_m)} on info."""
    if not ways_xy:
        return
    tree = STRtree(ways_xy)
    lines_xy = {c.cid: LineString(proj.to_xy(c.coords)) for c in corridors}
    by_pair: dict = {}
    for c in corridors:
        by_pair.setdefault(frozenset((c.node_a, c.node_b)), []).append(c)

    strays = {c.cid: _max_stray_xy(lines_xy[c.cid], tree, ways_xy)[0]
              for c in corridors}
    reconciled: dict = {}
    for c in corridors:
        s = strays[c.cid]
        if s is None or s <= cfg.track_snap_tol_m:
            continue
        # (a) co-endpoint sibling that follows the track
        adopted = None
        if not c.ring:
            for sib in by_pair.get(frozenset((c.node_a, c.node_b)), ()):
                if sib.cid == c.cid or sib.ring:
                    continue
                ss = strays.get(sib.cid)
                if ss is None or ss > cfg.track_snap_tol_m:
                    continue
                sxy = list(lines_xy[sib.cid].coords)
                if sib.node_a != c.node_a:  # orient to c's node_a -> node_b
                    sxy = list(reversed(sxy))
                cxy = list(lines_xy[c.cid].coords)
                sxy[0], sxy[-1] = cxy[0], cxy[-1]   # pin c's own endpoints
                adopted = _dedupe(sxy)
                method = "sibling"
                break
        if adopted is None:
            adopted = _snap_vertices_to_ways(
                list(lines_xy[c.cid].coords), tree, ways_xy, cfg)
            # projecting onto a curved way can fold a wye leg back on
            # itself — clean micro self-crossings, then require the
            # result be simple (a snap that stays tangled is worse than
            # the honest off-track chord: keep the original)
            adopted, _n, _rm = excise_corridor_loops(adopted, cfg)
            method = "waysnap"
            if len(adopted) < 2 or not LineString(adopted).is_simple:
                continue
        if len(adopted) < 2:
            continue
        after = _max_stray_xy(LineString(adopted), tree, ways_xy)[0]
        # never accept a change that fails to improve the stray materially
        if after is None or after >= s - 1e-6:
            continue
        c.coords = proj.to_ll(adopted)
        reconciled[c.cid] = (method, round(s, 1), round(after, 1))
    if reconciled:
        info["track_reconciled"] = reconciled


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


def _probe_through_xy(cg: CorridorGeom, side: str, dist: float, ck: str,
                      cgs: dict, ends_by_site: dict):
    """Probe point ~dist m beyond the node at `side` along the ribbon,
    walking THROUGH decisively-short corridors while the same-colour
    continuation is unambiguous.

    A probe inside a fully consumed crossing rung (a collapsed
    plan-view X — ~4 m after the linegraph refit pulls both junction
    nodes onto the true crossing point) sits practically ON the node,
    so the traversal-evidence order test (the pass projections of the
    two probes must straddle the node) becomes a coin flip and one end
    can pair twice (the 74 St / Roosevelt Av E double-pair). Extending
    the probe through the rung into the ribbon's continuation restores
    a real margin. Falls back to the plain capped probe on ambiguity."""
    remaining = dist
    seen = {cg.corridor.cid}
    for _ in range(8):
        if cg.corridor.ring or cg.length >= 0.5 * dist:
            break
        far_node = cg.node("a" if side == "b" else "b")
        conts = [
            (c2, s2) for c2, s2 in ends_by_site.get(far_node, ())
            if c2.cid not in seen
            and any(r.color_key == ck for r in c2.ribbons)
        ]
        if len(conts) != 1:
            break
        remaining = max(remaining - cg.length, 2.0)
        c2, side = conts[0]
        cg = cgs[c2.cid]
        seen.add(c2.cid)
    return cg.probe_xy(side, remaining)


def pair_entries(nid: int, entries: list, cgs: dict, shapes_xy: dict,
                 node_xy, cfg: SegmentConfig, info: dict,
                 ends_by_site: dict | None = None):
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
    ck = entries[0][2].color_key
    if ends_by_site is None:
        probes = [Point(cgs[c.cid].probe_xy(s, cfg.probe_dist_m))
                  for c, s, _r in entries]
    else:
        probes = [Point(_probe_through_xy(cgs[c.cid], s, cfg.probe_dist_m,
                                          ck, cgs, ends_by_site))
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

    def folds(i, j) -> bool:
        """The two ends double back on each other — a through-transition
        would fold (post-round-19 bundle mouths at multi-family Queens Blvd
        interlockings). Their end tangents at the node are anti-parallel,
        which is a stub-meet, not a flow-through: pairing them fabricates a
        self-intersecting connector. Demote to stubs instead."""
        ci, si, _ = entries[i]
        cj, sj, _ = entries[j]
        d_in = _end_dir_at_node(cgs[ci.cid], si, toward=True)
        d_out = _end_dir_at_node(cgs[cj.cid], sj, toward=False)
        dot = d_in[0] * d_out[0] + d_in[1] * d_out[1]
        return dot < -0.5  # > 120 deg turn: the arms run back on themselves

    pairs, used = [], set()
    if passes:
        for i in range(len(entries)):
            for j in range(i + 1, len(entries)):
                if (_shape_evidence(passes, node_pt, probes[i], probes[j],
                                    cfg.evidence_tol_m)
                        and not folds(i, j)):
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
            if i not in used and j not in used and not folds(i, j):
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
                   shapes: dict | None = None,
                   ways: list | None = None):
    """Segment the graph. shapes: {(feed_id, route_id): [[(lon, lat)...]]}
    (matched_shapes geometries) for junction pairing evidence; optional.
    ways: [[(lon, lat)...]] real OSM track polylines of the build's mode
    (geo_places railway ways) for the off-track corridor reconciliation;
    optional (no reconciliation without them). Returns (segments, info)."""
    corridors = walk_corridors(g, cfg.gap_px)
    sites = transition_sites(g)
    lon0 = sum(n.lon for n in g.nodes.values()) / len(g.nodes)
    lat0 = sum(n.lat for n in g.nodes.values()) / len(g.nodes)
    proj = LocalProj(lon0, lat0)
    info: dict = {"sites": sites, "corridors": len(corridors),
                  "site_transitions": {}, "merged": 0, "skipped": 0,
                  "stubs": 0, "fillet_clamped": 0}

    # Reconcile off-track corridors (off-graph agency-bridged runs) back
    # onto the real OSM track BEFORE any geometry is cut from them — every
    # steady/transition feature then inherits the corrected centerline.
    if ways:
        ways_xy = [LineString(proj.to_xy(w)) for w in ways if len(w) >= 2]
        reconcile_offtrack_corridors(corridors, ways_xy, proj, cfg, info)

    half = cfg.transition_len_m / 2.0
    cgs: dict[int, CorridorGeom] = {}
    segments: list[Segment] = []

    lines: dict[int, LineString] = {}
    for c in corridors:
        xy, n_loops, removed = excise_corridor_loops(
            proj.to_xy(c.coords), cfg)
        if n_loops:
            info.setdefault("corridor_loops_excised", {})[c.cid] = (
                n_loops, round(removed, 1))
        lines[c.cid] = LineString(xy)

    # Per-site transition-length clamp (PAR-12 Mott Haven wye): a
    # corridor whose far end is a FREE end (a ribbon terminus, not a
    # site) can donate no more than its own length to a transition — an
    # over-long band otherwise consumes the stub whole, folds the
    # transition back through the wye, and swallows the terminus into
    # a transition interior (two branch twins then share the terminus
    # point as a "matched" endpoint and the ribbon's end goes vacant).
    # The effective transition length at such a site clamps to the
    # shortest free-end corridor's available length (its full length —
    # the free end trims nothing), floored at the base band's length so
    # the top band is byte-identical everywhere. Corridors BETWEEN two
    # sites are not clamped on: they keep the existing shrink-to-fit /
    # consumed-corridor merge paths (Tower 18-class interlockings and
    # the Loop's long-band corner chains are that design working).
    base_len = min([cfg.transition_len_m]
                   + [float(ln) for _, ln in cfg.bands])
    site_half: dict[int, float] = {}
    for c in corridors:
        if c.ring:
            continue
        length = lines[c.cid].length
        for nid, oth in ((c.node_a, c.node_b), (c.node_b, c.node_a)):
            if nid not in sites or oth in sites:
                continue
            sup = max(length, base_len) / 2.0
            site_half[nid] = min(site_half.get(nid, half), sup)
    clamped = {nid: round(2 * h, 1) for nid, h in site_half.items()
               if h < half - 1e-9}
    if clamped:
        info["site_len_clamped"] = clamped

    for c in corridors:
        line = lines[c.cid]
        length = line.length
        t_a = (site_half.get(c.node_a, half)
               if (not c.ring and c.node_a in sites) else 0.0)
        t_b = (site_half.get(c.node_b, half)
               if (not c.ring and c.node_b in sites) else 0.0)
        if t_a + t_b > length:  # short corridor: shrink halves to fit,
            # never inflating a clamped side past its own request
            if t_a > 0 and t_b > 0:
                t_a = min(t_a, max(length / 2.0, length - t_b))
                t_b = min(t_b, length - t_a)
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
                lslot, lcount = legacy_slot_count(r.offset_px, cfg.gap_px)
                segments.append(Segment(
                    seg_id=-1, kind="steady", color_key=r.color_key,
                    route_short_names=r.route_short_names,
                    route_ids=r.route_ids, feed_id=r.feed_id,
                    route_type=r.route_type, route_color=r.route_color,
                    route_text_color=r.route_text_color, slot=lslot,
                    line_count=lcount, offset_px=r.offset_px,
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
                                        node_xy, cfg, info, ends_by_site)
            for (c1, s1, r1), (c2, s2, r2) in pairs:
                cg1, cg2 = cgs[c1.cid], cgs[c2.cid]
                tail = cg1.tail_xy(s1)   # deduped, ends at the node
                head = cg2.head_xy(s2)   # deduped, starts at the node
                joined = tail + head[1:]
                corner = len(tail) - 1
                # a paired tail+head that DOUBLES BACK on itself (the two
                # ends run anti-parallel near the node — a post-round-19
                # bundle mouth at Queens Blvd where the F/FX/M arms fold)
                # self-intersects before any fillet, so the corner index no
                # longer marks the real bend and the emitted feature would
                # be non-simple. Excise the doubled-back loop first (a
                # transition is a synthetic connector, so any-scale excision
                # is valid), then relocate the corner to the join point.
                if len(joined) >= 4 and not LineString(joined).is_simple:
                    node_pt = tail[-1]
                    tcfg = replace(cfg, loop_window_m=_length(joined) + 1.0)
                    healed, n_x, _rm = excise_corridor_loops(joined, tcfg)
                    if n_x and LineString(healed).is_simple:
                        joined = healed
                        corner = min(range(len(joined)),
                                     key=lambda i: math.dist(joined[i],
                                                             node_pt))
                        info["transition_fold_excised"] = \
                            info.get("transition_fold_excised", 0) + n_x
                radius = (max(r1.count, r2.count) * cfg.gap_px
                          * cfg.fillet_radius_factor)
                raw_min = raw_min_radius(joined, corner, cfg.densify_step_m)
                budget = (cfg.fillet_len_budget * 2.0
                          * site_half.get(nid, half))
                coords, turn, r_arc, clamped = fillet_corner(
                    joined, corner, radius, cfg, budget=budget)
                coords = densify(coords, cfg.densify_step_m)
                sign1 = cg1.frame_sign(s1, toward=True)
                sign2 = cg2.frame_sign(s2, toward=False)
                off_from = sign1 * r1.offset_px
                off_to = sign2 * r2.offset_px
                meta = _merge_ribbon_meta(r1, r2)
                big, big_sign = ((r1, sign1) if r1.count >= r2.count
                                 else (r2, sign2))
                # slot lives in the feature's travel frame, like the
                # offsets — mirror it when the source corridor is stored
                # against the direction of travel
                slot = (big.slot if big_sign > 0
                        else big.count - 1 - big.slot)
                if clamped:
                    info["fillet_clamped"] += 1
                transitions.append(Segment(
                    seg_id=-1, kind="transition", color_key=ck,
                    slot=slot, line_count=big.count, offset_px=None,
                    off_from_px=off_from, off_to_px=off_to,
                    len_m=_length(coords), coords=coords,  # xy for now
                    sites=(nid,), in_end=(c1.cid, s1), out_end=(c2.cid, s2),
                    turn_deg=turn, fillet_radius_m=r_arc,
                    fillet_clamped=clamped, raw_min_radius_m=raw_min,
                    fillet_target_m=radius, **meta))
                n_t += 1
            for c, side, r in stubs:
                cg = cgs[c.cid]
                coords = densify(cg.tail_xy(side), cfg.densify_step_m)
                sign = cg.frame_sign(side, toward=True)
                off = sign * r.offset_px
                slot, lcount = legacy_slot_count(off, cfg.gap_px)
                segments.append(Segment(
                    seg_id=-1, kind="steady", color_key=ck,
                    route_short_names=r.route_short_names,
                    route_ids=r.route_ids, feed_id=r.feed_id,
                    route_type=r.route_type, route_color=r.route_color,
                    route_text_color=r.route_text_color, slot=slot,
                    line_count=lcount, offset_px=off, off_from_px=None,
                    off_to_px=None, len_m=_length(coords),
                    coords=proj.to_ll(coords), corridor_id=c.cid,
                    sites=(nid,)))
                info["stubs"] += 1
        info["site_transitions"][nid] = n_t

    transitions = _merge_consumed(transitions, cgs, info)

    for t in transitions:  # excise collapsed-rung reversal hairpins
        coords, r_cusp, n_x = excise_reversal_cusps(t.coords, cfg)
        if n_x:
            t.coords = densify(coords, cfg.densify_step_m)
            t.len_m = _length(t.coords)
            info["cusp_excised"] = info.get("cusp_excised", 0) + n_x
            if r_cusp is not None:
                radii = [r for r in (t.fillet_radius_m, r_cusp)
                         if r is not None]
                t.fillet_radius_m = min(radii)
                target = (t.fillet_target_m if t.fillet_target_m
                          is not None else t.line_count * cfg.gap_px
                          * cfg.fillet_radius_factor)
                if r_cusp < target - 1e-6 and not t.fillet_clamped:
                    t.fillet_clamped = True
                    info["fillet_clamped"] += 1

    for t in transitions:  # heal any transition centerline that still folds
        # After the round-19 cross-family re-bundling, a wider bundle whose
        # arms approach a junction at a shallow angle can leave a fillet
        # centerline that crosses ITSELF (not a near-180 reversal cusp, so
        # excise_reversal_cusps misses it — an F/FX/M Queens Blvd merge
        # mouth). A folded centerline emits a self-intersecting feature and
        # fails ST_IsSimple. Clean micro self-crossing loops the same way a
        # corridor is cleaned; if still not simple, the fillet arc is the
        # culprit — fall back to the un-filleted joined polyline (straight
        # tail+head through the node), which is simple by construction.
        if len(t.coords) >= 4 and not LineString(t.coords).is_simple:
            # a transition is a synthetic connector, not real track, so its
            # fold may be excised at any scale up to its own length (unlike a
            # corridor, where a large loop is genuine topology) — use a loop
            # window that spans the whole feature.
            tcfg = replace(cfg, loop_window_m=max(cfg.loop_window_m,
                                                  _length(t.coords) + 1.0))
            healed, n_loops, _rm = excise_corridor_loops(t.coords, tcfg)
            if n_loops and LineString(healed).is_simple:
                t.coords = densify(healed, cfg.densify_step_m)
                t.len_m = _length(t.coords)
                info["transition_loops_excised"] = \
                    info.get("transition_loops_excised", 0) + n_loops
            elif t.in_end and t.out_end:
                # rebuild the plain joined centerline (no fillet) from the
                # corridor tails — geometry the fillet was smoothing
                ci, si = t.in_end
                cj, sj = t.out_end
                if ci in cgs and cj in cgs:
                    tail = cgs[ci].tail_xy(si)
                    head = cgs[cj].head_xy(sj)
                    plain = densify(_dedupe(tail + head[1:]),
                                    cfg.densify_step_m)
                    if len(plain) >= 2 and LineString(plain).is_simple:
                        t.coords = plain
                        t.len_m = _length(plain)
                        t.fillet_radius_m = None
                        t.fillet_clamped = True
                        info["transition_unfilleted"] = \
                            info.get("transition_unfilleted", 0) + 1

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
    offsets swap AND negate (right-of-travel becomes left-of-travel),
    and slot mirrors to stay in the emitted frame."""
    t.coords = list(reversed(t.coords))
    t.off_from_px, t.off_to_px = -t.off_to_px, -t.off_from_px
    t.slot = t.line_count - 1 - t.slot
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
                        # both offsets derive from the SAME corridor
                        # ribbon (frame-signed) — a mismatch means the
                        # merged feature would hide an offset step in its
                        # interior, violating C1. Fail loudly.
                        raise ValueError(
                            f"consumed-corridor merge offset mismatch at "
                            f"corridor {cid} ({a.color_key}): "
                            f"{a.off_to_px} != {b.off_from_px}")
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
                targets = [r for r in (a.fillet_target_m, b.fillet_target_m)
                           if r is not None]
                merged = replace(
                    a, off_to_px=b.off_to_px,
                    coords=_dedupe(a.coords + b.coords),
                    len_m=a.len_m + b.len_m, sites=a.sites + b.sites,
                    out_end=b.out_end, turn_deg=max(a.turn_deg, b.turn_deg),
                    fillet_radius_m=min(radii) if radii else None,
                    fillet_clamped=a.fillet_clamped or b.fillet_clamped,
                    raw_min_radius_m=min(raws) if raws else None,
                    fillet_target_m=min(targets) if targets else None,
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
