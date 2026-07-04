#!/usr/bin/env python3
"""linegraph.waygraph — pattern usage over the OSM way graph (stage 4 core).

The way-graph corridor pipeline (docs/transit-pipeline-v3.md stage 4,
way-graph era) replaces raster-skeleton corridor DISCOVERY with corridors
built directly on the shapesnap matching graph: the matched shapes lie
VERBATIM on the way edges (except explicitly bridged spans), so the set
of way edges each pattern rides — and therefore every corridor and every
junction — is exact, not inferred from ink.

matched_shapes.stats does NOT persist shapesnap's per-pattern edges_used
(it was never written), so this module RECONSTRUCTS it from geometry:
every pattern's matched shape is densified and snapped to the way-edge
STRtree at identity tolerance (the shape is on-graph up to the ~1 m
output simplification plus zip 1e-6 deg rounding; the nearest PARALLEL
track is >= 3.5 m away, so 2.5 m separates identity from neighborhood).
Reconstruction is verified against the one edge-set digest stats DID
keep: levels_m (per-vertical-class meters over edges_used) — see
verify_levels().

Coverage is tracked per edge as an interval union so that
  * a pattern ENDING mid-edge still claims the edge (its terminal station
    splits it later) but corridor TERMINALS are trimmed to covered track,
  * junction pass-through contamination (samples grabbing a foreign edge
    for a vertex or two around a shared node) stays below the membership
    threshold and never rides.

Sample runs that snap to NO edge are the agency/graph-bridged spans
(~2.7 km across both feeds): they become explicit OFF-GRAPH geometry
runs, anchored at (edge, along-position) so the corridor builder splits
the underlying edges and keeps the network connected.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import shapely
from pyproj import Transformer
from shapely import STRtree
from shapely.geometry import LineString

from shapesnap.graph import ModeGraph, load_graph

REPO_ROOT = Path(__file__).resolve().parents[1]
SHAPESNAP_CONFIG = REPO_ROOT / "config" / "shapesnap.json"


@dataclass(frozen=True)
class WaygraphConfig:
    """Every knob of the way-graph corridor builder (one dataclass, spec)."""

    # ── usage reconstruction ──
    snap_tol_m: float = 2.5        # shape->way identity tolerance (see module doc)
    sample_step_m: float = 3.0     # shape densification step
    edge_member_frac: float = 0.5  # covered fraction that claims an edge...
    edge_member_min_m: float = 25.0  # ...or this much covered length
    offgraph_min_m: float = 15.0   # unsnapped span that becomes explicit geometry
    terminal_trim_pad_m: float = 5.0  # keep this much past covered track at termini
    # connectivity repair: consecutive ridden edges that share no node are
    # reconnected via the shortest way-graph path that hugs the shape —
    # switch ladders contain near-coincident micro edges (spur twins) and
    # nearest-snap alone can pick the dead twin (CTA Pink at the Paulina
    # Connector: two 1.1 m fragments 0 m apart)
    repair_max_detour_m: float = 60.0
    repair_shape_tol_m: float = 15.0
    dangle_max_m: float = 8.0      # member micro-dangles this short are snap noise
    # ── corridor merges ──
    pair_gap_m: float = 15.0       # merge 1: directional pair (identical route sets)
    family_gap_m: float = 25.0     # merge 2: same colour family, different sets
    family_sustained_min_m: float = 450.0  # a family co-run this long merges
    #                                even when both corridors continue past
    #                                the window (local/express diverging at
    #                                a real fork — the 7th Av 1 vs 2/3);
    #                                kisses/crossings stay far below this
    cross_family_gap_m: float = 22.0   # merge 3: cross-family proximity
    #                                bundle. RAISED 10 -> 22 (round 19): a
    #                                KISS is a transient V-shaped convergence,
    #                                a BUNDLE is a STABLE parallel co-run —
    #                                the two are told apart by PROFILE (the
    #                                gates below), not by a low gap minimum.
    #                                At 10 m genuine parallels a bit farther
    #                                apart under-bundled into two messy ropes
    #                                (DeKalb: the orange B/D beside yellow
    #                                N/Q/R/W down the Manhattan Bridge
    #                                approach measure a stable 9-17 m gap,
    #                                83-100% of the run within 22 m, 0.9 deg
    #                                relative bearing, no mid-span crossing —
    #                                a textbook bundle that 10 m rejected).
    #                                22 comfortably includes such parallels;
    #                                the profile gates keep kisses out.
    cross_family_min_len_m: float = 450.0  # ...sustained at least this long
    cross_family_max_bearing_deg: float = 20.0    # parallel, not crossing
    # ── anti-kiss profile gates (round 19) — so RAISING the cross gap does
    #    not re-admit kisses. A kiss FAILS at least one: it crosses, its
    #    below-threshold span is a narrow valley, or its gap spikes.
    cross_family_min_frac_below: float = 0.60  # the co-run must be
    #                                SUSTAINED over its NEIGHBOURHOOD: at
    #                                least this fraction of a context window
    #                                (the merge window grown by
    #                                cross_family_min_len_m/2 each side) sits
    #                                below the gap threshold. The merge window
    #                                itself is all-below by construction, so
    #                                the context is what discriminates — a
    #                                kiss is a below-threshold VALLEY inside
    #                                an above-threshold neighbourhood (Rector,
    #                                Whitehall dip under only near closest
    #                                approach); a bundle stays under across
    #                                the whole context (DeKalb, Chicago's
    #                                Lake leg -> ~1.0).
    cross_family_max_gap_ratio: float = 6.0  # loose safety valve on the
    #                                below-threshold gap_max/gap_mean: genuine
    #                                bundles breathe a LOT (Chicago P+Red 4.1,
    #                                the Lake leg 3.2 — the gap dips to 0 at
    #                                shared switches and rises to ~14 m
    #                                between), so this only catches a
    #                                pathological spike, never a real co-run.
    cross_family_cross_slack_m: float = 40.0  # NON-CROSSING gate: the two
    #                                corridor geometries must not INTERSECT
    #                                in the window INTERIOR (endpoint
    #                                convergence at a shared switch is fine —
    #                                intersections within this slack of a
    #                                window end are ignored). A mid-span
    #                                crossing (Rector 1 x R/W, Whitehall
    #                                crossing tubes) is a kiss and never
    #                                merges, however low its gap dips.
    merge_min_len_m: float = 40.0  # merges 1-2: minimum sustained window
    merge_end_slack_m: float = 40.0  # window may stop this short of a corridor end
    merge_max_bearing_deg: float = 35.0  # merges 1-2 sanity (switch ladders bend)
    absorb_max_len_m: float = 120.0  # ridden crossover absorbed into its bundle
    tail_collapse_m: float = 8.0   # merge tails shorter than this join the
    #                                node (small: a longitudinal collapse
    #                                slides the junction along the track,
    #                                and forks must stay within ~5 m of the
    #                                real switch nodes)
    midline_step_m: float = 5.0    # merged midline resample step
    # window flap guard (Schmitt trigger): real 4-track spacing breathes
    # across the gap threshold (the Sea Beach pair tiled into 5 windows);
    # qualifying windows separated by a short dip coalesce, and a formed
    # bundle only releases where the gap CLEARLY lets go — beyond
    # release_gap_mult x gap sustained for release_sustain_m
    window_dip_coalesce_m: float = 200.0  # dips shorter than this never split
    release_gap_mult: float = 1.5
    release_sustain_m: float = 150.0
    # C1 merge-boundary easing: where a merged midline hands off to a
    # continuing constituent track the centerline used to step sideways
    # by half the pair gap; the midline now eases into the through track
    # over this length (clamped to the window) with a smoothstep — zero
    # step, tangent-continuous. Applies to the midline and to cut tails
    # of corridors CONTINUING past the boundary; diverging switch legs
    # keep the short blend below (dragging a real diverging track
    # 100 m sideways parallels it with the bundle — the Culver portal
    # lesson from the junction exam).
    ease_len_m: float = 100.0
    blend_m: float = 25.0          # endpoint retarget / diverging-leg blend
    cov_cut_margin_m: float = 30.0  # partial mid-edge coverage: a route
    #                                entering/leaving a long way edge
    #                                partway (the FX express rejoining the
    #                                Culver local mid-way) cuts the edge at
    #                                its coverage boundary instead of
    #                                claiming track it never rides; cuts
    #                                closer than this to an edge end are
    #                                snap noise
    ladder_contract_m: float = 16.0  # junction-to-junction micro corridors
    #                                (switch-ladder fragments) contract to a
    #                                single node — under half a merge gap,
    #                                so forks stay within ~5 m of the switch
    # ── downstream contract ──
    station_sliver_m: float = 18.0  # lg.merge_width_m: stations.py node sliver


# participates in the corridor cache digest — bump on builder semantics
# changes (2: series-safe absorption + collapse-safe retargets; 3: cross
# co-extensive-twin rule + ladder contraction; 4: gap-scaled coverage-
# biased connectivity repair + phantom-component pruning; 12: window
# coalescing/hysteresis + boundary snap + C1 seam easing + partial-
# coverage edge cuts; 13: cross-family gap raised 10->22 + anti-kiss
# profile gates — non-crossing, gap-stability frac/ratio)
CONFIG_FORMAT_VERSION = 13


def config_digest_token(cfg: WaygraphConfig) -> str:
    vals = ";".join(f"{k}={getattr(cfg, k)}" for k in sorted(cfg.__dataclass_fields__))
    return f"waygraph-v{CONFIG_FORMAT_VERSION};{vals}"


def waygraph_digest(shapes, cfg: WaygraphConfig, graph) -> str:
    """Cache digest: input shapes + every config knob + the graph cache era."""
    import hashlib

    h = hashlib.md5()
    h.update(config_digest_token(cfg).encode())
    h.update(graph_signature(graph).encode())
    for coords in shapes:
        for lon, lat in coords:
            h.update(f"{lon:.6f},{lat:.6f};".encode())
        h.update(b"|")
    return h.hexdigest()


# ── way graph loading (per feed, via config/shapesnap.json) ─────────────────


def load_way_graph(feed_id: str, mode: str) -> ModeGraph:
    """The SAME cached graph shapesnap matched against — never rebuilt here.

    A rebuild could silently shift edge ids/geometry out from under the
    matched shapes; verify_levels() is the cross-check that the loaded
    cache still matches the matched_shapes era.
    """
    cfg = json.loads(SHAPESNAP_CONFIG.read_text())
    fc = (cfg.get("feeds") or {}).get(str(feed_id)) or {}
    pbf = Path(fc.get("pbf") or "data/region.osm.pbf")
    if not pbf.is_absolute():
        pbf = REPO_ROOT / pbf
    stem = fc.get("graphStem")
    if stem is None:
        for suffix in (".osm.pbf", ".osm", ".pbf"):
            if pbf.name.endswith(suffix):
                stem = pbf.name[: -len(suffix)]
                break
    cache = pbf.parent / "shapesnap" / f"{stem}.{mode}.graph.pkl.gz"
    if not cache.exists():
        raise FileNotFoundError(
            f"way graph cache {cache} missing — run shapesnap.graph for "
            f"feed {feed_id} first (the corridor builder must consume the "
            "exact graph the shapes were matched on)"
        )
    return load_graph(cache)


def graph_signature(graph: ModeGraph) -> str:
    return (f"{Path(graph.source_path).name};{graph.source_size};"
            f"{graph.source_mtime};{len(graph.edges)}")


# ── projected index over way edges ───────────────────────────────────────────


class WayIndex:
    """Projected way-edge geometries + STRtree + node positions."""

    def __init__(self, graph: ModeGraph, epsg: int):
        self.graph = graph
        self.epsg = epsg
        to_xy = Transformer.from_crs(4326, epsg, always_xy=True)
        self.lines = []
        for e in graph.edges:
            xs, ys = to_xy.transform([c[0] for c in e.geometry],
                                     [c[1] for c in e.geometry])
            self.lines.append(LineString(list(zip(xs, ys))))
        self.tree = STRtree(self.lines)
        self.node_xy = {}
        for nid, (lon, lat) in graph.nodes.items():
            x, y = to_xy.transform(lon, lat)
            self.node_xy[nid] = (x, y)
        self._adj = None

    @property
    def adj(self) -> dict:
        """node id -> [edge indices] (built lazily; repair pass only)."""
        if self._adj is None:
            self._adj = self.graph.adjacency()
        return self._adj


def densify_xy(coords_xy, step_m: float) -> np.ndarray:
    pts = np.asarray(coords_xy, dtype=float)
    if len(pts) < 2:
        return pts
    seg = np.hypot(*(pts[1:] - pts[:-1]).T)
    cum = np.concatenate([[0.0], np.cumsum(seg)])
    total = float(cum[-1])
    if total <= 0.0:
        return pts[:1]
    n = max(1, math.ceil(total / step_m))
    t = np.linspace(0.0, total, n + 1)
    return np.column_stack([np.interp(t, cum, pts[:, 0]),
                            np.interp(t, cum, pts[:, 1])])


# ── per-pattern reconstruction ───────────────────────────────────────────────


@dataclass(slots=True)
class OffRun:
    """A contiguous stretch of matched geometry on NO way edge."""

    coords_xy: list          # verbatim sub-polyline (projected)
    start_anchor: tuple | None  # (edge_id, along_m) on-graph attachment
    end_anchor: tuple | None


@dataclass(slots=True)
class PatternCover:
    pattern_key: str
    route_id: str
    edges: dict              # edge_id -> (cov_lo_m, cov_hi_m) coverage bounds
    member_edges: set        # edges the pattern RIDES (threshold applied)
    runs: list               # [OffRun]
    n_samples: int = 0
    n_unsnapped: int = 0
    edge_ivals: dict = field(default_factory=dict)
    # edge_id -> ((lo, hi), ...) the FULL coverage interval union — the
    # (lo, hi) bounds above lie when a pattern only TOUCHES an edge at
    # both ends (the FX express grazing the Culver local at the two
    # bypass portals spans the bounds but rides ~6% of the edge)


def _interval_union(ivals):
    ivals = sorted(ivals)
    out = []
    for lo, hi in ivals:
        if out and lo <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def reconstruct_pattern(index: WayIndex, key: str, route_id: str, shape_xy,
                        cfg: WaygraphConfig) -> PatternCover:
    """Snap one matched shape back onto the way edges it was built from."""
    samples = densify_xy(shape_xy, cfg.sample_step_m)
    pts = shapely.points(samples[:, 0], samples[:, 1])
    nearest = np.full(len(pts), -1, dtype=np.int64)
    res = index.tree.query_nearest(pts, max_distance=cfg.snap_tol_m,
                                   all_matches=False)
    nearest[res[0]] = res[1]

    # per-edge coverage intervals (± half a sample step around each hit)
    cover: dict = {}
    half = cfg.sample_step_m / 2.0
    for i, eid in enumerate(nearest):
        if eid < 0:
            continue
        line = index.lines[eid]
        t = line.project(pts[i])
        cover.setdefault(int(eid), []).append(
            (max(0.0, t - half), min(line.length, t + half)))
    edges = {}
    edge_ivals = {}
    for eid, ivals in cover.items():
        u = _interval_union(ivals)
        edges[eid] = (u[0][0], u[-1][1], sum(hi - lo for lo, hi in u))
        edge_ivals[eid] = tuple(u)

    # off-graph runs: contiguous unsnapped sample spans
    runs = []
    cum = np.concatenate([[0.0], np.cumsum(
        np.hypot(*(samples[1:] - samples[:-1]).T))])
    i = 0
    n = len(samples)
    while i < n:
        if nearest[i] >= 0:
            i += 1
            continue
        j = i
        while j < n and nearest[j] < 0:
            j += 1
        span = cum[min(j, n - 1)] - cum[max(i - 1, 0)]
        if span >= cfg.offgraph_min_m:
            lo, hi = max(i - 1, 0), min(j, n - 1)
            coords = [tuple(p) for p in samples[lo:hi + 1]]
            def _anchor(k):
                eid = int(nearest[k])
                return (eid, float(index.lines[eid].project(pts[k])))
            runs.append(OffRun(
                coords_xy=coords,
                start_anchor=_anchor(lo) if nearest[lo] >= 0 else None,
                end_anchor=_anchor(hi) if nearest[hi] >= 0 else None,
            ))
        i = j

    # membership: a mid-route edge is ridden end-to-end, so it must be
    # covered for >= edge_member_frac of its length (a long foreign edge
    # sharing only its first 25 m of alignment must NOT ride — the G
    # once claimed 3.4 km of Culver express that way). Only edges a
    # pattern legitimately enters PARTWAY get the lenient absolute
    # floor: the first/last edges (pattern termini) and off-run anchor
    # edges (the bridge leaves mid-edge; the anchor splits them later).
    snapped = nearest[nearest >= 0]
    lenient = set()
    if len(snapped):
        lenient.update((int(snapped[0]), int(snapped[-1])))
    for run in runs:
        for anchor in (run.start_anchor, run.end_anchor):
            if anchor is not None:
                lenient.add(anchor[0])
    member = set()
    for eid, (lo, hi, cov_len) in edges.items():
        elen = index.lines[eid].length
        if cov_len >= cfg.edge_member_frac * elen:
            member.add(eid)
        elif eid in lenient and cov_len >= min(cfg.edge_member_min_m,
                                               0.9 * elen):
            member.add(eid)

    shape_line = LineString(shape_xy)
    shapely.prepare(shape_line)
    _repair_connectivity(index, member, set(edges), nearest, samples,
                         shape_line, cfg)
    _prune_dangles(index, member, nearest, cfg)
    return PatternCover(
        pattern_key=key, route_id=route_id,
        edges={eid: (lo, hi) for eid, (lo, hi, _c) in edges.items()},
        member_edges=member, runs=runs,
        n_samples=n, n_unsnapped=int((nearest < 0).sum()),
        edge_ivals=edge_ivals,
    )


def _repair_connectivity(index: WayIndex, member: set, covered: set, nearest,
                         samples, shape_line, cfg: WaygraphConfig):
    """Reconnect consecutive ridden edges that share no node.

    Dijkstra over way edges restricted to edges hugging the shape
    (repair_shape_tol_m). Two gap classes, one machinery:
      * near-coincident switch fragments the nearest-snap coin-flipped
        away from (a couple of meters);
      * PLAN-COINCIDENT stacked structure (the CTA State St subway ramp
        directly under the North Side elevated): sample flip-flop between
        the vertically stacked twins starves BOTH below the membership
        threshold, holing the chain for hundreds of meters — hence the
        gap-scaled detour cap and the cost bias toward edges the shape's
        own samples covered.
    """
    import heapq

    seq = []  # (edge_id, first sample idx, last sample idx) per run
    for i, eid in enumerate(nearest):
        if eid < 0 or int(eid) not in member:
            continue
        if seq and seq[-1][0] == int(eid):
            seq[-1][2] = i
        else:
            seq.append([int(eid), i, i])
    seen_pairs = set()
    for (e1, _f1, l1), (e2, f2, _l2) in zip(seq, seq[1:]):
        a = index.graph.edges[e1]
        b = index.graph.edges[e2]
        n1 = {a.from_node, a.to_node}
        n2 = {b.from_node, b.to_node}
        if n1 & n2 or (e1, e2) in seen_pairs:
            continue
        seen_pairs.add((e1, e2))
        # the gap's LOCAL stretch of the shape: candidate edges must hug
        # it everywhere — a whole-edge min-distance test would admit a
        # kilometers-long foreign edge that merely grazes the shape at
        # one junction (the G claiming Culver express track 310 m out)
        lo = max(0, l1 - 12)
        hi = min(len(samples) - 1, f2 + 12)
        local = LineString(samples[lo:hi + 1]) if hi - lo >= 1 else shape_line
        shapely.prepare(local)
        hug_cache: dict = {}

        def hugs(ei) -> bool:
            v = hug_cache.get(ei)
            if v is None:
                pts = shapely.points(densify_xy(
                    np.asarray(index.lines[ei].coords), 25.0))
                v = float(shapely.distance(pts, local).max()) \
                    <= cfg.repair_shape_tol_m
                hug_cache[ei] = v
            return v

        gap_m = min(
            math.hypot(index.node_xy[x][0] - index.node_xy[y][0],
                       index.node_xy[x][1] - index.node_xy[y][1])
            for x in n1 for y in n2)
        cap = max(cfg.repair_max_detour_m, 3.0 * gap_m + 50.0)
        dist = {n: 0.0 for n in n1}
        true_len = {n: 0.0 for n in n1}
        heap = [(0.0, n) for n in sorted(n1)]
        prev: dict = {}
        heapq.heapify(heap)
        found = None
        while heap:
            d, n = heapq.heappop(heap)
            if d > dist.get(n, float("inf")):
                continue
            if n in n2:
                found = n
                break
            for ei in index.adj.get(n, ()):
                e = index.graph.edges[ei]
                # covered edges (any sample coverage) are the shape's own
                # track; an uncovered parallel twin pays 4x
                w = e.length_m * (1.0 if ei in covered or ei in member
                                  else 4.0)
                if true_len[n] + e.length_m > cap:
                    continue
                if not hugs(ei):
                    continue
                m = e.to_node if e.from_node == n else e.from_node
                nd = d + w
                if nd < dist.get(m, float("inf")):
                    dist[m] = nd
                    true_len[m] = true_len[n] + e.length_m
                    prev[m] = (n, ei)
                    heapq.heappush(heap, (nd, m))
        if found is not None:
            n = found
            while n in prev:
                n, ei = prev[n]
                member.add(ei)


def _prune_dangles(index: WayIndex, member: set, nearest, cfg: WaygraphConfig):
    """Drop member micro-dangles (snap noise onto a coincident spur twin)."""
    snapped = nearest[nearest >= 0]
    protected = {int(snapped[0]), int(snapped[-1])} if len(snapped) else set()
    changed = True
    while changed:
        changed = False
        cnt: dict = {}
        for eid in member:
            e = index.graph.edges[eid]
            for n in (e.from_node, e.to_node):
                cnt[n] = cnt.get(n, 0) + 1
        for eid in sorted(member):
            if eid in protected:
                continue
            e = index.graph.edges[eid]
            if e.length_m >= cfg.dangle_max_m:
                continue
            if cnt[e.from_node] == 1 or cnt[e.to_node] == 1:
                member.discard(eid)
                changed = True
                break
    _prune_phantom_components(index, member, protected)


def _prune_phantom_components(index: WayIndex, member: set, protected: set):
    """Drop tiny disconnected member components — plan-coincident twins.

    A vertically stacked parallel way (the CTA North Side elevated over
    the State St subway ramp) collects flip-flopped samples and enters
    the member set as an ISLAND: connected to nothing, ridden by
    nothing. The true path is the dominant component; islands under 10%
    of its length (and not carrying the pattern's own terminals) are
    snap noise, never track the pattern rides.
    """
    parent: dict = {}

    def find(x):
        root = x
        while parent.setdefault(root, root) != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    for eid in member:
        e = index.graph.edges[eid]
        ra, rb = find(e.from_node), find(e.to_node)
        if ra != rb:
            parent[rb] = ra
    comp_len: dict = {}
    comp_edges: dict = {}
    for eid in member:
        root = find(index.graph.edges[eid].from_node)
        comp_len[root] = comp_len.get(root, 0.0) + index.graph.edges[eid].length_m
        comp_edges.setdefault(root, set()).add(eid)
    if len(comp_len) <= 1:
        return
    main_len = max(comp_len.values())
    for root, edges in comp_edges.items():
        if comp_len[root] < 0.10 * main_len and not (edges & protected):
            member -= edges


# ── feed-level usage aggregation ─────────────────────────────────────────────


@dataclass(slots=True)
class Usage:
    epsg: int
    edge_routes: dict        # edge_id -> frozenset(route_id)
    edge_cover: dict         # edge_id -> (cov_lo_m, cov_hi_m) union over riders
    off_runs: list           # [(frozenset(route_id), OffRun)] deduped
    covers: list             # [PatternCover] in pattern order
    route_color: dict        # route_id -> color_key (display SQL semantics)
    n_offgraph_m: float = 0.0
    edge_route_cov: dict = field(default_factory=dict)
    # edge_id -> {route_id: [(lo_m, hi_m), ...] | None}: the route's
    # coverage interval union on the edge (None = full-edge claim, e.g.
    # repair-added). A route whose coverage ends mid-edge entered/left
    # partway — the corridor builder cuts the edge there instead of
    # painting the route over track it never rides (cov_cut_margin_m).


def color_key_of(route_color: str, route_id: str) -> str:
    return (route_color or "").strip() or f"rid:{route_id}"


def build_usage(index: WayIndex, patterns, cfg: WaygraphConfig,
                verbose: bool = True) -> Usage:
    """Reconstruct every pattern; aggregate per-edge route sets (directions
    merged) and dedupe off-graph runs by rounded geometry."""
    to_xy = Transformer.from_crs(4326, index.epsg, always_xy=True)
    edge_routes: dict = {}
    edge_cover: dict = {}
    edge_route_cov: dict = {}
    covers: list = []
    route_color: dict = {}
    runs_by_hash: dict = {}
    off_m = 0.0

    for p in patterns:
        if not p.shape or len(p.shape) < 2:
            covers.append(PatternCover(p.key, p.route_id, {}, set(), []))
            continue
        route_color.setdefault(p.route_id,
                               color_key_of(p.route_color, p.route_id))
        xs, ys = to_xy.transform([c[0] for c in p.shape],
                                 [c[1] for c in p.shape])
        pc = reconstruct_pattern(index, p.key, p.route_id,
                                 list(zip(xs, ys)), cfg)
        covers.append(pc)
        for eid in pc.member_edges:
            edge_routes.setdefault(eid, set()).add(p.route_id)
            # repair-added members carry no sample coverage: whole edge
            lo, hi = pc.edges.get(eid, (0.0, index.lines[eid].length))
            cur = edge_cover.get(eid)
            edge_cover[eid] = (min(lo, cur[0]), max(hi, cur[1])) if cur \
                else (lo, hi)
            # per-route coverage interval union (None = full-edge claim)
            rc = edge_route_cov.setdefault(eid, {})
            if eid not in pc.edge_ivals:
                rc[p.route_id] = None
            elif p.route_id in rc:
                old = rc[p.route_id]
                if old is not None:
                    rc[p.route_id] = _interval_union(
                        list(old) + list(pc.edge_ivals[eid]))
            else:
                rc[p.route_id] = list(pc.edge_ivals[eid])
        for run in pc.runs:
            h = tuple(np.round(np.asarray(run.coords_xy)[[0, -1]].ravel(), 0)) \
                + (round(LineString(run.coords_xy).length, 0),)
            entry = runs_by_hash.get(h)
            if entry is None:
                runs_by_hash[h] = [set([p.route_id]), run]
                off_m += LineString(run.coords_xy).length
            else:
                entry[0].add(p.route_id)

    usage = Usage(
        epsg=index.epsg,
        edge_routes={eid: frozenset(r) for eid, r in edge_routes.items()},
        edge_cover=edge_cover,
        off_runs=[(frozenset(rs), run) for rs, run in runs_by_hash.values()],
        covers=covers,
        route_color=route_color,
        n_offgraph_m=off_m,
        edge_route_cov=edge_route_cov,
    )
    if verbose:
        n_un = sum(c.n_unsnapped for c in covers)
        n_s = sum(c.n_samples for c in covers)
        print(f"[waygraph] usage: {len(usage.edge_routes)} way edges ridden, "
              f"{len(usage.off_runs)} off-graph runs ({off_m:.0f} m), "
              f"{n_un}/{n_s} samples off-graph "
              f"({100.0 * n_un / max(n_s, 1):.2f}%)", flush=True)
    return usage


# ── verification against matched_shapes.stats.levels_m ──────────────────────


def _level_of(tags) -> str:
    raw = (tags.get("layer") or "").strip()
    try:
        layer = int(raw) if raw else 0
    except ValueError:
        layer = 0
    if (tags.get("tunnel") not in (None, "no")) or layer < 0:
        return "subway_m"
    if (tags.get("bridge") not in (None, "no")) or layer >= 1:
        return "elevated_m"
    return "surface_m"


def verify_levels(graph: ModeGraph, covers, dsn: str, feed_id: str,
                  rel_tol: float = 0.03, abs_tol_m: float = 120.0,
                  verbose: bool = True):
    """Cross-check the reconstruction against shapesnap's own edge-set digest.

    matched_shapes.stats.levels_m was computed from the ORIGINAL
    edges_used over this exact graph cache; if the reconstruction's
    per-class meters agree per pattern, the id space and the edge sets
    match. Terminal edges shapesnap counted in FULL but the shape only
    grazes are the expected residual — hence the tolerances.

    Returns (n_checked, mismatches) where mismatches lists
    (pattern_id, class, ours_m, theirs_m).
    """
    import psycopg

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT pattern_id, stats->'levels_m' FROM matched_shapes
               WHERE feed_id = %s AND stats ? 'levels_m'""", (feed_id,))
        theirs = {pid: lv for pid, lv in cur.fetchall()}

    mismatches = []
    n_checked = 0
    for pc in covers:
        lv = theirs.get(pc.pattern_key)
        if lv is None or not pc.member_edges:
            continue
        n_checked += 1
        ours = {"elevated_m": 0.0, "subway_m": 0.0, "surface_m": 0.0}
        for eid in pc.member_edges:
            e = graph.edges[eid]
            ours[_level_of(e.tags)] += e.length_m
        for k in ours:
            a, b = ours[k], float(lv.get(k) or 0.0)
            if abs(a - b) > max(abs_tol_m, rel_tol * max(a, b)):
                mismatches.append((pc.pattern_key, k, round(a, 1), round(b, 1)))
    if verbose:
        print(f"[waygraph] levels_m verification: {n_checked} patterns, "
              f"{len(mismatches)} class mismatches beyond tolerance", flush=True)
        for pid, k, a, b in mismatches[:12]:
            print(f"  levels_m mismatch {pid} {k}: ours {a} vs stats {b}")
    return n_checked, mismatches
