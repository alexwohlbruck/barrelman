#!/usr/bin/env python3
"""shapesnap.match — two-regime GTFS→OSM path matching (pipeline v3 stage 3).

Unit of work: route × direction × stop-pattern. One Viterbi core, two
regimes:

  Regime A (dense; the feed has a usable shape)
    observations = the shape resampled to ~cfg.resample_m spacing
    emission     = Gaussian(snap distance, sigma_dense)
    transition   = exp(-|along-shape − network|/beta_dense); the network
                   path comes from a bounded Dijkstra whose EDGE WEIGHTS
                   carry class_penalty × route-relation bonus (×0.5 on
                   edges whose OSM route relations match the GTFS route),
                   turn-restriction forbidden pairs and a 180°-reversal
                   penalty (free at genuine stubs) — but the |along −
                   network| comparison uses the path's TRUE geometric
                   length, so penalties steer path CHOICE without
                   distorting the probability model.

  Regime B (sparse; no/degenerate shape)
    observations = the pattern's stop coordinates in sequence; transition
    penalizes only the EXCESS of network over straight-line distance
    (beta_sparse) plus a stations-passed-without-stopping penalty;
    emissions gain station-name (token-subset / cheap-TED) and platform
    ref bonuses when an OSM station index is supplied.

The Viterbi is decoded as a Dijkstra over trellis states (Meili-style
lazy expansion): state = (observation index, candidate directed-edge
position); transitions are only ever computed for states that get popped.
All component costs are ≥ 0 so Dijkstra optimality holds.

Break, don't force: an observation with no candidates, or a layer no
feasible transition reaches, ends the current sub-trace; matching
restarts at the next matchable observation and the gap is bridged with
the ORIGINAL shape segment clipped between the matched endpoints (stop
chord for regime B) and recorded in stats. A wrong-level match is never
forced.

Output per pattern: ordered edge path → concatenated full-fidelity OSM
geometry → topology-preserving ~1 m simplification in a local UTM →
quality gates (shapesnap.gates). ANY gate failure returns the original
geometry unchanged (method="fallback"). Methods: hmm_dense | hmm_sparse
| fallback | passthrough (passthrough = nothing matchable at all).
Confidence = exp(−mean trellis cost per matched observation) / (1 +
breaks) ∈ (0, 1].

Patterns are read from a GTFS zip (load_patterns). For feed 29 the DB
tables gtfs_trip_patterns / gtfs_shapes are empty on this host, so the
zip is the documented source of truth here (it is also the artifact the
import pipeline rewrites).

Per-feed output dedup (spec item 5): the CLI driver hashes every result's
coordinates (geometry_hash) and flags repeats as dup_of=<first pattern>;
the pipeline write stage keeps one geometry per hash.

CLI (repo convention — uv, never system python):
  uv run --with-requirements shapesnap/requirements.txt \
      python -m shapesnap.match --graph data/shapesnap/il-chicago.rail.graph.pkl.gz \
      --feed data/gtfs/29.zip --routes Brn,Blue
  # --pbf data/il.osm.pbf overrides the station-index source (regime B
  # bonuses); by default the graph's recorded source pbf is used.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import math
import sys
import time
import zipfile
from dataclasses import dataclass, field
from heapq import heappop, heappush
from itertools import count
from pathlib import Path

from shapely.geometry import LineString, Point
from shapely.ops import substring

from shapesnap.candidates import (
    Candidate,
    MatchGraph,
    RouteMatcher,
    StationIndex,
    load_stations,
)
from shapesnap.gates import GateConfig, GateReport, evaluate_gates
from shapesnap.graph import load_graph

__all__ = [
    "MatchConfig",
    "MatchResult",
    "Pattern",
    "geometry_hash",
    "load_patterns",
    "match_pattern",
    "route_type_to_mode",
]

_INF = float("inf")


# ── configuration ────────────────────────────────────────────────────────────


@dataclass
class MatchConfig:
    # candidate search (meters)
    dense_radius: dict = field(default_factory=lambda: {"rail": 50.0, "bus": 35.0, "ferry": 100.0})
    sparse_radius: dict = field(default_factory=lambda: {"rail": 200.0, "bus": 100.0, "ferry": 500.0})
    k_candidates: int = 10
    # regime A resampling
    resample_m: float = 30.0
    max_obs: int = 2000
    min_step_m: float = 5.0
    # emission / transition
    sigma_dense: float = 15.0
    sigma_sparse: float = 30.0
    beta_dense: float = 30.0
    beta_sparse: float = 300.0
    # bounded network search
    cutoff_factor: float = 3.0
    cutoff_slack_dense_m: float = 150.0
    cutoff_slack_sparse_m: float = 1500.0
    # per-edge weight shaping
    route_bonus_mult: float = 0.5
    uturn_penalty_m: float = 200.0
    uturn_cos: float = -0.866  # reversal = turn sharper than ~150°
    # vertical disambiguation: where elevated and subway tracks stack
    # (Lake St: the Lake 'L' runs directly ABOVE the Milwaukee-Dearborn
    # subway) horizontal emissions tie, so candidates whose OSM route
    # relations do NOT match the GTFS route pay this emission prior.
    # Only active when the route has relation coverage among the
    # pattern's candidates; layers without any match add a constant,
    # which cannot change the decoded chain.
    relation_prior: float = 0.35
    # regime B extras
    station_search_radius_m: float = 120.0
    name_bonus_weight: float = 1.5
    station_pass_radius_m: float = 25.0
    station_pass_penalty: float = 1.0
    # degenerate-shape detection (regime A -> B demotion)
    degenerate_min_points: int = 4
    degenerate_min_len_ratio: float = 0.5  # shape length vs stop-chain chord
    # output
    simplify_m: float = 1.0
    gates: GateConfig = field(default_factory=GateConfig)


@dataclass
class MatchResult:
    method: str            # hmm_dense | hmm_sparse | fallback | passthrough
    confidence: float
    coords: list           # [(lon, lat), ...]
    stats: dict
    gates: GateReport | None
    edges_used: list       # edge indices into the graph (matched sub-traces)


# ── GTFS pattern extraction ──────────────────────────────────────────────────


@dataclass
class Pattern:
    route_id: str
    direction_id: int | None
    stop_ids: tuple
    stop_coords: list      # [(lon, lat), ...]
    stop_names: list
    trip_count: int
    shape_id: str | None
    shape: list | None     # representative shape [(lon, lat), ...], densest trips
    route_short_name: str
    route_long_name: str
    route_color: str
    route_type: int
    stop_platforms: list = field(default_factory=list)  # GTFS platform_code, "" when absent
    trip_ids: tuple = ()   # every trip that runs this pattern (trips.txt remap)

    @property
    def key(self) -> str:
        d = "x" if self.direction_id is None else self.direction_id
        return f"{self.route_id}/{d}/{hashlib.md5('|'.join(self.stop_ids).encode()).hexdigest()[:8]}"


def route_type_to_mode(rt: int) -> str | None:
    """GTFS route_type (basic + extended) -> shapesnap mode class."""
    if rt in (0, 1, 2, 5, 7, 12):
        return "rail"
    if rt in (3, 11):
        return "bus"
    if rt == 4:
        return "ferry"
    if 100 <= rt < 200 or 400 <= rt < 500 or 900 <= rt < 1000 or rt == 1400:
        return "rail"
    if 200 <= rt < 300 or 700 <= rt < 900:
        return "bus"
    if rt == 1000 or rt == 1200:
        return "ferry"
    return None


def _csv_rows(zf: zipfile.ZipFile, name: str):
    with zf.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))


def _equirect_len_m(coords) -> float:
    if len(coords) < 2:
        return 0.0
    lat0 = math.radians(coords[0][1])
    kx, ky = 111320.0 * math.cos(lat0), 110574.0
    return sum(
        math.hypot((b[0] - a[0]) * kx, (b[1] - a[1]) * ky)
        for a, b in zip(coords, coords[1:])
    )


def load_patterns(zip_path, route_ids=None, modes=None) -> list:
    """Pull route × direction × stop-pattern units from a GTFS zip.

    Representative shape per pattern = the LONGEST shape (geometric
    length) among the pattern's trips. Returns Patterns sorted by
    descending trip_count.
    """
    zip_path = Path(zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())

        routes: dict = {}
        for r in _csv_rows(zf, "routes.txt"):
            rid = r["route_id"]
            if route_ids is not None and rid not in route_ids:
                continue
            rt = int(r.get("route_type") or 3)
            if modes is not None and route_type_to_mode(rt) not in modes:
                continue
            routes[rid] = {
                "short": (r.get("route_short_name") or "").strip(),
                "long": (r.get("route_long_name") or "").strip(),
                "color": (r.get("route_color") or "").strip(),
                "type": rt,
            }

        trips: dict = {}
        for t in _csv_rows(zf, "trips.txt"):
            rid = t["route_id"]
            if rid not in routes:
                continue
            d = t.get("direction_id")
            trips[t["trip_id"]] = (
                rid,
                int(d) if d not in (None, "") else None,
                (t.get("shape_id") or "").strip() or None,
            )

        stop_seqs: dict = {tid: [] for tid in trips}
        for st in _csv_rows(zf, "stop_times.txt"):
            seq = stop_seqs.get(st["trip_id"])
            if seq is not None:
                seq.append((int(st["stop_sequence"]), st["stop_id"]))

        needed_stops = {sid for seq in stop_seqs.values() for _, sid in seq}
        stops: dict = {}
        for s in _csv_rows(zf, "stops.txt"):
            if s["stop_id"] in needed_stops:
                stops[s["stop_id"]] = (
                    float(s["stop_lon"]),
                    float(s["stop_lat"]),
                    (s.get("stop_name") or "").strip(),
                    (s.get("platform_code") or "").strip(),
                )

        needed_shapes = {sh for (_, _, sh) in trips.values() if sh}
        shapes: dict = {sh: [] for sh in needed_shapes}
        if "shapes.txt" in names and needed_shapes:
            for p in _csv_rows(zf, "shapes.txt"):
                pts = shapes.get(p["shape_id"])
                if pts is not None:
                    pts.append(
                        (int(p["shape_pt_sequence"]),
                         float(p["shape_pt_lon"]), float(p["shape_pt_lat"]))
                    )
        shape_coords = {
            sh: [(lon, lat) for _, lon, lat in sorted(pts)]
            for sh, pts in shapes.items() if pts
        }
        shape_len = {sh: _equirect_len_m(c) for sh, c in shape_coords.items()}

    groups: dict = {}
    for tid, (rid, direction, sh) in trips.items():
        seq = stop_seqs.get(tid) or []
        if len(seq) < 2:
            continue
        sids = tuple(sid for _, sid in sorted(seq))
        groups.setdefault((rid, direction, sids), []).append((tid, sh))

    patterns = []
    for (rid, direction, sids), members in groups.items():
        if any(sid not in stops for sid in sids):
            continue
        meta = routes[rid]
        best_shape = max(
            (sh for _tid, sh in members if sh in shape_coords),
            key=lambda sh: shape_len[sh],
            default=None,
        )
        patterns.append(
            Pattern(
                route_id=rid,
                direction_id=direction,
                stop_ids=sids,
                stop_coords=[stops[s][:2] for s in sids],
                stop_names=[stops[s][2] for s in sids],
                stop_platforms=[stops[s][3] for s in sids],
                trip_count=len(members),
                trip_ids=tuple(tid for tid, _sh in members),
                shape_id=best_shape,
                shape=shape_coords.get(best_shape),
                route_short_name=meta["short"],
                route_long_name=meta["long"],
                route_color=meta["color"],
                route_type=meta["type"],
            )
        )
    patterns.sort(key=lambda p: (-p.trip_count, p.route_id, str(p.direction_id)))
    return patterns


# ── observation preparation ──────────────────────────────────────────────────


def _dedup_close(coords, min_step: float) -> list:
    out = []
    for xy in coords:
        if not out or math.hypot(xy[0] - out[-1][0], xy[1] - out[-1][1]) >= min_step:
            out.append(xy)
    if len(coords) >= 2 and len(out) >= 2:
        # always keep the true endpoint
        if out[-1] != coords[-1]:
            out[-1] = coords[-1]
    return out


def resample_line(line: LineString, cfg: MatchConfig):
    """~cfg.resample_m spacing, capped at cfg.max_obs points.

    Returns (points [(x, y)], alongs [m along the line]).
    """
    total = line.length
    spacing = max(cfg.resample_m, total / max(1, cfg.max_obs - 1))
    n = max(2, int(round(total / spacing)) + 1)
    alongs = [total * i / (n - 1) for i in range(n)]
    pts, keep_alongs = [], []
    for a in alongs:
        p = line.interpolate(a)
        if pts and math.hypot(p.x - pts[-1][0], p.y - pts[-1][1]) < cfg.min_step_m:
            continue
        pts.append((p.x, p.y))
        keep_alongs.append(a)
    return pts, keep_alongs


# ── bounded multi-target network search ──────────────────────────────────────


def _turn_ok(mg: MatchGraph, prev_edge: int, next_edge: int, via: int) -> bool:
    """Turn-restriction check (no_* forbidden pairs, only_* whitelists)."""
    rs = mg.restrictions_at.get(via)
    if not rs:
        return True
    from_way = mg.graph.edges[prev_edge].way_id
    to_way = mg.graph.edges[next_edge].way_id
    only_targets = None
    for r in rs:
        if not r.applies_to_psv or r.from_way != from_way:
            continue
        if r.kind.startswith("only_"):
            only_targets = (only_targets or set()) | {r.to_way}
        elif r.kind.startswith("no_") and r.to_way == to_way:
            return False
    if only_targets is not None and to_way not in only_targets:
        return False
    return True


def _turn_penalty(mg: MatchGraph, cfg: MatchConfig, pe: int, pd: int, ne: int, nd: int, via: int) -> float:
    """180°-reversal penalty, waived at genuine stubs."""
    if mg.is_stub(via):
        return 0.0
    ex, ey = mg.exit_vec(pe, pd)
    nx, ny = mg.entry_vec(ne, nd)
    if ex * nx + ey * ny < cfg.uturn_cos:
        return cfg.uturn_penalty_m
    return 0.0


def _route_multi(mg, cfg, mult, src: Candidate, targets: list, cutoff_w: float) -> dict:
    """Bounded Dijkstra from a directed-edge position to many others.

    Edge weights = length × mult(edge) (+ turn penalties); returns
    {target_index: (weighted_cost, true_length_m, path)} where path is
    the list of (edge, dir) states entered after src's edge, ending with
    the target's state ([] = same-directed-edge hop).
    """
    results: dict = {}
    by_state: dict = {}
    for ti, t in enumerate(targets):
        by_state.setdefault((t.edge, t.dir), []).append(ti)

    # direct hops along src's own directed edge
    for ti in by_state.get((src.edge, src.dir), []):
        t = targets[ti]
        if t.offset >= src.offset:
            w = (t.offset - src.offset) * mult(src.edge)
            if w <= cutoff_w:
                results[ti] = (w, t.offset - src.offset, [])

    # every target state stays live: a routed path can still serve targets
    # BEHIND src on the same directed edge (loop around / stub reversal)
    unresolved = {(t.edge, t.dir) for t in targets}

    heap: list = []
    parents: dict = {}
    settled: dict = {}
    # monotonic tiebreaker: on exact (w, true) ties heapq must never fall
    # through to comparing parent (None vs tuple raises TypeError)
    tick = count()

    e0, d0 = src.edge, src.dir
    rem = max(0.0, mg.lengths[e0] - src.offset)
    exit_w, exit_true = rem * mult(e0), rem
    v0 = mg.end_node(e0, d0)
    if exit_w <= cutoff_w:
        for e1, d1 in mg.out_edges.get(v0, []):
            if not _turn_ok(mg, e0, e1, v0):
                continue
            w = exit_w + _turn_penalty(mg, cfg, e0, d0, e1, d1, v0)
            if w <= cutoff_w:
                heappush(heap, (w, exit_true, next(tick), (e1, d1), None))

    while heap and unresolved:
        w, true, _, state, parent = heappop(heap)
        if state in settled:
            continue
        settled[state] = True
        parents[state] = parent
        e, d = state
        m = mult(e)
        for ti in by_state.get(state, []):
            t = targets[ti]
            tw = w + t.offset * m
            if tw <= cutoff_w and (ti not in results or tw < results[ti][0]):
                # reconstruct the entered-state chain
                chain, s = [], state
                while s is not None:
                    chain.append(s)
                    s = parents[s]
                results[ti] = (tw, true + t.offset, chain[::-1])
        unresolved.discard(state)

        exit_w = w + mg.lengths[e] * m
        if exit_w > cutoff_w:
            continue
        exit_true = true + mg.lengths[e]
        v = mg.end_node(e, d)
        for e2, d2 in mg.out_edges.get(v, []):
            if (e2, d2) in settled or not _turn_ok(mg, e, e2, v):
                continue
            w2 = exit_w + _turn_penalty(mg, cfg, e, d, e2, d2, v)
            if w2 <= cutoff_w:
                heappush(heap, (w2, exit_true, next(tick), (e2, d2), state))
    return results


def _hop_coords(mg: MatchGraph, a: Candidate, b: Candidate, path: list) -> list:
    """Full-fidelity coords travelled from candidate a to candidate b."""
    if not path:
        return mg.dir_substring(a.edge, a.dir, a.offset, b.offset)
    out = mg.dir_substring(a.edge, a.dir, a.offset, mg.lengths[a.edge])
    for e, d in path[:-1]:
        seg = mg.dir_coords(e, d)
        out += seg[1:] if out and out[-1] == seg[0] else seg
    tail = mg.dir_substring(b.edge, b.dir, 0.0, b.offset)
    out += tail[1:] if out and tail and out[-1] == tail[0] else tail
    return out


# ── trellis decode (Viterbi as Dijkstra, lazy expansion) ─────────────────────


def _emission(c: Candidate, sigma: float) -> float:
    return max(0.0, 0.5 * (c.dist / sigma) ** 2 - c.bonus) + c.prior


def _decode_segment(mg, cfg, layers, alongs, start, mult, dense, station_idx=None):
    """Decode one sub-trace beginning at observation `start`.

    Returns (end_layer, cand_indices, hops, cost) — hops[k] is the
    (path, true_len) between layer start+k and start+k+1.
    """
    sigma = cfg.sigma_dense if dense else cfg.sigma_sparse
    beta = cfg.beta_dense if dense else cfg.beta_sparse
    slack = cfg.cutoff_slack_dense_m if dense else cfg.cutoff_slack_sparse_m
    last = len(layers) - 1

    best: dict = {}
    parent: dict = {}
    settled: set = set()
    layer_best: dict = {}
    heap: list = []
    for j, c in enumerate(layers[start]):
        cost = _emission(c, sigma)
        best[(start, j)] = cost
        heappush(heap, (cost, start, j))

    end_state = None
    while heap:
        cost, i, j = heappop(heap)
        if (i, j) in settled or cost > best.get((i, j), _INF):
            continue
        settled.add((i, j))
        if i not in layer_best or cost < layer_best[i][0]:
            layer_best[i] = (cost, j)
        if i == last:
            end_state = (i, j)
            break
        nxt = layers[i + 1]
        if not nxt:
            continue  # dead layer: this sub-trace can never pass it
        src = layers[i][j]
        cutoff = max(cfg.cutoff_factor * alongs[i], alongs[i] + slack)
        reached = _route_multi(mg, cfg, mult, src, nxt, cutoff)
        for tj, (w, true, path) in reached.items():
            t = nxt[tj]
            if dense:
                trans = abs(alongs[i] - true) / beta
            else:
                trans = max(0.0, true - alongs[i]) / beta
                if station_idx is not None and station_idx.tree is not None:
                    hop = _hop_coords(mg, src, t, path)
                    if len(hop) >= 2:
                        trans += cfg.station_pass_penalty * station_idx.count_passed(
                            LineString(hop),
                            cfg.station_pass_radius_m,
                            [(src.x, src.y), (t.x, t.y)],
                        )
            ncost = cost + trans + _emission(t, sigma)
            key = (i + 1, tj)
            if ncost < best.get(key, _INF):
                best[key] = ncost
                parent[key] = (j, path, true)
                heappush(heap, (ncost, i + 1, tj))

    if end_state is None:
        # heap exhausted before the last layer: break at the furthest
        # observation any feasible chain reached (start layer always settles)
        end_layer = max(layer_best)
        end_state = (end_layer, layer_best[end_layer][1])

    # backtrace
    i, j = end_state
    cost = best[(i, j)]
    cands, hops = [j], []
    while i > start:
        pj, path, true = parent[(i, j)]
        hops.append((path, true))
        j = pj
        i -= 1
        cands.append(j)
    return end_state[0], cands[::-1], hops[::-1], cost


# ── pattern matching ─────────────────────────────────────────────────────────


def geometry_hash(coords) -> str:
    """Per-feed dedup key: md5 over 1e-6°-rounded coordinates."""
    h = hashlib.md5()
    for lon, lat in coords:
        h.update(f"{lon:.6f},{lat:.6f};".encode())
    return h.hexdigest()


def match_pattern(
    mg: MatchGraph,
    pattern: Pattern,
    cfg: MatchConfig | None = None,
    station_idx: StationIndex | None = None,
) -> MatchResult:
    cfg = cfg or MatchConfig()
    t0 = time.perf_counter()
    mode = mg.graph.mode

    stops_xy = mg.project_lonlat(pattern.stop_coords)
    chord = LineString(stops_xy).length if len(stops_xy) >= 2 else 0.0

    shape_xy = None
    if pattern.shape and len(pattern.shape) >= 2:
        cleaned = _dedup_close(mg.project_lonlat(pattern.shape), cfg.min_step_m)
        if len(cleaned) >= 2:
            shape_xy = cleaned
    shape_line = LineString(shape_xy) if shape_xy else None
    dense = bool(
        shape_line is not None
        and len(shape_xy) >= cfg.degenerate_min_points
        and shape_line.length >= cfg.degenerate_min_len_ratio * chord
        and shape_line.length > 0
    )

    def original_coords():
        return list(pattern.shape) if pattern.shape else list(pattern.stop_coords)

    if dense:
        obs, obs_along = resample_line(shape_line, cfg)
        radius = cfg.dense_radius.get(mode, 50.0)
    else:
        obs = _dedup_close(stops_xy, cfg.min_step_m)
        obs_along = None
        radius = cfg.sparse_radius.get(mode, 200.0)
    n = len(obs)
    alongs = [
        (obs_along[i + 1] - obs_along[i]) if dense
        else math.hypot(obs[i + 1][0] - obs[i][0], obs[i + 1][1] - obs[i][1])
        for i in range(n - 1)
    ]

    # candidate layers; route-relation-matched edges are always admitted
    # so junction fan-out can't crowd the decorated track out of the top-k
    # (admission uses ANY match tier — the identity/colour threshold below
    # is only known once every layer's candidates exist)
    rm = RouteMatcher(pattern.route_short_name, pattern.route_long_name, pattern.route_color)
    edges = mg.graph.edges
    layers = [
        mg.candidates(
            x, y, radius, cfg.k_candidates,
            include=lambda i: rm.matches_edge(i, edges[i]),
        )
        for x, y in obs
    ]

    # identity (ref/name) matches outrank colour-only matches: route_color
    # is a FAMILY key on colour-collapsed networks (NYC N/Q/R/W, 1/2/3), so
    # when any candidate identity-matches, colour-only relations are treated
    # as non-matching — otherwise express/local disambiguation collapses.
    best_strength = max(
        (rm.match_strength(c.edge, edges[c.edge]) for layer in layers for c in layer),
        default=0,
    )
    rel_threshold = 2 if best_strength >= 2 else 1
    rel_match = lambda i: rm.match_strength(i, edges[i]) >= rel_threshold  # noqa: E731
    if not dense and station_idx is not None and station_idx.tree is not None:
        stop_names = pattern.stop_names if len(pattern.stop_names) == n else [""] * n
        stop_plats = pattern.stop_platforms if len(pattern.stop_platforms) == n else [""] * n
        for i, layer in enumerate(layers):
            for c in layer:
                c.bonus = cfg.name_bonus_weight * station_idx.best_name_bonus(
                    c.x, c.y, stop_names[i], stop_plats[i], cfg.station_search_radius_m
                )

    base_mult = [e.class_penalty for e in mg.graph.edges]

    # route-relation emission prior (vertical stack disambiguation): only
    # active when the route's relations decorate at least one candidate
    cand_matches = {
        c.edge: rel_match(c.edge) for layer in layers for c in layer
    }
    relation_prior_active = any(cand_matches.values())
    if relation_prior_active:
        for layer in layers:
            for c in layer:
                if not cand_matches[c.edge]:
                    c.prior = cfg.relation_prior

    def mult(e: int) -> float:
        m = base_mult[e]
        if rel_match(e):
            m *= cfg.route_bonus_mult
        return m

    stats: dict = {
        "regime": "dense" if dense else "sparse",
        "n_obs": n,
        "n_empty_layers": sum(1 for l in layers if not l),
        "mean_candidates": round(sum(len(l) for l in layers) / max(1, n), 2),
        "radius_m": radius,
        "relation_prior_active": relation_prior_active,
        "relation_match_tier": best_strength,  # 2=ref/name, 1=colour-only, 0=none
    }

    if all(not l for l in layers):
        stats["runtime_s"] = round(time.perf_counter() - t0, 2)
        return MatchResult("passthrough", 0.0, original_coords(), stats, None, [])

    # decode sub-traces, breaking (never forcing) at infeasible spots
    segments = []
    total_cost, matched_obs = 0.0, 0
    i = 0
    while i < n:
        if not layers[i]:
            i += 1
            continue
        end, cands, hops, cost = _decode_segment(
            mg, cfg, layers, alongs, i, mult, dense, station_idx
        )
        segments.append({"start": i, "end": end, "cands": cands, "hops": hops})
        total_cost += cost
        matched_obs += end - i + 1
        i = end + 1

    # assemble geometry (matched pieces + original-shape bridges)
    edges_used: set = set()
    out_xy: list = []
    gaps: list = []

    def bridge(a_obs: int, b_obs: int):
        """Original geometry between observation indices (exclusive gap)."""
        if dense:
            seg = substring(shape_line, obs_along[a_obs], obs_along[b_obs])
            pts = list(seg.coords) if not seg.is_empty and seg.geom_type != "Point" else []
        else:
            pts = obs[a_obs : b_obs + 1]
        if len(pts) >= 2:
            gaps.append({
                "from_obs": a_obs, "to_obs": b_obs,
                "bridged_m": round(LineString(pts).length, 1),
            })
        return pts

    def extend(pts):
        for p in pts:
            if not out_xy or math.hypot(p[0] - out_xy[-1][0], p[1] - out_xy[-1][1]) > 1e-9:
                out_xy.append(p)

    prev_end = None
    for seg in segments:
        if prev_end is None:
            if seg["start"] > 0:
                extend(bridge(0, seg["start"]))
        else:
            extend(bridge(prev_end, seg["start"]))
        layer = seg["start"]
        cand = layers[layer][seg["cands"][0]]
        extend([(cand.x, cand.y)])
        edges_used.add(cand.edge)
        for k, (path, _true) in enumerate(seg["hops"]):
            a = layers[layer + k][seg["cands"][k]]
            b = layers[layer + k + 1][seg["cands"][k + 1]]
            extend(_hop_coords(mg, a, b, path))
            edges_used.add(b.edge)
            edges_used.update(e for e, _d in path)
        prev_end = seg["end"]
    if prev_end is not None and prev_end < n - 1:
        extend(bridge(prev_end, n - 1))

    breaks = max(0, len(segments) - 1)
    stats.update(
        breaks=breaks,
        gaps=gaps,
        bridged_m=round(sum(g["bridged_m"] for g in gaps), 1),
        matched_obs=matched_obs,
    )

    if len(out_xy) < 2:
        stats["runtime_s"] = round(time.perf_counter() - t0, 2)
        return MatchResult("passthrough", 0.0, original_coords(), stats, None, [])

    simplified = LineString(out_xy).simplify(cfg.simplify_m, preserve_topology=True)
    stats["output_points"] = len(simplified.coords)
    stats["output_len_m"] = round(simplified.length, 1)

    ref_line = LineString(obs) if dense else None
    report = evaluate_gates(
        simplified,
        mode,
        cfg.gates,
        ref_line=ref_line,
        obs_points=obs if dense else None,
        stops_xy=stops_xy,
        stop_radius=radius,
        dense=dense,
    )

    avg_cost = total_cost / max(1, matched_obs)
    confidence = round(math.exp(-avg_cost) / (1.0 + breaks), 4)
    stats["avg_cost_per_obs"] = round(avg_cost, 4)
    stats["runtime_s"] = round(time.perf_counter() - t0, 2)

    if not report.passed:
        return MatchResult(
            "fallback", confidence, original_coords(), stats, report, sorted(edges_used)
        )
    method = "hmm_dense" if dense else "hmm_sparse"
    return MatchResult(
        method, confidence, mg.unproject(simplified.coords), stats, report, sorted(edges_used)
    )


# ── cli ──────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m shapesnap.match",
        description="Match GTFS patterns onto a cached OSM mode graph.",
    )
    ap.add_argument("--graph", type=Path, required=True, help="Phase A graph cache")
    ap.add_argument("--feed", type=Path, required=True, help="GTFS zip")
    ap.add_argument("--routes", default=None, help="comma-separated route_ids")
    ap.add_argument("--limit", type=int, default=None, help="max patterns")
    ap.add_argument(
        "--pbf", type=Path, default=None,
        help="OSM extract for the regime-B station index "
             "(default: the graph's source pbf when it still exists)",
    )
    args = ap.parse_args(argv)

    if args.pbf is not None and not args.pbf.exists():
        ap.error(f"pbf not found: {args.pbf}")

    graph = load_graph(args.graph)
    mg = MatchGraph(graph)

    # regime B station index (name / platform-ref bonuses, pass penalties)
    station_idx = None
    pbf = args.pbf or Path(graph.source_path)
    if pbf.exists():
        stations = load_stations(pbf, graph.mode)
        station_idx = StationIndex(stations, mg)
        print(f"[shapesnap.match] station index: {len(stations)} stations ({pbf.name})")
    else:
        print("[shapesnap.match] no station index (graph source pbf missing; pass --pbf)")

    route_ids = set(args.routes.split(",")) if args.routes else None
    patterns = load_patterns(args.feed, route_ids=route_ids, modes={graph.mode})
    if args.limit:
        patterns = patterns[: args.limit]
    print(f"[shapesnap.match] {len(patterns)} patterns, graph {graph.mode} "
          f"({len(graph.edges)} edges)")
    cfg = MatchConfig()
    seen: dict = {}  # per-feed output dedup: geometry_hash -> first pattern key
    n_dup = 0
    for p in patterns:
        r = match_pattern(mg, p, cfg, station_idx=station_idx)
        dup_of = seen.setdefault(geometry_hash(r.coords), p.key)
        if dup_of != p.key:
            n_dup += 1
        g = r.gates.as_dict() if r.gates else {}
        print(
            f"  {p.key} trips={p.trip_count} stops={len(p.stop_ids)} "
            f"method={r.method} conf={r.confidence} pts={r.stats.get('output_points')} "
            f"breaks={r.stats.get('breaks')} gates={g.get('failures') or 'ok'} "
            f"t={r.stats.get('runtime_s')}s"
            + (f" dup_of={dup_of}" if dup_of != p.key else "")
        )
    print(f"[shapesnap.match] {len(seen)} unique geometries, {n_dup} duplicates")
    return 0


if __name__ == "__main__":
    from shapesnap.match import main as _main

    sys.exit(_main())
