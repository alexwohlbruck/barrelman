#!/usr/bin/env python3
"""linegraph.vector — skeleton pixels -> planar graph.

Pipeline:
  1. pixel graph: skeleton pixels with 8-connected neighbor count != 2
     are node pixels (junctions >2, endpoints ==1); adjacent node pixels
     cluster into single nodes; the degree-2 pixel runs between clusters
     are traced into edges; pure cycles (no node pixel at all) get one
     synthetic node.
  2. cleanup: prune dangling spurs shorter than the stroke scale (thick-
     stroke endcaps and junction whiskers are skeletonization artifacts),
     contract short junction-junction edges (an X crossing can thin into
     an "H"), drop tiny self-loop bubbles, then merge degree-2 nodes so
     corridors are single edges. Iterated to a fixed point.
  3. geometry: pixel chains -> UTM, endpoints snapped to node centroids,
     light 1-2-1 smoothing that PRESERVES endpoints, ~1-px Douglas-
     Peucker, then lon/lat.

Everything iterates in sorted pixel order — same skeleton in, same graph
out (the determinism test).
"""

from __future__ import annotations

import numpy as np
from pyproj import Transformer
from shapely.geometry import LineString

from linegraph.model import LGEdge, LGNode, polyline_length_m
from linegraph.raster import RasterGrid

_NEIGH = ((-1, -1), (-1, 0), (-1, 1), (0, -1), (0, 1), (1, -1), (1, 0), (1, 1))


# ── step 1: pixel graph ──────────────────────────────────────────────────────


def _neighbors(p, px_set):
    r, c = p
    return [q for dr, dc in _NEIGH if (q := (r + dr, c + dc)) in px_set]


def extract_pixel_graph(skel: np.ndarray):
    """skeleton -> (clusters, edges) in pixel space.

    clusters: {cid: {"pixels": [px...], "kind": str}}
    edges:    [{"a": cid, "b": cid, "chain": [px...]}]  chain runs a -> b
              and includes one boundary pixel of each end cluster.
    """
    px_list = [tuple(p) for p in np.argwhere(skel)]
    px_set = set(px_list)
    px_list.sort()

    deg = {p: len(_neighbors(p, px_set)) for p in px_list}
    node_px = {p for p in px_list if deg[p] != 2 and deg[p] > 0}

    # cluster adjacent node pixels (8-connectivity), smallest pixel first
    cluster_of: dict = {}
    clusters: dict = {}
    cid = 0
    for p in px_list:
        if p not in node_px or p in cluster_of:
            continue
        stack, members = [p], []
        cluster_of[p] = cid
        while stack:
            cur = stack.pop()
            members.append(cur)
            for q in _neighbors(cur, node_px):
                if q not in cluster_of:
                    cluster_of[q] = cid
                    stack.append(q)
        clusters[cid] = {"pixels": sorted(members), "kind": "junction"}
        cid += 1

    edges: list = []
    visited: set = set()          # path pixels consumed by a trace
    direct_seen: set = set()      # node-pixel adjacency pairs already emitted

    def trace(start, first):
        """Walk degree-2 pixels from a node pixel until the next node pixel."""
        chain = [start, first]
        prev, cur = start, first
        while cur not in node_px:
            visited.add(cur)
            nxt = [q for q in _neighbors(cur, px_set) if q != prev]
            if not nxt:            # dead end without a node pixel: degenerate
                break
            prev, cur = cur, nxt[0]
            chain.append(cur)
        return chain

    for c in sorted(clusters):
        for p in clusters[c]["pixels"]:
            for q in sorted(_neighbors(p, px_set)):
                if q in node_px:
                    if cluster_of[q] != c:
                        key = (min(p, q), max(p, q))
                        if key not in direct_seen:
                            direct_seen.add(key)
                            edges.append({"a": c, "b": cluster_of[q], "chain": [p, q]})
                    continue
                if q in visited:
                    continue
                chain = trace(p, q)
                end = chain[-1]
                if end in node_px:
                    edges.append({"a": c, "b": cluster_of[end], "chain": chain})
                # else: degenerate open run swallowed into visited

    # pure cycles: degree-2 rings never touched by a trace
    remaining = sorted(p for p in px_list if deg[p] == 2 and p not in visited
                       and p not in node_px)
    remaining_set = set(remaining)
    for m in remaining:
        if m not in remaining_set:
            continue
        clusters[cid] = {"pixels": [m], "kind": "cycle"}
        nb = sorted(_neighbors(m, px_set))
        chain = [m]
        prev, cur = m, nb[0]
        while cur != m:
            chain.append(cur)
            remaining_set.discard(cur)
            nxt = [q for q in _neighbors(cur, px_set) if q != prev]
            if not nxt:
                break
            prev, cur = cur, nxt[0]
        chain.append(m)
        remaining_set.discard(m)
        edges.append({"a": cid, "b": cid, "chain": chain})
        cid += 1

    return clusters, edges


# ── step 2: cleanup ──────────────────────────────────────────────────────────


def _chain_len_m(chain, res: float) -> float:
    return polyline_length_m(chain) * res


def _incidence(clusters, edges):
    inc = {c: [] for c in clusters}
    for i, e in enumerate(edges):
        inc[e["a"]].append(i)
        if e["b"] != e["a"]:
            inc[e["b"]].append(i)
    return inc


def _degree(clusters, edges):
    deg = {c: 0 for c in clusters}
    for e in edges:
        deg[e["a"]] += 1
        deg[e["b"]] += 1
    return deg


def cleanup_graph(clusters, edges, res: float, merge_width: float,
                  max_rounds: int = 10):
    """Prune spurs, contract crossing artifacts, merge degree-2 nodes."""
    prune_m = 2.0 * merge_width
    contract_m = merge_width
    bubble_m = 2.0 * merge_width

    for _ in range(max_rounds):
        changed = False
        deg = _degree(clusters, edges)

        # (a) dangling spurs + tiny isolated fragments
        keep = []
        for e in edges:
            if e["a"] != e["b"] and _chain_len_m(e["chain"], res) < prune_m and (
                deg[e["a"]] == 1 or deg[e["b"]] == 1
            ):
                changed = True
                continue
            keep.append(e)
        edges = keep

        # (b) tiny self-loop bubbles (skeleton artifacts at crossings)
        keep = []
        for e in edges:
            if e["a"] == e["b"] and _chain_len_m(e["chain"], res) < bubble_m:
                changed = True
                continue
            keep.append(e)
        edges = keep

        # (c) contract short junction-junction edges ("H" at an X crossing)
        deg = _degree(clusters, edges)
        merged: dict = {}

        def root(c):
            while c in merged:
                c = merged[c]
            return c

        for e in sorted(edges, key=lambda e: (e["a"], e["b"], e["chain"][0])):
            a, b = root(e["a"]), root(e["b"])
            if a == b:
                continue
            if deg[e["a"]] >= 3 and deg[e["b"]] >= 3 and \
                    _chain_len_m(e["chain"], res) < contract_m:
                lo, hi = min(a, b), max(a, b)
                merged[hi] = lo
                clusters[lo]["pixels"] = sorted(
                    clusters[lo]["pixels"] + clusters[hi]["pixels"]
                    + e["chain"][1:-1]
                )
                del clusters[hi]
                changed = True
        if merged:
            keep = []
            for e in edges:
                a, b = root(e["a"]), root(e["b"])
                if a == b and e["a"] != e["b"] and \
                        _chain_len_m(e["chain"], res) < contract_m:
                    continue  # the contracted edge itself
                e["a"], e["b"] = a, b
                keep.append(e)
            edges = keep

        # (d) merge degree-2 nodes (join their two edges)
        deg = _degree(clusters, edges)
        inc = _incidence(clusters, edges)
        for c in sorted(clusters):
            if deg.get(c) != 2 or clusters[c]["kind"] == "cycle":
                continue
            eids = inc[c]
            if len(eids) != 2:
                continue  # self-loop anchor
            e1, e2 = edges[eids[0]], edges[eids[1]]
            if e1 is e2:
                continue
            # orient both chains through c
            ch1 = e1["chain"] if e1["b"] == c else e1["chain"][::-1]
            u = e1["a"] if e1["b"] == c else e1["b"]
            ch2 = e2["chain"] if e2["a"] == c else e2["chain"][::-1]
            v = e2["b"] if e2["a"] == c else e2["a"]
            joined = ch1 + (ch2[1:] if ch1[-1] == ch2[0] else ch2)
            e1["a"], e1["b"], e1["chain"] = u, v, joined
            edges = [e for i, e in enumerate(edges) if i != eids[1]]
            del clusters[c]
            changed = True
            deg = _degree(clusters, edges)
            inc = _incidence(clusters, edges)

        # drop orphaned nodes
        used = {e["a"] for e in edges} | {e["b"] for e in edges}
        for c in sorted(clusters):
            if c not in used:
                del clusters[c]
                changed = True

        if not changed:
            break
    return clusters, edges


# ── step 3: geometry ─────────────────────────────────────────────────────────


def _smooth_preserving_endpoints(coords, iterations: int = 2):
    """1-2-1 kernel on interior vertices; endpoints never move."""
    if len(coords) < 3:
        return coords
    pts = np.asarray(coords, dtype=float)
    for _ in range(iterations):
        interior = 0.25 * pts[:-2] + 0.5 * pts[1:-1] + 0.25 * pts[2:]
        pts = np.vstack([pts[:1], interior, pts[-1:]])
    return [tuple(p) for p in pts]


def finish_geometry(clusters, edges, grid: RasterGrid, epsg: int,
                    simplify_m: float | None = None):
    """Pixel graph -> LGNode/LGEdge lists in lon/lat (+ projected twins)."""
    simplify_m = grid.res if simplify_m is None else simplify_m
    to_wgs = Transformer.from_crs(epsg, 4326, always_xy=True)

    # node ids ordered by smallest member pixel (deterministic)
    order = sorted(clusters, key=lambda c: clusters[c]["pixels"][0])
    node_id = {c: i for i, c in enumerate(order)}
    centroid_xy = {}
    for c in order:
        pxs = clusters[c]["pixels"]
        r = sum(p[0] for p in pxs) / len(pxs)
        col = sum(p[1] for p in pxs) / len(pxs)
        centroid_xy[c] = grid.px_to_xy(r, col)

    out_edges = []
    deg = _degree(clusters, edges)
    edges_sorted = sorted(
        edges, key=lambda e: (node_id[e["a"]], node_id[e["b"]], e["chain"][0])
    )
    for i, e in enumerate(edges_sorted):
        xy = [grid.px_to_xy(r, c) for r, c in e["chain"]]
        xy[0] = centroid_xy[e["a"]]
        xy[-1] = centroid_xy[e["b"]]
        xy = _smooth_preserving_endpoints(xy)
        line = LineString(xy).simplify(simplify_m, preserve_topology=False)
        sxy = list(line.coords)
        lons, lats = to_wgs.transform([p[0] for p in sxy], [p[1] for p in sxy])
        out_edges.append(
            LGEdge(
                edge_id=i,
                from_node=node_id[e["a"]],
                to_node=node_id[e["b"]],
                px_len=len(e["chain"]),
                length_m=line.length,
                coords=list(zip(lons, lats)),
                coords_xy=sxy,
            )
        )

    out_nodes = []
    for c in order:
        x, y = centroid_xy[c]
        lon, lat = to_wgs.transform(x, y)
        kind = clusters[c]["kind"]
        d = deg.get(c, 0)
        if kind != "cycle":
            kind = "endpoint" if d == 1 else "junction"
        out_nodes.append(
            LGNode(node_id=node_id[c], lon=lon, lat=lat, x=x, y=y,
                   degree=d, kind=kind)
        )
    out_nodes.sort(key=lambda n: n.node_id)
    return out_nodes, out_edges


def vectorize(skel: np.ndarray, grid: RasterGrid, epsg: int,
              merge_width: float):
    """skeleton grid -> (nodes, edges) — the full step 1-3 chain."""
    clusters, edges = extract_pixel_graph(skel)
    clusters, edges = cleanup_graph(clusters, edges, grid.res, merge_width)
    return finish_geometry(clusters, edges, grid, epsg)
