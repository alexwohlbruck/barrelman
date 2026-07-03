#!/usr/bin/env python3
"""linegraph.stations — station complexes onto graph nodes (phase B.2).

Station complexes come from the feed zip: location_type 1 parents are
preferred; child stops without a parent record cluster by parent_station
id, else by stop_name (centroid coordinate). Only stops actually served
by the mode's patterns participate.

Each complex snaps to the graph within MAX_SNAP_M (150 m):

  * if the nearest point on the skeleton falls within a sliver
    (~MERGE_WIDTH) of an existing node, that node is labeled — this is
    the "snap to the nearest graph node" case (junction stations such as
    Clark/Lake land here);
  * otherwise a new degree-2 STATION NODE is materialized by splitting
    the nearest edge at the complex's projection point. Geometry NEVER
    moves — the split node sits exactly on the existing centerline, the
    pieces' union is the original line. (The skeleton graph only has
    junction/endpoint nodes — 28 for CTA rail vs ~143 station complexes
    — so labeling had to materialize nodes to satisfy the downstream
    contract, where transit_graph_nodes carries the station layer and
    LOOM emitted one node per station.)

Conflicts (two complexes wanting the same node / split point): nearest
wins. A loser carrying the SAME station name merges into the winner's
node instead of failing — feeds list one physical station as several
complexes (MTA's Queensboro Plaza appears once per division), and the
duplicates all want the same snap target. Losers with a different name
stay unlabeled. Complexes farther than MAX_SNAP_M from the skeleton
label nothing.

Run stations BEFORE attribute: splitting first gives attribution
station-to-station granularity, so a route terminating mid-corridor of a
longer route's path only claims edges up to its terminal station.
"""

from __future__ import annotations

import csv
import io
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

import shapely
from pyproj import Transformer
from shapely import STRtree
from shapely.geometry import LineString
from shapely.ops import substring

from linegraph.model import LGEdge, LGNode

MAX_SNAP_M = 150.0


@dataclass(slots=True)
class StationComplex:
    station_id: str
    label: str
    lon: float
    lat: float
    n_stops: int


@dataclass(slots=True)
class StationSnapResult:
    labels: dict = field(default_factory=dict)      # node_id -> (station_id, label)
    labeled: list = field(default_factory=list)     # (complex, node_id, dist_m)
    unlabeled: list = field(default_factory=list)   # (complex, reason, dist_m|None)
    n_split_nodes: int = 0


def load_station_complexes(zip_path, member_stop_ids) -> list:
    """Group the given (mode-filtered) stop ids into station complexes."""
    member_stop_ids = set(member_stop_ids)
    parents: dict = {}
    members: dict = {}
    with zipfile.ZipFile(Path(zip_path)) as zf:
        with zf.open("stops.txt") as f:
            for s in csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")):
                sid = s["stop_id"]
                rec = (
                    float(s["stop_lon"]),
                    float(s["stop_lat"]),
                    (s.get("stop_name") or "").strip(),
                    (s.get("parent_station") or "").strip(),
                )
                if (s.get("location_type") or "").strip() == "1":
                    parents[sid] = rec
                if sid in member_stop_ids:
                    members[sid] = rec

    groups: dict = {}
    for sid, (lon, lat, name, parent) in members.items():
        key = ("p", parent) if parent else ("n", name.lower())
        groups.setdefault(key, []).append((sid, lon, lat, name))

    complexes = []
    for key, rows in groups.items():
        rows.sort()
        kind, val = key
        if kind == "p" and val in parents:
            plon, plat, pname, _ = parents[val]
            complexes.append(StationComplex(val, pname, plon, plat, len(rows)))
        else:
            station_id = val if kind == "p" else rows[0][0]
            label = rows[0][3]
            lon = sum(r[1] for r in rows) / len(rows)
            lat = sum(r[2] for r in rows) / len(rows)
            complexes.append(StationComplex(station_id, label, lon, lat, len(rows)))
    complexes.sort(key=lambda c: c.station_id)
    return complexes


def _apply_splits(lg, splits: dict) -> dict:
    """Split edges at the planned offsets; return {(edge_pos, t): node_id}.

    splits: {edge position in lg.edges: sorted [t_meters, ...]}. Edge ids
    are renumbered sequentially afterwards (emit's loom_id stays the
    list index); new station nodes get ids after the existing maximum.
    """
    if not splits:
        return {}
    to_wgs = Transformer.from_crs(lg.epsg, 4326, always_xy=True)
    next_node = max(n.node_id for n in lg.nodes) + 1
    node_at: dict = {}
    new_nodes: list = []
    new_edges: list = []

    for pos, e in enumerate(lg.edges):
        ts = splits.get(pos)
        if not ts:
            new_edges.append(e)
            continue
        line = LineString(e.coords_xy)
        cut_nodes = []
        for t in ts:
            pt = line.interpolate(t)
            lon, lat = to_wgs.transform(pt.x, pt.y)
            node = LGNode(node_id=next_node, lon=lon, lat=lat, x=pt.x, y=pt.y,
                          degree=2, kind="station")
            node_at[(pos, t)] = next_node
            new_nodes.append(node)
            cut_nodes.append(node)
            next_node += 1
        bounds = [0.0] + list(ts) + [line.length]
        hops = [e.from_node] + [n.node_id for n in cut_nodes] + [e.to_node]
        for j in range(len(bounds) - 1):
            piece = substring(line, bounds[j], bounds[j + 1])
            sxy = list(piece.coords)
            lons, lats = to_wgs.transform([p[0] for p in sxy], [p[1] for p in sxy])
            new_edges.append(
                LGEdge(
                    edge_id=-1,  # renumbered below
                    from_node=hops[j],
                    to_node=hops[j + 1],
                    px_len=max(2, round(e.px_len * piece.length / line.length)),
                    length_m=piece.length,
                    coords=list(zip(lons, lats)),
                    coords_xy=sxy,
                    families=e.families,  # unfuse lock survives splits
                    routes=e.routes,      # exact attribution survives too
                )
            )

    for i, e in enumerate(new_edges):
        e.edge_id = i
    lg.edges = new_edges
    lg.nodes = sorted(lg.nodes + new_nodes, key=lambda n: n.node_id)
    return node_at


def snap_stations(lg, complexes, *, max_snap_m: float = MAX_SNAP_M,
                  sliver_m: float | None = None):
    """Label graph nodes with station complexes (splitting edges as needed).

    Mutates lg (edges split, station nodes appended) and returns
    (lg, StationSnapResult). Geometry is never moved.
    """
    if sliver_m is None:
        sliver_m = lg.merge_width_m
    res = StationSnapResult()
    if not complexes:
        return lg, res

    to_xy = Transformer.from_crs(4326, lg.epsg, always_xy=True)
    xs, ys = to_xy.transform([c.lon for c in complexes], [c.lat for c in complexes])
    points = shapely.points(xs, ys)
    lines = [LineString(e.coords_xy) for e in lg.edges]
    tree = STRtree(lines)
    pairs, dists = tree.query_nearest(
        points, max_distance=max_snap_m, all_matches=False, return_distance=True
    )
    hit = {int(i): (int(t), float(d)) for i, t, d in zip(pairs[0], pairs[1], dists)}

    plans = []  # (dist, station_id, complex, target)
    for i, comp in enumerate(complexes):
        if i not in hit:
            res.unlabeled.append((comp, "too_far", None))
            continue
        pos, dist = hit[i]
        line = lines[pos]
        t = line.project(points[i])
        e = lg.edges[pos]
        if t <= sliver_m:
            target = ("node", e.from_node)
        elif line.length - t <= sliver_m:
            target = ("node", e.to_node)
        else:
            target = ("split", pos, t)
        plans.append((dist, comp.station_id, comp, target))

    # nearest wins on conflict; ties broken by station_id for determinism.
    # A same-name loser MERGES into the winner's node (duplicate complex
    # records of one physical station), recorded as labeled.
    def same_name(a, b) -> bool:
        return a.label.strip().casefold() == b.label.strip().casefold()

    plans.sort(key=lambda p: (p[0], p[1]))
    claimed_nodes: dict = {}
    splits: dict = {}
    split_claims: dict = {}  # (pos, t) -> winning complex
    accepted = []  # (complex, target, dist)
    for dist, _, comp, target in plans:
        if target[0] == "node":
            nid = target[1]
            if nid in claimed_nodes:
                if same_name(comp, claimed_nodes[nid]):
                    accepted.append((comp, target, dist))
                else:
                    res.unlabeled.append((comp, "conflict", dist))
                continue
            claimed_nodes[nid] = comp
            accepted.append((comp, target, dist))
        else:
            _, pos, t = target
            taken = splits.setdefault(pos, [])
            near = [t2 for t2 in taken if abs(t - t2) < sliver_m]
            if near:
                winner = split_claims[(pos, near[0])]
                if same_name(comp, winner):
                    accepted.append((comp, ("split", pos, near[0]), dist))
                else:
                    res.unlabeled.append((comp, "conflict", dist))
                continue
            taken.append(t)
            split_claims[(pos, t)] = comp
            accepted.append((comp, target, dist))

    for pos in splits:
        splits[pos].sort()
    node_at = _apply_splits(lg, splits)
    res.n_split_nodes = len(node_at)

    for comp, target, dist in accepted:
        nid = target[1] if target[0] == "node" else node_at[(target[1], target[2])]
        # setdefault: a same-name merged duplicate never overwrites the
        # winning complex's station_id
        res.labels.setdefault(nid, (comp.station_id, comp.label))
        res.labeled.append((comp, nid, dist))
    return lg, res
