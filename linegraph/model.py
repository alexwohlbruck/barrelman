#!/usr/bin/env python3
"""linegraph.model — output dataclasses + gzip-pickle cache.

Cache conventions follow shapesnap.graph: gzipped pickle of the LineGraph
dataclass stamped with a format_version and an input digest (md5 over the
deduped input shape coordinates + the build parameters). Loading raises
ValueError on unreadable caches, version mismatches, or digest mismatches
— callers treat ValueError as "rebuild".
"""

from __future__ import annotations

import gzip
import hashlib
import math
import pickle
from dataclasses import dataclass, field
from pathlib import Path

# 2: shape-evidence refit era (linegraph.refit). The cache still holds
#    the RAW skeleton only, but phase-B geometry semantics changed, so
#    pre-refit caches must never masquerade as current v3 artifacts.
# 3: corridor-unfuse era (linegraph.unfuse). LGEdge grew the `families`
#    lock slot; caches pickled without it would raise on access.
FORMAT_VERSION = 3

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CACHE_DIR = REPO_ROOT / "data" / "linegraph"


@dataclass(slots=True)
class LGNode:
    node_id: int
    lon: float
    lat: float
    x: float                # projected (UTM) meters
    y: float
    degree: int             # incident edge ends (self-loop counts twice)
    kind: str               # junction | endpoint | cycle


@dataclass(slots=True)
class LGEdge:
    edge_id: int
    from_node: int
    to_node: int
    px_len: int             # skeleton pixels traced (pre-simplification)
    length_m: float         # simplified geometry length (meters)
    coords: list            # [(lon, lat), ...] simplified + smoothed
    coords_xy: list = field(default_factory=list)  # projected twin of coords
    families: frozenset | None = None  # unfuse family lock: only these
    #                       color_keys may attribute/refit onto the edge
    #                       (None = unrestricted; survives station splits)


@dataclass(slots=True)
class LineGraph:
    format_version: int
    build_key: str
    feed_id: str
    mode: str
    merge_width_m: float
    resolution_m: float
    epsg: int
    origin: tuple           # (x0, y0) UTM meters, snapped to res multiples
    grid_shape: tuple       # (rows, cols)
    grid_bytes: int         # peak raster size (uint8/bool bytes)
    input_digest: str
    n_input_shapes: int
    build_seconds: float
    nodes: list             # [LGNode]
    edges: list             # [LGEdge]

    def total_length_m(self) -> float:
        return sum(e.length_m for e in self.edges)

    def components(self) -> list:
        """Union-find over edge endpoints -> list of node-id sets."""
        parent: dict = {n.node_id: n.node_id for n in self.nodes}

        def find(a):
            root = a
            while parent[root] != root:
                root = parent[root]
            while parent[a] != root:
                parent[a], a = root, parent[a]
            return root

        for e in self.edges:
            ra, rb = find(e.from_node), find(e.to_node)
            if ra != rb:
                parent[rb] = ra
        groups: dict = {}
        for nid in parent:
            groups.setdefault(find(nid), set()).add(nid)
        return sorted(groups.values(), key=lambda s: (-len(s), min(s)))


# ── input digest ─────────────────────────────────────────────────────────────


def input_digest(shapes, merge_width_m: float, resolution_m: float) -> str:
    """md5 over 1e-6°-rounded shape coords + parameters + format version.

    Shapes are hashed in the order given (callers pass a deterministic,
    deduped list), so identical inputs and knobs -> identical digest.
    """
    h = hashlib.md5()
    h.update(f"v{FORMAT_VERSION};mw={merge_width_m:.3f};res={resolution_m:.3f};".encode())
    for coords in shapes:
        for lon, lat in coords:
            h.update(f"{lon:.6f},{lat:.6f};".encode())
        h.update(b"|")
    return h.hexdigest()


# ── cache io (shapesnap conventions) ─────────────────────────────────────────


def default_cache_path(feed_id: str, mode: str) -> Path:
    return DEFAULT_CACHE_DIR / f"{feed_id}.{mode}.linegraph.pkl.gz"


def save_linegraph(lg: LineGraph, out_path) -> None:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with gzip.open(tmp, "wb", compresslevel=6) as f:
        pickle.dump(lg, f, protocol=pickle.HIGHEST_PROTOCOL)
    tmp.replace(out_path)


def load_linegraph(cache_path, expect_digest: str | None = None) -> LineGraph:
    """Load a cached LineGraph. ValueError = rebuild (never opaque errors)."""
    try:
        with gzip.open(cache_path, "rb") as f:
            lg = pickle.load(f)
    except FileNotFoundError:
        raise
    except Exception as err:
        raise ValueError(
            f"cache {cache_path} unreadable ({type(err).__name__}: {err}); rebuild"
        ) from err
    version = getattr(lg, "format_version", None)
    if version != FORMAT_VERSION:
        raise ValueError(f"cache format {version} != current {FORMAT_VERSION}; rebuild")
    if expect_digest is not None and lg.input_digest != expect_digest:
        raise ValueError(f"cache {cache_path} digest mismatch; rebuild")
    return lg


def polyline_length_m(coords_xy) -> float:
    return sum(
        math.hypot(b[0] - a[0], b[1] - a[1]) for a, b in zip(coords_xy, coords_xy[1:])
    )
