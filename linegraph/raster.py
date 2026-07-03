#!/usr/bin/env python3
"""[DEPRECATED (way-graph era)] linegraph.raster — project shapes to a local UTM grid and stroke them.

The grid is deterministic: its origin is snapped DOWN to a whole multiple
of the resolution, so the same inputs always land on the same pixels.
Strokes are stamped exactly (rounded caps + joins) with skimage.draw —
per segment a rectangle polygon plus vertex disks of radius
MERGE_WIDTH/2 — instead of a whole-grid morphological dilation, so the
half-width can be fractional in pixels and memory stays one boolean grid.

Memory guard: rasters are dense numpy bool (1 byte/px). The grid is
cropped to the shape-collection bbox + padding; if it would still exceed
MAX_GRID_BYTES (~1.5 GB) the build aborts with a clear error instead of
swapping the machine to death.

After stamping, enclosed background holes THINNER than the stroke are
filled (fill_sliver_holes): two centerlines MERGE_WIDTH..2x MERGE_WIDTH
apart leave a sliver of background between their strokes that is
everywhere within MERGE_WIDTH/2 of ink — below the merge criterion — and
skeletonizing around it yields parallel duplicate centerlines plus
line-less ladder rungs (NYC: the 6th Ave/Houston St trench, the Bowling
Green turnback tangle). Holes with genuine clearance (the Chicago Loop
interior, real flying-junction eyes) are untouched.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
from pyproj import Transformer
from scipy import ndimage
from skimage.draw import disk as draw_disk
from skimage.draw import polygon as draw_polygon

MAX_GRID_BYTES = int(1.5e9)


def utm_epsg_for(lon: float, lat: float) -> int:
    """EPSG code of the UTM zone containing (lon, lat)."""
    zone = min(60, max(1, int((lon + 180.0) // 6.0) + 1))
    return (32600 if lat >= 0 else 32700) + zone


def pick_epsg(shapes_lonlat) -> int:
    """UTM zone of the centroid of all shape vertices."""
    n = 0
    sx = sy = 0.0
    for coords in shapes_lonlat:
        for lon, lat in coords:
            sx += lon
            sy += lat
            n += 1
    if n == 0:
        raise ValueError("no shape vertices; cannot pick a projection")
    return utm_epsg_for(sx / n, sy / n)


def project_shapes(shapes_lonlat, epsg: int) -> list:
    """[(lon, lat), ...] lists -> [(x, y), ...] lists in the given CRS."""
    tf = Transformer.from_crs(4326, epsg, always_xy=True)
    out = []
    for coords in shapes_lonlat:
        xs, ys = tf.transform([c[0] for c in coords], [c[1] for c in coords])
        out.append(list(zip(xs, ys)))
    return out


@dataclass(slots=True)
class RasterGrid:
    origin: tuple           # (x0, y0) — UTM coords of pixel (row 0, col 0)
    res: float              # meters per pixel
    epsg: int
    grid: np.ndarray        # bool, [row, col]; row grows with y (no flip)

    @property
    def shape(self) -> tuple:
        return self.grid.shape

    @property
    def nbytes(self) -> int:
        return self.grid.nbytes

    def xy_to_px(self, x: float, y: float) -> tuple:
        """UTM -> fractional (row, col)."""
        return (y - self.origin[1]) / self.res, (x - self.origin[0]) / self.res

    def px_to_xy(self, row: float, col: float) -> tuple:
        """(row, col) -> UTM meters (pixel represents the lattice point)."""
        return self.origin[0] + col * self.res, self.origin[1] + row * self.res


def _snap_down(v: float, res: float) -> float:
    return math.floor(v / res) * res


def plan_grid(shapes_xy, merge_width: float, res: float,
              max_bytes: int = MAX_GRID_BYTES):
    """Compute the deterministic grid frame; abort if it would be too big.

    Returns (origin, rows, cols).
    """
    minx = min(x for s in shapes_xy for x, _ in s)
    maxx = max(x for s in shapes_xy for x, _ in s)
    miny = min(y for s in shapes_xy for _, y in s)
    maxy = max(y for s in shapes_xy for _, y in s)
    pad = merge_width + 2.0 * res
    x0 = _snap_down(minx - pad, res)
    y0 = _snap_down(miny - pad, res)
    cols = int(math.ceil((maxx + pad - x0) / res)) + 1
    rows = int(math.ceil((maxy + pad - y0) / res)) + 1
    nbytes = rows * cols  # bool = 1 byte/px
    if nbytes > max_bytes:
        raise RuntimeError(
            f"raster grid {rows}x{cols} px at {res} m/px would need "
            f"{nbytes / 1e9:.2f} GB (> {max_bytes / 1e9:.2f} GB budget). "
            f"Shape bbox is {(maxx - minx) / 1000:.1f} x {(maxy - miny) / 1000:.1f} km — "
            f"raise --res or split the region into cells."
        )
    return (x0, y0), rows, cols


def fill_sliver_holes(grid: np.ndarray, half_px: float) -> int:
    """Fill enclosed background holes with max clearance < half_px.

    A hole every point of which lies within MERGE_WIDTH/2 of ink means
    the surrounding strokes' centerlines run closer than 2x MERGE_WIDTH —
    a sliver below the merge criterion, not a genuine separation. Holes
    containing at least one pixel >= half_px clear of all ink survive
    whole (per-hole decision, never partial fills). Bool morphology
    only — no labeling pass, no distance transform. Mutates grid,
    returns the pixel count filled.
    """
    ink_filled = ndimage.binary_fill_holes(grid)
    holes = ink_filled & ~grid
    if not holes.any():
        return 0
    n = int(math.ceil(half_px))
    yy, xx = np.mgrid[-n:n + 1, -n:n + 1]
    footprint = (yy * yy + xx * xx) <= half_px * half_px
    # pixels >= half_px from every ink pixel = outside the ink dilation
    clear = ~ndimage.binary_dilation(grid, structure=footprint)
    cores = holes & clear
    keep = ndimage.binary_propagation(cores, mask=holes)
    fill = holes & ~keep
    grid |= fill
    return int(fill.sum())


def rasterize(shapes_xy, merge_width: float, res: float,
              epsg: int = 0, max_bytes: int = MAX_GRID_BYTES) -> RasterGrid:
    """Stroke every shape at merge_width meters onto one boolean grid."""
    if merge_width <= 0 or res <= 0:
        raise ValueError("merge_width and res must be > 0")
    origin, rows, cols = plan_grid(shapes_xy, merge_width, res, max_bytes)
    grid = np.zeros((rows, cols), dtype=bool)
    rg = RasterGrid(origin=origin, res=res, epsg=epsg, grid=grid)
    half_px = merge_width / 2.0 / res

    for coords in shapes_xy:
        pts = [rg.xy_to_px(x, y) for x, y in coords]
        # rounded caps + joins: a disk at every vertex
        for r, c in pts:
            rr, cc = draw_disk((r, c), half_px, shape=grid.shape)
            grid[rr, cc] = True
        # segment bodies: oriented rectangles
        for (r0, c0), (r1, c1) in zip(pts, pts[1:]):
            dr, dc = r1 - r0, c1 - c0
            seg_len = math.hypot(dr, dc)
            if seg_len < 1e-9:
                continue
            nr, nc = -dc / seg_len * half_px, dr / seg_len * half_px
            rr, cc = draw_polygon(
                [r0 + nr, r1 + nr, r1 - nr, r0 - nr],
                [c0 + nc, c1 + nc, c1 - nc, c0 - nc],
                shape=grid.shape,
            )
            grid[rr, cc] = True
    fill_sliver_holes(grid, half_px)
    return rg
