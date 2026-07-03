#!/usr/bin/env python3
"""[DEPRECATED (way-graph era)] linegraph.skeleton — 1-px skeletonization of the stroked raster.

Thin wrapper over skimage.morphology.skeletonize (Zhang-Suen): topology-
preserving thinning of the boolean stroke grid down to 1-px centerlines.
Kept separate so the method (zhang vs lee/medial-axis) stays a single
swappable knob.
"""

from __future__ import annotations

import numpy as np
from skimage.morphology import skeletonize as _skeletonize


def skeletonize_grid(grid: np.ndarray) -> np.ndarray:
    """bool stroke grid -> bool 1-px skeleton (same shape)."""
    if grid.dtype != bool:
        grid = grid.astype(bool)
    return _skeletonize(grid)
