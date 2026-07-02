"""linegraph — raster-skeleton-vectorize centerline pipeline (transit v3 stage 4).

Replaces LOOM's gtfs2graph|topo. Route shapes (already OSM-matched by
shapesnap) are stroked onto a metric raster at MERGE_WIDTH — line
thickness IS the merge criterion — then skeletonized (scikit-image) and
vectorized back into a planar graph. Genuinely-parallel tracks within
MERGE_WIDTH fuse into one display centerline; crossing-but-not-parallel
tracks meet at a junction node without exchanging geometry (the Chicago
Tower 18 exam).

Modules:
  raster   — UTM projection + deterministic grid + stroke rasterization
  skeleton — 1-px skeletonization wrapper
  vector   — skeleton pixels -> planar graph (nodes, edges, cleanup)
  model    — dataclasses + gzip-pickle cache (format_version + digest)
  build    — CLI: python -m linegraph.build --feed 29 --mode rail ...
"""
