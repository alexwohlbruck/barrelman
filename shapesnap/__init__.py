"""shapesnap — GTFS→OSM shape matching (transit pipeline v3, stage 3).

shapesnap.graph — per-mode OSM matching graph extraction.
shapesnap.match — two-regime Viterbi matching core + quality gates.
shapesnap.run   — feed-level CLI: rewrite the processed zip + PostGIS metadata.
See docs/shapesnap.md and docs/transit-pipeline-v3.md.
"""

__version__ = "0.1.0"
