"""tools — always-current measurement + introspection for the transit pipeline.

Two rerunnable, committed utilities (docs/transit-tuning-log.md points here):

  * ``python -m tools.pipeline_dials`` — every config dataclass field across
    shapesnap / linegraph / lineorder / segments with its current default,
    type, and docstring/inline comment, grouped by stage. The authoritative
    dial manifest (values never drift because they are read from the source).
  * ``python -m tools.scorecard --build-key <city>`` — the quantitative
    exams distilled to a one-page scorecard (junction deviation, track stray,
    bundle counts, kissing count, corridor count, on-OSM %).

Both take ``--json`` for machine consumption.
"""
