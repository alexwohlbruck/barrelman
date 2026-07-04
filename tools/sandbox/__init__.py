"""tools.sandbox — visual verification sandbox for the transit map.

A reusable, committed harness that RENDERS the emitted transit_line_segments
WITH the client's exact zoom-scaled line-offset expression applied, and gives
a per-site quantitative verdict (measured on-screen px gap between family
ribbons; separate/centered/straight checks). It reconciles "the data is
bundled but the client shows a gap" by measuring the actual rendered geometry.

  * ``tools.sandbox.sites``  — the site registry (Part 1): every window we've
    worked, its build_key, bbox, representative client zoom, families present,
    and EXPECTED behaviour (bundle / separate / centered / straight).
  * ``tools.sandbox.verify`` — the verify harness (Part 2): reads the CURRENT
    DB, renders one panel per site (client offset applied), prints a verdict
    table, writes per-site PNGs + a contact-sheet, emits a verdict JSON.
  * ``tools.sandbox.rebuild`` — the fast local-rebuild sandbox (Part 3):
    re-run waygraph -> lineorder -> segments for ONE site's bbox (+buffer)
    with WaygraphConfig dial overrides, in seconds, and re-verify.

Run (both cities, current DB):

  uv run --with-requirements segments/requirements.txt \
      python -m tools.sandbox.verify

Iterate a dial for one site:

  uv run --with-requirements segments/requirements.txt \
      python -m tools.sandbox.rebuild --site dekalb --set cross_family_gap_m=22
"""
