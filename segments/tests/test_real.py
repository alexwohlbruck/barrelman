"""Real-data exam: chicago:l-v3 segmentation (stage 6, phase A).

Builds the full segmentation in memory, emits transit_line_segments for
build_key chicago:l-v3 (delete-and-replace, same rows the CLI writes),
and checks the v3 contract:

  - exactly the stage-5 exam's 13 transition sites (11 junctions +
    Howard's and Ashland's deg-2 composition changes; the DETERMINISTIC
    committed-source build — see the pin history in test_transition_sites),
    every site producing transitions,
  - every long steady feature's underlying edge composition is constant
    (PostGIS sampling query against transit_graph_edge_lines),
  - transition endpoints coincide with adjacent steady endpoints within
    0.5 m and carry EXACTLY the steady features' offset_px on each side
    (direction-aware sign),
  - total per-ribbon geometry length conserved within 1% of the corridor
    sum (fillet shortening vs Loop-corner tail reuse),
  - emitted rows round-trip the in-memory build.

Requires the dev DB (postgresql://barrelman:barrelman@localhost:5434);
skips if unreachable. Run:
  uv run --with-requirements segments/requirements.txt \
      python -m pytest segments/tests/test_real.py -v -s
"""

import math
import os
from collections import Counter, defaultdict

import pytest

from segments.corridors import load_graph, walk_corridors
from segments.segment import (LocalProj, SegmentConfig, _circumradius,
                              build_segments)
from segments.tests.helpers import offset_at_shared_endpoint

BUILD_KEY = "chicago:l-v3"
DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)
CFG = SegmentConfig()


@pytest.fixture(scope="module")
def built():
    """Build every zoom band and emit them all (the CLI's exact rows);
    the in-memory assertions run on the DEFAULT (z15 / 60 m) band."""
    psycopg = pytest.importorskip("psycopg")
    from dataclasses import replace

    from segments.build import load_shapes
    from segments.emit import emit_segments
    from segments.segment import band_ranges
    try:
        g = load_graph(BUILD_KEY, DSN)
    except psycopg.OperationalError as err:
        pytest.skip(f"dev DB unreachable: {err}")
    shapes = load_shapes(g, DSN)
    band_segments = []
    segments = info = None
    for mz, mxz, length in band_ranges(CFG.bands):
        segs, binfo = build_segments(
            g, replace(CFG, transition_len_m=length), shapes=shapes)
        band_segments.append((mz, mxz, segs))
        if length == CFG.transition_len_m:
            segments, info = segs, binfo
    assert segments is not None, "default transition length must be a band"
    n = emit_segments(band_segments, build_key=BUILD_KEY, dsn=DSN)
    n_default = len(band_segments[0][2])
    assert n == sum(len(s) for _, _, s in band_segments)
    return g, segments, info, n_default


def test_transition_sites(built):
    g, segments, info, _ = built
    sites = info["sites"]
    kinds = Counter(sites.values())
    # way-graph era pin (matches segments_exam check1): real junctions
    # + Howard's composition change. Round 19 (cross-family gap 10->22):
    # the wider gap splits the Tower 18 multi-family interlocking two
    # switch nodes finer, 10 -> 12 junctions (loop exam pins the Loop
    # bundles + Tower 18 unchanged; Howard stays the sole composition).
    # Round 21 (transitive cross-family bundling): the North Side P/Red now
    # bundles onto the Brown's shared centerline, so those lines share one
    # ribbon and every station/junction where a line joins or leaves the
    # bundle becomes a composition-change JUNCTION node.
    # Re-synced to the DETERMINISTIC committed-source build (18 -> 12 sites,
    # 17 -> 11 junctions): a fresh `linegraph.build --feed 29` reproducibly
    # emits 144 edges / 12 transition sites. The round-21 pin (18/17) was from
    # a transient build the committed source no longer reproduces.
    # Re-pinned 11 -> 10 junctions / 1 -> 2 composition after PAR-12 stop
    # conflation: moving the Ashland (Green/Pink) stop 29 m onto its OSM
    # platform shifted the station-split node so the Ashland site was a deg-2
    # composition change alongside Howard.
    # Re-pinned 12 -> 18 sites (10 -> 17 junctions, Ashland composition ->
    # junction) in round 24 (junction-anchored merge start).
    # Re-pinned 18 -> 13 sites (17 -> 11 junctions; Ashland composition
    # RESTORED alongside Howard) — PAR-12 CACHE-DIGEST FIX. The round-24 pin
    # (18/17, Howard sole composition) came from a STALE corridor cache: the
    # old shapes-only waygraph_digest reused a cache built before the
    # round-22/23 conflation + anti-hop re-match (the round-21 "transient"
    # 167-edge topology). The DETERMINISTIC committed-source build (clean
    # cache, waygraph_digest v17 which also hashes colour + route + STOP
    # positions) reproducibly emits 145 edges / 13 transition sites: 11 genuine
    # switch junctions + TWO deg-2 composition changes — Howard
    # (-87.6729, 42.0192, Red<->Y) and Ashland (-87.6696, 41.8852, Pink
    # terminates off the Green), the Ashland split being exactly the round-22
    # stop-conflation effect the stale cache had lost. The Clark/Lake Blue
    # bundle join survives (loop_exam / stability_exam node 14 deg-3
    # {Blue,Brn,G,Org,P,Pink}); chicago:l LOOM md5-identical. Two clean
    # rebuilds are byte-identical (linegraph/tests/test_determinism.py).
    # NOTE: reads the DB build (load_graph BUILD_KEY), so run after a way-graph
    # `linegraph.build --feed 29 --emit`; test_real_emit is now hermetic (its
    # own throwaway key) so the two suites no longer collide.
    assert len(sites) == 13
    assert kinds["junction"] == 11
    assert kinds["composition"] == 2
    # the composition split nodes are unlabeled (label None); pin by
    # coordinate — Howard (-87.673, 42.0191) + Ashland (-87.6696, 41.8852)
    comp_coords = {(round(g.nodes[nid].lon, 4), round(g.nodes[nid].lat, 4))
                   for nid, k in sites.items() if k == "composition"}
    assert comp_coords == {(-87.673, 42.0191), (-87.6696, 41.8852)}


def test_every_site_produces_transitions(built):
    g, segments, info, _ = built
    assert set(info["site_transitions"]) == set(info["sites"])
    assert all(n >= 1 for n in info["site_transitions"].values())
    assert not info.get("greedy_paired_sites"), \
        "all >=3-way ribbons pair from matched_shapes evidence"
    assert not info.get("two_end_unsupported_sites"), \
        "every two-end pairing is supported by a matched_shapes pass"
    composition = {nid for nid, k in info["sites"].items()
                   if k == "composition"}
    assert set(info.get("two_end_shape_gap_sites", [])) <= composition, \
        "shared-route shape gaps only at Howard (Red terminal tail)"


def test_transitions_meet_steady_with_exact_offsets(built):
    """Every transition endpoint hands its offset to an adjacent
    same-ribbon feature EXACTLY (direction-aware sign). Way-graph era:
    the adjacent feature is usually a steady, but a branch-divergence
    twin legitimately shares its branch-point end with the OTHER twin
    TRANSITION (Tower 18's enter/exit movements share the branch tail),
    so the walk matches against every same-ribbon feature — the same
    kind-agnostic rule as the exam's check1 boundary walk, including its
    merge-relocated divergence tail escape: a consumed-corridor merge can
    swallow the twins' shared endpoint into the through-chain's INTERIOR
    (Pink at Tower 18), leaving the twin's end resting ON the chain."""
    from shapely.geometry import LineString, Point

    from segments.tests.helpers import to_m
    g, segments, info, _ = built
    feats_by_ck = defaultdict(list)
    for s in segments:
        feats_by_ck[s.color_key].append(s)

    def end_seg(xy, at_start):
        return ((xy[1][0] - xy[0][0], xy[1][1] - xy[0][1]) if at_start
                else (xy[-1][0] - xy[-2][0], xy[-1][1] - xy[-2][1]))

    n_checked = 0
    for tr in segments:
        if tr.kind != "transition":
            continue
        t_xy = to_m(tr.coords)
        for at_start, off in ((True, tr.off_from_px),
                              (False, tr.off_to_px)):
            tp = t_xy[0] if at_start else t_xy[-1]
            td = end_seg(t_xy, at_start)
            matched = False
            for s in feats_by_ck[tr.color_key]:
                if s is tr:
                    continue
                s_xy = to_m(s.coords)
                for s_start in (True, False):
                    sp = s_xy[0] if s_start else s_xy[-1]
                    if math.dist(tp, sp) > 0.5:
                        continue
                    sd = end_seg(s_xy, s_start)
                    dot = td[0] * sd[0] + td[1] * sd[1]
                    s_off = (s.offset_px if s.kind == "steady"
                             else (s.off_from_px if s_start
                                   else s.off_to_px))
                    want = s_off if dot > 0 else -s_off
                    assert off == pytest.approx(want, abs=1e-9), \
                        (tr.seg_id, s.seg_id)
                    matched = True
                    n_checked += 1
            if not matched:  # merge-relocated divergence tail: the end
                # must rest ON a same-ribbon feature's interior
                matched = any(
                    LineString(to_m(s.coords)).distance(Point(tp)) <= 0.5
                    for s in feats_by_ck[tr.color_key] if s is not tr)
            assert matched, \
                f"transition {tr.seg_id} ({tr.route_short_names}) end " \
                f"touches no same-ribbon feature"
    print(f"\n[real] {n_checked} transition-endpoint offsets matched exactly")
    assert n_checked >= 2 * sum(1 for s in segments
                                if s.kind == "transition")


def test_steady_composition_constant_query(built):
    """Sample every long steady feature in PostGIS; each sampled point's
    nearest graph edge must carry ONE line-set signature. Top band only:
    its len > 61 m steadies are exactly the corridor pieces (the longer
    bands' steady pieces are shorter subsets of the SAME corridors, and
    their skip-converted CONNECTORS legitimately span composition-change
    sites — a 480 m straight-through connector crosses the site by
    design, so sampling it would flag the contract it deliberately
    relaxes)."""
    import psycopg
    g, segments, info, _ = built
    top_band = max(mz for mz, _ in CFG.bands)
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """WITH pts AS (
                 SELECT s.seg_id,
                        ST_LineInterpolatePoint(s.geom, gs.f) AS pt
                 FROM transit_line_segments s
                 CROSS JOIN LATERAL
                   generate_series(0.025, 0.975, 0.025) AS gs(f)
                 WHERE s.build_key = %(b)s AND s.kind = 'steady'
                   AND s.band_minzoom = %(band)s
                   AND s.len_m > 61
               ),
               sig AS (
                 SELECT p.seg_id,
                        (SELECT string_agg(l.feed_id || ':' || l.route_id,
                                           ',' ORDER BY l.feed_id, l.route_id)
                         FROM transit_graph_edge_lines l
                         WHERE l.edge_id = e.id) AS lineset
                 FROM pts p
                 CROSS JOIN LATERAL (
                   SELECT e.id FROM transit_graph_edges e
                   WHERE e.build_key = %(b)s
                     AND ST_DWithin(e.geom, p.pt, 3e-5)
                   ORDER BY e.geom <-> p.pt LIMIT 1
                 ) e
               )
               SELECT seg_id, count(DISTINCT lineset)
               FROM sig GROUP BY seg_id
               HAVING count(DISTINCT lineset) > 1""",
            {"b": BUILD_KEY, "band": top_band})
        bad = cur.fetchall()
        cur.execute(
            """SELECT count(*) FROM transit_line_segments
               WHERE build_key = %s AND band_minzoom = %s
                 AND kind = 'steady' AND len_m > 61""",
            (BUILD_KEY, top_band))
        (n_long,) = cur.fetchone()
    print(f"\n[real] {n_long} long steady features sampled, "
          f"{len(bad)} with mixed composition")
    # flap-guard era: 24 corridors (the North Side P/Red co-run coalesced
    # into one window) leave 32 steadies over 61 m at z15, down from the
    # pre-coalescing 43 and the raster's 50+
    assert n_long > 28
    assert bad == []


def test_transition_curvature_meets_min_radius(built):
    """C3/stage-6 curvature: every transition's discrete min radius
    (circumradius over consecutive vertex triples, aeqd metres) must meet
    the configured minimum (line_count * gap_px * fillet_radius_factor)
    unless (a) the fillet was clamped by short halves — flagged, achieved
    window radius recorded — or (b) the raw track inside the piece is
    itself sharper (recorded pre-fillet, corner vertex excluded; a corner
    fillet cannot fix inherited curvature). Guards against fillet-
    introduced seam kinks: the v3 review measured 30/44 sub-radius
    transitions, 18 of them unflagged, on the chord-tangent fillet."""
    g, segments, info, _ = built
    lon0 = sum(n.lon for n in g.nodes.values()) / len(g.nodes)
    lat0 = sum(n.lat for n in g.nodes.values()) / len(g.nodes)
    proj = LocalProj(lon0, lat0)
    inf = float("inf")
    n_tr = n_target = 0
    bad = []
    for t in segments:
        if t.kind != "transition":
            continue
        n_tr += 1
        target = (t.fillet_target_m if t.fillet_target_m is not None
                  else t.line_count * CFG.gap_px * CFG.fillet_radius_factor)
        raw = t.raw_min_radius_m if t.raw_min_radius_m is not None else inf
        ach = t.fillet_radius_m if t.fillet_radius_m is not None else inf
        xy = proj.to_xy(t.coords)
        measured = min((_circumradius(a, b, c)
                        for a, b, c in zip(xy, xy[1:], xy[2:])), default=inf)
        if measured >= 0.9 * target:
            n_target += 1
        floor = min(ach, raw) if t.fillet_clamped else min(target, raw)
        if measured < 0.9 * floor:
            bad.append((t.seg_id, t.route_short_names, round(measured, 1),
                        round(floor, 1), t.fillet_clamped))
    print(f"\n[real] {n_tr} transitions, {n_target} meet 0.9x min radius, "
          f"{info['fillet_clamped']} clamped by short halves")
    assert bad == [], f"sub-floor transition curvature: {bad}"
    # proportional, like the exam's check3: the linegraph refit collapses
    # crossing rungs to near-point consumed corridors whose transitions
    # clamp by design, so the absolute pre-refit census (>= 34) no longer
    # holds; the unconditional floor assertion above stays the contract.
    # Re-calibrated 0.75 -> 0.65 for the cluster-weighted refit era; then
    # 0.65 -> 0.55 in round 19 (cross-family gap 10->22): the two extra
    # Tower 18 switch junctions add short-half transitions that clamp by
    # design (21/36 = 58% full target), the unconditional floor assertion
    # above staying the safety contract. Mirrors segments_exam check3.
    assert n_target >= 0.55 * n_tr, \
        "most transitions meet the full min radius"
    # round 19: the two extra Tower 18 switch junctions add short-half
    # transitions (17 clamped here; this fixture builds without the
    # off-track reconciliation the exam uses, so a couple more clamp)
    assert info["fillet_clamped"] <= 18, \
        "clamping must stay the flagged exception, not the rule"


def test_steady_offset_slot_consistency(built):
    """offset_px is authoritative, but slot/line_count must agree with it
    in the emitted travel frame on every steady row — a consumer that
    re-derives offsets from slot must not place ribbons on the wrong side
    (PAR-12 v3 review: 5/92 rows sign-flipped). Skip-converted connectors
    may differ by up to offset_eps_px (the from/to grids)."""
    g, segments, info, _ = built
    for s in segments:
        if s.kind != "steady":
            continue
        expect = (s.slot - (s.line_count - 1) / 2.0) * CFG.gap_px
        assert abs(s.offset_px - expect) <= CFG.offset_eps_px + 1e-9, \
            (s.seg_id, s.route_short_names, s.offset_px, s.slot, s.line_count)


def test_length_conserved_per_ribbon(built):
    g, segments, info, _ = built
    corridors = walk_corridors(g, CFG.gap_px)
    lon0 = sum(n.lon for n in g.nodes.values()) / len(g.nodes)
    lat0 = sum(n.lat for n in g.nodes.values()) / len(g.nodes)
    proj = LocalProj(lon0, lat0)
    cor_sum: dict = defaultdict(float)
    for c in corridors:
        xy = proj.to_xy(c.coords)
        length = sum(math.dist(a, b) for a, b in zip(xy, xy[1:]))
        for r in c.ribbons:
            cor_sum[r.color_key] += length
    seg_sum: dict = defaultdict(float)
    for s in segments:
        seg_sum[s.color_key] += s.len_m
    print("\n[real] per-ribbon length conservation:")
    for ck in sorted(cor_sum):
        ratio = seg_sum[ck] / cor_sum[ck]
        print(f"  {ck}: corridors {cor_sum[ck] / 1000:.2f} km, "
              f"segments {seg_sum[ck] / 1000:.2f} km, ratio {ratio:.4f}")
        assert 0.99 <= ratio <= 1.01
    total = sum(seg_sum.values()) / sum(cor_sum.values())
    assert 0.99 <= total <= 1.01


def test_emit_roundtrip(built):
    import psycopg
    g, segments, info, n = built
    assert n == len(segments), "no degenerate geometries dropped on emit"
    top_band = max(mz for mz, _ in CFG.bands)
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT kind, count(*), sum(len_m)
               FROM transit_line_segments
               WHERE build_key = %s AND band_minzoom = %s
               GROUP BY kind ORDER BY kind""", (BUILD_KEY, top_band))
        db = {k: (c, s) for k, c, s in cur.fetchall()}
        cur.execute(
            """SELECT count(*) FROM transit_line_segments
               WHERE build_key = %s AND (
                 (kind = 'steady' AND (offset_px IS NULL
                    OR off_from_px IS NOT NULL OR off_to_px IS NOT NULL))
                 OR (kind = 'transition' AND (offset_px IS NOT NULL
                    OR off_from_px IS NULL OR off_to_px IS NULL))
                 OR ST_NPoints(geom) < 2
                 OR NOT ST_IsValid(geom))""", (BUILD_KEY,))
        (n_bad,) = cur.fetchone()
        # every band present, zoom axis partitioned without overlap
        cur.execute(
            """SELECT band_minzoom, band_maxzoom, count(*)
               FROM transit_line_segments WHERE build_key = %s
               GROUP BY 1, 2 ORDER BY 1 DESC""", (BUILD_KEY,))
        bands_db = cur.fetchall()
    from segments.segment import band_ranges
    assert [(mz, mxz) for mz, mxz, _ in bands_db] == \
        [(mz, mxz) for mz, mxz, _ in band_ranges(CFG.bands)]
    assert all(c > 0 for _, _, c in bands_db)
    mem = Counter(s.kind for s in segments)
    mem_len = defaultdict(float)
    for s in segments:
        mem_len[s.kind] += s.len_m
    for kind in ("steady", "transition"):
        assert db[kind][0] == mem[kind]
        assert db[kind][1] == pytest.approx(mem_len[kind], rel=1e-6)
    assert n_bad == 0, "NULL-by-kind contract and geometry validity"
    print(f"\n[real] emitted (band z{top_band}): "
          + ", ".join(f"{k}: {c} rows / {s / 1000:.2f} km"
                      for k, (c, s) in sorted(db.items())))
