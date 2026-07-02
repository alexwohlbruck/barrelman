"""Real-data exam: chicago:l-v3 segmentation (stage 6, phase A).

Builds the full segmentation in memory, emits transit_line_segments for
build_key chicago:l-v3 (delete-and-replace, same rows the CLI writes),
and checks the v3 contract:

  - exactly the stage-5 exam's 18 transition sites (17 junctions +
    Howard's deg-2 composition change), every site producing transitions,
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
from segments.segment import LocalProj, SegmentConfig, build_segments
from segments.tests.helpers import offset_at_shared_endpoint

BUILD_KEY = "chicago:l-v3"
DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)
CFG = SegmentConfig()


@pytest.fixture(scope="module")
def built():
    psycopg = pytest.importorskip("psycopg")
    from segments.build import load_shapes
    from segments.emit import emit_segments
    try:
        g = load_graph(BUILD_KEY, DSN)
    except psycopg.OperationalError as err:
        pytest.skip(f"dev DB unreachable: {err}")
    shapes = load_shapes(g, DSN)
    segments, info = build_segments(g, CFG, shapes=shapes)
    n = emit_segments(segments, build_key=BUILD_KEY, dsn=DSN)
    return g, segments, info, n


def test_transition_sites(built):
    g, segments, info, _ = built
    sites = info["sites"]
    kinds = Counter(sites.values())
    assert len(sites) == 18
    assert kinds["junction"] == 17
    assert kinds["composition"] == 1
    howard = [nid for nid, k in sites.items() if k == "composition"]
    assert g.nodes[howard[0]].label == "Howard"


def test_every_site_produces_transitions(built):
    g, segments, info, _ = built
    assert set(info["site_transitions"]) == set(info["sites"])
    assert all(n >= 1 for n in info["site_transitions"].values())
    assert "merge_offset_mismatch" not in info
    assert not info.get("greedy_paired_sites"), \
        "all >=3-way ribbons pair from matched_shapes evidence"


def test_transitions_meet_steady_with_exact_offsets(built):
    g, segments, info, _ = built
    steadies_by_ck = defaultdict(list)
    for s in segments:
        if s.kind == "steady":
            steadies_by_ck[s.color_key].append(s)
    n_checked = 0
    for tr in segments:
        if tr.kind != "transition":
            continue
        matches = [m for s in steadies_by_ck[tr.color_key]
                   if (m := offset_at_shared_endpoint(tr, s)) is not None]
        assert len(matches) >= 2, \
            f"transition {tr.seg_id} ({tr.route_short_names}) endpoints " \
            f"must touch steady features"
        for got, expected in matches:
            assert got == pytest.approx(expected, abs=1e-9)
            n_checked += 1
    print(f"\n[real] {n_checked} transition-endpoint offsets matched exactly")
    assert n_checked >= 2 * sum(1 for s in segments
                                if s.kind == "transition")


def test_steady_composition_constant_query(built):
    """Sample every long steady feature in PostGIS; each sampled point's
    nearest graph edge must carry ONE line-set signature."""
    import psycopg
    g, segments, info, _ = built
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """WITH pts AS (
                 SELECT s.seg_id,
                        ST_LineInterpolatePoint(s.geom, gs.f) AS pt
                 FROM transit_line_segments s
                 CROSS JOIN LATERAL
                   generate_series(0.025, 0.975, 0.025) AS gs(f)
                 WHERE s.build_key = %(b)s AND s.kind = 'steady'
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
            {"b": BUILD_KEY})
        bad = cur.fetchall()
        cur.execute(
            """SELECT count(*) FROM transit_line_segments
               WHERE build_key = %s AND kind = 'steady' AND len_m > 61""",
            (BUILD_KEY,))
        (n_long,) = cur.fetchone()
    print(f"\n[real] {n_long} long steady features sampled, "
          f"{len(bad)} with mixed composition")
    assert n_long > 50
    assert bad == []


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
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT kind, count(*), sum(len_m)
               FROM transit_line_segments WHERE build_key = %s
               GROUP BY kind ORDER BY kind""", (BUILD_KEY,))
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
    mem = Counter(s.kind for s in segments)
    mem_len = defaultdict(float)
    for s in segments:
        mem_len[s.kind] += s.len_m
    for kind in ("steady", "transition"):
        assert db[kind][0] == mem[kind]
        assert db[kind][1] == pytest.approx(mem_len[kind], rel=1e-6)
    assert n_bad == 0, "NULL-by-kind contract and geometry validity"
    print(f"\n[real] emitted: "
          + ", ".join(f"{k}: {c} rows / {s / 1000:.2f} km"
                      for k, (c, s) in sorted(db.items())))
