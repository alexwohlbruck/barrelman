"""Real-data exam: CTA feed 29 rail through the full raster-skeleton path.

Builds the centerline graph from data/gtfs-processed/29.zip (OSM-matched
shapes; falls back to data/gtfs/29.zip with a loud warning) and checks:

  - the graph is ONE connected component (plan-view crossings such as the
    subways passing under the elevated Loop legs are junction nodes, so
    the Blue/Red subways join the component; Yellow meets Red/Purple at
    Howard),
  - total skeleton km is within 0.75-1.25x of the merge-width-fused
    network length, computed INDEPENDENTLY of the raster path as
    area(union of shapes buffered at MERGE_WIDTH/2)) / MERGE_WIDTH,
  - a sanity band against the raw union of used ways. NOTE: the raw union
    counts BOTH directional tracks of every double-tracked corridor
    (CTA's OSM tracks are mapped as separate parallel ways ~4-10 m
    apart), while fusing them into one centerline is this pipeline's
    whole job — so skeleton/union sits near 0.55, structurally below the
    naive 0.75 floor. The 0.75-1.25 contract is asserted against the
    fused reference above; the raw union gets a 0.40-1.0 band.

Prints node/edge counts, total km, wall time, and peak grid size.

Run (slow-ish — parses the feed's stop_times.txt once):
  uv run --with-requirements linegraph/requirements.txt \
      python -m pytest linegraph/tests/test_real.py -v -s
"""

import sys
import time

import pytest
from shapely.geometry import LineString
from shapely.ops import unary_union

from linegraph.build import REPO_ROOT, build_linegraph, collect_shapes
from linegraph.raster import MAX_GRID_BYTES, pick_epsg, project_shapes

FEED_PROCESSED = REPO_ROOT / "data" / "gtfs-processed" / "29.zip"
FEED_RAW = REPO_ROOT / "data" / "gtfs" / "29.zip"

MERGE_WIDTH = 18.0
RES = 2.0


@pytest.fixture(scope="module")
def real():
    if FEED_PROCESSED.exists():
        zip_path = FEED_PROCESSED
    elif FEED_RAW.exists():
        print(
            "\n" + "!" * 72
            + f"\nWARNING: {FEED_PROCESSED} missing — using RAW {FEED_RAW}."
            "\nRaw shapes are NOT OSM-matched; km/topology checks may drift."
            "\n" + "!" * 72,
            file=sys.stderr,
        )
        zip_path = FEED_RAW
    else:
        pytest.skip("no feed 29 zip (processed or raw)")

    t0 = time.perf_counter()
    shapes, n_patterns, n_skipped = collect_shapes(zip_path, "rail")
    t_load = time.perf_counter() - t0
    assert shapes, "feed 29 rail must yield shapes"

    t1 = time.perf_counter()
    lg = build_linegraph(
        shapes, MERGE_WIDTH, RES,
        build_key="chicago:l-v3", feed_id="29", mode="rail",
    )
    t_build = time.perf_counter() - t1

    print(
        f"\n[real] {n_patterns} patterns ({n_skipped} shapeless) -> "
        f"{len(shapes)} unique shapes in {t_load:.1f}s"
    )
    print(
        f"[real] {len(lg.nodes)} nodes, {len(lg.edges)} edges, "
        f"{lg.total_length_m() / 1000:.1f} km, "
        f"grid {lg.grid_shape[0]}x{lg.grid_shape[1]} px "
        f"({lg.grid_bytes / 1e6:.0f} MB peak), build {t_build:.1f}s "
        f"(wall incl. patterns {t_load + t_build:.1f}s)"
    )
    return lg, shapes


def test_counts_sane(real):
    lg, shapes = real
    assert len(lg.edges) >= 10, "CTA rail should vectorize into dozens of edges"
    assert len(lg.nodes) >= 10
    assert lg.grid_bytes <= MAX_GRID_BYTES
    junctions = [n for n in lg.nodes if n.degree >= 3]
    assert junctions, "the L has junctions"
    # every edge references existing nodes
    ids = {n.node_id for n in lg.nodes}
    assert all(e.from_node in ids and e.to_node in ids for e in lg.edges)


def test_single_connected_component(real):
    lg, _ = real
    comps = lg.components()
    if len(comps) != 1:
        by_node = {n.node_id: n for n in lg.nodes}
        for i, comp in enumerate(comps):
            sample = by_node[min(comp)]
            print(
                f"[real] component {i}: {len(comp)} nodes, "
                f"e.g. node {sample.node_id} at ({sample.lon:.5f}, {sample.lat:.5f})"
            )
    assert len(comps) == 1, (
        f"expected one component (subway/elevated crossings are plan-view "
        f"junctions; Yellow joins at Howard), got {len(comps)} — see prints"
    )


def test_total_km_vs_matched_network(real):
    lg, shapes = real
    skeleton_km = lg.total_length_m() / 1000.0

    epsg = pick_epsg(shapes)
    lines = [LineString(s) for s in project_shapes(shapes, epsg)]
    union_km = unary_union(lines).length / 1000.0
    fused_km = (
        unary_union([l.buffer(MERGE_WIDTH / 2.0, cap_style=2) for l in lines]).area
        / MERGE_WIDTH / 1000.0
    )
    print(
        f"[real] skeleton {skeleton_km:.1f} km | union of used ways "
        f"{union_km:.1f} km (both directional tracks) | merge-width-fused "
        f"network {fused_km:.1f} km | skeleton/fused {skeleton_km / fused_km:.3f} "
        f"| skeleton/union {skeleton_km / union_km:.3f}"
    )
    # the spec band, against the fused network the raster is asked to draw
    assert 0.75 <= skeleton_km / fused_km <= 1.25, (skeleton_km, fused_km)
    # raw-union sanity band (double-track fusing halves it; see module doc)
    assert 0.40 <= skeleton_km / union_km <= 1.00, (skeleton_km, union_km)


def test_loop_exam_topology(real):
    """Tower 18 geometry sanity: the Dearborn subway stays its own N-S
    corridor through the Loop interior; junctions with the elevated legs
    are point crossings, not merged geometry."""
    lg, _ = real
    # Dearborn subway interior corridor: ~lon -87.629, lat 41.877..41.8857
    dearborn = [
        e for e in lg.edges
        if e.length_m > 500
        and all(-87.6320 <= lon <= -87.6270 for lon, _ in e.coords)
        and any(41.878 < lat < 41.884 for _, lat in e.coords)
    ]
    assert dearborn, "expected a distinct Dearborn/State subway corridor edge"
    # no edge may run diagonally across the Loop interior: any edge whose
    # coords enter the interior window must stay in a narrow N-S band
    window = (-87.6335, 41.8775, -87.6265, 41.8852)
    for e in lg.edges:
        inside = [
            (lon, lat) for lon, lat in e.coords
            if window[0] < lon < window[2] and window[1] < lat < window[3]
        ]
        if len(inside) < 2:
            continue
        lon_spread = (max(p[0] for p in inside) - min(p[0] for p in inside))
        assert lon_spread < 0.0012, (
            f"edge {e.edge_id} sweeps {lon_spread:.4f} deg across the Loop "
            f"interior — fabricated geometry (LOOM's failure mode)"
        )
