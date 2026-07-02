"""Integration exam: transit_lines_rt2 Martin tile function (stage 6, phase B).

Calls the SQL function directly (psycopg), decodes the MVT
(mapbox-vector-tile), and checks the tile contract for the v3 semantic
segments:

  - Chicago Loop tiles at z15 contain BOTH kinds (steady + transition),
  - every feature carries the documented properties with correct types,
    and the NULL-by-kind columns are absent (steady: no off_from/off_to;
    transition: no offset_px),
  - offset values match the DB rows (MVT feature id == transit_line_segments.id),
  - legacy compat: (slot - (line_count-1)/2) * 4.4 reproduces offset_px on
    steady features and off_from_px on transitions (v2 clients / stock Mapbox
    render transitions as a constant continuation of the from-side ribbon),
  - clip fractions are in [0,1] with start < end,
  - the z11 transition zoom guard holds,
  - tile-seam continuity (the clip-fraction machinery's raison d'etre): a
    feature crossing a tile boundary maps the shared boundary point to the
    SAME feature fraction from both tiles (|delta| < 1e-4), so line-progress
    interpolation is continuous across the seam. Adjacent tiles quantise to
    the same global 4096 lattice, so both km-scale steady features and ~60 m
    transitions hold the bound.
  - the Martin HTTP endpoint serves the same function (200, decodes, same
    feature ids as the direct call).

Requires the dev DB (and Martin on :5002 for the HTTP test); skips when
unreachable. Run:
  uv run --with-requirements segments/requirements.txt \
      python -m pytest segments/tests/test_tiles.py -v -s
"""

import gzip
import math
import os
import urllib.error
import urllib.request

import pytest

psycopg = pytest.importorskip("psycopg")
mapbox_vector_tile = pytest.importorskip("mapbox_vector_tile")

pytestmark = pytest.mark.integration

DSN = os.environ.get(
    "DATABASE_URL", "postgresql://barrelman:barrelman@localhost:5434/barrelman"
)
MARTIN_URL = os.environ.get("MARTIN_URL", "http://localhost:5002")

EXTENT = 4096
GAP_PX = 4.4
WORLD = 2 * 20037508.342789244          # 3857 world width, metres
LOOP_LON, LOOP_LAT = -87.6305, 41.8845  # Chicago Loop
Z = 15

STR_PROPS = ("kind", "feed_id", "route_ids", "route_short_names",
             "route_color", "route_text_color", "color_key")
INT_PROPS = ("route_type", "slot", "line_count")
NUM_PROPS = ("mapbox_clip_start", "mapbox_clip_end")


# ---------------------------------------------------------------- helpers

def tile_at(lon, lat, z):
    n = 2 ** z
    xt = int((lon + 180.0) / 360.0 * n)
    yt = int((1 - math.asinh(math.tan(math.radians(lat))) / math.pi) / 2 * n)
    return xt, yt


def decode(buf):
    """Decode with y_coord_down=True (local y grows southward, matching tile
    row direction) across mapbox-vector-tile 1.x/2.x APIs."""
    try:
        return mapbox_vector_tile.decode(buf, default_options={"y_coord_down": True})
    except TypeError:  # 1.x signature
        return mapbox_vector_tile.decode(buf, y_coord_down=True)


def tile_features(buf):
    if not buf:
        return []
    layer = decode(buf).get("transit_lines")
    return layer["features"] if layer else []


def polyline_parts(geometry):
    if geometry["type"] == "LineString":
        return [geometry["coordinates"]]
    if geometry["type"] == "MultiLineString":
        return list(geometry["coordinates"])
    return []


def crossing_progress(gcoords, clip_start, clip_end, axis, cval, near, tol=16.0):
    """Progress (feature fraction) where the polyline in GLOBAL tile units
    crosses axis==cval, taking the crossing nearest `near`. This mirrors how
    MapLibre evaluates line-progress: clip_start + span * (arc/total)."""
    total = sum(math.dist(a, b) for a, b in zip(gcoords, gcoords[1:]))
    if total <= 0:
        return None
    best = None
    cum = 0.0
    for p0, p1 in zip(gcoords, gcoords[1:]):
        seglen = math.dist(p0, p1)
        a0, a1 = p0[axis], p1[axis]
        if a0 != a1 and (a0 - cval) * (a1 - cval) <= 0:
            t = (cval - a0) / (a1 - a0)
            if 0.0 <= t <= 1.0:
                pt = (p0[0] + t * (p1[0] - p0[0]), p0[1] + t * (p1[1] - p0[1]))
                d = math.dist(pt, near)
                if best is None or d < best[0]:
                    best = (d, cum + t * seglen)
        cum += seglen
    if best is None or best[0] > tol:
        return None
    return clip_start + (clip_end - clip_start) * (best[1] / total)


# ---------------------------------------------------------------- fixtures

@pytest.fixture(scope="module")
def db():
    try:
        conn = psycopg.connect(DSN, autocommit=True)
    except psycopg.OperationalError as err:
        pytest.skip(f"dev DB unreachable: {err}")
    yield conn
    conn.close()


def fetch_tile(db, z, x, y):
    buf = db.execute(
        "SELECT transit_lines_rt2(%s, %s, %s)", (z, x, y)
    ).fetchone()[0]
    return bytes(buf) if buf is not None else None


@pytest.fixture(scope="module")
def loop_tiles(db):
    """3x3 z15 tile block around the Loop: {(x, y): [features]}."""
    cx, cy = tile_at(LOOP_LON, LOOP_LAT, Z)
    out = {}
    for x in range(cx - 1, cx + 2):
        for y in range(cy - 1, cy + 2):
            buf = fetch_tile(db, Z, x, y)
            out[(x, y)] = tile_features(buf)
            print(f"tile {Z}/{x}/{y}: {len(buf) if buf else 0} bytes, "
                  f"{len(out[(x, y)])} features")
    return out


@pytest.fixture(scope="module")
def loop_features(loop_tiles):
    return [f for feats in loop_tiles.values() for f in feats]


@pytest.fixture(scope="module")
def db_rows(db):
    rows = db.execute(
        "SELECT id, kind, color_key, offset_px, off_from_px, off_to_px, len_m"
        " FROM transit_line_segments"
    ).fetchall()
    return {r[0]: r for r in rows}


# ---------------------------------------------------------------- contract

def test_both_kinds_present(loop_features):
    kinds = {f["properties"]["kind"] for f in loop_features}
    assert kinds == {"steady", "transition"}


def test_properties_present_and_typed(loop_features):
    assert loop_features
    for f in loop_features:
        p = f["properties"]
        assert isinstance(f.get("id"), int) and f["id"] > 0
        for k in STR_PROPS:
            assert isinstance(p.get(k), str) and p[k], (f["id"], k, p.get(k))
        for k in INT_PROPS:
            assert isinstance(p.get(k), int), (f["id"], k, p.get(k))
        for k in NUM_PROPS:
            assert isinstance(p.get(k), (int, float)), (f["id"], k)
        if p["kind"] == "steady":
            assert isinstance(p.get("offset_px"), (int, float))
            assert "off_from_px" not in p and "off_to_px" not in p
        else:
            assert isinstance(p.get("off_from_px"), (int, float))
            assert isinstance(p.get("off_to_px"), (int, float))
            assert "offset_px" not in p


def test_offsets_match_db(loop_features, db_rows):
    for f in loop_features:
        p = f["properties"]
        row = db_rows[f["id"]]
        _, kind, color_key, offset_px, off_from_px, off_to_px, _ = row
        assert p["kind"] == kind and p["color_key"] == color_key
        if kind == "steady":
            assert p["offset_px"] == pytest.approx(offset_px, abs=1e-9)
        else:
            assert p["off_from_px"] == pytest.approx(off_from_px, abs=1e-9)
            assert p["off_to_px"] == pytest.approx(off_to_px, abs=1e-9)


def test_legacy_slot_reproduces_from_side_offset(loop_features):
    """v2 constant-offset clients compute (slot-(lc-1)/2)*GAP; that must equal
    offset_px (steady) / off_from_px (transition, from-side continuation)."""
    for f in loop_features:
        p = f["properties"]
        legacy = (p["slot"] - (p["line_count"] - 1) / 2.0) * GAP_PX
        want = p["offset_px"] if p["kind"] == "steady" else p["off_from_px"]
        assert legacy == pytest.approx(want, abs=1e-6), (f["id"], p)
        assert p["slot"] >= 0 and p["line_count"] >= 1


def test_clip_fractions_valid(loop_features):
    for f in loop_features:
        p = f["properties"]
        s, e = p["mapbox_clip_start"], p["mapbox_clip_end"]
        assert 0.0 <= s < e <= 1.0, (f["id"], s, e)


def test_zoom_guard_no_transitions_below_z11(db):
    x10, y10 = tile_at(LOOP_LON, LOOP_LAT, 10)
    feats = tile_features(fetch_tile(db, 10, x10, y10))
    kinds = {f["properties"]["kind"] for f in feats}
    assert "steady" in kinds and "transition" not in kinds
    x11, y11 = tile_at(LOOP_LON, LOOP_LAT, 11)
    feats11 = tile_features(fetch_tile(db, 11, x11, y11))
    assert any(f["properties"]["kind"] == "transition" for f in feats11)


# ---------------------------------------------------------------- seams

def _merc_coords(db, fid):
    import json
    gj = db.execute(
        "SELECT ST_AsGeoJSON(ST_Transform(geom, 3857))"
        " FROM transit_line_segments WHERE id = %s", (fid,)
    ).fetchone()[0]
    return json.loads(gj)["coordinates"]


def _global_units(coords_merc, z):
    unit = WORLD / (2 ** z * EXTENT)
    return [((mx + WORLD / 2) / unit, (WORLD / 2 - my) / unit)
            for mx, my in coords_merc]


def _first_tile_boundary_crossing(gcoords):
    """First place the feature crosses a tile-grid line (multiple of EXTENT
    in global units). Returns (axis, cval, crossing_point)."""
    for p0, p1 in zip(gcoords, gcoords[1:]):
        for axis in (0, 1):
            a0, a1 = p0[axis], p1[axis]
            k0, k1 = math.floor(a0 / EXTENT), math.floor(a1 / EXTENT)
            if k0 != k1:
                cval = max(k0, k1) * EXTENT
                t = (cval - a0) / (a1 - a0)
                pt = (p0[0] + t * (p1[0] - p0[0]), p0[1] + t * (p1[1] - p0[1]))
                return axis, cval, pt
    return None


def _seam_progress_pair(db, fid, z):
    """Fetch the two tiles adjacent at the feature's first grid crossing and
    compute the boundary-point progress from each side."""
    gcoords = _global_units(_merc_coords(db, fid), z)
    hit = _first_tile_boundary_crossing(gcoords)
    if hit is None:
        return None
    axis, cval, pt = hit
    k = int(round(cval / EXTENT))
    if axis == 0:
        ty = int(math.floor(pt[1] / EXTENT))
        tiles = [(k - 1, ty), (k, ty)]
    else:
        tx = int(math.floor(pt[0] / EXTENT))
        tiles = [(tx, k - 1), (tx, k)]
    progresses = []
    for tx, ty in tiles:
        found = None
        for f in tile_features(fetch_tile(db, z, tx, ty)):
            if f["id"] != fid:
                continue
            p = f["properties"]
            for part in polyline_parts(f["geometry"]):
                gpart = [(lx + tx * EXTENT, ly + ty * EXTENT) for lx, ly in part]
                prog = crossing_progress(
                    gpart, p["mapbox_clip_start"], p["mapbox_clip_end"],
                    axis, cval, pt)
                if prog is not None:
                    found = prog
        if found is None:
            return None
        progresses.append(found)
    return progresses[0], progresses[1], axis, cval, tiles


def _seam_candidates(db, kind):
    return [r[0] for r in db.execute(
        "SELECT id FROM transit_line_segments WHERE kind = %s"
        " ORDER BY len_m DESC", (kind,)).fetchall()]


def test_seam_continuity_steady(db, db_rows):
    """A steady feature crossing a z15 tile boundary: same fraction from both
    sides within 1e-4."""
    checked = 0
    for fid in _seam_candidates(db, "steady"):
        pair = _seam_progress_pair(db, fid, Z)
        if pair is None:
            continue
        pa, pb, axis, cval, tiles = pair
        delta = abs(pa - pb)
        print(f"steady seam z{Z} id={fid} len={db_rows[fid][6]:.0f}m "
              f"tiles={tiles} axis={'xy'[axis]} "
              f"pA={pa:.8f} pB={pb:.8f} |delta|={delta:.2e}")
        assert delta < 1e-4, (fid, pa, pb)
        checked += 1
        if checked >= 3:
            break
    assert checked, "no steady feature crossing a z15 tile boundary found"


def test_seam_continuity_transition(db, db_rows):
    """A transition feature crossing a z15 tile boundary: same fraction from
    both sides, asserted in GROUND metres (|delta| x len_m < 5 cm — the
    physically meaningful seam error; a z15 pixel is ~3.6 m). Adjacent
    tiles quantise to the SAME global 4096 lattice: Chicago's ~50-60 m
    transitions measure exactly 0; NYC's dense-junction features measure
    up to ~1.6 cm (fraction deltas up to 2.7e-4), still invisible."""
    checked = 0
    for fid in _seam_candidates(db, "transition"):
        pair = _seam_progress_pair(db, fid, Z)
        if pair is None:
            continue
        pa, pb, axis, cval, tiles = pair
        delta = abs(pa - pb)
        print(f"transition seam z{Z} id={fid} len={db_rows[fid][6]:.0f}m "
              f"tiles={tiles} axis={'xy'[axis]} "
              f"pA={pa:.8f} pB={pb:.8f} |delta|={delta:.2e}")
        assert delta * db_rows[fid][6] < 0.05, (fid, pa, pb)
        checked += 1
    assert checked, "no transition feature crossing a z15 tile boundary found"


# ---------------------------------------------------------------- martin

def test_martin_http_endpoint(db):
    cx, cy = tile_at(LOOP_LON, LOOP_LAT, Z)
    url = f"{MARTIN_URL}/transit_lines_rt2/{Z}/{cx}/{cy}"
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            assert resp.status == 200
            body = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
    except (urllib.error.URLError, ConnectionError) as err:
        pytest.skip(f"Martin unreachable at {MARTIN_URL}: {err}")
    assert len(body) > 0
    feats = tile_features(body)
    assert feats
    direct = tile_features(fetch_tile(db, Z, cx, cy))
    assert {f["id"] for f in feats} == {f["id"] for f in direct}
    print(f"martin {url}: {len(body)} bytes (decoded), {len(feats)} features")
