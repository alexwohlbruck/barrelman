"""Unit tests for shapesnap.run's zip-rewrite plumbing (no graph needed).

Covers: dist-unit inference (feet), the monotonic stop projection, and a
full rewrite_zip round trip on a tiny synthetic feed — trips of matched
patterns remapped, untouched trips keeping their original shapes,
shape_dist_traveled recomputed in the feed's own unit in both shapes.txt
and stop_times.txt, and non-GTFS members copied byte-identical.

  uv run --with-requirements shapesnap/requirements.txt \
      python -m pytest shapesnap/tests/test_run_unit.py -v
"""

import csv
import io
import math
import zipfile

from shapely.geometry import LineString

from shapesnap.run import (
    cumulative_m,
    fmt_dist,
    project_stops_monotonic,
    rewrite_zip,
    scan_shapes,
)

FT = 3.280839895


def _feet_dist(coords):
    """Cumulative feet along lon/lat coords (equirectangular)."""
    out = [0.0]
    for (lon1, lat1), (lon2, lat2) in zip(coords, coords[1:]):
        lat0 = math.radians(lat1)
        m = math.hypot((lon2 - lon1) * 111320.0 * math.cos(lat0), (lat2 - lat1) * 110574.0)
        out.append(out[-1] + m * FT)
    return out


# A straight ~2.2 km west-east line at Chicago's latitude
SHAPE_A = [(-87.65 + 0.005 * i, 41.88) for i in range(6)]
# An unrelated shape kept by an untouched (bus) trip
SHAPE_B = [(-87.70, 41.90), (-87.69, 41.90), (-87.68, 41.90)]


def _make_zip(path):
    dist_a = _feet_dist(SHAPE_A)
    dist_b = _feet_dist(SHAPE_B)
    shapes = ["shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence,shape_dist_traveled"]
    for i, (lon, lat) in enumerate(SHAPE_A):
        shapes.append(f"shpA,{lat},{lon},{i + 1},{round(dist_a[i])}")
    for i, (lon, lat) in enumerate(SHAPE_B):
        shapes.append(f"shpB,{lat},{lon},{i + 1},{round(dist_b[i])}")
    trips = [
        "route_id,service_id,trip_id,direction_id,shape_id",
        "R1,svc,t1,0,shpA",
        "R1,svc,t2,0,shpA",
        "B1,svc,t3,0,shpB",
    ]
    stop_times = ["trip_id,arrival_time,departure_time,stop_id,stop_sequence,shape_dist_traveled"]
    for tid in ("t1", "t2"):
        for k, seq in enumerate((1, 2, 3)):
            stop_times.append(f"{tid},08:0{k}:00,08:0{k}:00,s{k},{seq},{round(dist_a[k * 2])}")
    stop_times.append("t3,09:00:00,09:00:00,b0,1,0")
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("shapes.txt", "\n".join(shapes) + "\n")
        zf.writestr("trips.txt", "\n".join(trips) + "\n")
        zf.writestr("stop_times.txt", "\n".join(stop_times) + "\n")
        zf.writestr("agency.txt", "agency_id,agency_name\na,Test\n")


def _rows(zf, name):
    with zf.open(name) as f:
        return list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig")))


def test_scan_shapes_infers_feet(tmp_path):
    path = tmp_path / "feed.zip"
    _make_zip(path)
    with zipfile.ZipFile(path) as zf:
        info = scan_shapes(zf)
    assert info["present"] and info["has_dist_col"] and info["has_dist_values"]
    assert info["unit"] == "ft"
    assert info["integral"] is True
    assert info["rows"] == len(SHAPE_A) + len(SHAPE_B)


def test_project_stops_monotonic_never_backtracks():
    line = LineString([(0, 0), (100, 0), (100, 100), (0, 100)])  # U shape
    # second stop is nearest to the FIRST leg but must land after the first
    stops = [(95, 0), (99, 20), (50, 100)]
    along = project_stops_monotonic(line, stops)
    assert along == sorted(along)
    assert along[0] <= along[1] <= along[2] <= line.length
    assert abs(along[2] - 250.0) < 1.0


def test_rewrite_zip_round_trip(tmp_path):
    path = tmp_path / "feed.zip"
    _make_zip(path)

    # pretend the matcher produced a slightly different geometry for shpA's
    # pattern (t1 + t2) and left the bus trip t3 alone
    new_coords = [(-87.6501 + 0.005 * i, 41.8801) for i in range(6)]
    lat0 = math.radians(41.8801)
    xy = [((lon + 87.65) * 111320.0 * math.cos(lat0), (lat - 41.88) * 110574.0)
          for lon, lat in new_coords]
    cum = cumulative_m(xy)
    snap_id = "snap_abcdef123456"
    stop_alongs = [0.0, cum[2], cum[5]]
    stats = rewrite_zip(
        path,
        remap={"t1": snap_id, "t2": snap_id},
        snap_shapes={snap_id: (new_coords, cum)},
        trip_dists={"t1": stop_alongs, "t2": stop_alongs},
    )

    assert stats["trips_remapped"] == 2
    assert stats["retained_original_shapes"] == 1  # shpB via t3
    assert stats["dist_unit"] == "ft"
    assert stats["shapes_rows_before"] == len(SHAPE_A) + len(SHAPE_B)
    assert stats["shapes_rows_after"] == len(SHAPE_B) + len(new_coords)
    assert stats["stop_times_rows_updated"] == 6

    with zipfile.ZipFile(path) as zf:
        shape_rows = _rows(zf, "shapes.txt")
        by_shape = {}
        for r in shape_rows:
            by_shape.setdefault(r["shape_id"], []).append(r)
        assert set(by_shape) == {"shpB", snap_id}
        # snap dist recomputed in the feed's unit (feet), integral
        snap_rows = sorted(by_shape[snap_id], key=lambda r: int(r["shape_pt_sequence"]))
        total_ft = float(snap_rows[-1]["shape_dist_traveled"])
        assert abs(total_ft - cum[-1] * FT) < 2.0
        assert all(float(r["shape_dist_traveled"]) == int(float(r["shape_dist_traveled"]))
                   for r in snap_rows)

        trip_rows = {r["trip_id"]: r for r in _rows(zf, "trips.txt")}
        assert trip_rows["t1"]["shape_id"] == snap_id
        assert trip_rows["t2"]["shape_id"] == snap_id
        assert trip_rows["t3"]["shape_id"] == "shpB"

        st = _rows(zf, "stop_times.txt")
        t1 = sorted((r for r in st if r["trip_id"] == "t1"),
                    key=lambda r: int(r["stop_sequence"]))
        got = [float(r["shape_dist_traveled"]) for r in t1]
        want = [a * FT for a in stop_alongs]
        assert all(abs(g - w) < 2.0 for g, w in zip(got, want))
        # untouched trip keeps its original value
        t3 = [r for r in st if r["trip_id"] == "t3"]
        assert t3[0]["shape_dist_traveled"] == "0"

        # non-GTFS members copied through intact
        assert zf.read("agency.txt").decode() == "agency_id,agency_name\na,Test\n"


def test_fmt_dist_units():
    assert fmt_dist(100.0, FT, True) == "328"
    assert fmt_dist(100.0, 1.0, False) == "100.00"
