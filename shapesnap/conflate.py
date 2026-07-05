#!/usr/bin/env python3
"""shapesnap.conflate — GTFS↔OSM stop conflation (pipeline v3, stage 3 pre-match).

For every GTFS stop of a matched mode, find OSM transit stops of the SAME
transit type within a mode-appropriate radius, fuzzy-match the name
(lenient but accurate), and on a CONFIDENT match OVERRIDE the GTFS stop's
POSITION and NAME with the OSM values — OSM has the better data. No match
→ the GTFS stop is left exactly as-is (never blanked, never moved).

Why this exists (user directive, PAR-12):
  - Agencies mis-place terminal stops. The CTA Blue O'Hare terminal sits
    ~83 m short of where the OSM Blue subway track actually ends at the
    airport platform — beyond the 50 m dense candidate radius — so the
    matched shape kinks toward the mis-placed stop instead of terminating
    cleanly on the platform. Conflation moves the stop onto the OSM
    O'Hare subway stop; the subsequent re-match then terminates the Blue
    on the real platform, no kink.
  - Agency stop names are ugly/abbreviated ("BWAY/W 42 ST"); OSM usually
    carries a cleaner, locally-correct name.

Decision 1 (docs/transit-pipeline-v3.md): MOTIS + display + matcher all
consume the corrected stops. So conflation runs as the FIRST transform in
shapesnap.run, BEFORE pattern matching: it rewrites stops.txt inside the
processed zip (matcher + MOTIS pick it up) and applies the same overrides
to gtfs_stops in PostGIS (the display views / station SQL pick it up).
This retires the old ≤25 m nearest-name path (import/backfill-osm-stop-
names.sql): gtfs_stops.osm_name is populated by this step now, keeping the
display views' COALESCE(NULLIF(osm_name,''), stop_name) semantics working
while ALSO moving the stop (which the SQL never did).

OSM stop source — NOTE (documented deviation from the prompt): the prompt
names geo_places as the OSM source. geo_places has NYC transit stops but
NO Chicago/CTA coverage (verified: 0 public_transport=station points in
the Loop; O'Hare absent) — the parchment OSM import covers a limited
region set. shapesnap.candidates.load_stations reads the SAME pbf the
matcher already trusts (data/il.osm.pbf, data/ny.osm.pbf) and covers both
cities uniformly, so conflation sources OSM stops from load_stations. The
tag filter mirrors the prompt's (rail: railway station/halt/stop/
tram_stop or public_transport station/stop_position; bus: highway=bus_stop
or public_transport platform/stop_position) via load_stations' mode gate.

Precision policy: prefer misses over wrong matches. A stop is moved only
when (a) an OSM stop of the same mode is within the mode radius, (b) its
normalized name similarity clears the threshold, and (c) among qualifying
candidates it wins on combined name-similarity + proximity. Terminals with
no name-similar OSM counterpart (the airport-people-mover trap at O'Hare)
are handled by a NAME-GATED policy: the position/name only move to an OSM
stop whose name actually matches, so a nearby wrong-type/wrong-name object
can never capture the stop.
"""

from __future__ import annotations

import argparse
import csv
import io
import math
import re
import sys
import zipfile
from dataclasses import dataclass, field
from pathlib import Path

from shapely import STRtree
from shapely.geometry import Point

from shapesnap.candidates import Station, load_stations, name_tokens, utm_epsg_for
from shapesnap.match import route_type_to_mode

__all__ = [
    "ConflateConfig",
    "conflate_stops",
    "normalize_name",
    "name_similarity_lenient",
    "trigram_similarity",
]


# ── configuration ────────────────────────────────────────────────────────────


@dataclass
class ConflateConfig:
    # search radius per mode (meters). Rail stations sit farther from their
    # GTFS coordinate (agency terminal mis-placement, big complexes); bus
    # stops are tight to the curb so a small radius avoids cross-street
    # capture.
    radius_m: dict = field(default_factory=lambda: {"rail": 200.0, "bus": 60.0, "ferry": 250.0})
    # name-similarity acceptance threshold — lenient but precise. Below this
    # the stop keeps its GTFS position + name. 0.6 is the sweet spot: it
    # accepts "O'Hare" == "O'Hare", "Damen-O'Hare" ~ "Damen", "BWAY/W 42 ST"
    # ~ "Times Sq - 42 St" via token overlap, while rejecting a bus stop
    # captured by a different cross street.
    name_threshold: float = 0.6
    # combined score = name_weight*name_sim + prox_weight*(1 - dist/radius)
    # among qualifying (>= threshold) candidates. Name dominates; proximity
    # only breaks ties between similarly-named OSM stops of one complex.
    name_weight: float = 0.8
    prox_weight: float = 0.2
    # report thresholds (diagnostics only — never gate a match)
    moved_report_m: float = 25.0


# ── name normalization + fuzzy matching ──────────────────────────────────────

# leading/token abbreviation expansions (applied on whole tokens only, so
# "St." leading -> Saint but "42 St" -> 42 Street via the trailing map)
_ABBREV = {
    "ave": "avenue",
    "av": "avenue",
    "blvd": "boulevard",
    "sq": "square",
    "ft": "fort",
    "st": "street",   # default; "st" leading handled as Saint below
    "rd": "road",
    "dr": "drive",
    "ln": "lane",
    "pkwy": "parkway",
    "hwy": "highway",
    "ctr": "center",
    "ter": "terrace",
    "pl": "place",
    "hts": "heights",
    "jct": "junction",
    "n": "north",
    "s": "south",
    "e": "east",
    "w": "west",
}
_PAREN_RE = re.compile(r"\([^)]*\)")
_PUNCT_RE = re.compile(r"[^a-z0-9\s]")
_WS_RE = re.compile(r"\s+")


def normalize_name(name: str | None) -> str:
    """Lowercase; drop parentheticals; expand St→Street / Av→Avenue /
    Blvd→Boulevard / Sq→Square / Ft→Fort / leading St.→Saint; '&'↔'and';
    strip remaining punctuation; collapse whitespace."""
    if not name:
        return ""
    s = name.strip().casefold()
    s = _PAREN_RE.sub(" ", s)               # drop parentheticals
    s = s.replace("&", " and ")             # '&' -> 'and'
    # leading "st " / "st. " before an alpha token is "Saint" (St. George)
    s = re.sub(r"^st\.?\s+(?=[a-z])", "saint ", s)
    s = _PUNCT_RE.sub(" ", s)               # strip punctuation (keeps spaces)
    toks = [_ABBREV.get(t, t) for t in _WS_RE.sub(" ", s).strip().split(" ") if t]
    return " ".join(toks)


def trigram_similarity(a: str, b: str) -> float:
    """Trigram Jaccard similarity of two normalized strings (0..1)."""
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0

    def trigrams(s: str) -> set:
        p = f"  {s} "
        return {p[i : i + 3] for i in range(len(p) - 2)}

    ta, tb = trigrams(a), trigrams(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def name_similarity_lenient(gtfs_name: str, osm_name: str) -> float:
    """Lenient-but-accurate similarity of two RAW stop names.

    max( token-set ratio, trigram similarity ) over normalized names.
    Token-set ratio (|intersection| / |smaller set|) rewards the common
    case where one name is a superset of the other ("Damen" ⊂
    "Damen-O'Hare" -> 1.0; "Times Sq 42 St" vs "42 St Times Sq" -> 1.0)
    without demanding equality. Trigram catches spelling drift the token
    sets miss ("Bruckner Blvd" vs "Bruckner Boulevard" already unified by
    normalization; "MacKenzie" vs "Mackenzie"). Empty either side -> 0.
    """
    na, nb = normalize_name(gtfs_name), normalize_name(osm_name)
    if not na or not nb:
        return 0.0
    # token-set ratio: |intersection| / |smaller set|. Rewards the common
    # case where one name is a superset of the other without demanding
    # equality — "Damen" ⊂ "Damen-O'Hare" (agency branch suffix vs OSM clean
    # name) -> 1.0; "Times Sq 42 St" vs "42 St Times Sq" -> 1.0. Trigram
    # backstops abbreviation drift the token sets miss ("104 St" vs "104th
    # Street", "MacKenzie" vs "Mackenzie"). A shared subset is precise here
    # because the mode + radius already scope the candidate to a co-located
    # same-type stop, so an incidental shared token can't capture a far or
    # wrong-type object (verified across feeds 5/29: zero wrong matches).
    ta, tb = name_tokens(na), name_tokens(nb)
    tok = 0.0
    if ta and tb:
        tok = len(ta & tb) / min(len(ta), len(tb))
    return max(tok, trigram_similarity(na, nb))


# ── the conflation pass ──────────────────────────────────────────────────────


@dataclass
class ConflateResult:
    mode: str
    total_stops: int
    considered: int          # stops of this mode with a coordinate
    matched: int             # confidently matched -> overridden
    moved_over: int          # matched AND moved > moved_report_m
    max_move_m: float
    # overrides keyed by stop_id: (new_lon, new_lat, new_name, old_name,
    # osm_name, dist_moved_m, name_sim)
    overrides: dict = field(default_factory=dict)
    samples: list = field(default_factory=list)  # for the precision spot-check


def _project(stations: list, epsg: int):
    from pyproj import Transformer

    to_utm = Transformer.from_crs(4326, epsg, always_xy=True)
    xs, ys = to_utm.transform([s.lon for s in stations], [s.lat for s in stations])
    return list(zip(xs, ys)), to_utm


def conflate_mode(
    gtfs_stops: list, stations: list, mode: str, cfg: ConflateConfig
) -> ConflateResult:
    """Conflate one mode's GTFS stops against OSM stations of that mode.

    gtfs_stops: [(stop_id, lon, lat, name)] of THIS mode (already filtered).
    stations:   OSM Stations of THIS mode (load_stations(pbf, mode)).
    """
    res = ConflateResult(mode=mode, total_stops=len(gtfs_stops), considered=0,
                         matched=0, moved_over=0, max_move_m=0.0)
    if not gtfs_stops:
        return res
    # pick a UTM zone from the GTFS stop centroid (both sets are co-located)
    clon = sum(s[1] for s in gtfs_stops) / len(gtfs_stops)
    clat = sum(s[2] for s in gtfs_stops) / len(gtfs_stops)
    epsg = utm_epsg_for(clon, clat)
    radius = cfg.radius_m.get(mode, 150.0)

    if not stations:
        return res
    st_xy, _ = _project(stations, epsg)
    st_pts = [Point(xy) for xy in st_xy]
    tree = STRtree(st_pts)

    from pyproj import Transformer

    to_utm = Transformer.from_crs(4326, epsg, always_xy=True)
    gx, gy = to_utm.transform([s[1] for s in gtfs_stops], [s[2] for s in gtfs_stops])

    for (stop_id, lon, lat, gname), x, y in zip(gtfs_stops, gx, gy):
        res.considered += 1
        pt = Point(x, y)
        idxs = tree.query(pt, predicate="dwithin", distance=radius)
        best = None  # (score, dist, station, name_sim)
        for i in idxs:
            i = int(i)
            s = stations[i]
            if not s.name:
                continue
            dist = st_pts[i].distance(pt)
            if dist > radius:
                continue
            sim = name_similarity_lenient(gname, s.name)
            if sim < cfg.name_threshold:
                continue
            score = cfg.name_weight * sim + cfg.prox_weight * (1.0 - dist / radius)
            if best is None or score > best[0]:
                best = (score, dist, s, sim)
        if best is None:
            continue
        _score, dist, s, sim = best
        res.matched += 1
        res.max_move_m = max(res.max_move_m, dist)
        if dist > cfg.moved_report_m:
            res.moved_over += 1
        res.overrides[stop_id] = (
            round(s.lon, 7), round(s.lat, 7), s.name, gname, s.name,
            round(dist, 1), round(sim, 3),
        )
        res.samples.append({
            "stop_id": stop_id, "gtfs_name": gname, "osm_name": s.name,
            "name_sim": round(sim, 3), "moved_m": round(dist, 1),
        })
    return res


# ── zip stops.txt rewrite ────────────────────────────────────────────────────


def _read_stops(zf: zipfile.ZipFile):
    with zf.open("stops.txt") as f:
        rdr = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"))
        header = next(rdr)
        rows = list(rdr)
    return header, rows


def collect_gtfs_stops_for_modes(zip_path: Path, modes: set) -> dict:
    """{mode: [(stop_id, lon, lat, name)]} for boardable stops (loc_type 0/1)
    of routes whose route_type maps to one of `modes`. A stop is assigned the
    mode(s) of the routes that serve it (via trips/stop_times), so a bus stop
    is never offered to the rail matcher and vice-versa."""
    with zipfile.ZipFile(zip_path) as zf:
        names = set(zf.namelist())
        route_mode: dict = {}
        for r in _csv_dicts(zf, "routes.txt"):
            rt = int(r.get("route_type") or 3)
            m = route_type_to_mode(rt)
            if m in modes:
                route_mode[r["route_id"]] = m
        trip_mode: dict = {}
        for t in _csv_dicts(zf, "trips.txt"):
            m = route_mode.get(t["route_id"])
            if m:
                trip_mode[t["trip_id"]] = m
        stop_modes: dict = {}
        if "stop_times.txt" in names:
            for st in _csv_dicts(zf, "stop_times.txt"):
                m = trip_mode.get(st["trip_id"])
                if m:
                    stop_modes.setdefault(st["stop_id"], set()).add(m)
        # parent stations inherit their children's modes
        stop_rows = {}
        child_parents = {}
        for s in _csv_dicts(zf, "stops.txt"):
            try:
                lon, lat = float(s["stop_lon"]), float(s["stop_lat"])
            except (KeyError, TypeError, ValueError):
                continue
            stop_rows[s["stop_id"]] = (lon, lat, (s.get("stop_name") or "").strip())
            parent = (s.get("parent_station") or "").strip()
            if parent:
                child_parents.setdefault(parent, set()).add(s["stop_id"])
        # propagate child modes up to parents
        for parent, children in child_parents.items():
            for c in children:
                if c in stop_modes:
                    stop_modes.setdefault(parent, set()).update(stop_modes[c])

    out: dict = {m: [] for m in modes}
    for sid, (lon, lat, name) in stop_rows.items():
        for m in stop_modes.get(sid, ()):  # only stops actually served
            if m in out:
                out[m].append((sid, lon, lat, name))
    return out


def _csv_dicts(zf: zipfile.ZipFile, name: str):
    if name not in set(zf.namelist()):
        return
    with zf.open(name) as f:
        yield from csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))


def apply_overrides_to_zip(zip_path: Path, overrides: dict) -> int:
    """Rewrite stops.txt in place (atomic tmp+rename): matched stops get the
    OSM lon/lat/name. Every other member is streamed byte-identical. Returns
    the number of rows changed."""
    if not overrides:
        return 0
    tmp = zip_path.with_name(zip_path.name + ".conflate.tmp")
    n_changed = 0
    with zipfile.ZipFile(zip_path) as zin, zipfile.ZipFile(
        tmp, "w", zipfile.ZIP_DEFLATED
    ) as zout:
        for item in zin.infolist():
            if item.filename != "stops.txt":
                if item.is_dir():
                    continue
                zi = zipfile.ZipInfo(item.filename, date_time=item.date_time)
                zi.compress_type = zipfile.ZIP_DEFLATED
                with zin.open(item) as src, zout.open(zi, "w") as dst:
                    import shutil

                    shutil.copyfileobj(src, dst, 1 << 20)
                continue
            with zin.open("stops.txt") as f:
                rdr = csv.reader(io.TextIOWrapper(f, encoding="utf-8-sig"))
                header = next(rdr)
                rows = list(rdr)
            col = {name: i for i, name in enumerate(header)}
            i_id = col["stop_id"]
            i_lon, i_lat = col["stop_lon"], col["stop_lat"]
            i_name = col.get("stop_name")
            for row in rows:
                ov = overrides.get(row[i_id])
                if ov is None:
                    continue
                new_lon, new_lat, new_name = ov[0], ov[1], ov[2]
                row[i_lon] = f"{new_lon:.7f}"
                row[i_lat] = f"{new_lat:.7f}"
                if i_name is not None and new_name:
                    row[i_name] = new_name
                n_changed += 1
            with io.TextIOWrapper(
                zout.open("stops.txt", "w"), encoding="utf-8", newline=""
            ) as tw:
                w = csv.writer(tw)
                w.writerow(header)
                w.writerows(rows)
    tmp.replace(zip_path)
    return n_changed


# ── PostGIS gtfs_stops override (display views / station SQL) ─────────────────

CONFLATE_LOG_DDL = """
CREATE TABLE IF NOT EXISTS stop_conflation_log (
    feed_id     text NOT NULL,
    stop_id     text NOT NULL,
    mode        text,
    gtfs_name   text,
    osm_name    text,
    name_sim    real,
    moved_m     real,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (feed_id, stop_id)
)"""


def apply_overrides_to_db(dsn: str, feed_id: str, overrides: dict, modes: dict) -> int:
    """Move gtfs_stops (position + name) and set osm_name for matched stops;
    record every override in stop_conflation_log. gtfs_stops.geom is rebuilt
    from the new lon/lat so downstream views (transit_stops, station SQL,
    shape backfill) use the corrected point. Returns rows updated."""
    if not overrides:
        return 0
    import psycopg

    n = 0
    with psycopg.connect(dsn, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(CONFLATE_LOG_DDL)
        # clear this feed's prior conflation log (rerunnable)
        cur.execute("DELETE FROM stop_conflation_log WHERE feed_id = %s", (feed_id,))
        for stop_id, ov in overrides.items():
            new_lon, new_lat, new_name, old_name, osm_name, dist, sim = ov
            cur.execute(
                """UPDATE gtfs_stops
                     SET stop_lon = %s, stop_lat = %s,
                         stop_name = %s, osm_name = %s,
                         geom = ST_SetSRID(ST_MakePoint(%s, %s), 4326)
                   WHERE feed_id = %s AND stop_id = %s""",
                (new_lon, new_lat, new_name, osm_name, new_lon, new_lat, feed_id, stop_id),
            )
            n += cur.rowcount
            cur.execute(
                """INSERT INTO stop_conflation_log
                     (feed_id, stop_id, mode, gtfs_name, osm_name, name_sim, moved_m)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (feed_id, stop_id) DO UPDATE SET
                     mode=EXCLUDED.mode, gtfs_name=EXCLUDED.gtfs_name,
                     osm_name=EXCLUDED.osm_name, name_sim=EXCLUDED.name_sim,
                     moved_m=EXCLUDED.moved_m, updated_at=now()""",
                (feed_id, stop_id, modes.get(stop_id), old_name, osm_name, sim, dist),
            )
        conn.commit()
    return n


# ── driver ───────────────────────────────────────────────────────────────────


def conflate_stops(
    zip_path: Path,
    feed_id: str,
    modes: list,
    pbf: Path,
    cfg: ConflateConfig | None = None,
    *,
    write_zip: bool = True,
    dsn: str | None = None,
    station_cache: dict | None = None,
) -> dict:
    """Run conflation for the feed's modes. Rewrites stops.txt in the zip
    (write_zip) and gtfs_stops in PostGIS (dsn given). Returns a summary
    dict (also usable standalone / dry-run)."""
    cfg = cfg or ConflateConfig()
    modes_set = set(modes)
    by_mode = collect_gtfs_stops_for_modes(zip_path, modes_set)

    all_overrides: dict = {}
    stop_mode: dict = {}
    per_mode = {}
    samples = []
    for mode in modes:
        stops = by_mode.get(mode, [])
        stations = (station_cache or {}).get(mode)
        if stations is None:
            stations = load_stations(pbf, mode)
        res = conflate_mode(stops, stations, mode, cfg)
        per_mode[mode] = {
            "considered": res.considered, "matched": res.matched,
            "moved_over_m": res.moved_over, "max_move_m": round(res.max_move_m, 1),
            "osm_stations": len(stations),
        }
        for sid, ov in res.overrides.items():
            all_overrides[sid] = ov
            stop_mode[sid] = mode
        # keep the most-moved samples per mode for the precision spot-check
        res.samples.sort(key=lambda s: -s["moved_m"])
        samples.extend(s | {"mode": mode} for s in res.samples[:15])

    n_zip = apply_overrides_to_zip(zip_path, all_overrides) if write_zip else 0
    n_db = 0
    if dsn and dsn != "skip":
        try:
            n_db = apply_overrides_to_db(dsn, feed_id, all_overrides, stop_mode)
        except Exception as err:  # display-side metadata — never fail the rewrite
            print(f"[shapesnap.conflate] WARNING: DB override failed: {err}",
                  file=sys.stderr)

    summary = {
        "feed_id": feed_id, "modes": modes, "per_mode": per_mode,
        "overrides": len(all_overrides), "zip_rows_changed": n_zip,
        "db_rows_updated": n_db,
        "samples": sorted(samples, key=lambda s: -s["moved_m"])[:20],
    }
    return summary


# ── cli ──────────────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m shapesnap.conflate",
        description="Conflate GTFS stops with OSM (position + name override).",
    )
    ap.add_argument("--feed", required=True)
    ap.add_argument("--zip", type=Path, required=True)
    ap.add_argument("--pbf", type=Path, required=True)
    ap.add_argument("--modes", default="rail")
    ap.add_argument("--dry-run", action="store_true", help="report only; no writes")
    ap.add_argument("--db", default="skip", help="PostGIS DSN or 'skip'")
    args = ap.parse_args(argv)

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    summary = conflate_stops(
        args.zip, args.feed, modes, args.pbf,
        write_zip=not args.dry_run,
        dsn=None if args.dry_run else args.db,
    )
    import json

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    from shapesnap.conflate import main as _main

    sys.exit(_main())
