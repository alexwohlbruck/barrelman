#!/usr/bin/env python3
"""segments acceptance exam (stage 6, phase C).

Validates the EMITTED transit_line_segments rows of chicago:l-v3 (the
exact rows transit_lines_rt2 serves) against the v3 contract that killed
the v2 attempt, plus the receipts:

  0  emitted rows equal a fresh deterministic rebuild (kind, colour,
     offsets, slots, lengths, endpoint geometry) — a stale table fails
     the exam; rerun `python -m segments.build --build-key chicago:l-v3
     --emit` first
  1  C1 — offsets transition ONLY at the known transition sites: the
     sites derived here (junction deg>=3 / deg-2 composition change)
     equal lineorder's stage-5 transition-site list coordinate-for-
     coordinate (18 sites: 17 junctions + Howard); walking every ribbon
     end to end, ALL feature-boundary offsets match exactly (direction-
     aware sign), so the only offset changes anywhere are the
     off_from_px -> off_to_px ramps INSIDE transition features, every
     one anchored to a listed site; head-on steady meets at a site with
     unequal offsets are allowed only for VERIFIED unpaired stubs (two
     steady features whose shared endpoint is a listed site node in
     both .sites and whose corridors both terminate there — no through
     pairing); every feature end is then ACCOUNTED for: shared with
     another same-ribbon end, sitting on one of the ribbon's genuine
     termini (degree-1 nodes of its corridor subgraph, every terminus
     occupied), or — long bands only in practice — resting ON a
     same-ribbon feature's interior (a branch-divergence twin whose
     shared endpoint a consumed-corridor merge swallowed into the
     through-chain); a dropped or mislocated transition orphans an end
     mid-ribbon over NOTHING and fails, closing the gap pairwise
     matching alone would miss
  2  C3 — every transition's ground length within [0.4, 1.1] x
     transition_len_m (short-corridor shrink allowed at the low end,
     nothing longer); vertex spacing <= densify_step_m on transitions
  3  fillet — every transition's min discrete curvature radius (aeqd
     metres, circumradius over vertex triples, measured on the EMITTED
     DB geometry so quantization kinks cannot hide) meets the
     configured floor: min radius target = line_count * gap_px *
     fillet_radius_factor, relaxed only by a recorded clamp (short
     halves) or sharper INHERITED track curvature (recorded pre-fillet,
     corner excluded); worst case reported; no self-intersections
     (ST_IsSimple over every emitted row)
  4  coverage — per ribbon (colour), steady+transition lengths cover
     the ribbon's corridors within 1% NET of legitimately shared
     divergence tails; no geometric overlap > 1 m between features of
     a ribbon except the branch-divergence tail shared by two
     site-anchored features at the same site (kind-agnostic — a
     straight equal-offset twin skip-classifies to steady; reported,
     capped at transition_len_m (way-graph era: twins share real
     track to the physical switch); the shared typed end is not
     required — long-band merges relocate it into a chain interior)
  5  receipt — per-Loop-leg feature table (kind, routes, slot/count,
     offsets) for Lake / Wabash / Van Buren / Wells + the interior
     subways, plus every Loop-window transition with its ramp

Read-only apart from nothing — the exam never writes. Exits non-zero if
any check fails. Run:

  uv run --with-requirements segments/requirements.txt \
      python segments/exam/segments_exam.py

`--build-key` (default chicago:l-v3) points the generic checks (0-4) at
another build; the Chicago-specific assertions (check 1's 18-site
inventory, check 3's absolute clamp cap, check 5's Loop receipt) only
run for the default build (check 3 uses a proportional 30% clamp cap on
other builds — refit-collapsed crossing rungs clamp by design).

Zoom bands: checks 0-4 run once per SegmentConfig.bands entry (each band
is a complete feature set with its own transition length; the emitted
rows are validated band by band against a fresh rebuild at that length).
`--band <minzoom>` restricts to one band. The Chicago-verbatim bounds
(check 2's single-site length bound, check 3's absolute clamp cap) apply
only to the DEFAULT band (z15 / 60 m) — longer bands consume short
corridors and merge transitions far more often BY DESIGN, so they use
the site-count length bound and the proportional clamp cap everywhere.
Check 5's receipt reads the default band.
"""

from __future__ import annotations

import math
import sys
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from segments.corridors import DEFAULT_DSN, load_graph, walk_corridors  # noqa: E402
from segments.segment import (LocalProj, SegmentConfig, _circumradius,  # noqa: E402
                              band_ranges, build_segments,
                              transition_sites)

BUILD = "chicago:l-v3"      # default; overridden by --build-key in main()
CHICAGO_BUILD = "chicago:l-v3"
CFG = SegmentConfig()
DEFAULT_BAND = max(mz for mz, _ in CFG.bands)   # the z15 / 60 m band

# same Loop window as the stage-4/5 exams
LOOP_WINDOW = (-87.6355, 41.8755, -87.6245, 41.8875)
# leg centrelines (from the emitted bundles; tolerance segregates the
# State/Dearborn subways from the Wabash/Wells legs)
LOOP_LEGS = {
    "Lake":      ("EW", 41.88575),
    "Van Buren": ("EW", 41.87693),
    "Wells":     ("NS", -87.63379),
    "Wabash":    ("NS", -87.62619),
}
LEG_TOL_M = 60.0

ENDPOINT_TOL_M = 0.5
OFFSET_TOL_PX = 1e-9

FAILURES: list[str] = []
BAND_TAG = ""       # set by main()'s band loop for failure attribution


def report(check: str, ok: bool, detail: str = "") -> None:
    print(f"  -> {'PASS' if ok else 'FAIL'}{': ' + detail if detail else ''}")
    if not ok:
        FAILURES.append(f"{BAND_TAG}{check}: {detail}")


def in_window(lon: float, lat: float, box=LOOP_WINDOW) -> bool:
    w, s, e, n = box
    return w <= lon <= e and s <= lat <= n


# ------------------------------------------------------------- rebuild

def load_inputs():
    from segments.build import load_shapes
    g = load_graph(BUILD, DEFAULT_DSN)
    shapes = load_shapes(g, DEFAULT_DSN)
    lon0 = sum(n.lon for n in g.nodes.values()) / len(g.nodes)
    lat0 = sum(n.lat for n in g.nodes.values()) / len(g.nodes)
    return g, shapes, LocalProj(lon0, lat0)


def rebuild_band(g, shapes, proj, cfg):
    segments, info = build_segments(g, cfg, shapes=shapes)
    for s in segments:
        s.xy = proj.to_xy(s.coords)
    return segments, info


def check0_db_matches_rebuild(segments, band_minzoom):
    print("\nCHECK 0 — emitted rows equal a fresh deterministic rebuild")
    import psycopg

    with psycopg.connect(DEFAULT_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT seg_id, kind, color_key, slot, line_count, offset_px,
                      off_from_px, off_to_px, len_m, ST_NPoints(geom),
                      ST_X(ST_StartPoint(geom)), ST_Y(ST_StartPoint(geom)),
                      ST_X(ST_EndPoint(geom)), ST_Y(ST_EndPoint(geom))
               FROM transit_line_segments
               WHERE build_key = %s AND band_minzoom = %s
               ORDER BY seg_id""", (BUILD, band_minzoom))
        db = {r[0]: r[1:] for r in cur.fetchall()}

    mism = []
    for s in segments:
        row = db.get(s.seg_id)
        if row is None:
            mism.append((s.seg_id, "missing from DB"))
            continue
        kind, ck, slot, lc, off, offa, offb, lm, npts, x0, y0, x1, y1 = row

        def neq(a, b, tol=OFFSET_TOL_PX):
            if a is None or b is None:
                return (a is None) != (b is None)
            return abs(a - b) > tol

        if (kind != s.kind or ck != s.color_key or slot != s.slot
                or lc != s.line_count or neq(off, s.offset_px)
                or neq(offa, s.off_from_px) or neq(offb, s.off_to_px)
                or abs(lm - s.len_m) > 1e-6 * max(1.0, s.len_m)
                or npts != len(s.coords)
                or neq(x0, s.coords[0][0], 1e-6)
                or neq(y0, s.coords[0][1], 1e-6)
                or neq(x1, s.coords[-1][0], 1e-6)
                or neq(y1, s.coords[-1][1], 1e-6)):
            mism.append((s.seg_id, "row differs"))
    extra = sorted(set(db) - {s.seg_id for s in segments})
    print(f"  DB rows {len(db)}, rebuilt {len(segments)}, "
          f"mismatched {len(mism)}, extra {len(extra)}")
    report("check0.db-in-sync", not mism and not extra,
           f"{len(mism)} mismatched + {len(extra)} extra rows "
           f"(rerun segments.build --emit if stale)")


# ------------------------------------------------- C1: sites + ribbon walk

def lineorder_site_coords():
    """Stage-5 transition sites straight from lineorder's own loader —
    the independent cross-check the C1 contract is anchored to."""
    from lineorder.model import load_build
    inst = load_build(BUILD, DEFAULT_DSN)
    g, reg = inst.graph, inst.registry
    out = {}
    for nid in sorted(g.nodes):
        inc = g.order[nid]
        if len(inc) >= 3:
            kind = "junction"
        elif len(inc) == 2:
            e, f = inc
            se = {(reg.get(u).feed_id, reg.get(u).route_id)
                  for u in g.edges[e].lines}
            sf = {(reg.get(u).feed_id, reg.get(u).route_id)
                  for u in g.edges[f].lines}
            if se == sf:
                continue
            kind = "composition"
        else:
            continue
        n = g.nodes[nid]
        out[(round(n.x, 7), round(n.y, 7))] = kind
    return out


def check1_c1_contract(g, proj, segments, chicago: bool = True,
                       cfg: SegmentConfig = CFG, site_checks: bool = True):
    print("\nCHECK 1 — C1: offsets transition only at the known sites")
    sites = transition_sites(g)
    site_coords = {(round(g.nodes[nid].lon, 7), round(g.nodes[nid].lat, 7)):
                   kind for nid, kind in sites.items()}
    n_junc = sum(1 for k in sites.values() if k == "junction")
    n_comp = sum(1 for k in sites.values() if k == "composition")
    howard = [g.nodes[nid].label for nid, k in sites.items()
              if k == "composition"]
    print(f"  {len(sites)} transition sites: {n_junc} junctions, "
          f"{n_comp} deg-2 composition changes {howard}")
    if chicago and site_checks:
        # way-graph era pin: 10 real junctions + Howard's composition
        # change. Re-pinned 15 -> 10 after the window flap guard: the
        # North Side P/Red co-run used to tile into three cross windows
        # (6.3 km + 2.9 km + 115 m twins) whose flap seams each minted a
        # junction; the Schmitt-trigger coalescing forms it as ONE
        # 12.3 km bundle (loop exam pins Tower 18 + the leg bundles
        # unchanged).
        report("check1.site-inventory",
               len(sites) == 11 and n_junc == 10 and howard == ["Howard"],
               f"expected 11 sites (10 junctions + Howard), got {len(sites)} "
               f"({n_junc} junctions, composition at {howard})")

    if site_checks:  # band-independent (graph-derived): run once
        lo_sites = lineorder_site_coords()
        only_seg = sorted(set(site_coords) - set(lo_sites))
        only_lo = sorted(set(lo_sites) - set(site_coords))
        kind_diff = [c for c in site_coords
                     if c in lo_sites and lo_sites[c] != site_coords[c]]
        report("check1.lineorder-cross-check",
               not only_seg and not only_lo and not kind_diff,
               f"site lists agree coordinate-for-coordinate "
               f"({len(lo_sites)} lineorder vs {len(site_coords)} segments; "
               f"{len(only_seg)}/{len(only_lo)} one-sided, "
               f"{len(kind_diff)} kind mismatches)")

    # every transition feature is anchored to listed sites only
    bad_anchor = [s.seg_id for s in segments if s.kind == "transition"
                  and (not s.sites
                       or any(nid not in sites for nid in s.sites))]
    report("check1.transitions-at-sites", not bad_anchor,
           f"{len(bad_anchor)} transition features not anchored to a "
           f"listed site")

    # ribbon walk: every shared feature endpoint carries equal offsets
    def end_tangent(s, at_start: bool):
        a, b = (s.xy[0], s.xy[1]) if at_start else (s.xy[-2], s.xy[-1])
        d = (b[0] - a[0], b[1] - a[1])
        n = math.hypot(*d) or 1.0
        return (d[0] / n, d[1] / n)

    def end_offset(s, at_start: bool):
        if s.kind == "steady":
            return s.offset_px
        return s.off_from_px if at_start else s.off_to_px

    by_ck = defaultdict(list)
    for s in segments:
        by_ck[s.color_key].append(s)

    # ribbon termini + corridor ends, for (a) the unpaired-stub escape
    # (verified no-through pairing, not just non-empty .sites) and
    # (b) end-to-end accounting: pairwise endpoint matching alone would
    # stay silent if a whole transition were dropped or mislocated (the
    # v2 failure class), because a gap has NO shared endpoints
    corridors = walk_corridors(g, cfg.gap_px)
    cor_by_id = {c.cid: c for c in corridors}
    node_xy = {nid: proj.to_xy([(n.lon, n.lat)])[0]
               for nid, n in g.nodes.items()}
    deg = defaultdict(lambda: defaultdict(int))
    for c in corridors:
        for r in c.ribbons:
            deg[r.color_key][c.node_a] += 1
            deg[r.color_key][c.node_b] += 1
    expected_term = {ck: {nid for nid, d in degs.items() if d == 1}
                     for ck, degs in deg.items()}

    def stub_meet(a, b, pa) -> bool:
        """True only for the deliberately-unpaired case: two steady
        stubs whose shared endpoint IS a transition-site node listed in
        both features' .sites, and both features' corridors terminate
        at that node (no through pairing exists there by construction —
        the builder emits a transition feature otherwise)."""
        if a.kind != "steady" or b.kind != "steady":
            return False
        shared = set(a.sites) & set(b.sites)
        for nid in shared:
            if nid not in sites:
                continue
            if math.dist(pa, node_xy[nid]) > ENDPOINT_TOL_M:
                continue
            ends_ok = True
            for f in (a, b):
                cor = cor_by_id.get(f.corridor_id)
                if cor is None or nid not in (cor.node_a, cor.node_b):
                    ends_ok = False
            if ends_ok:
                return True
        return False

    # same-colour endpoint clusters: a >=3-end cluster is a FORK where a
    # colour's ribbon genuinely splits (the D/N/R/W trunk forking into
    # Sea Beach + 4th Av at 59 St puts FOUR yellow ends on one
    # coordinate: two ribbons + two transitions). Pairwise offset
    # equality is ill-defined across the duplicated arms — the pair
    # check below stays strict for the plain 2-end case.
    clusters: dict = {}
    for ck in sorted(by_ck):
        pts: list = []
        for s in by_ck[ck]:
            for p in (s.xy[0], s.xy[-1]):
                for q in pts:
                    if math.dist(p, q[0]) <= ENDPOINT_TOL_M:
                        q[1] += 1
                        break
                else:
                    pts.append([p, 1])
        clusters[ck] = pts

    def fork_cluster(ck, pa) -> bool:
        return any(cnt >= 3 and math.dist(pa, p) <= ENDPOINT_TOL_M
                   for p, cnt in clusters.get(ck, ()))

    n_adj = n_mismatch = n_termini = n_fork = 0
    mismatches = []
    matched: set = set()
    for ck in sorted(by_ck):
        feats = by_ck[ck]
        for i, a in enumerate(feats):
            if math.dist(a.xy[0], a.xy[-1]) <= ENDPOINT_TOL_M:
                matched.add((a.seg_id, True))   # closed ring feature
                matched.add((a.seg_id, False))
            for b in feats[i + 1:]:
                for a_start in (True, False):
                    pa = a.xy[0] if a_start else a.xy[-1]
                    for b_start in (True, False):
                        pb = b.xy[0] if b_start else b.xy[-1]
                        if math.dist(pa, pb) > ENDPOINT_TOL_M:
                            continue
                        matched.add((a.seg_id, a_start))
                        matched.add((b.seg_id, b_start))
                        # a's offset at the joint is signed in a's travel
                        # frame. The frames agree when the end tangents
                        # are co-directional (flow-through AND the
                        # branch-sibling merges/splits at Loop corners,
                        # where two features share an end co-directed);
                        # anti-parallel tangents mean one frame is
                        # reversed, so the offset flips (head-on stub
                        # meets). Same rule as tests/helpers.py's proven
                        # offset_at_shared_endpoint.
                        ta = end_tangent(a, a_start)
                        tb = end_tangent(b, b_start)
                        dot = ta[0] * tb[0] + ta[1] * tb[1]
                        oa = end_offset(a, a_start)
                        ob = end_offset(b, b_start)
                        head_on = dot < 0
                        want = -ob if head_on else ob
                        n_adj += 1
                        if abs(oa - want) > OFFSET_TOL_PX:
                            if head_on and stub_meet(a, b, pa):
                                n_termini += 1  # unpaired stubs, allowed
                            elif fork_cluster(ck, pa):
                                n_fork += 1  # ribbon-split fork, allowed
                            else:
                                n_mismatch += 1
                                mismatches.append(
                                    (ck, a.seg_id, b.seg_id, oa, want))
    for m in mismatches[:10]:
        print(f"  VIOLATION {m}")
    print(f"  {n_adj} shared feature endpoints walked, "
          f"{n_termini} unpaired same-colour termini allowed, "
          f"{n_fork} ribbon-split fork ends allowed")
    report("check1.boundary-offsets-equal", n_mismatch == 0,
           f"{n_mismatch} offset discontinuities outside transitions")

    # end-to-end accounting: every feature end is either shared with
    # another same-ribbon feature end or sits at one of the ribbon's
    # genuine termini (degree-1 nodes of its corridor subgraph), and
    # every expected terminus is occupied — a dropped or mislocated
    # feature leaves an orphan end mid-ribbon and fails here.
    # Long bands add one legitimate case: a branch-divergence twin's
    # shared endpoint can be swallowed INTO the other chain's interior
    # by a consumed-corridor merge (Tower 18 at 480 m: Pink's enter and
    # exit movements share the branch tail; the through-chain merges
    # over the split point the twin still starts at). Such an end lies
    # ON a same-ribbon feature's interior (<= tol) — a genuinely
    # dropped transition instead leaves a coverage gap with NO feature
    # under the orphan end (and fails check4's coverage too).
    from shapely.geometry import LineString as _LS
    from shapely.geometry import Point as _Pt
    geoms_by_ck = {ck: [(s.seg_id, _LS(s.xy)) for s in by_ck[ck]]
                   for ck in by_ck}
    n_unmatched = n_interior = 0
    bad_ends = []
    seen_term: dict = defaultdict(set)
    for ck in sorted(by_ck):
        for s in by_ck[ck]:
            for at_start in (True, False):
                if (s.seg_id, at_start) in matched:
                    continue
                n_unmatched += 1
                pt = s.xy[0] if at_start else s.xy[-1]
                hits = [nid for nid in expected_term.get(ck, ())
                        if math.dist(pt, node_xy[nid]) <= ENDPOINT_TOL_M]
                if hits:
                    seen_term[ck].update(hits)
                elif any(sid != s.seg_id
                         and ls.distance(_Pt(pt)) <= ENDPOINT_TOL_M
                         for sid, ls in geoms_by_ck[ck]):
                    n_interior += 1  # merge-relocated divergence tail
                else:
                    bad_ends.append((ck, s.seg_id,
                                     "start" if at_start else "end"))
    n_expected = sum(len(v) for v in expected_term.values())
    vacant = [(ck, sorted(g.nodes[n].label or str(n) for n in miss))
              for ck in sorted(expected_term)
              if (miss := expected_term[ck] - seen_term[ck])]
    term_labels = sorted(g.nodes[n].label or str(n)
                         for ck in expected_term for n in expected_term[ck])
    print(f"  {n_unmatched} unshared feature ends over {n_expected} "
          f"expected ribbon termini ({n_interior} merge-relocated "
          f"divergence tails on same-ribbon interiors): {term_labels}")
    report("check1.ends-accounted",
           not bad_ends and not vacant
           and n_unmatched == n_expected + n_interior,
           f"{len(bad_ends)} orphan ends off-terminus {bad_ends[:6]}; "
           f"vacant termini {vacant}")


# ----------------------------------------------------------- C3: lengths

def check2_c3_contract(segments, chicago: bool = True,
                       cfg: SegmentConfig = CFG,
                       default_band: bool = True,
                       site_len: dict | None = None):
    print("\nCHECK 2 — C3: fixed ground length + densification")
    # Per-site clamped transition lengths (Mott Haven wye): a site whose
    # local corridors cannot support the band's length is clamped by the
    # builder (info["site_len_clamped"]) — its features are legitimately
    # SHORT for the band, so the relative bounds run against each
    # feature's EFFECTIVE length (min/sum of its sites' lengths).
    # Unclamped sites keep the band length: bounds identical to the
    # pre-clamp exam everywhere the clamp never fires.
    site_len = site_len or {}
    L = cfg.transition_len_m

    def eff_len(t):
        return min((site_len.get(nid, L) for nid in set(t.sites)),
                   default=L)

    # Way-graph-era calibration: an interlocking (Tower 18) is a cluster
    # of REAL switch nodes a few tens of meters apart, so a transition
    # squeezed between two of them is corridor-limited — the same ~21 m
    # feature in every band, unable to grow with the band's target. The
    # relative floor applies above this scale; a genuinely degenerate
    # (collapsed/empty) feature still fails, sitting far below it.
    lo_interlock = 20.0

    def lo_for(t):
        # short-corridor shrink is allowed by the contract; 0.4x is the
        # calibrated floor for every build (the NYC Bowling Green
        # cluster that briefly needed 0.3 was a skeleton artifact, since
        # fixed at the raster by sliver-hole filling).
        # NEAR-REVERSAL allowance: at a shallow fork whose node sits
        # past the branch divergence, the branch's transition legs
        # retrace the trunk and the fillet+cusp machinery collapses the
        # feature's NET path — the geometry class always sat at the
        # floor (Grand Army Plaza fork, pre-refit 24.7 m at a
        # 176-degree turn; ~21 m at 170 degrees under the cluster-
        # weighted refit), so entry/exit tangents opposing beyond ~150
        # degrees earn a 0.3x floor. turn_deg carries the pre-fillet
        # corner turn of the freshly rebuilt feature (check0 pins DB
        # rows == this rebuild).
        frac = 0.3 if t.turn_deg > cfg.cusp_turn_deg else 0.4
        return min(frac * eff_len(t), lo_interlock)

    def hi_for(t):
        # a consumed-corridor merge chains k transition sites into one
        # feature (len ~= sum of the sites' effective lengths).
        # Way-graph-era: interlocking switch clusters make multi-site
        # merges routine on EVERY band (the raster era pinned chicago's
        # default band to single sites — its blob junctions never sat
        # closer than a transition length)
        if not t.sites:
            return 1.1 * L
        return 1.1 * sum(site_len.get(nid, L) for nid in set(t.sites))

    trs = [s for s in segments if s.kind == "transition"]
    bad_len = [(t.seg_id, round(t.len_m, 1)) for t in trs
               if not (lo_for(t) <= t.len_m <= hi_for(t))]
    lens = sorted(t.len_m for t in trs)
    print(f"  {len(trs)} transitions, len_m min {lens[0]:.1f} / "
          f"median {lens[len(lens) // 2]:.1f} / max {lens[-1]:.1f} "
          f"(bounds [0.4, 1.1] x {L:.0f} m x per-site clamps, "
          f"{len(site_len)} clamped sites)")
    report("check2.fixed-ground-length", not bad_len,
           f"{len(bad_len)} transitions outside [0.4, 1.1] x "
           f"{cfg.transition_len_m:.0f} m (x site count, site-clamp "
           f"adjusted): {bad_len[:6]}")

    worst = 0.0
    bad_sp = []
    for t in trs:
        sp = max(math.dist(a, b) for a, b in zip(t.xy, t.xy[1:]))
        worst = max(worst, sp)
        if sp > cfg.densify_step_m + 1e-6:
            bad_sp.append((t.seg_id, round(sp, 2)))
    report("check2.vertex-spacing", not bad_sp,
           f"max transition vertex spacing {worst:.2f} m "
           f"(limit {cfg.densify_step_m} m); {len(bad_sp)} over")


# ------------------------------------------------------------- fillets

def check3_fillets(segments, proj, chicago: bool = True,
                   cfg: SegmentConfig = CFG, band_minzoom: int = DEFAULT_BAND,
                   default_band: bool = True):
    print("\nCHECK 3 — fillet curvature + self-intersections")
    import json

    import psycopg

    # measure the SERVED geometry, not the in-memory rebuild: the rows
    # transit_lines_rt2 serves are what the fork's per-vertex normals
    # see, and emit-time quantization coarser than the fillet vertex
    # spacing (8-22 cm) would put micro-kinks on them that the rebuild
    # never had (check0 compares endpoints/NPoints only)
    with psycopg.connect(DEFAULT_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT seg_id, ST_AsGeoJSON(geom, 15)
               FROM transit_line_segments
               WHERE build_key = %s AND band_minzoom = %s""",
            (BUILD, band_minzoom))
        db_xy = {r[0]: proj.to_xy(json.loads(r[1])["coordinates"])
                 for r in cur.fetchall()}

    inf = float("inf")
    n_target = n_clamped = 0
    bad = []
    missing = []
    worst = (inf, None)  # (measured / target ratio, detail)
    trs = [s for s in segments if s.kind == "transition"]
    for t in trs:
        # the target each corner was BUILT to (recorded at pair time,
        # min across merged corners): a merged chain's line_count is
        # the bigger end's bundle, but an interior corner in a
        # 1-ribbon stretch was legitimately filleted at the smaller
        # local target (NYC z14 seg 564: 11 m corner inside an
        # line_count-2 chain)
        target = (t.fillet_target_m if t.fillet_target_m is not None
                  else t.line_count * cfg.gap_px * cfg.fillet_radius_factor)
        raw = t.raw_min_radius_m if t.raw_min_radius_m is not None else inf
        ach = t.fillet_radius_m if t.fillet_radius_m is not None else inf
        xy = db_xy.get(t.seg_id)
        if xy is None:
            missing.append(t.seg_id)
            continue
        measured = min((_circumradius(a, b, c) for a, b, c in
                        zip(xy, xy[1:], xy[2:])), default=inf)
        if t.fillet_clamped:
            n_clamped += 1
        if measured >= 0.9 * target:
            n_target += 1
        ratio = measured / target
        if ratio < worst[0]:
            worst = (ratio, (t.seg_id, t.route_short_names,
                             round(measured, 1), round(target, 1),
                             t.fillet_clamped))
        floor = min(ach, raw) if t.fillet_clamped else min(target, raw)
        if measured < 0.9 * floor:
            bad.append((t.seg_id, t.route_short_names,
                        round(measured, 1), round(floor, 1)))
    print(f"  {len(trs)} transitions: {n_target} meet 0.9x the full "
          f"min-radius target, {n_clamped} clamped by short halves "
          f"(curvature measured on the emitted DB rows)")
    print(f"  worst: seg {worst[1]} at {worst[0]:.2f}x its target")
    report("check3.min-radius", not bad and not missing,
           f"{len(bad)} transitions under their curvature floor: "
           f"{bad[:6]}; {len(missing)} missing from DB")
    # non-chicago / non-default-band cap 0.3: the linegraph refit
    # collapses plan-view X crossings to near-point rungs (true crossing
    # geometry), and every such consumed corridor's transitions clamp by
    # design; longer bands additionally consume/merge short corridors far
    # more often — the safety contract stays check3.min-radius, which
    # measures the emitted curvature floor unconditionally.
    # Full-target aggregate: 65% on the default band; 60% on longer
    # bands — a 240/480 m window sweeps far more inherited track
    # curvature (recorded raw floors, each still floor-checked above),
    # so fewer chains can meet the FULL fillet target (chicago z0:
    # 21/29 = 72% purely from Loop-leg track curvature, 0 clamps).
    # Default band re-calibrated 75% -> 65% (and the chicago clamp cap
    # 10 -> 15) for the cluster-weighted refit era: the evidence average
    # centers on TRACK clusters and Y-nodes pin to their dominant
    # through-pair, restoring truer — sometimes sharper — curvature at
    # junction mouths (chicago:l-v3 z15: 32/46 full target, 13 clamps,
    # every one floor-checked above; junction exam straight-through
    # mean improved 2.20 -> 0.80 m in the same change).
    # chicago default band re-measured for the flap-guard era: the
    # coalesced P/Red bundle removed five seam sites whose transitions
    # were mostly full-target straightaways, so the remaining population
    # concentrates at real interlocking mouths (21/33 = 64% full target,
    # 12 clamps, every one floor-checked above).
    clamp_cap = (15 if chicago and default_band
                 else math.ceil(0.3 * len(trs)))
    target_frac = (0.6 if chicago else 0.65) if default_band else 0.6
    report("check3.clamping-is-exceptional",
           n_clamped <= clamp_cap and n_target >= target_frac * len(trs),
           f"{n_clamped} clamped (<={clamp_cap}), {n_target}/{len(trs)} "
           f"at full target (>={target_frac:.0%})")

    with psycopg.connect(DEFAULT_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT seg_id FROM transit_line_segments
               WHERE build_key = %s AND band_minzoom = %s
                 AND NOT ST_IsSimple(geom)""", (BUILD, band_minzoom))
        not_simple = [r[0] for r in cur.fetchall()]
    report("check3.st-issimple", not not_simple,
           f"{len(not_simple)} self-intersecting features: "
           f"{not_simple[:6]}")


# ------------------------------------------------------------- coverage

def check4_coverage(g, proj, segments, cfg: SegmentConfig = CFG):
    print("\nCHECK 4 — per-ribbon coverage + overlaps")
    from shapely.geometry import LineString

    by_ck = defaultdict(list)
    for s in segments:
        by_ck[s.color_key].append(s)
    BUF_M = 3.0  # fillets separate the twins' geometries by a few metres
    n_allowed = 0
    allowed_ov: dict = defaultdict(float)
    bad_ov = []
    for ck in sorted(by_ck):
        feats = by_ck[ck]
        geoms = [LineString(s.xy) for s in feats]
        for i, a in enumerate(feats):
            for j in range(i + 1, len(feats)):
                b = feats[j]
                inter = geoms[i].intersection(geoms[j])
                ov = getattr(inter, "length", 0.0)
                if ov <= 1.0:
                    continue
                shared_site = set(a.sites or ()) & set(b.sites or ())
                # kind-agnostic on purpose: a divergence twin whose
                # offsets are equal and whose (refit-straightened)
                # geometry is straight gets skip-classified to steady,
                # but it still shares the trunk tail legitimately —
                # site anchoring + the half-transition bound identify
                # the divergence structure (corridor steadies carry no
                # sites). A shared typed (cid, side) end is NOT
                # required: consumed-corridor merges swallow the twins'
                # shared endpoint into a chain interior on long bands
                # (Tower 18 at 480 m), leaving the shared site as the
                # divergence witness.
                # Way-graph-era calibration: the twins share the trunk
                # up to the REAL switch, and physical ladder throats sit
                # up to a full transition length past the junction node
                # (7 Av/53 St: 55 m of a 60 m band; W 4 St at 480 m) —
                # the raster's blob junctions never pushed past len/2.
                branch = (shared_site
                          and ov <= cfg.transition_len_m + 1e-6)
                if branch:
                    # the genuinely double-drawn length is measured by
                    # BUF_M proximity, not vertex-exact intersection —
                    # corner fillets pull the twins a few metres apart
                    # over most of the shared tail (Pink at 480 m:
                    # 21.6 m exact vs 208.3 m within 3 m; the ribbon's
                    # gross length excess was 201 m); the coverage
                    # bound below subtracts it
                    ov_buf = min(geoms[i].intersection(
                        geoms[j].buffer(BUF_M)).length,
                        cfg.transition_len_m + 2 * BUF_M)
                    n_allowed += 1
                    allowed_ov[ck] += ov_buf
                    print(f"  branch tail reuse: {ck} segs "
                          f"{a.seg_id}/{b.seg_id} share {ov_buf:.1f} m "
                          f"(exact {ov:.1f}) at site "
                          f"{set(a.sites) & set(b.sites)}")
                else:
                    bad_ov.append((ck, a.seg_id, b.seg_id, round(ov, 1)))
    report("check4.no-unexplained-overlaps", not bad_ov,
           f"{len(bad_ov)} overlaps > 1 m outside branch-divergence "
           f"tails ({n_allowed} allowed): {bad_ov[:6]}")

    # coverage, net of the legitimately double-covered divergence
    # tails: those stretches are REAL drawn geometry on two features of
    # the ribbon, so gross feature length exceeds corridor length by
    # exactly the allowed overlap (60 m band: ~30 m on 28 km, invisible
    # in the 1% slack; 480 m band: up to 240 m per divergence site —
    # subtract it so the bound keeps catching genuine over/under
    # coverage at every band)
    corridors = walk_corridors(g, cfg.gap_px)
    cor_sum: dict = defaultdict(float)
    for c in corridors:
        xy = proj.to_xy(c.coords)
        length = sum(math.dist(a, b) for a, b in zip(xy, xy[1:]))
        for r in c.ribbons:
            cor_sum[r.color_key] += length
    seg_sum: dict = defaultdict(float)
    for s in segments:
        seg_sum[s.color_key] += s.len_m
    bad_cov = []
    for ck in sorted(cor_sum):
        ratio = (seg_sum[ck] - allowed_ov[ck]) / cor_sum[ck]
        note = (f" (gross {seg_sum[ck] / cor_sum[ck]:.4f} - "
                f"{allowed_ov[ck]:.0f} m shared tails)"
                if allowed_ov[ck] > 0 else "")
        print(f"  {ck}: corridors {cor_sum[ck] / 1000:7.2f} km, "
              f"features {seg_sum[ck] / 1000:7.2f} km, "
              f"ratio {ratio:.4f}{note}")
        if not (0.99 <= ratio <= 1.01):
            bad_cov.append((ck, round(ratio, 4)))
    report("check4.coverage-within-1pct", not bad_cov,
           f"{len(bad_cov)} ribbons outside [0.99, 1.01] net of allowed "
           f"tails: {bad_cov}")


# -------------------------------------------------------------- receipt

def check5_loop_receipt(segments):
    print("\nCHECK 5 — per-Loop-leg receipt (steady bundles + transitions)")
    mx = 111319.4908 * math.cos(math.radians((LOOP_WINDOW[1]
                                              + LOOP_WINDOW[3]) / 2))
    my = 110574.2727

    def fmt(v):
        return "     " if v is None else f"{v:+5.1f}"

    legs: dict = defaultdict(list)
    trans_in = []
    for s in segments:
        lons = [c[0] for c in s.coords]
        lats = [c[1] for c in s.coords]
        mid = (lons[len(lons) // 2], lats[len(lats) // 2])
        inside = [c for c in s.coords if in_window(*c)]
        if not inside:
            continue
        if s.kind == "transition":
            if in_window(*mid):
                trans_in.append(s)
            continue
        # Way-graph-era calibration: merged corridors chain around the
        # Loop corners, so one steady feature legitimately spans several
        # legs — assign it to EVERY leg whose band >= 2 of its in-window
        # vertices ride (midpoint-only assignment starved legs the
        # feature merely passes through).
        assigned = set()
        for name, (ori, ref) in LOOP_LEGS.items():
            band = [c for c in inside
                    if (abs(c[1] - ref) * my if ori == "EW"
                        else abs(c[0] - ref) * mx) <= LEG_TOL_M]
            if len(band) >= 2:
                assigned.add(name)
        for name in sorted(assigned) or ["interior (subways)"]:
            legs[name].append(s)

    order = list(LOOP_LEGS) + ["interior (subways)"]
    bad_legs = []
    for leg in order:
        feats = sorted(legs.get(leg, []),
                       key=lambda s: (-s.len_m, s.color_key))
        ribbons = {s.color_key for s in feats}
        print(f"  {leg}: {len(feats)} steady features, "
              f"{len(ribbons)} ribbons")
        for s in feats:
            print(f"    seg {s.seg_id:4d} steady     "
                  f"{s.route_short_names:<12} slot {s.slot}/{s.line_count} "
                  f"offset {fmt(s.offset_px)} px  {s.len_m:6.0f} m")
        if leg in LOOP_LEGS and len(ribbons) < 2:
            bad_legs.append(leg)
    print(f"  Loop-window transitions: {len(trans_in)}")
    for s in sorted(trans_in, key=lambda s: (s.sites, s.color_key)):
        print(f"    seg {s.seg_id:4d} transition {s.route_short_names:<12} "
              f"slot {s.slot}/{s.line_count} "
              f"offset {fmt(s.off_from_px)} -> {fmt(s.off_to_px)} px  "
              f"{s.len_m:6.0f} m  site {list(s.sites)}")
    report("check5.legs-populated", not bad_legs,
           f"every Loop leg carries a multi-ribbon bundle "
           f"(thin: {bad_legs})")
    report("check5.loop-has-transitions", len(trans_in) >= 12,
           f"{len(trans_in)} transition features in the Loop window")


def main() -> int:
    import argparse

    global BUILD
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--build-key", default=CHICAGO_BUILD)
    ap.add_argument("--band", type=int, default=None, metavar="MINZOOM",
                    help="validate only the band with this min_zoom "
                         "(default: every band)")
    args = ap.parse_args()
    BUILD = args.build_key
    chicago = BUILD == CHICAGO_BUILD

    bands = band_ranges(CFG.bands)
    if args.band is not None:
        bands = [b for b in bands if b[0] == args.band]
        if not bands:
            ap.error(f"no band with min_zoom {args.band} in {CFG.bands}")

    print(f"segments acceptance exam — build {BUILD}\ndsn {DEFAULT_DSN}")
    g, shapes, proj = load_inputs()
    site_checks = True
    default_segments = None
    global BAND_TAG
    for band_minzoom, band_maxzoom, length in bands:
        bcfg = replace(CFG, transition_len_m=length)
        default_band = band_minzoom == DEFAULT_BAND
        BAND_TAG = f"band z{band_minzoom}: "
        print(f"\n{'#' * 64}\nBAND z{band_minzoom}..{band_maxzoom} — "
              f"transition {length:.0f} m"
              + (" (default band)" if default_band else ""))
        segments, info = rebuild_band(g, shapes, proj, bcfg)
        print(f"rebuilt {len(segments)} features "
              f"({sum(1 for s in segments if s.kind == 'steady')} steady, "
              f"{sum(1 for s in segments if s.kind == 'transition')} "
              f"transition) from {len(g.edges)} edges")
        if default_band:
            default_segments = segments

        check0_db_matches_rebuild(segments, band_minzoom)
        check1_c1_contract(g, proj, segments, chicago, bcfg, site_checks)
        site_checks = False
        check2_c3_contract(segments, chicago, bcfg, default_band,
                           info.get("site_len_clamped"))
        check3_fillets(segments, proj, chicago, bcfg, band_minzoom,
                       default_band)
        check4_coverage(g, proj, segments, bcfg)

    BAND_TAG = f"band z{DEFAULT_BAND}: "
    if chicago and default_segments is not None:
        check5_loop_receipt(default_segments)
    elif chicago:
        print(f"\nCHECK 5 — skipped (default band z{DEFAULT_BAND} not in "
              f"the selected band set)")
    else:
        print(f"\nCHECK 5 — skipped (Loop receipt is chicago-only; "
              f"build {BUILD})")

    print("\n" + "=" * 64)
    if FAILURES:
        print(f"EXAM FAILED — {len(FAILURES)} failing check(s):")
        for f in FAILURES:
            print(f"  * {f}")
        return 1
    print("EXAM PASSED — all checks green")
    return 0


if __name__ == "__main__":
    sys.exit(main())
