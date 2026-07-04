"""tools.sandbox.sites — the site registry.

Every site we have worked on, each with: name, build_key, center lon/lat,
bbox (w, s, e, n), representative CLIENT zoom (the zoom the user actually
looks at the site through), the families present, an EXPECTED behaviour tag,
and the specific pair(s) whose on-screen relationship is being judged.

EXPECTED tags
  bundle    — two colour families collapse to ONE ribbon set on a shared
              centerline; the two family ribbons render `ribbon_gap * slots`
              px apart (the correct Apple-style tight bundle). VERDICT fails
              if the measured on-screen gap is > 1.5x expected OR if the two
              families' PRE-offset centerlines are NOT coincident (i.e. the
              data was never actually bundled).
  separate  — two families must NOT come within `kiss_px` on shared track;
              they run as independent ribbons (a kiss/crossing, not a bundle).
  centered  — the ribbon must sit within ~platform half-width of the named
              platform's centroid (island-platform centring).
  straight  — a through-path must not deviate more than a chord tolerance
              from its +-100 m chord (no bend at a crossing).

A "pair" is (family_a_color, family_b_color) as the 6-hex route_color the
pipeline stores (no leading '#'). NYC feed-5 family colours:
  F6BC26 Broadway yellow (N/Q/R/W)   EB6800 orange (B/D/F/M)
  0062CF blue (A/C/E)                009952 green (4/5/6)
  D82233 red (1/2/3)                 A626AA purple (7)
  6E3219/996633 brown (J/Z)          799534 G   7C858C L
Chicago feed-29 uses the CTA line colours directly.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# ── NYC feed-5 family colours (route_color, no leading '#') ──────────────
YELLOW = "F6BC26"   # N Q R W  (Broadway / 4th Av / Sea Beach / West End / Brighton)
ORANGE = "EB6800"   # B D F M  (6th Av / Chrystie / Culver)
BLUE = "0062CF"     # A C E    (8th Av)
GREEN = "009952"    # 4 5 6    (Lexington)
RED = "D82233"      # 1 2 3    (7th Av)
PURPLE = "9A38A1"   # 7 / 7X   (Flushing) — feed-5 route_color
GREY = "808183"     # shuttles
JZ = "8E5C33"       # J Z      (Nassau / Jamaica) — feed-5 brown route_color
G_COLOR = "799534"  # G        (Crosstown)
L_COLOR = "7C858C"  # L        (Canarsie)

# ── Chicago feed-29 CTA line colours ─────────────────────────────────────
CTA_BLUE = "00A1DE"
CTA_RED = "C60C30"
CTA_BROWN = "62361B"
CTA_GREEN = "009B3A"
CTA_ORANGE = "F9461C"
CTA_PINK = "E27EA6"
CTA_PURPLE = "522398"
CTA_YELLOW = "F9E300"


@dataclass(frozen=True)
class Site:
    key: str                       # short id, e.g. "dekalb"
    name: str                      # human title for the panel
    build_key: str                 # transit_line_segments build_key
    center: tuple                  # (lon, lat)
    bbox: tuple                    # (w, s, e, n)
    zoom: float                    # representative CLIENT zoom
    families: tuple                # route_colors present (for the legend)
    expected: str                  # bundle | separate | centered | straight
    pairs: tuple = ()              # ((colorA, colorB), ...) judged pairs
    platform_hint: str = ""        # for centered: substring of platform/name
    through_color: str = ""        # for straight: the route_color whose
    #                                through-path must stay straight (judged
    #                                alone — an unrelated line curving in the
    #                                window is not this site's concern)
    note: str = ""                 # what the user actually reported / expects


# The registry. bbox windows reuse segments/exam/nyc_visual.py's proven
# windows where they exist, tightened/added where the user named a new site.
SITES: list[Site] = [
    # ── Chicago Loop ────────────────────────────────────────────────────
    Site(
        key="chicago-loop", name="Chicago Loop — Tower 18 + per-leg bundles",
        build_key="chicago:l-v3",
        center=(-87.6300, 41.8815), bbox=(-87.6355, 41.8755, -87.6245, 41.8875),
        zoom=15.0,
        families=(CTA_BROWN, CTA_ORANGE, CTA_PINK, CTA_PURPLE, CTA_GREEN,
                  CTA_BLUE, CTA_RED),
        expected="bundle",
        pairs=((CTA_BROWN, CTA_ORANGE), (CTA_BROWN, CTA_PINK),
               (CTA_ORANGE, CTA_PURPLE)),
        note="5-6 line bundles per leg; Tower 18 a junction not a merge",
    ),
    Site(
        key="chicago-lake-blue",
        name="Chicago Lake leg — Blue subway bundles under the elevated",
        build_key="chicago:l-v3",
        center=(-87.6470, 41.8855), bbox=(-87.6560, 41.8835, -87.6360, 41.8875),
        zoom=15.0,
        families=(CTA_BLUE, CTA_GREEN, CTA_PINK),
        expected="bundle", pairs=((CTA_BLUE, CTA_GREEN),),
        note="Lake-leg Blue subway runs under Lake St elevated (one bundle). "
             "Green and Pink share ONE elevated ribbon here (always the "
             "G/Pink lc-2 pair), so Blue x Green is the representative "
             "cross-family bundle pair; the redundant Blue x Pink pair picks "
             "the far Pink slot of the 3-line bundle (Blue|Green|Pink), which "
             "renders 2 slots wide by design, not a loose bundle.",
    ),

    # ── NYC bundle sites ────────────────────────────────────────────────
    Site(
        key="broadway", name="Broadway trunk — N/Q/R/W one yellow ribbon",
        build_key="nyc:subway-v3",
        center=(-73.994, 40.7368), bbox=(-74.008, 40.7165, -73.980, 40.757),
        zoom=15.0, families=(YELLOW, RED, ORANGE, BLUE),
        expected="bundle", pairs=((YELLOW, YELLOW),),
        note="N/Q/R/W collapse to ONE yellow ribbon Times Sq -> Canal",
    ),
    Site(
        key="dekalb",
        name="DeKalb / Manhattan Bridge — B/D beside N/Q/R/W (one bundle)",
        build_key="nyc:subway-v3",
        center=(-73.9836, 40.6900), bbox=(-73.997, 40.688, -73.980, 40.720),
        zoom=15.5, families=(ORANGE, YELLOW),
        expected="bundle", pairs=((ORANGE, YELLOW),),
        note="THE core puzzle: user sees orange/yellow gap over the bridge "
             "approach; data has B,D and N,Q,R,W at 0.0 m centerline dist",
    ),
    Site(
        key="jay-st-metrotech",
        name="Jay St-MetroTech — A/C beside F (shared-track bundle?)",
        build_key="nyc:subway-v3",
        center=(-73.9873, 40.6923), bbox=(-73.9945, 40.6875, -73.9800, 40.6975),
        zoom=15.5, families=(BLUE, ORANGE),
        expected="bundle", pairs=((BLUE, ORANGE),),
        note="A/C (8th Av) and F (Culver) share track Jay St->Bergen; "
             "EXPECTED bundle IF they share track >=450 m within 18 m",
    ),
    Site(
        key="carroll-st", name="Carroll St — F/G bundle on the Culver local",
        build_key="nyc:subway-v3",
        center=(-73.9950, 40.6800), bbox=(-74.002, 40.6740, -73.988, 40.6865),
        zoom=15.5, families=(ORANGE, G_COLOR),
        expected="bundle", pairs=((ORANGE, G_COLOR),),
        note="F and G share the Culver local track through Carroll St",
    ),
    Site(
        key="flatbush",
        name="Flatbush Av — B/Q beside 2/3/4/5 (one bundle under Flatbush)",
        build_key="nyc:subway-v3",
        center=(-73.9585, 40.6620), bbox=(-73.9680, 40.6540, -73.9490, 40.6710),
        zoom=15.0, families=(YELLOW, RED, GREEN),
        expected="bundle", pairs=((YELLOW, RED), (YELLOW, GREEN)),
        note="B/Q (Brighton) beside 2/3/4/5 side-by-side tunnels under Flatbush",
    ),

    # ── NYC separate sites ──────────────────────────────────────────────
    Site(
        key="brooklyn-bridge",
        name="Brooklyn Bridge-City Hall — J/Z SEPARATE from 4/5/6",
        build_key="nyc:subway-v3",
        center=(-74.005, 40.7132), bbox=(-74.012, 40.708, -73.998, 40.7185),
        zoom=15.5, families=(JZ, GREEN, GREY),
        expected="separate", pairs=((JZ, GREEN),),
        note="J/Z (Nassau) passes ~7-8 m from Lexington 4/5/6 — must NOT fuse",
    ),
    Site(
        key="whitehall",
        name="Whitehall/South Ferry — 4/5 SEPARATE from R/W",
        build_key="nyc:subway-v3",
        center=(-74.0125, 40.7023), bbox=(-74.019, 40.698, -74.006, 40.7065),
        zoom=15.5, families=(GREEN, YELLOW, RED),
        expected="separate", pairs=((GREEN, YELLOW),),
        note="4/5 Joralemon tube plan-crosses R/W Montague tube — crossing "
             "not bundle",
    ),
    Site(
        key="rector",
        name="Rector St -> South Ferry — 1 SEPARATE from R/W",
        build_key="nyc:subway-v3",
        center=(-74.0135, 40.7055), bbox=(-74.018, 40.702, -74.009, 40.709),
        zoom=15.5, families=(RED, YELLOW),
        expected="separate", pairs=((RED, YELLOW),),
        note="1 (7th Av) beside R/W (Broadway) — a kiss, no bundle outside "
             "shared track",
    ),
    Site(
        key="queensboro-plaza",
        name="Queensboro Plaza — 7 vs N/W (borderline separate)",
        build_key="nyc:subway-v3",
        center=(-73.9403, 40.7500), bbox=(-73.9480, 40.7455, -73.9330, 40.7545),
        zoom=15.5, families=(PURPLE, YELLOW), through_color=PURPLE,
        expected="straight", pairs=(),
        note="7 stacked OVER N/W at Queensboro Plaza — structurally stacked "
             "(they run parallel ~435 m in plan, per the docs' 'stacked "
             "structures stay fused'), so a clean separate/bundle tag "
             "doesn't apply; judged on whether the 7's through-path stays "
             "on its own track (not captured/bent by N/W)",
    ),

    # ── NYC centered sites ──────────────────────────────────────────────
    Site(
        key="bowling-green",
        name="Bowling Green — 4/5 centered on the island platform",
        build_key="nyc:subway-v3",
        center=(-74.0140, 40.7047), bbox=(-74.019, 40.7005, -74.009, 40.708),
        zoom=16.0, families=(GREEN,),
        expected="centered", pairs=(), platform_hint="Bowling Green",
        note="4/5 ribbon rides the island-platform centerline (directional "
             "pair midline)",
    ),

    # ── NYC straight-through sites ──────────────────────────────────────
    Site(
        key="lafayette-av",
        name="Lafayette Av — G through-path straight (no bend)",
        build_key="nyc:subway-v3",
        center=(-73.9743, 40.6865), bbox=(-73.982, 40.6825, -73.9665, 40.6905),
        zoom=15.5, families=(G_COLOR, BLUE),
        expected="straight", pairs=(), through_color=G_COLOR,
        note="G merges the A/C Fulton corridor; through-path stays straight",
    ),
    Site(
        key="15st-prospect",
        name="15 St-Prospect Park — F/FX on track, no stray express",
        build_key="nyc:subway-v3",
        center=(-73.9800, 40.6600), bbox=(-73.988, 40.654, -73.972, 40.666),
        zoom=15.5, families=(ORANGE,),
        expected="straight", pairs=(), through_color=ORANGE,
        note="F/FX ride the Culver track; no phantom express chord (FX bypass "
             "must not paint through the station)",
    ),
    Site(
        key="mott-haven",
        name="Mott Haven wye — 5 through-path, no self-intersect",
        build_key="nyc:subway-v3",
        center=(-73.9270, 40.8090), bbox=(-73.9360, 40.8030, -73.9180, 40.8150),
        zoom=15.0, families=(GREEN,),
        expected="straight", pairs=(), through_color=GREEN,
        note="5's merged corridor at E 149 St wye — no micro self-intersection "
             "loop, no folded transition",
    ),

    # ── NYC convergence sites (through-geometry sanity, bundle-ish) ─────
    Site(
        key="nevins",
        name="Nevins St — 2/3 + 4/5 convergence toward Atlantic Av",
        build_key="nyc:subway-v3",
        center=(-73.9820, 40.6880), bbox=(-73.993, 40.680, -73.971, 40.696),
        zoom=15.0, families=(RED, GREEN),
        expected="straight", pairs=(), through_color=RED,
        note="2/3 and 4/5 converge toward Atlantic; through-tracks stay "
             "tangent, no fabricated bend (judged on the 2/3 through-path)",
    ),
    Site(
        key="borough-hall",
        name="Borough Hall — R (Montague) around 2/3, never captured",
        build_key="nyc:subway-v3",
        center=(-73.9910, 40.6930), bbox=(-73.999, 40.687, -73.983, 40.699),
        zoom=15.5, families=(YELLOW, RED),
        expected="separate", pairs=((YELLOW, RED),),
        note="R passes around the 2/3, never captured into their bundle",
    ),
    Site(
        key="broadway-lafayette",
        name="Broadway-Lafayette / Bleecker — B/D + F/M bundle onset",
        build_key="nyc:subway-v3",
        center=(-73.9945, 40.7248), bbox=(-74.004, 40.718, -73.987, 40.732),
        zoom=15.5, families=(ORANGE,),
        expected="bundle", pairs=((ORANGE, ORANGE),),
        note="B/D and F/M (all orange) converge on the 6th Av trunk toward "
             "Broadway-Lafayette; the bundle must FORM at the onset of "
             "sustained parallelism on the approach, not snap on at the "
             "station node.",
    ),
    Site(
        key="eastern-parkway",
        name="Eastern Parkway — 2/3 (red) + 4/5 (green) bundled trunk",
        build_key="nyc:subway-v3",
        center=(-73.9540, 40.6700), bbox=(-73.975, 40.662, -73.935, 40.676),
        zoom=15.0, families=(RED, GREEN),
        expected="bundle", pairs=((RED, GREEN),),
        note="2/3 and 4/5 run bundled along Eastern Parkway past Franklin Av / "
             "Brooklyn Museum; judged on whether red and green read as one "
             "clean multi-slot ribbon through the shared trunk (the merge and "
             "split should not double-cross).",
    ),
]


SITES_BY_KEY = {s.key: s for s in SITES}


def get(key: str) -> Site:
    if key not in SITES_BY_KEY:
        raise KeyError(f"unknown site '{key}'. known: {sorted(SITES_BY_KEY)}")
    return SITES_BY_KEY[key]
