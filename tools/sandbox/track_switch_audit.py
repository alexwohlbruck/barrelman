"""tools.sandbox.track_switch_audit — network anti-hop track-switch sweep.

Companion to bundle_audit.py (missed bundles) and crossing_audit.py
(avoidable double-flips). This one is on the MATCHING side: it sums the
per-pattern track-switch count shapesnap.match records in
matched_shapes.stats->'track_switches' — the number of UNJUSTIFIED
way-changes a matched route made (a hop off its current OSM way at a node
where that way ALSO continued; genuine forks are excluded by construction,
see shapesnap.match._way_change_penalty).

The anti-hop penalty (MatchConfig.track_switch_penalty_m, user rule 11)
should DROP the network total: a route must not zig-zag between adjacent
parallel tracks via crossovers just because the raw geometry aligns
marginally closer. Run before/after the penalty change per feed — the
total must fall, and the worst offenders (the 15 St FX / Nevins wander)
must clean up — while on-OSM % must not regress (checked separately).

  uv run --with-requirements tools/sandbox/requirements.txt \
      python -m tools.sandbox.track_switch_audit
  uv run --with-requirements tools/sandbox/requirements.txt \
      python -m tools.sandbox.track_switch_audit --feed 5 --json
"""

from __future__ import annotations

import argparse
import json
import sys

DEFAULT_DSN = "postgresql://barrelman:barrelman@localhost:5434/barrelman"
FEEDS = ("5", "29")


def sweep(feed_id: str, dsn: str = DEFAULT_DSN) -> dict:
    """Read matched_shapes for one feed; total + per-pattern track switches."""
    import psycopg

    with psycopg.connect(dsn, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT pattern_id, route_id,
                      stats->>'route_short_name' AS route,
                      COALESCE((stats->>'track_switches')::int, 0) AS sw,
                      method
                 FROM matched_shapes
                WHERE feed_id = %s
                ORDER BY COALESCE((stats->>'track_switches')::int, 0) DESC""",
            (feed_id,),
        )
        rows = cur.fetchall()

    patterns = [
        {"pattern": p, "route": route or rid, "switches": sw, "method": m}
        for (p, rid, route, sw, m) in rows
    ]
    total = sum(r["switches"] for r in patterns)
    with_switch = sum(1 for r in patterns if r["switches"] > 0)
    return {
        "feed_id": feed_id,
        "patterns": len(patterns),
        "total_track_switches": total,
        "patterns_with_switch": with_switch,
        "worst": patterns[:15],
    }


def render(card: dict) -> str:
    L = []
    L.append("=" * 64)
    L.append(f" TRACK-SWITCH AUDIT — feed {card['feed_id']}  "
             f"({card['patterns']} patterns)")
    L.append("=" * 64)
    L.append(f" total unjustified track switches: {card['total_track_switches']}")
    L.append(f" patterns with >=1 switch:         {card['patterns_with_switch']}")
    L.append("-" * 64)
    if card["worst"] and card["worst"][0]["switches"] > 0:
        L.append(" worst offenders (route : switches):")
        for r in card["worst"]:
            if r["switches"] == 0:
                break
            L.append(f"   {str(r['route']):>6}  {r['switches']:>3}  "
                     f"[{r['method']}]  {r['pattern']}")
    else:
        L.append(" (none — no pattern hops between parallel tracks)")
    L.append("=" * 64)
    return "\n".join(L)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Network anti-hop track-switch sweep")
    ap.add_argument("--feed", action="append", help="default: 5 and 29")
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    feeds = args.feed or list(FEEDS)
    cards = [sweep(f, args.dsn) for f in feeds]
    if args.json:
        print(json.dumps(cards, indent=2))
    else:
        for c in cards:
            print(render(c))
    return 0


if __name__ == "__main__":
    sys.exit(main())
