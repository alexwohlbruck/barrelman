#!/usr/bin/env python3
"""shapesnap.gates — per-pattern quality gates (computed in the local UTM).

Gates (docs/transit-pipeline-v3.md stage 3), evaluated on projected
geometry so every threshold is honest meters:

  1. coverage   — fraction of resampled input points within
                  coverage_tol_m[mode] of the output line >= min_coverage
  2. deviation  — discrete Fréchet (shapely.frechet_distance, densified)
                  between the input reference line and the output
                  <= max_frechet_m. When the matcher excised foreign-run
                  observations (shapesnap.match), the reference arrives
                  as PIECES (ref_pieces): each trusted contiguous stretch
                  is compared against the output substring between its
                  endpoint projections and the max is reported — the
                  fabricated straight jump across an excised run is not
                  agency geometry and must not fail the pattern.
  3. length     — output length / input length within length_ratio
  4. stops      — every pattern stop within stop_radius of the output
                  (stop_radius = the regime's candidate radius)

ANY failed gate -> the caller (shapesnap.match) walks the on-OSM
fallback chain: revert retry splices / graph bridges back to the
agency-bridged baseline, then re-match the pattern in the SPARSE regime
(the rescue is gated on the sparse gates below with the rescue length
bounds — never on Fréchet-vs-agency, since the agency shape is exactly
what failed), and only when that also fails return the ORIGINAL
geometry unchanged (method="passthrough_agency") — a match below the
bar is never emitted.

The sparse regime (regime B: no input shape) has no reference line, so
gates 1–2 are skipped and gate 3 compares against the stop-chain chord
length with wider bounds (network paths are always >= the chord) —
documented deviation from the dense thresholds. The sparse rescue runs
these same gates with MatchConfig.rescue_length_ratio_min/max
substituted, plus a no-empty-stop-layers check enforced by the caller
(a chord bridge passes straight through a missing stop's coordinate and
would fake gate 4).

All thresholds live in the single GateConfig dataclass.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import shapely
from shapely.geometry import LineString, Point
from shapely.ops import substring

__all__ = ["GateConfig", "GateReport", "evaluate_gates"]


@dataclass
class GateConfig:
    """Every gate threshold in one place."""

    coverage_tol_m: dict = field(
        default_factory=lambda: {"rail": 25.0, "bus": 15.0, "ferry": 50.0}
    )
    min_coverage: float = 0.95
    max_frechet_m: float = 100.0
    length_ratio_min: float = 0.95
    length_ratio_max: float = 1.15
    # sparse regime: output vs stop-chain chord length (chord is a lower bound)
    sparse_length_ratio_min: float = 0.95
    sparse_length_ratio_max: float = 1.60
    # discrete Fréchet is vertex-coupled: densify both lines to a fixed
    # spacing first or a long simplified segment dominates the metric
    frechet_segmentize_m: float = 20.0


@dataclass
class GateReport:
    passed: bool
    coverage: float | None = None
    frechet_m: float | None = None
    length_ratio: float | None = None
    max_stop_dist_m: float | None = None
    failures: list = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "passed": self.passed,
            "coverage": None if self.coverage is None else round(self.coverage, 4),
            "frechet_m": None if self.frechet_m is None else round(self.frechet_m, 1),
            "length_ratio": None
            if self.length_ratio is None
            else round(self.length_ratio, 4),
            "max_stop_dist_m": None
            if self.max_stop_dist_m is None
            else round(self.max_stop_dist_m, 1),
            "failures": list(self.failures),
        }


def _piecewise_frechet(ref_pieces: list, output_line: LineString, step: float) -> float:
    """Max densified Fréchet over trusted reference pieces, each compared
    against the output substring between its endpoint projections. Falls
    back to piece-vs-whole when the projections misorder (self-overlap)."""
    worst = 0.0
    for piece in ref_pieces:
        if piece.is_empty or piece.length == 0:
            continue
        a = output_line.project(Point(piece.coords[0]))
        b = output_line.project(Point(piece.coords[-1]))
        out = substring(output_line, a, b) if a < b else output_line
        if out.is_empty or out.geom_type == "Point" or out.length == 0:
            out = output_line
        worst = max(
            worst,
            float(
                shapely.frechet_distance(
                    shapely.segmentize(piece, step), shapely.segmentize(out, step)
                )
            ),
        )
    return worst


def evaluate_gates(
    output_line: LineString,
    mode: str,
    cfg: GateConfig,
    *,
    ref_line: LineString | None = None,
    ref_pieces: list | None = None,
    obs_points: list | None = None,
    stops_xy: list | None = None,
    stop_radius: float = 50.0,
    dense: bool = True,
) -> GateReport:
    """Run every gate; collect ALL failures (diagnostics, not fail-fast).

    output_line / ref_line are projected LineStrings; obs_points and
    stops_xy are lists of projected (x, y). ref_pieces (optional) are the
    trusted contiguous reference stretches left after foreign-run
    excision — when given (and more than one), the Fréchet gate runs
    piecewise; ref_line still anchors the length-ratio gate.
    """
    report = GateReport(passed=True)
    if (
        output_line is None
        or output_line.is_empty
        or len(output_line.coords) < 2
        or output_line.length == 0
    ):
        # includes all-identical coordinates (zero length): segmentize would
        # raise a GEOSException on such degenerate input
        report.passed = False
        report.failures.append("empty_output")
        return report

    tol = cfg.coverage_tol_m.get(mode, 25.0)

    if dense and obs_points:
        inside = sum(1 for xy in obs_points if output_line.distance(Point(xy)) <= tol)
        report.coverage = inside / len(obs_points)
        if report.coverage < cfg.min_coverage:
            report.failures.append(
                f"coverage {report.coverage:.3f} < {cfg.min_coverage} (tol {tol} m)"
            )

    # zero-length ref (all-identical coords) would crash segmentize; skip 2–3
    if dense and ref_line is not None and not ref_line.is_empty and ref_line.length > 0:
        step = cfg.frechet_segmentize_m
        if ref_pieces and len(ref_pieces) > 1:
            report.frechet_m = _piecewise_frechet(ref_pieces, output_line, step)
        else:
            report.frechet_m = float(
                shapely.frechet_distance(
                    shapely.segmentize(ref_line, step),
                    shapely.segmentize(output_line, step),
                )
            )
        if report.frechet_m > cfg.max_frechet_m:
            report.failures.append(
                f"frechet {report.frechet_m:.0f} m > {cfg.max_frechet_m} m"
            )
        report.length_ratio = output_line.length / ref_line.length
        if not (cfg.length_ratio_min <= report.length_ratio <= cfg.length_ratio_max):
            report.failures.append(
                f"length_ratio {report.length_ratio:.3f} outside "
                f"[{cfg.length_ratio_min}, {cfg.length_ratio_max}]"
            )

    if not dense and stops_xy and len(stops_xy) >= 2:
        chord = LineString(stops_xy).length
        if chord > 0:
            report.length_ratio = output_line.length / chord
            if not (
                cfg.sparse_length_ratio_min
                <= report.length_ratio
                <= cfg.sparse_length_ratio_max
            ):
                report.failures.append(
                    f"sparse_length_ratio {report.length_ratio:.3f} outside "
                    f"[{cfg.sparse_length_ratio_min}, {cfg.sparse_length_ratio_max}]"
                )

    if stops_xy:
        worst = max(output_line.distance(Point(xy)) for xy in stops_xy)
        report.max_stop_dist_m = worst
        if worst > stop_radius:
            report.failures.append(
                f"stop {worst:.0f} m from output > candidate radius {stop_radius} m"
            )

    report.passed = not report.failures
    return report
