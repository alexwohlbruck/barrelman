#!/usr/bin/env python3
"""shapesnap.gates — per-pattern quality gates (computed in the local UTM).

Gates (docs/transit-pipeline-v3.md stage 3), evaluated on projected
geometry so every threshold is honest meters:

  1. coverage   — fraction of resampled input points within
                  coverage_tol_m[mode] of the output line >= min_coverage
  2. deviation  — discrete Fréchet (shapely.frechet_distance, densified)
                  between the input reference line and the output
                  <= max_frechet_m
  3. length     — output length / input length within length_ratio
  4. stops      — every pattern stop within stop_radius of the output
                  (stop_radius = the regime's candidate radius)

ANY failed gate -> the caller must return the ORIGINAL geometry unchanged
(method="fallback"); good feeds are never degraded.

The sparse regime (regime B: no input shape) has no reference line, so
gates 1–2 are skipped and gate 3 compares against the stop-chain chord
length with wider bounds (network paths are always >= the chord) —
documented deviation from the dense thresholds.

All thresholds live in the single GateConfig dataclass.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import shapely
from shapely.geometry import LineString, Point

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


def evaluate_gates(
    output_line: LineString,
    mode: str,
    cfg: GateConfig,
    *,
    ref_line: LineString | None = None,
    obs_points: list | None = None,
    stops_xy: list | None = None,
    stop_radius: float = 50.0,
    dense: bool = True,
) -> GateReport:
    """Run every gate; collect ALL failures (diagnostics, not fail-fast).

    output_line / ref_line are projected LineStrings; obs_points and
    stops_xy are lists of projected (x, y).
    """
    report = GateReport(passed=True)
    if output_line is None or output_line.is_empty or len(output_line.coords) < 2:
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

    if dense and ref_line is not None and not ref_line.is_empty:
        step = cfg.frechet_segmentize_m
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
        if ref_line.length > 0:
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
