#!/usr/bin/env python3
"""Report meaningful changes from the perfbench artifacts of two revisions."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

# The backend model of every compared process.
MODEL = {
    "backend": "memory",
    # Virtual time is not compressed: the paused clock is the model clock.
    "timeMode": "virtual",
    "modelTimeSpeedup": 1,
}

# The version of the perfbench results that the comparison reads.
SCHEMA_VERSION = 3
# The version of the manifest that the driver writes.
MANIFEST_VERSION = 3
# The case with four shapes at the same time on one leaf.
SHARED_LEAF_CASE = "shapes_shared_leaf"
TIMING_FILE = "timing.json"
COUNTERS_FILE = "counters.json"

SHARED_LEAF_SHAPES = ("rw-single", "rw-many", "ro-single", "ro-multi")
# The latency metrics of every shape of every case.
LATENCY_METRICS = (
    ("p50Ms", "model ms/tx", "time"),
    ("p90Ms", "model ms/tx", "time"),
    ("txPerSec", "tx/model s", "rate"),
)
# Unchanged-engine controls retain about 1–2% uncertainty after 32 pairs.
# Keep the effect floor separate from the test for a repeatable direction.
TIMING_THRESHOLD = 0.02
# Allow small object-size variation only when the complete cost range is tight.
# This does not suppress changes between disjoint cost ranges.
COST_NOISE_THRESHOLD = 0.01


class ReportError(ValueError):
    """An artifact is missing, invalid, or not comparable."""


@dataclass
class Metric:
    """The values of one metric, one for each pair of processes."""

    unit: str
    kind: str
    values: list[float] = field(default_factory=list)

    def add(self, value) -> None:
        value = number(value)
        if self.kind != "cost" and value == 0:
            raise ReportError("timing and rate measurements must be positive")
        self.values.append(value)


def number(value) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ReportError(f"invalid nonnegative measurement: {value!r}")
    return float(value)


def read_json(path: Path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as error:
        raise ReportError(f"cannot read {path}: {error}") from error


def read_report(path: Path) -> dict:
    """Read one perfbench result on the compared backend model."""
    report = read_json(path)
    # A result of an older harness has no time mode and ran on real time, which
    # measures different latencies.
    if report.get("schemaVersion") != SCHEMA_VERSION or any(
        report.get(key) != value for key, value in MODEL.items()
    ):
        raise ReportError("unsupported perfbench schema or backend model")
    return report


def read_case(path: Path, case: str, latency_jitter: bool) -> dict:
    """Read the cell of the experiment `case` from a perfbench result.

    The result must have the latency profile of the comparison, and the
    experiment must have one run of one cell that did not fail.
    """
    report = read_report(path)
    if report.get("latencyJitter", True) is not latency_jitter:
        raise ReportError(f"{case} latency profile differs from comparison settings")
    experiments = [e for e in report["experiments"] if e["name"] == case]
    if len(experiments) != 1:
        raise ReportError(f"expected one {case} experiment, found {len(experiments)}")
    runs = experiments[0]["runs"]
    if len(runs) != 1 or len(runs[0]["cells"]) != 1:
        raise ReportError(f"expected one run of one cell of {case}")
    cell = runs[0]["cells"][0]
    if cell["failures"]:
        raise ReportError(f"{case} cell failed")
    return cell


def add(metrics: dict, name: str, unit: str, kind: str, value) -> None:
    metric = metrics.get(name)
    if metric is None:
        metric = Metric(unit, kind)
    metric.add(value)
    metrics[name] = metric


def read_measurement(
    directory: Path, cases: list[str] | tuple[str, ...], *, latency_jitter: bool
) -> tuple[dict, list[str]]:
    """Read and validate the artifacts of all cases in a repetition.

    The latency of a case comes from the process with the latency profile of the
    comparison. Its counters come from a process without jitter: jitter moves
    the count of background operations, such as listings of the garbage
    collector, by one per process, which the counters cannot tell from a change.
    Without jitter, the counters repeat exactly.
    """
    metrics, warnings = {}, []
    side, repetition = directory.parent.name, directory.name
    for name in cases:
        label = f"{side}/{repetition}/{name}"
        try:
            timing = read_case(directory / TIMING_FILE, name, latency_jitter)
            load_latency(name, timing, metrics, warnings, label)
        except (ReportError, KeyError, TypeError, ValueError, IndexError) as error:
            warnings.append(f"{label}: invalid latency measurement ({error})")
        try:
            counters = read_case(directory / COUNTERS_FILE, name, False)
            load_costs(name, counters["costs"], metrics, warnings, label)
        except (ReportError, KeyError, TypeError, ValueError, IndexError) as error:
            warnings.append(f"{label}: invalid counter measurement ({error})")
    return metrics, warnings


def load_latency(name, cell, metrics, warnings, label):
    try:
        if name == SHARED_LEAF_CASE:
            check_shared_leaf(cell)
        shapes = cell["shapes"]
        if not shapes:
            raise ReportError("no shapes")
        for shape in shapes:
            if number(shape["committed"]) < 100 or not shape["converged"]:
                warnings.append(
                    f"{label}/{shape['shape']}: insufficient latency/throughput observations"
                )
                continue
            # A case with one shape needs no shape in the name of its metrics.
            prefix = name if len(shapes) == 1 else f"{name}/{shape['shape']}"
            for key, unit, kind in LATENCY_METRICS:
                add(metrics, f"{prefix}: {key}", unit, kind, shape[key])
    except (ReportError, KeyError, TypeError, IndexError) as error:
        warnings.append(f"{label}: invalid latency measurements ({error})")


def load_costs(name, costs, metrics, warnings, label):
    try:
        if number(costs["transactions"]) == 0:
            raise ReportError("no completed transactions")
        for window in ("workload", "shutdown", "combined"):
            for counter in (
                "reads",
                "writes",
                "lists",
                "readBodyBytes",
                "writeBodyBytes",
                "coordinatorSubmissions",
                "coordinatorRounds",
            ):
                unit = "bytes/tx" if "Bytes" in counter else "count/tx"
                add(metrics, f"{name}/{window}: {counter}", unit, "cost", costs[window][counter])
    except (ReportError, KeyError, TypeError) as error:
        warnings.append(f"{label}: invalid cost measurements ({error})")


def check_shared_leaf(cell):
    """Check that the case still has the settings that the comparison assumes."""
    if (
        cell["workload"] != "low-conflict"
        or cell["affinityPct"] != 100
        or cell["databases"] != 1
        or cell["workersPerShape"] != 1
    ):
        raise ReportError("shared-leaf settings changed")
    if {shape["shape"] for shape in cell["shapes"]} != set(SHARED_LEAF_SHAPES) or len(
        cell["shapes"]
    ) != len(SHARED_LEAF_SHAPES):
        raise ReportError("shared-leaf shape set changed")


@lru_cache(maxsize=None)
def t_critical(degrees: int, family_size: int) -> float:
    """Return the two-sided Student-t bound for simultaneous 95% coverage."""
    # Bonferroni assigns .05/family_size to each planned two-sided interval.
    # https://www.itl.nist.gov/div898/handbook/prc/section4/prc463.htm
    target = 1 - 0.05 / (2 * family_size)
    lower, upper = 0.0, 1.0
    while t_cdf(upper, degrees) < target:
        upper *= 2
    for _ in range(60):
        middle = (lower + upper) / 2
        if t_cdf(middle, degrees) < target:
            lower = middle
        else:
            upper = middle
    return upper


def t_cdf(value: float, degrees: int) -> float:
    """Evaluate the Student-t distribution for positive integer degrees."""
    # With t=sqrt(degrees)*tan(theta), integrate cos(theta)^(degrees-1).
    # Its reduction formula avoids a numerical-integration dependency for the
    # small integer sample counts used here. Density and normalization:
    # https://www.itl.nist.gov/div898/handbook/eda/section3/eda3664.htm
    theta = math.atan(value / math.sqrt(degrees))
    sine, cosine = math.sin(theta), math.cos(theta)
    power = degrees - 1
    integral = theta if power % 2 == 0 else sine
    for exponent in range(2 if power % 2 == 0 else 3, power + 1, 2):
        integral = (
            sine * cosine ** (exponent - 1) + (exponent - 1) * integral
        ) / exponent
    normalizer = math.exp(
        math.lgamma((degrees + 1) / 2) - math.lgamma(degrees / 2)
    ) / math.sqrt(math.pi)
    return 0.5 + normalizer * integral


@dataclass(frozen=True)
class Comparison:
    relative: float | None
    interval: tuple[float, float] | None
    report: bool
    uncertain: bool


def compare(base: Metric, candidate: Metric, family_size: int = 1) -> Comparison:
    """Distinguish repeatable changes from effects the measurements cannot resolve."""
    before, after = statistics.median(base.values), statistics.median(candidate.values)
    if base.kind == "cost":
        separated = max(base.values) < min(candidate.values) or max(
            candidate.values
        ) < min(base.values)
        spread = max(
            max(base.values) - min(base.values),
            max(candidate.values) - min(candidate.values),
        )
        noisy = spread > 0.1 * max(before, after) if max(before, after) else False
        small = max(*base.values, *candidate.values) - min(
            *base.values, *candidate.values
        ) <= COST_NOISE_THRESHOLD * max(before, after)
        # Small overlapping ranges are expected for variable-size objects.
        # Unequal medians alone do not make their costs inconclusive.
        return Comparison(
            (after - before) / before if before else None,
            None,
            before != after and separated,
            (before != after and not separated and not small) or noisy,
        )

    # Each fresh process pair is one observation.
    ratios = [
        math.log(b / a) for a, b in zip(base.values, candidate.values, strict=True)
    ]
    mean = statistics.mean(ratios)
    count = len(ratios)
    variance = statistics.variance(ratios) / count
    half = t_critical(min(count - 1, 30), family_size) * math.sqrt(variance)
    relative = math.expm1(mean)
    lower, upper = math.expm1(mean - half), math.expm1(mean + half)
    # Effect size and statistical significance answer different questions:
    # a small repeatable change must not need an interval wholly beyond the floor.
    increase = relative >= TIMING_THRESHOLD and lower > 0
    decrease = relative <= -TIMING_THRESHOLD and upper < 0
    report = increase or decrease
    resolved_small = lower >= -TIMING_THRESHOLD and upper <= TIMING_THRESHOLD
    return Comparison(
        relative, (lower, upper), report, not report and not resolved_small
    )


def escape(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ").replace("`", "'")


def validate_manifest(manifest: dict) -> None:
    cases = manifest.get("cases")
    if (
        manifest.get("schemaVersion") != MANIFEST_VERSION
        or not isinstance(cases, list)
        or any(not isinstance(name, str) or not name for name in cases)
        or len(set(cases)) != len(cases)
        or SHARED_LEAF_CASE not in cases
        or type(manifest.get("latencyJitter")) is not bool
    ):
        raise ReportError("unsupported comparison manifest")
    checkpoints = manifest.get("checkpoints")
    if (
        not isinstance(checkpoints, list)
        or not checkpoints
        or any(type(n) is not int or n < 4 or n % 2 for n in checkpoints)
        or checkpoints != sorted(set(checkpoints))
        or type(manifest.get("completedPairs")) is not int
        or not 0 <= manifest["completedPairs"] <= checkpoints[-1]
    ):
        raise ReportError("invalid checkpoint plan or completed pair count")


@dataclass
class Analysis:
    pairs: int
    metrics: dict[str, tuple[Metric, Metric, Comparison]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    @property
    def unresolved(self) -> list[str]:
        """Return the names of the timing metrics whose change is unclear."""
        return [
            name
            for name, (base, _, comparison) in self.metrics.items()
            if base.kind != "cost" and comparison.uncertain
        ]

    @property
    def resolved(self) -> bool:
        return bool(self.metrics) and not self.warnings and not self.unresolved


def analyze(root: Path, manifest: dict) -> Analysis:
    """Evaluate all cases at the last completed, planned checkpoint."""
    completed = manifest["completedPairs"]
    repetitions = max((n for n in manifest["checkpoints"] if n <= completed), default=0)
    result = Analysis(repetitions)
    if not repetitions:
        result.warnings.append(f"no completed checkpoint ({completed} complete pairs)")
        return result
    if completed != repetitions:
        result.warnings.append(
            f"stopped after {completed} complete pairs; using checkpoint at {repetitions} pairs"
        )
    sides = []
    for side in ("main", "pr"):
        metrics = {}
        for repetition in range(1, repetitions + 1):
            samples, warnings = read_measurement(
                root / side / f"{repetition:02d}",
                manifest["cases"],
                latency_jitter=manifest["latencyJitter"],
            )
            result.warnings.extend(warnings)
            for key, sample in samples.items():
                add(metrics, key, sample.unit, sample.kind, sample.values[0])
        sides.append(metrics)
    base, candidate = sides
    # Include every planned comparison, even if its measurements are missing.
    # All checkpoints share the error budget; stopping early cannot narrow it.
    shapes = sum(
        len(SHARED_LEAF_SHAPES) if case == SHARED_LEAF_CASE else 1
        for case in manifest["cases"]
    )
    family_size = shapes * len(LATENCY_METRICS) * len(manifest["checkpoints"])
    for name in sorted(set(base) | set(candidate)):
        if (
            name not in base
            or name not in candidate
            or len(base[name].values) != repetitions
            or len(candidate[name].values) != repetitions
        ):
            result.warnings.append(f"{name}: incomplete paired measurements")
            continue
        a, b = base[name], candidate[name]
        result.metrics[name] = (a, b, compare(a, b, family_size))
    return result


def render_report(root: Path, base_label: str, candidate_label: str) -> str:
    manifest = read_json(root / "manifest.json")
    validate_manifest(manifest)
    result = analyze(root, manifest)
    warnings = [*manifest.get("warnings", []), *result.warnings]
    rows = []
    for name, (a, b, comparison) in sorted(result.metrics.items()):
        interval = (
            f"[{comparison.interval[0]:+.1%}, {comparison.interval[1]:+.1%}]"
            if comparison.interval is not None
            else "—"
        )
        if comparison.uncertain:
            detail = f" {interval}" if comparison.interval is not None else ""
            warnings.append(f"{name}: noisy or inconclusive{detail}")
        if not comparison.report:
            continue
        before, after = statistics.median(a.values), statistics.median(b.values)
        delta = after - before
        relative = (
            f"{comparison.relative:+.1%}"
            if comparison.relative is not None
            else "new from zero"
        )
        if a.kind == "cost":
            direction = "changed"
        else:
            # Only a cost can change from zero, which has no relative change.
            assert comparison.relative is not None
            improved = (
                comparison.relative > 0 if a.kind == "rate" else comparison.relative < 0
            )
            direction = "improved" if improved else "regressed"
        change = f"{delta:+,.3f} ({relative})" if a.kind == "cost" else relative
        rows.append(
            f"| {escape(name)} | {before:,.3f} | {after:,.3f} | {change} | {interval} | {a.unit} | {direction} |"
        )
    lines = [
        "# Performance comparison",
        "",
        f"Base: `{escape(base_label)}`; candidate: `{escape(candidate_label)}`.",
        "",
    ]
    if rows:
        lines += [
            "| Metric | Base median | Candidate median | Change | 95% simultaneous interval | Unit | Result |",
            "| --- | ---: | ---: | ---: | --- | --- | --- |",
            *rows,
            "",
        ]
    else:
        lines += [
            "No conclusive changes; some measurements remain unresolved."
            if warnings
            else "No meaningful changes detected in complete measurements.",
            "",
        ]
    if warnings:
        lines += [
            "## Measurement warnings",
            "",
            "Timing ranges are simultaneous 95% confidence intervals for the change.",
            "",
            *[f"- {escape(warning)}" for warning in sorted(set(warnings))],
            "",
        ]
    lines += [
        "Full results and logs are retained as artifacts.",
        "",
    ]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--base-label", required=True)
    parser.add_argument("--candidate-label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.write_text(
        render_report(args.input, args.base_label, args.candidate_label)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
