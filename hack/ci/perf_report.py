#!/usr/bin/env python3
"""Report meaningful changes from Criterion, cost-pass, and perfbench artifacts."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

MIXED_SHAPES = ("rwSingle", "rwMany", "roSingle", "roMulti")
MIXED_METRICS = (
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
    unit: str
    kind: str
    values: list[float] = field(default_factory=list)
    lower: list[float] = field(default_factory=list)
    upper: list[float] = field(default_factory=list)
    standard_errors: list[float] = field(default_factory=list)

    def add(self, value, lower=None, upper=None, standard_error=0) -> None:
        value = number(value)
        if self.kind != "cost" and value == 0:
            raise ReportError("timing and rate measurements must be positive")
        lower = value if lower is None else number(lower)
        upper = value if upper is None else number(upper)
        standard_error = number(standard_error)
        if not lower <= value <= upper:
            raise ReportError("invalid measurement interval")
        self.values.append(value)
        self.lower.append(lower)
        self.upper.append(upper)
        self.standard_errors.append(standard_error)


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


def read_costs(path: Path):
    prefix = "diagnostic-costs: "
    try:
        records = [
            line[len(prefix) :]
            for line in path.read_text().splitlines()
            if line.startswith(prefix)
        ]
        if len(records) != 1:
            raise ReportError("expected one diagnostic cost record")
        return json.loads(records[0])
    except (OSError, ValueError) as error:
        raise ReportError(f"cannot read costs from {path}: {error}") from error


def add(
    metrics: dict,
    name: str,
    unit: str,
    kind: str,
    value,
    lower=None,
    upper=None,
    standard_error=0,
) -> None:
    metric = metrics.get(name)
    if metric is None:
        metric = Metric(unit, kind)
    metric.add(value, lower, upper, standard_error)
    metrics[name] = metric


def read_measurement(
    directory: Path,
    name: str,
    *,
    legacy_cases: set[str] | None = None,
    mixed_args: list[str] | tuple[str, ...] = (),
) -> tuple[dict, list[str]]:
    """Read and validate one benchmark process's artifacts."""
    metrics, warnings = {}, []
    side, repetition = directory.parent.name, directory.name
    if name == "mixed":
        load_mixed(
            directory,
            metrics,
            warnings,
            side,
            repetition,
            latency_jitter="--latency-jitter=false" not in mixed_args,
        )
    else:
        load_diagnostics(
            directory,
            {name},
            metrics,
            warnings,
            side,
            repetition,
            cost_log=(
                "criterion.log" if legacy_cases is not None else f"criterion-{name}.log"
            ),
            cost_cases=legacy_cases,
        )
    return metrics, warnings


def load_diagnostics(
    directory,
    expected,
    metrics,
    warnings,
    side,
    repetition,
    *,
    cost_log="criterion.log",
    cost_cases=None,
):
    for name in sorted(expected):
        try:
            # These private Criterion 0.8.2 artifacts must be checked on upgrades.
            estimate = read_json(
                directory / "criterion/diagnostic" / name / "new/estimates.json"
            )["mean"]
            interval = estimate["confidence_interval"]
            add(
                metrics,
                f"{name}: mean group time",
                "ns/group",
                "time",
                estimate["point_estimate"],
                interval["lower_bound"],
                interval["upper_bound"],
                estimate["standard_error"],
            )
        except (ReportError, KeyError, TypeError) as error:
            warnings.append(
                f"{side}/{repetition}/{name}: missing or invalid Criterion measurement ({error})"
            )
    try:
        costs = read_costs(directory / cost_log)
        if costs["schemaVersion"] != 1:
            raise ReportError("unsupported cost schema")
        rows = {row["name"]: row for row in costs["cases"]}
        if (
            set(rows) != (expected if cost_cases is None else cost_cases)
            or len(rows) != len(costs["cases"])
        ):
            raise ReportError("cost case set changed")
        for name, row in rows.items():
            if name not in expected:
                continue
            if number(row["transactions"]) == 0:
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
                    add(
                        metrics,
                        f"{name}/{window}: {counter}",
                        unit,
                        "cost",
                        row[window][counter],
                    )
    except (ReportError, KeyError, TypeError) as error:
        warnings.append(f"{side}/{repetition}: invalid cost measurements ({error})")


def load_mixed(directory, metrics, warnings, side, repetition, *, latency_jitter=True):
    try:
        mixed = read_json(directory / "mixed.json")
        if (
            mixed["schemaVersion"] != 1
            or mixed["scenario"] != "mixed"
            or mixed["backend"] != "memory"
            or mixed["modelTimeSpeedup"] != 5
        ):
            raise ReportError("unsupported mixed schema or backend model")
        if mixed.get("latencyJitter", True) is not latency_jitter:
            raise ReportError("mixed latency profile differs from comparison settings")
        if len(mixed["runs"]) != 1 or len(mixed["runs"][0]["cells"]) != 1:
            raise ReportError("expected one mixed cell")
        cell = mixed["runs"][0]["cells"][0]
        if (
            cell["failures"]
            or cell["mode"] != "lo"
            or cell["affinityPct"] != 100
            or cell["databases"] != 1
            or cell["workersPerShape"] != 1
        ):
            raise ReportError("mixed cell failed or settings changed")
        shapes = cell["shapes"]
        if len(shapes) != len(MIXED_SHAPES) or {
            shape["shape"] for shape in shapes
        } != set(MIXED_SHAPES):
            raise ReportError("mixed shape set changed")
        for shape in shapes:
            name = shape["shape"]
            if number(shape["committed"]) < 100 or not shape["converged"]:
                warnings.append(
                    f"{side}/{repetition}/{name}: insufficient latency/throughput observations"
                )
                continue
            for key, unit, kind in MIXED_METRICS:
                add(metrics, f"mixed/{name}: {key}", unit, kind, shape[key])
    except (ReportError, KeyError, TypeError, IndexError) as error:
        warnings.append(f"{side}/{repetition}: invalid mixed measurements ({error})")


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
        separated = max(base.upper) < min(candidate.lower) or max(
            candidate.upper
        ) < min(base.lower)
        spread = max(
            max(base.upper) - min(base.lower),
            max(candidate.upper) - min(candidate.lower),
        )
        noisy = spread > 0.1 * max(before, after) if max(before, after) else False
        small = (
            max(*base.upper, *candidate.upper) - min(*base.lower, *candidate.lower)
            <= COST_NOISE_THRESHOLD * max(before, after)
        )
        # Small overlapping ranges are expected for variable-size objects.
        # Unequal medians alone do not make their costs inconclusive.
        return Comparison(
            (after - before) / before if before else None,
            None,
            before != after and separated,
            (before != after and not separated and not small) or noisy,
        )

    # Each fresh process pair is one observation. Criterion's narrow intervals
    # within a process do not measure variation between independent repetitions.
    ratios = [
        math.log(b / a) for a, b in zip(base.values, candidate.values, strict=True)
    ]
    mean = statistics.mean(ratios)
    count = len(ratios)
    # Criterion's bootstrap SE estimates uncertainty within each process.
    # The delta method gives log-SE ≈ SE/mean. Use this as a variance floor:
    # repeated point estimates can agree by chance despite imprecise samples.
    # Between-pair variance already includes sampling noise, so do not add it
    # a second time. Mixed measurements provide only between-pair variation.
    sampling_variance = (
        sum(
            (error / value) ** 2
            for metric in (base, candidate)
            for value, error in zip(metric.values, metric.standard_errors, strict=True)
        )
        / count**2
    )
    variance = max(statistics.variance(ratios) / count, sampling_variance)
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
        not isinstance(cases, (list, tuple))
        or any(
            not isinstance(name, str) or not name or name == "mixed" for name in cases
        )
        or len(set(cases)) != len(cases)
    ):
        raise ReportError("invalid benchmark case list")
    if manifest.get("schemaVersion") == 2:
        checkpoints = manifest.get("checkpoints")
        counts = manifest.get("completedPairs")
        if (
            not isinstance(checkpoints, list)
            or not checkpoints
            or any(type(n) is not int or n < 4 or n % 2 for n in checkpoints)
            or checkpoints != sorted(set(checkpoints))
            or not isinstance(counts, dict)
            or set(counts) != {*cases, "mixed"}
            or any(
                type(n) is not int or not 0 <= n <= checkpoints[-1]
                for n in counts.values()
            )
        ):
            raise ReportError("invalid checkpoint plan or completed pair counts")
        return
    if (
        manifest.get("schemaVersion") != 1
        or type(manifest.get("repetitions")) is not int
        or manifest["repetitions"] < 3
        or type(manifest.get("diagnosticRepetitions", manifest["repetitions"]))
        is not int
        or manifest.get("diagnosticRepetitions", manifest["repetitions"]) < 3
    ):
        raise ReportError("unsupported comparison manifest")


@dataclass
class BenchmarkResult:
    pairs: int
    metrics: dict[str, tuple[Metric, Metric, Comparison]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)

    @property
    def resolved(self) -> bool:
        return bool(self.metrics) and not self.warnings and all(
            not comparison.uncertain
            for base, _, comparison in self.metrics.values()
            if base.kind != "cost"
        )


def analyze_benchmark(root: Path, manifest: dict, name: str) -> BenchmarkResult:
    """Evaluate a benchmark at its last completed, planned checkpoint."""
    adaptive = manifest["schemaVersion"] == 2
    if adaptive:
        completed = manifest["completedPairs"][name]
        repetitions = max(
            (n for n in manifest["checkpoints"] if n <= completed), default=0
        )
    else:
        repetitions = (
            manifest["repetitions"]
            if name == "mixed"
            else manifest.get("diagnosticRepetitions", manifest["repetitions"])
        )
        completed = repetitions
    result = BenchmarkResult(repetitions)
    if not repetitions:
        result.warnings.append(
            f"{name}: no completed checkpoint ({completed} complete pairs)"
        )
        return result
    if completed != repetitions:
        result.warnings.append(
            f"{name}: stopped after {completed} complete pairs; using checkpoint at {repetitions} pairs"
        )
    sides = []
    for side in ("main", "pr"):
        metrics = {}
        for repetition in range(1, repetitions + 1):
            directory = root / side / f"{repetition:02d}"
            samples, warnings = read_measurement(
                directory,
                name,
                legacy_cases=None if adaptive else set(manifest["cases"]),
                mixed_args=manifest.get("mixedArgs", ()),
            )
            result.warnings.extend(warnings)
            for key, sample in samples.items():
                add(
                    metrics,
                    key,
                    sample.unit,
                    sample.kind,
                    sample.values[0],
                    sample.lower[0],
                    sample.upper[0],
                    sample.standard_errors[0],
                )
        sides.append(metrics)
    base, candidate = sides
    # Include every planned comparison, even if its measurements are missing.
    # All checkpoints share the error budget; stopping early cannot narrow it.
    family_size = len(manifest["cases"]) + len(MIXED_SHAPES) * len(MIXED_METRICS)
    if adaptive:
        family_size *= len(manifest["checkpoints"])
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
    warnings = list(manifest.get("warnings", []))
    metrics = {}
    for name in (*manifest["cases"], "mixed"):
        result = analyze_benchmark(root, manifest, name)
        metrics.update(result.metrics)
        warnings.extend(result.warnings)
    rows = []
    for name, (a, b, comparison) in sorted(metrics.items()):
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
