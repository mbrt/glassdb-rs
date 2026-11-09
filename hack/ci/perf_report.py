#!/usr/bin/env python3
"""Report meaningful changes between the perfbench results of two revisions.

Each revision has a directory with two results. `counters.json` comes from the
experiments with the tag `ci/counters`, in one run without latency jitter:
their counters repeat exactly. `timing.json` comes from the experiments with
the tag `ci/timing`, in several runs with latency jitter: each run is one
independent sample of the latency.
"""

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
TIMING_FILE = "timing.json"
COUNTERS_FILE = "counters.json"

# The latency metrics of every shape of every cell.
LATENCY_METRICS = (
    ("p50Ms", "model ms/tx", "time"),
    ("p90Ms", "model ms/tx", "time"),
    ("txPerSec", "tx/model s", "rate"),
)
# The smallest timing change that the report shows. The floor of a metric is
# also at least the spread of one of its runs: a smaller change is not
# meaningful even if it is repeatable, because the runs of an unchanged
# revision can differ by that much. The test for a repeatable direction is
# separate from this floor.
TIMING_THRESHOLD = 0.02
# The smallest change of the spread of one run that the report shows, as a
# ratio. With 32 runs a side, a spread that grows by 3 times or more shows for
# most metrics, and one that grows by 2 times for some of them.
SPREAD_THRESHOLD = 1.5
SPREAD_NOTE = (
    "The spread in this table is the mean distance of a run from the median,"
    " scaled to a standard deviation, which one outlier run moves less than the"
    " standard deviation of the floor. A change of the spread shows when it is"
    f" {SPREAD_THRESHOLD}× or more, and the runs show its direction."
)
# The size of a record with a timestamp changes by a byte with the start time
# of the process, so a byte count can move by this much with no change.
BYTES_NOISE_THRESHOLD = 0.01


class ReportError(ValueError):
    """An artifact is missing, invalid, or not comparable."""


@dataclass
class Metric:
    """The values of one metric: one for each run of the timing experiments,
    or one for the counters."""

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


def read_cells(path: Path, latency_jitter: bool) -> list[tuple[int, str, dict]]:
    """Read the cells of all experiments of a perfbench result.

    Returns `(run, name, cell)` for each cell of each run. The result must have
    the latency profile `latency_jitter`, and the same runs of each experiment.
    The name of a cell is the name of its experiment, and the labels of the
    cell if the experiment has more than one cell.
    """
    report = read_report(path)
    if report.get("latencyJitter", True) is not latency_jitter:
        raise ReportError("latency profile differs from comparison settings")
    if not report["experiments"]:
        raise ReportError("no experiments")
    cells, runs = [], None
    for experiment in report["experiments"]:
        name = experiment["name"]
        numbers = [run["run"] for run in experiment["runs"]]
        if runs is None:
            runs = numbers
        if numbers != runs or not runs:
            raise ReportError(f"{name} has other runs than the other experiments")
        for run in experiment["runs"]:
            many = len(run["cells"]) > 1
            for cell in run["cells"]:
                labels = list(cell["labels"].values()) if many else []
                cells.append((run["run"], "/".join([name, *labels]), cell))
    return cells


def add(metrics: dict, name: str, unit: str, kind: str, value) -> None:
    metric = metrics.get(name)
    if metric is None:
        metric = Metric(unit, kind)
    metric.add(value)
    metrics[name] = metric


def read_side(directory: Path) -> tuple[dict, list[str]]:
    """Read and validate the results of one revision.

    The latency comes from the runs with latency jitter. The counters come from
    a run without it: jitter moves the count of background operations, such as
    listings of the garbage collector, and the path of some transactions, which
    the counters cannot tell from a change. Without jitter, they repeat
    exactly.
    """
    metrics, warnings = {}, []
    side = directory.name
    for path, jitter, load, kind in (
        (directory / TIMING_FILE, True, load_latency, "latency"),
        (directory / COUNTERS_FILE, False, load_costs, "counter"),
    ):
        try:
            for run, name, cell in read_cells(path, jitter):
                label = f"{side}/{name}/run {run}"
                if cell["failures"]:
                    warnings.append(f"{label}: cell failed")
                    continue
                load(name, cell, metrics, warnings, label)
        except (ReportError, KeyError, TypeError, ValueError, IndexError) as error:
            warnings.append(f"{side}: invalid {kind} measurement ({error})")
    return metrics, warnings


def load_latency(name, cell, metrics, warnings, label):
    try:
        shapes = cell["shapes"]
        if not shapes:
            raise ReportError("no shapes")
        for shape in shapes:
            if number(shape["committed"]) < 100 or not shape["converged"]:
                warnings.append(
                    f"{label}/{shape['shape']}: insufficient latency/throughput observations"
                )
                continue
            # A cell with one shape needs no shape in the name of its metrics.
            prefix = name if len(shapes) == 1 else f"{name}/{shape['shape']}"
            for key, unit, kind in LATENCY_METRICS:
                add(metrics, f"{prefix}: {key}", unit, kind, shape[key])
    except (ReportError, KeyError, TypeError, IndexError) as error:
        warnings.append(f"{label}: invalid latency measurements ({error})")


def load_costs(name, cell, metrics, warnings, label):
    try:
        costs = cell["costs"]
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
class Spread:
    """The spread of one run on each side, as a relative change."""

    before: float
    after: float
    # The change of the spread from the base to the candidate.
    ratio: float
    report: bool


@dataclass(frozen=True)
class Comparison:
    relative: float | None
    interval: tuple[float, float] | None
    report: bool
    uncertain: bool
    # The smallest change that the report shows, for a timing metric.
    floor: float | None = None
    spread: Spread | None = None

    @property
    def noisy(self) -> bool:
        """The runs cannot resolve the change, and the spread test detects no
        change of their spread. With 32 runs, the test detects a spread that
        grows by 3 times for most metrics, but by 2 times only for some."""
        return self.uncertain and self.spread is not None and not self.spread.report


def welch(a: list[float], b: list[float], family_size: int) -> tuple[float, float]:
    """Return the change of the mean from `a` to `b`, and the half-width of its
    simultaneous 95% interval."""
    va, vb = statistics.variance(a) / len(a), statistics.variance(b) / len(b)
    variance = va + vb
    if variance == 0:
        degrees = len(a) + len(b) - 2
    else:
        degrees = int(
            variance**2 / (va**2 / (len(a) - 1) + vb**2 / (len(b) - 1))
        )
    half = t_critical(max(1, degrees), family_size) * math.sqrt(variance)
    return statistics.mean(b) - statistics.mean(a), half


def compare_spread(a: list[float], b: list[float], family_size: int) -> Spread:
    """Compare the spread of the log values of two sides."""
    # Brown-Forsythe: the distances from the median of each side are compared
    # as values, so a change of the spread is a change of their mean. It is
    # robust to the skew of latency.
    # https://www.itl.nist.gov/div898/handbook/eda/section3/eda35a.htm
    distances = [
        [abs(value - statistics.median(side)) for value in side] for side in (a, b)
    ]
    change, half = welch(*distances, family_size)
    # The mean distance, scaled to the standard deviation of a normal
    # distribution, so that the table shows the same statistic as the test.
    before, after = (
        math.sqrt(math.pi / 2) * statistics.mean(side) for side in distances
    )
    ratio = after / before if before else math.inf if after else 1.0
    large = ratio >= SPREAD_THRESHOLD or ratio <= 1 / SPREAD_THRESHOLD
    return Spread(
        math.expm1(before),
        math.expm1(after),
        ratio,
        large and (change - half > 0 or change + half < 0),
    )


def compare(base: Metric, candidate: Metric, family_size: int = 1) -> Comparison:
    """Distinguish repeatable changes from effects the measurements cannot resolve."""
    if base.kind == "cost":
        # The counters repeat exactly, so each side has one value.
        before, after = base.values[0], candidate.values[0]
        relative = (after - before) / before if before else None
        noise = BYTES_NOISE_THRESHOLD * max(before, after) if base.unit == "bytes/tx" else 0
        return Comparison(relative, None, abs(after - before) > noise, False)

    # Each run is one independent sample, so the two sides are compared with
    # an unpaired interval of the change of the log mean (Welch).
    a = [math.log(value) for value in base.values]
    b = [math.log(value) for value in candidate.values]
    if len(a) < 2 or len(b) < 2:
        return Comparison(None, None, False, True)
    mean, half = welch(a, b, family_size)
    relative = math.expm1(mean)
    lower, upper = math.expm1(mean - half), math.expm1(mean + half)
    # The spread of one run, pooled over the two sides.
    spread = math.sqrt(
        (statistics.variance(a) * (len(a) - 1) + statistics.variance(b) * (len(b) - 1))
        / (len(a) + len(b) - 2)
    )
    floor = max(TIMING_THRESHOLD, math.expm1(spread))
    # Effect size and statistical significance answer different questions:
    # a small repeatable change must not need an interval wholly beyond the floor.
    increase = relative >= floor and lower > 0
    decrease = relative <= -floor and upper < 0
    report = increase or decrease
    resolved_small = lower >= -floor and upper <= floor
    return Comparison(
        relative,
        (lower, upper),
        report,
        not report and not resolved_small,
        floor,
        compare_spread(a, b, family_size),
    )


def escape(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ").replace("`", "'")


@dataclass
class Analysis:
    metrics: dict[str, tuple[Metric, Metric, Comparison]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


def analyze(base_dir: Path, candidate_dir: Path) -> Analysis:
    """Compare the results of two revisions."""
    result = Analysis()
    sides = []
    for directory in (base_dir, candidate_dir):
        metrics, warnings = read_side(directory)
        result.warnings.extend(warnings)
        sides.append(metrics)
    base, candidate = sides
    # Include every timing comparison that either side has, even if the other
    # side misses it: both sides run the same experiments, so a metric that a
    # side misses is still in the family.
    timings = {name for side in sides for name, m in side.items() if m.kind != "cost"}
    runs = {len(m.values) for side in sides for m in side.values() if m.kind != "cost"}
    family_size = max(1, len(timings))
    for name in sorted(set(base) | set(candidate)):
        a, b = base.get(name), candidate.get(name)
        expected = 1 if (a or b).kind == "cost" else max(runs, default=0)
        if (
            a is None
            or b is None
            or len(a.values) != expected
            or len(b.values) != expected
        ):
            result.warnings.append(f"{name}: incomplete measurements")
            continue
        result.metrics[name] = (a, b, compare(a, b, family_size))
    return result


def render_report(
    base_dir: Path,
    candidate_dir: Path,
    base_label: str,
    candidate_label: str,
    warnings: list[str] | None = None,
) -> str:
    result = analyze(base_dir, candidate_dir)
    warnings = [*(warnings or []), *result.warnings]
    rows, spreads, noisy = [], [], []
    for name, (a, b, comparison) in sorted(result.metrics.items()):
        interval = (
            f"[{comparison.interval[0]:+.1%}, {comparison.interval[1]:+.1%}]"
            if comparison.interval is not None
            else "—"
        )
        floor = (
            f"±{comparison.floor:.1%}" if comparison.floor is not None else "—"
        )
        spread = comparison.spread
        if spread is not None and spread.report:
            stability = "less stable" if spread.ratio > 1 else "more stable"
            spreads.append(
                f"| {escape(name)} | ±{spread.before:.1%} | ±{spread.after:.1%} | ×{spread.ratio:.2f} | {stability} |"
            )
        if comparison.uncertain:
            detail = f" {interval}" if comparison.interval is not None else ""
            if comparison.floor is not None:
                detail += f" (floor {floor})"
            if comparison.noisy:
                noisy.append(f"{name}:{detail}")
            else:
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
            f"| {escape(name)} | {before:,.3f} | {after:,.3f} | {change} | {interval} | {floor} | {a.unit} | {direction} |"
        )
    lines = [
        "# Performance comparison",
        "",
        f"Base: `{escape(base_label)}`; candidate: `{escape(candidate_label)}`.",
        "",
    ]
    if rows:
        lines += [
            "| Metric | Base median | Candidate median | Change | 95% simultaneous interval | Floor | Unit | Result |",
            "| --- | ---: | ---: | ---: | --- | ---: | --- | --- |",
            *rows,
            "",
        ]
    elif not spreads:
        if warnings:
            headline = "No conclusive changes; some measurements remain unresolved."
        elif noisy:
            headline = "No conclusive changes; some noisy metrics remain inconclusive."
        else:
            headline = "No meaningful changes detected in complete measurements."
        lines += [headline, ""]
    if spreads:
        lines += [
            "## Spread changes",
            "",
            "| Metric | Base spread | Candidate spread | Change | Result |",
            "| --- | ---: | ---: | ---: | --- |",
            *spreads,
            "",
            SPREAD_NOTE,
            "",
        ]
    if noisy:
        # Inconclusive metrics with no detected change of spread are frequent,
        # and a list of them in the warnings hides the other warnings.
        lines += [
            "<details>",
            f"<summary>Inconclusive metrics with no detected change of spread: {len(noisy)}</summary>",
            "",
            *[f"- {escape(line)}" for line in noisy],
            "",
            "</details>",
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


def summarize(directory: Path) -> str:
    """Return the main metrics of the results of one revision, as tables."""
    metrics, warnings = read_side(directory)
    if not metrics:
        raise ReportError(f"no results in {directory}: {'; '.join(warnings)}")

    def spread(values: list[float]) -> str:
        if len(values) < 2:
            return ""
        return f" ±{statistics.stdev(values) / statistics.mean(values):.1%}"

    latency = {}
    for name, metric in metrics.items():
        if metric.kind != "cost":
            case, key = name.rsplit(": ", 1)
            latency.setdefault(case, {})[key] = metric.values
    runs = max((len(v) for row in latency.values() for v in row.values()), default=0)
    lines = [
        f"## Latency (median of {runs} runs, ± the spread of one run)",
        "",
        "| Case | p50 (model ms) | p90 (model ms) | Throughput (tx/model s) |",
        "| --- | ---: | ---: | ---: |",
    ]
    for case, row in sorted(latency.items()):
        cells = [
            f"{statistics.median(row[key]):,.2f}{spread(row[key])}" if key in row else "—"
            for key, _, _ in LATENCY_METRICS
        ]
        lines.append(f"| {escape(case)} | {' | '.join(cells)} |")
    counters = ("reads", "writes", "lists", "coordinatorRounds")
    costs = {}
    for name, metric in metrics.items():
        window, _, counter = name.partition(": ")
        if metric.kind == "cost" and window.endswith("/combined") and counter in counters:
            # The counter experiment of a case has the name of the case and a
            # suffix; the table names the case, as the latency table does.
            case = window.removesuffix("/combined").removesuffix("-counters")
            costs.setdefault(case, {})[counter] = metric.values[0]
    lines += [
        "",
        "## Backend operations and coordinator rounds per transaction",
        "",
        "| Case | Reads | Writes | Lists | Coordinator rounds |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for case, row in sorted(costs.items()):
        cells = [f"{row[c]:.3f}" if c in row else "—" for c in counters]
        lines.append(f"| {escape(case)} | {' | '.join(cells)} |")
    if warnings:
        lines += ["", "## Measurement warnings", "", *[f"- {escape(w)}" for w in warnings]]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, metavar="DIR")
    parser.add_argument("--base", type=Path)
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--base-label", default="base")
    parser.add_argument("--candidate-label", default="candidate")
    parser.add_argument(
        "--manifest",
        type=Path,
        help="the manifest of the build, for its labels and its warnings",
    )
    parser.add_argument(
        "--warning",
        action="append",
        default=[],
        help="a warning to add to the report; it can be given more than once",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.summary:
        try:
            print(summarize(args.summary), end="")
        except ReportError as error:
            parser.exit(1, f"{error}\n")
        return 0
    if not (args.base and args.candidate and args.output):
        parser.error("--base, --candidate, and --output are needed for a comparison")
    labels, warnings = (args.base_label, args.candidate_label), list(args.warning)
    if args.manifest:
        manifest = read_json(args.manifest)
        labels = (manifest.get("base", labels[0]), manifest.get("candidate", labels[1]))
        warnings += manifest.get("warnings", [])
    args.output.write_text(render_report(args.base, args.candidate, *labels, warnings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
