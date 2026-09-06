#!/usr/bin/env python3
"""Report meaningful changes from Criterion, cost-pass, and perfbench artifacts."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass, field
from pathlib import Path


class ReportError(ValueError):
    """An artifact is missing, invalid, or not comparable."""


@dataclass
class Metric:
    unit: str
    kind: str
    values: list[float] = field(default_factory=list)
    lower: list[float] = field(default_factory=list)
    upper: list[float] = field(default_factory=list)

    def add(self, value, lower=None, upper=None) -> None:
        value = number(value)
        lower = value if lower is None else number(lower)
        upper = value if upper is None else number(upper)
        if not lower <= value <= upper:
            raise ReportError("invalid measurement interval")
        self.values.append(value)
        self.lower.append(lower)
        self.upper.append(upper)


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
    metrics: dict, name: str, unit: str, kind: str, value, lower=None, upper=None
) -> None:
    metric = metrics.setdefault(name, Metric(unit, kind))
    metric.add(value, lower, upper)


def load_side(root: Path, manifest: dict, side: str) -> tuple[dict, list[str]]:
    metrics, warnings = {}, []
    expected = set(manifest["cases"])
    for repetition in range(1, manifest["repetitions"] + 1):
        directory = root / side / f"{repetition:02d}"
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
                )
            except (ReportError, KeyError, TypeError) as error:
                warnings.append(
                    f"{side}/{repetition}/{name}: missing or invalid Criterion measurement ({error})"
                )
        try:
            costs = read_costs(directory / "criterion.log")
            if costs["schemaVersion"] != 1:
                raise ReportError("unsupported cost schema")
            rows = {row["name"]: row for row in costs["cases"]}
            if set(rows) != expected or len(rows) != len(costs["cases"]):
                raise ReportError("cost case set changed")
            for name, row in rows.items():
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
        try:
            mixed = read_json(directory / "mixed.json")
            if (
                mixed["schemaVersion"] != 1
                or mixed["scenario"] != "mixed"
                or mixed["backend"] != "memory"
                or mixed["modelTimeSpeedup"] != 5
            ):
                raise ReportError("unsupported mixed schema or backend model")
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
            if len(shapes) != 4 or {shape["shape"] for shape in shapes} != {
                "rwSingle",
                "rwMany",
                "roSingle",
                "roMulti",
            }:
                raise ReportError("mixed shape set changed")
            for shape in shapes:
                name = shape["shape"]
                if number(shape["committed"]) < 100 or not shape["converged"]:
                    warnings.append(
                        f"{side}/{repetition}/{name}: insufficient latency/throughput observations"
                    )
                    continue
                for key, unit, kind in (
                    ("meanMs", "model ms/tx", "time"),
                    ("p90Ms", "model ms/tx", "time"),
                    ("txPerSec", "tx/model s", "rate"),
                ):
                    add(metrics, f"mixed/{name}: {key}", unit, kind, shape[key])
        except (ReportError, KeyError, TypeError, IndexError) as error:
            warnings.append(
                f"{side}/{repetition}: invalid mixed measurements ({error})"
            )
    return metrics, warnings


def changed(base: Metric, candidate: Metric) -> tuple[bool, bool]:
    """Return (report change, warn about uncertainty), without a numeric gate."""
    before, after = statistics.median(base.values), statistics.median(candidate.values)
    separated = max(base.upper) < min(candidate.lower) or max(candidate.upper) < min(
        base.lower
    )
    movement = abs(after - before) / before if before else (math.inf if after else 0)
    substantial = movement >= 0.05 if base.kind != "cost" else before != after
    spread = max(
        max(base.upper) - min(base.lower), max(candidate.upper) - min(candidate.lower)
    )
    noisy = spread > 0.1 * max(before, after) if max(before, after) else False
    return substantial and separated, (substantial and not separated) or noisy


def escape(text: str) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ").replace("`", "'")


def render_report(root: Path, base_label: str, candidate_label: str) -> str:
    manifest = read_json(root / "manifest.json")
    if (
        manifest.get("schemaVersion") != 1
        or not isinstance(manifest.get("repetitions"), int)
        or manifest["repetitions"] < 3
    ):
        raise ReportError("unsupported comparison manifest")
    base, warnings_a = load_side(root, manifest, "main")
    candidate, warnings_b = load_side(root, manifest, "pr")
    warnings = [*manifest.get("warnings", []), *warnings_a, *warnings_b]
    rows = []
    for name in sorted(set(base) | set(candidate)):
        if (
            name not in base
            or name not in candidate
            or len(base[name].values) != manifest["repetitions"]
            or len(candidate[name].values) != manifest["repetitions"]
        ):
            warnings.append(f"{name}: incomplete paired measurements")
            continue
        a, b = base[name], candidate[name]
        report, uncertain = changed(a, b)
        if uncertain:
            warnings.append(f"{name}: noisy or inconclusive")
        if not report:
            continue
        before, after = statistics.median(a.values), statistics.median(b.values)
        delta = after - before
        relative = f"{delta / before:+.1%}" if before else "new from zero"
        if a.kind == "cost":
            direction = "changed"
        else:
            improved = after > before if a.kind == "rate" else after < before
            direction = "improved" if improved else "regressed"
        rows.append(
            f"| {escape(name)} | {before:,.3f} | {after:,.3f} | {delta:+,.3f} ({relative}) | {a.unit} | {direction} |"
        )
    lines = [
        "# Performance comparison",
        "",
        f"Base: `{escape(base_label)}`; candidate: `{escape(candidate_label)}`.",
        "",
    ]
    if rows:
        lines += [
            "| Metric | Base | Candidate | Change | Unit | Result |",
            "| --- | ---: | ---: | ---: | --- | --- |",
            *rows,
            "",
        ]
    else:
        lines += ["No meaningful changes detected in complete measurements.", ""]
    if warnings:
        lines += [
            "## Measurement warnings",
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
