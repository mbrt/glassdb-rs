#!/usr/bin/env python3
"""Render a compact Markdown comparison from PR autoresearch JSON artifacts."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

SCORE_RUNS = 11
WORKLOADS = (
    "singleRMW",
    "multiRMW10",
    "batchRead10",
    "batchWrite100",
    "readRepeat",
)
SECONDARY = (
    ("Allocation bytes/tx", "allocBytesPerTx", 0),
    ("Allocations/tx", "allocsPerTx", 1),
    ("Wall ns/tx", "nsPerTx", 0),
    ("CPU ns/tx", "cpuNsPerTx", 0),
)


class ReportError(ValueError):
    """The result artifact is incomplete or has an incompatible schema."""


@dataclass(frozen=True)
class ScoreRun:
    score: float
    backend_latency_ms: int
    workloads: dict[str, float]
    secondary: dict[str, float]


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReportError(f"{field} must be a number")
    result = float(value)
    if not math.isfinite(result):
        raise ReportError(f"{field} must be finite")
    return result


def _nonnegative(value: Any, field: str) -> float:
    result = _number(value, field)
    if result < 0:
        raise ReportError(f"{field} must be nonnegative")
    return result


def _positive_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ReportError(f"{field} must be a positive integer")
    return value


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ReportError(f"{field} must be an object")
    return value


def _array(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ReportError(f"{field} must be an array")
    return value


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise ReportError(f"cannot read {path}: {error}") from error


def _result_files(path: Path) -> list[Path]:
    files = sorted(path.glob("*.json"))
    expected_names = [f"{run:02d}.json" for run in range(1, SCORE_RUNS + 1)]
    if [result.name for result in files] != expected_names:
        raise ReportError(
            f"{path} must contain score runs {expected_names}; "
            f"found {[result.name for result in files]}"
        )
    return files


def load_score_runs(path: Path) -> list[ScoreRun]:
    runs = []
    for result_path in _result_files(path):
        raw = _object(_read_json(result_path), str(result_path))
        workload_rows = _array(raw.get("workloads"), f"{result_path}: workloads")
        workloads: dict[str, float] = {}
        for index, row_value in enumerate(workload_rows):
            row = _object(row_value, f"{result_path}: workloads[{index}]")
            name = row.get("name")
            if not isinstance(name, str):
                raise ReportError(f"{result_path}: workload name must be a string")
            if name in workloads:
                raise ReportError(f"{result_path}: duplicate workload {name}")
            workloads[name] = _nonnegative(
                row.get("costPerTx"), f"{result_path}: {name}.costPerTx"
            )
        if set(workloads) != set(WORKLOADS):
            raise ReportError(
                f"{result_path}: workloads are {sorted(workloads)}; "
                f"expected {sorted(WORKLOADS)}"
            )

        secondary_raw = _object(raw.get("secondary"), f"{result_path}: secondary")
        secondary = {
            field: _nonnegative(secondary_raw.get(field), f"{result_path}: {field}")
            for _, field, _ in SECONDARY
        }
        runs.append(
            ScoreRun(
                score=_nonnegative(raw.get("score"), f"{result_path}: score"),
                backend_latency_ms=_positive_integer(
                    raw.get("backendLatencyMs"),
                    f"{result_path}: backendLatencyMs",
                ),
                workloads=workloads,
                secondary=secondary,
            )
        )
    return runs


def _values(items: Iterable[Any], field: str) -> list[float]:
    return [float(getattr(item, field)) for item in items]


def _summary(values: Iterable[float], digits: int = 2) -> str:
    samples = list(values)
    median = statistics.median(samples)
    low, high = min(samples), max(samples)
    if math.isclose(low, high):
        return f"{median:,.{digits}f}"
    return f"{median:,.{digits}f} ({low:,.{digits}f}–{high:,.{digits}f})"


def _change(base: Iterable[float], candidate: Iterable[float]) -> str:
    base_median = statistics.median(base)
    candidate_median = statistics.median(candidate)
    if base_median == 0:
        return "n/a"
    return f"{(candidate_median / base_median - 1.0) * 100:+.2f}%"


def _escape(value: str) -> str:
    return value.replace("|", "\\|")


def render_report(input_dir: Path, base_label: str, candidate_label: str) -> str:
    base_scores = load_score_runs(input_dir / "score" / "main")
    candidate_scores = load_score_runs(input_dir / "score" / "pr")
    backend_latencies = {
        run.backend_latency_ms for run in [*base_scores, *candidate_scores]
    }
    if len(backend_latencies) != 1:
        raise ReportError(
            "score runs must use one backend latency; "
            f"found {sorted(backend_latencies)} ms"
        )
    backend_latency_ms = next(iter(backend_latencies))

    base_score_values = _values(base_scores, "score")
    candidate_score_values = _values(candidate_scores, "score")
    lines = [
        "# Performance comparison",
        "",
        f"- Base: `{_escape(base_label)}`",
        f"- Candidate: `{_escape(candidate_label)}`",
        f"- Backend model: fixed {backend_latency_ms} ms operation latency over memory.",
        "- Numeric changes are informational and never fail the PR check.",
        "",
        "## Backend-operation score",
        "",
        "Lower is better. Values are medians with the observed min–max range "
        "from 11 interleaved runs.",
        "",
        "| Metric | Main | PR | Change |",
        "| --- | ---: | ---: | ---: |",
        f"| Primary weighted cost/tx | {_summary(base_score_values)} | "
        f"{_summary(candidate_score_values)} | "
        f"{_change(base_score_values, candidate_score_values)} |",
    ]
    for workload in WORKLOADS:
        base = [run.workloads[workload] for run in base_scores]
        candidate = [run.workloads[workload] for run in candidate_scores]
        lines.append(
            f"| `{workload}` cost/tx | {_summary(base)} | {_summary(candidate)} | "
            f"{_change(base, candidate)} |"
        )

    lines.extend(
        [
            "",
            "> The fixed backend latency runs on a single-thread runtime so deferred "
            "protocol work is scheduled consistently; the primary remains "
            "operation-count based.",
            "",
            "<details>",
            "<summary>Latency-stabilized in-memory secondary metrics (informational)</summary>",
            "",
            "Lower is better. These values use the same interleaved runs; their "
            "observed ranges show the run-to-run variability.",
            "",
            "| Metric | Main | PR | Change |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for label, field, digits in SECONDARY:
        base = [run.secondary[field] for run in base_scores]
        candidate = [run.secondary[field] for run in candidate_scores]
        lines.append(
            f"| {label} | {_summary(base, digits)} | {_summary(candidate, digits)} | "
            f"{_change(base, candidate)} |"
        )
    lines.extend(["", "</details>", ""])
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--base-label", required=True)
    parser.add_argument("--candidate-label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    report = render_report(args.input, args.base_label, args.candidate_label)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
