from __future__ import annotations

import json
import math
import statistics
import sys
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from hack.ci import perf_report

SHARED_LEAF = "shapes-shared-leaf"
SHARED_LEAF_SHAPES = ("rw-single", "rw-many", "ro-single", "ro-multi")
# The runs of the timing experiments of each side.
RUNS = 4
COUNTER_KEYS = (
    "reads",
    "writes",
    "lists",
    "readBodyBytes",
    "writeBodyBytes",
    "coordinatorSubmissions",
    "coordinatorRounds",
)


def costs_record():
    return {
        "transactions": 30,
        **{
            window: dict.fromkeys(COUNTER_KEYS, 0)
            for window in ("workload", "shutdown", "combined")
        },
    }


def example_cell(latency=100):
    """Return a case with one shape whose median latency is `latency`."""
    return {
        "labels": {"case": "example"},
        "failures": 0,
        "shapes": [
            {
                "shape": "example",
                "committed": 200,
                "converged": True,
                "p50Ms": latency,
                "p90Ms": 2 * latency,
                "txPerSec": 1000 / latency,
            }
        ],
        "costs": costs_record(),
    }


def shared_leaf_cell():
    return {
        "labels": {"case": SHARED_LEAF},
        "workload": "low-conflict",
        "affinityPct": 100,
        "databases": 1,
        "workersPerShape": 1,
        "failures": 0,
        "shapes": [
            {
                "shape": name,
                "committed": 200,
                "converged": True,
                "meanMs": 10,
                "p50Ms": 10,
                "p90Ms": 20,
                "txPerSec": 100,
            }
            for name in SHARED_LEAF_SHAPES
        ],
        "costs": costs_record(),
    }


def result(latency_jitter, runs):
    return {
        "schemaVersion": 3,
        "backend": "memory",
        "timeMode": "virtual",
        "modelTimeSpeedup": 1,
        "latencyJitter": latency_jitter,
        "experiments": [
            {
                "name": make()["labels"]["case"],
                "runs": [{"run": run, "cells": [make()]} for run in range(1, runs + 1)],
            }
            for make in (example_cell, shared_leaf_cell)
        ],
    }


class PerfReportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for side in ("main", "pr"):
            # The counters come from one run without latency jitter.
            self.write(self.root / side / "timing.json", result(True, RUNS))
            self.write(self.root / side / "counters.json", result(False, 1))

    @staticmethod
    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def update(self, kind, change, *, sides=("pr",)):
        """Change the result file `kind` of `sides`."""
        for side in sides:
            path = self.root / side / f"{kind}.json"
            value = json.loads(path.read_text())
            change(value)
            self.write(path, value)

    def update_cell(self, kind, case, change, *, runs=None, **options):
        """Change the cell of `case` in `runs`, by default in all of them."""

        def apply(value):
            (experiment,) = [e for e in value["experiments"] if e["name"] == case]
            for run in experiment["runs"]:
                if runs is None or run["run"] in runs:
                    change(run["cells"][0])

        self.update(kind, apply, **options)

    def remove_cell(self, kind, case, *, runs, **options):
        """Remove the cell of `case` from `runs`."""

        def apply(value):
            (experiment,) = [e for e in value["experiments"] if e["name"] == case]
            for run in experiment["runs"]:
                if run["run"] in runs:
                    run["cells"] = []

        self.update(kind, apply, **options)

    def set_latency(self, run, before, after):
        for side, latency in (("main", before), ("pr", after)):
            self.update_cell(
                "timing",
                "example",
                lambda cell, latency=latency: cell.update(
                    shapes=example_cell(latency)["shapes"]
                ),
                runs=[run],
                sides=(side,),
            )

    def set_latencies(self, after, before=100):
        for run, value in enumerate(after, 1):
            self.set_latency(run, before, value)

    def report(self):
        return perf_report.render_report(
            self.root / "main", self.root / "pr", "base", "candidate"
        )

    def row(self, report, prefix):
        return next(r for r in report.splitlines() if r.startswith(prefix))

    def test_run_variation_prevents_a_regression_claim(self):
        # Every run is slower, but the variation between runs leaves the
        # direction unresolved.
        self.set_latencies((101, 112, 109, 104))
        report = self.report()
        self.assertNotIn("| Metric", report)
        self.assertRegex(
            report,
            r"example: p50Ms: noisy or inconclusive\s*"
            r"\[-\d+\.\d+%, \+\d+\.\d+%\]",
        )
        self.assertIn("No conclusive changes", report)
        self.assertNotIn("No meaningful changes detected", report)

    def test_repeatable_regression_remains_visible(self):
        self.set_latencies((102.8, 103.0, 103.2, 103.0))
        report = self.report()
        self.assertIn("example: p50Ms |", report)
        self.assertIn("regressed", report)
        self.assertNotIn("inconclusive", report)

    def test_significance_is_tested_against_zero_not_the_size_threshold(self):
        self.set_latencies((102.18, 102.20, 102.22, 102.20))
        report = self.report()
        self.assertIn("+2.2%", report)
        self.assertIn("regressed", report)

    def test_repeatable_subthreshold_effect_is_not_reported(self):
        self.set_latencies((100.98, 101.00, 101.02, 101.00))
        report = self.report()
        self.assertIn("No meaningful changes detected", report)
        self.assertNotIn("regressed", report)

    def test_eight_runs_resolve_a_3_percent_change_at_the_noise_of_a_p90(self):
        # Log changes (%) from unchanged processes, scaled to a spread of 0.9%
        # in one run: the p90 of `rmw-shared-leaf` in one run of 300 s, the
        # noisiest metric of the CI experiments.
        noise = (
            -0.413, -0.332, 0.237, -0.763, 0.440, -0.267, -0.573, -1.435,
            -0.066, 0.013, -0.254, 2.728, 0.252, -1.897, -0.230, 1.276,
        )
        scale = 0.9 / 1.043
        for effect in (0, -0.03, 0.03):
            with self.subTest(effect=effect):
                base = perf_report.Metric("ns", "time")
                candidate = perf_report.Metric("ns", "time")
                for before, after in zip(noise[:8], noise[8:]):
                    base.add(100 * math.exp(scale * before / 100))
                    candidate.add(100 * (1 + effect) * math.exp(scale * after / 100))
                # About 12 shapes with 3 metrics each.
                comparison = perf_report.compare(base, candidate, family_size=36)
                self.assertEqual(comparison.report, effect != 0)
                if effect:
                    assert comparison.relative is not None
                    self.assertGreater(comparison.relative * effect, 0)

    def test_small_estimate_with_large_uncertainty_is_not_unchanged(self):
        self.set_latencies((99, 103, 106, 100))
        report = self.report()
        self.assertIn("inconclusive", report)
        self.assertNotIn("No meaningful changes detected", report)

    def test_simultaneous_intervals_protect_against_multiple_false_alarms(self):
        base = perf_report.Metric("ns", "time")
        candidate = perf_report.Metric("ns", "time")
        for after in (101.44, 102.16, 102.66, 101.96):
            base.add(100)
            candidate.add(after)
        self.assertTrue(perf_report.compare(base, candidate).report)
        simultaneous = perf_report.compare(base, candidate, family_size=20)
        self.assertFalse(simultaneous.report)
        self.assertTrue(simultaneous.uncertain)

    def test_missing_metrics_do_not_reduce_multiple_comparison_correction(self):
        self.set_latencies((102.0, 102.1, 102.2, 102.1))
        before = self.row(self.report(), "| example: p50Ms")
        self.remove_cell("timing", SHARED_LEAF, runs=[RUNS])
        report = self.report()
        self.assertEqual(self.row(report, "| example: p50Ms"), before)
        self.assertIn("incomplete measurements", report)

    def test_student_t_critical_values_match_reference_values(self):
        # NIST's two-sided 95% values, independent of the CDF implementation.
        for degrees, expected in (
            (1, 12.706205),
            (2, 4.302653),
            (3, 3.182446),
            (7, 2.364624),
            (30, 2.042272),
        ):
            with self.subTest(degrees=degrees):
                self.assertAlmostEqual(
                    perf_report.t_critical(degrees, 1), expected, places=5
                )
        # For two degrees of freedom the quantile has a closed form.
        for family_size in (2, 20, 100):
            probability = 1 - 0.05 / family_size
            expected = math.sqrt(2) * probability / math.sqrt(1 - probability**2)
            self.assertAlmostEqual(
                perf_report.t_critical(2, family_size), expected, places=8
            )

    def test_the_interval_of_many_runs_uses_their_degrees_of_freedom(self):
        # 32 runs on each side give about 62 degrees of freedom, so a cap at
        # 30 would make the interval about 2% wider than needed.
        base = perf_report.Metric("ns", "time")
        candidate = perf_report.Metric("ns", "time")
        for run in range(32):
            base.add(100 + run % 2)
            candidate.add(103 + run % 2)
        comparison = perf_report.compare(base, candidate)
        a = [math.log(v) for v in base.values]
        b = [math.log(v) for v in candidate.values]
        standard_error = math.sqrt(
            (statistics.variance(a) + statistics.variance(b)) / 32
        )
        mean = statistics.mean(b) - statistics.mean(a)
        assert comparison.interval is not None
        lower = math.expm1(mean - perf_report.t_critical(62, 1) * standard_error)
        # The Welch degrees can truncate to 61; a cap at 30 moves the bound
        # by about 5e-5.
        self.assertAlmostEqual(comparison.interval[0], lower, delta=1e-5)

    def test_a_change_smaller_than_the_spread_of_one_run_is_not_reported(self):
        # The runs of each side spread by about 5%, as the noisiest p90 of
        # `contention` does. A change of 3.5% is repeatable, but a revision
        # compared with itself can give a change of that size.
        def compare(effect):
            base = perf_report.Metric("ns", "time")
            candidate = perf_report.Metric("ns", "time")
            for run in range(32):
                noise = math.exp(0.05 if run % 2 else -0.05)
                base.add(100 * noise)
                candidate.add(100 * (1 + effect) * noise)
            return perf_report.compare(base, candidate)

        small = compare(0.035)
        assert small.interval is not None and small.floor is not None
        self.assertGreater(small.interval[0], 0)
        self.assertAlmostEqual(small.floor, math.expm1(0.05), delta=0.002)
        self.assertFalse(small.report)
        self.assertTrue(compare(0.10).report)

    def test_the_floor_of_a_metric_with_a_small_spread_is_the_threshold(self):
        base = perf_report.Metric("ns", "time")
        candidate = perf_report.Metric("ns", "time")
        for run in range(32):
            base.add(100 + run % 2)
            candidate.add(100 + run % 2)
        comparison = perf_report.compare(base, candidate)
        self.assertEqual(comparison.floor, perf_report.TIMING_THRESHOLD)
        self.assertFalse(comparison.uncertain)

    def test_a_reported_change_shows_its_floor(self):
        self.set_latencies((102.8, 103.0, 103.2, 103.0))
        self.assertIn("| ±2.0% |", self.row(self.report(), "| example: p50Ms |"))

    def test_the_floor_is_explained_once_and_only_for_a_timing_change(self):
        self.set_latencies((102.8, 103.0, 103.2, 103.0))
        self.remove_cell("timing", SHARED_LEAF, runs=[RUNS])
        report = self.report()
        self.assertIn("incomplete measurements", report)
        self.assertEqual(report.count(perf_report.FLOOR_NOTE), 1)
        # The note explains the column of the table, so it comes before the
        # warnings.
        self.assertLess(
            report.index(perf_report.FLOOR_NOTE), report.index("Measurement warnings")
        )
        self.set_latencies((100,) * RUNS)
        self.assertNotIn(perf_report.FLOOR_NOTE, self.report())

    def test_an_inconclusive_metric_shows_its_floor(self):
        self.set_latencies((101, 112, 109, 104))
        report = self.report()
        self.assertRegex(
            report, r"example: p50Ms: noisy or inconclusive .*\(floor ±\d+\.\d%\)"
        )
        self.assertIn(perf_report.FLOOR_NOTE, report)

    def test_one_run_of_a_timing_metric_is_inconclusive(self):
        base = perf_report.Metric("ns", "time")
        candidate = perf_report.Metric("ns", "time")
        base.add(100)
        candidate.add(150)
        comparison = perf_report.compare(base, candidate)
        self.assertFalse(comparison.report)
        self.assertTrue(comparison.uncertain)

    def test_experiments_with_other_runs_are_invalid(self):
        def drop(value):
            value["experiments"][0]["runs"].pop()

        self.update("timing", drop)
        self.assertIn("other runs than the other experiments", self.report())

    def test_unchanged_rows_are_hidden(self):
        report = self.report()
        self.assertIn("No meaningful changes detected", report)
        self.assertNotIn("| Metric", report)
        self.assertNotIn("Measurement warnings", report)

    def test_the_mean_is_not_compared(self):
        self.update_cell(
            "timing", SHARED_LEAF, lambda cell: cell["shapes"][0].update(meanMs=100)
        )
        report = self.report()
        self.assertIn("No meaningful changes detected", report)
        self.assertNotIn("meanMs", report)

    def test_more_transactions_do_not_hide_variation_between_shared_leaf_runs(self):
        for run, p50 in enumerate((10.1, 11.2, 10.9, 10.4), 1):
            self.update_cell(
                "timing",
                SHARED_LEAF,
                lambda cell, p50=p50: cell["shapes"][0].update(p50Ms=p50),
                runs=[run],
            )
        before = self.report()
        self.assertIn(f"{SHARED_LEAF}/rw-single: p50Ms: noisy or inconclusive", before)
        self.assertNotIn("| Metric", before)
        self.update_cell(
            "timing",
            SHARED_LEAF,
            lambda cell: cell["shapes"][0].update(committed=1_000_000),
        )
        self.assertEqual(self.report(), before)

    def test_missing_percentile_is_not_reported_as_unchanged(self):
        self.update_cell(
            "timing", SHARED_LEAF, lambda cell: cell["shapes"][0].pop("p50Ms")
        )
        report = self.report()
        self.assertIn("invalid latency measurements", report)
        self.assertIn("incomplete measurements", report)
        self.assertNotIn("No meaningful changes detected", report)

    def test_every_case_is_compared_by_median_tail_and_throughput(self):
        self.set_latencies((130,) * RUNS)
        report = self.report()
        for metric in ("p50Ms", "p90Ms", "txPerSec"):
            self.assertIn("regressed", self.row(report, f"| example: {metric}"))

    def test_p50_and_p90_changes_have_correct_direction(self):
        def change(cell):
            shapes = cell["shapes"]
            shapes[0]["p90Ms"] = 30
            shapes[1]["txPerSec"] = 120
            shapes[2]["p50Ms"] = 8

        self.update_cell("timing", SHARED_LEAF, change)
        report = self.report()
        self.assertIn(f"{SHARED_LEAF}/rw-single: p90Ms | 20.000 | 30.000", report)
        self.assertIn(f"{SHARED_LEAF}/rw-many: txPerSec | 100.000 | 120.000", report)
        self.assertIn(f"{SHARED_LEAF}/ro-single: p50Ms | 10.000 | 8.000", report)
        self.assertIn("regressed", report)
        self.assertIn("improved", report)
        self.assertNotIn(f"{SHARED_LEAF}/ro-multi:", report)

    def test_small_timing_change_is_hidden(self):
        self.update_cell(
            "timing", SHARED_LEAF, lambda cell: cell["shapes"][0].update(p50Ms=10.04)
        )
        self.assertNotIn("| Metric", self.report())

    def test_cost_change_from_zero_is_visible(self):
        self.update_cell(
            "counters",
            "example",
            lambda cell: cell["costs"]["shutdown"].update(writeBodyBytes=0.125),
        )
        report = self.report()
        self.assertIn("example/shutdown: writeBodyBytes", report)
        self.assertIn("new from zero", report)

    def test_any_count_change_is_visible(self):
        base = perf_report.Metric("count/tx", "cost")
        candidate = perf_report.Metric("count/tx", "cost")
        base.add(1.000)
        candidate.add(1.001)
        comparison = perf_report.compare(base, candidate)
        self.assertTrue(comparison.report)
        self.assertFalse(comparison.uncertain)

    def test_a_byte_of_a_timestamp_is_not_a_byte_change(self):
        for after, reported in ((1855.5, False), (1880.0, True)):
            with self.subTest(after=after):
                base = perf_report.Metric("bytes/tx", "cost")
                candidate = perf_report.Metric("bytes/tx", "cost")
                base.add(1854.5)
                candidate.add(after)
                comparison = perf_report.compare(base, candidate)
                self.assertEqual(comparison.report, reported)
                self.assertFalse(comparison.uncertain)

    def test_missing_run_is_not_reported_as_unchanged(self):
        self.remove_cell("timing", SHARED_LEAF, runs=[RUNS])
        report = self.report()
        self.assertIn("Measurement warnings", report)
        self.assertIn("incomplete measurements", report)

    def test_an_experiment_of_one_side_is_not_compared(self):
        def rename(value):
            value["experiments"][0]["name"] = "other"

        self.update("timing", rename)
        report = self.report()
        self.assertIn("example: p50Ms: incomplete measurements", report)
        self.assertIn("other: p50Ms: incomplete measurements", report)

    def test_a_cell_of_an_experiment_with_several_cells_is_named_by_its_labels(self):
        def split(value):
            (experiment,) = [e for e in value["experiments"] if e["name"] == "example"]
            for run in experiment["runs"]:
                (cell,) = run["cells"]
                run["cells"] = [
                    {**cell, "labels": {"size": size, "keys": "keys1"}}
                    for size in ("small", "large")
                ]

        self.update("timing", split, sides=("main", "pr"))
        self.update("counters", split, sides=("main", "pr"))
        self.update(
            "timing",
            lambda value: [
                run["cells"][0].update(shapes=example_cell(130)["shapes"])
                for run in value["experiments"][0]["runs"]
            ],
        )
        report = self.report()
        self.assertIn("regressed", self.row(report, "| example/small/keys1: p50Ms"))
        self.assertNotIn("| example/large/keys1: p50Ms", report)
        metrics, _ = perf_report.read_side(self.root / "pr")
        self.assertIn("example/large/keys1/workload: reads", metrics)

    def test_a_failed_cell_is_a_warning(self):
        self.update_cell("counters", SHARED_LEAF, lambda cell: cell.update(failures=1))
        report = self.report()
        self.assertIn(f"{SHARED_LEAF}/run 1: cell failed", report)
        self.assertIn("incomplete measurements", report)

    def test_invalid_numbers_and_low_sample_counts_are_warnings(self):
        self.update_cell(
            "timing", SHARED_LEAF, lambda cell: cell["shapes"][0].update(committed=20)
        )
        self.assertIn("insufficient latency/throughput observations", self.report())
        self.update_cell(
            "counters",
            "example",
            lambda cell: cell["costs"]["workload"].update(reads=float("nan")),
        )
        self.assertIn("invalid cost measurements", self.report())

    def test_missing_or_malformed_results_are_warnings(self):
        for contents in ("", "invalid JSON", "{}"):
            with self.subTest(contents=contents):
                (self.root / "pr" / "timing.json").write_text(contents)
                self.assertIn("invalid latency measurement", self.report())

    def test_models_must_match_before_latency_is_compared(self):
        directory = self.root / "pr"
        metrics, warnings = perf_report.read_side(directory)
        self.assertFalse(warnings)
        self.assertIn("example: p50Ms", metrics)
        self.assertIn("example/workload: reads", metrics)
        for key, value in (
            ("backend", "s3"),
            ("modelTimeSpeedup", 5),
            # A result of real time, with or without a time mode, measures
            # other latencies.
            ("timeMode", "real"),
            ("timeMode", None),
            ("schemaVersion", 1),
            ("schemaVersion", 2),
            # The latency needs the jitter of the provider.
            ("latencyJitter", False),
        ):
            with self.subTest(key=key, value=value):
                self.update("timing", lambda v: v.update({key: value}))
                metrics, warnings = perf_report.read_side(directory)
                # The counters come from another file, and stay valid.
                self.assertNotIn("example: p50Ms", metrics)
                self.assertIn("example/workload: reads", metrics)
                self.assertIn("invalid latency measurement", warnings[0])
                self.write(directory / "timing.json", result(True, RUNS))

    def test_counters_of_a_process_with_latency_jitter_are_not_compared(self):
        self.update("counters", lambda v: v.update(latencyJitter=True))
        metrics, warnings = perf_report.read_side(self.root / "pr")
        self.assertIn("example: p50Ms", metrics)
        self.assertNotIn("example/workload: reads", metrics)
        self.assertIn("invalid counter measurement", warnings[0])

    def test_a_summary_shows_the_latency_and_the_counters_of_one_side(self):
        self.set_latencies((100, 110, 120, 130))
        self.update_cell(
            "counters",
            "example",
            lambda cell: cell["costs"]["combined"].update(reads=2, coordinatorRounds=1),
        )
        summary = perf_report.summarize(self.root / "pr")
        self.assertIn("median of 4 runs", summary)
        # The median of the runs, and the spread of one run.
        self.assertRegex(self.row(summary, "| example |"), r"\| 115\.00 ±\d+\.\d% \|")
        self.assertIn(f"| {SHARED_LEAF}/rw-single | 10.00 ±0.0% |", summary)
        self.assertIn("| example | 2.000 | 0.000 | 0.000 | 1.000 |", summary)
        self.assertNotIn("Measurement warnings", summary)

    def test_the_manifest_gives_the_labels_and_the_warnings_of_the_build(self):
        self.write(
            self.root / "manifest.json",
            {"base": "abc", "candidate": "def", "warnings": ["used the base harness"]},
        )
        output = self.root / "report.md"
        arguments = [
            "perf_report.py",
            f"--base={self.root / 'main'}",
            f"--candidate={self.root / 'pr'}",
            f"--manifest={self.root / 'manifest.json'}",
            "--warning=the measurement step failed",
            f"--output={output}",
        ]
        with mock.patch.object(sys, "argv", arguments):
            self.assertEqual(perf_report.main(), 0)
        report = output.read_text()
        self.assertIn("Base: `abc`; candidate: `def`.", report)
        self.assertIn("- used the base harness", report)
        self.assertIn("- the measurement step failed", report)

    def test_model_mismatch_is_not_compared(self):
        self.update("timing", lambda value: value.update(modelTimeSpeedup=5))
        self.assertIn("unsupported perfbench schema or backend model", self.report())


if __name__ == "__main__":
    unittest.main()
