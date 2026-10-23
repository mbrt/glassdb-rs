from __future__ import annotations

import json
import math
from pathlib import Path
import shutil
import tempfile
import unittest
from typing import Any

from hack.ci import perf_report

SHARED_LEAF = perf_report.SHARED_LEAF_CASE
PAIRS = 4
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
            for name in perf_report.SHARED_LEAF_SHAPES
        ],
        "costs": costs_record(),
    }


def result(latency_jitter):
    return {
        "schemaVersion": 2,
        "scenario": perf_report.SUITE,
        "backend": "memory",
        "timeMode": "virtual",
        "modelTimeSpeedup": 1,
        "latencyJitter": latency_jitter,
        "runs": [{"run": 1, "cells": [example_cell(), shared_leaf_cell()]}],
    }


class PerfReportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest: dict[str, Any] = {
            "schemaVersion": perf_report.MANIFEST_VERSION,
            "cases": ["example", SHARED_LEAF],
            "checkpoints": [PAIRS],
            "completedPairs": PAIRS,
            "latencyJitter": True,
        }
        self.write_manifest()
        for side in ("main", "pr"):
            for repetition in range(1, PAIRS + 1):
                directory = self.directory(side, repetition)
                # The counters come from a process without latency jitter.
                self.write(directory / "timing.json", result(True))
                self.write(directory / "counters.json", result(False))

    def directory(self, side, repetition):
        return self.root / side / f"{repetition:02d}"

    @staticmethod
    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))

    def write_manifest(self, **changes):
        self.manifest.update(changes)
        self.write(self.root / "manifest.json", self.manifest)

    def extend(self, pairs):
        """Add repetitions that repeat the last one."""
        for side in ("main", "pr"):
            for repetition in range(PAIRS + 1, pairs + 1):
                shutil.copytree(
                    self.directory(side, PAIRS), self.directory(side, repetition)
                )

    def update(self, kind, change, *, sides=("pr",), repetitions=None):
        """Change the result file `kind` of repetitions, by default all of them."""
        for side in sides:
            for repetition in repetitions or range(1, PAIRS + 1):
                path = self.directory(side, repetition) / f"{kind}.json"
                value = json.loads(path.read_text())
                change(value)
                self.write(path, value)

    def update_cell(self, kind, case, change, **options):
        def apply(value):
            (cell,) = [
                c for c in value["runs"][0]["cells"] if c["labels"]["case"] == case
            ]
            change(cell)

        self.update(kind, apply, **options)

    def remove_cell(self, kind, case, **options):
        def apply(value):
            cells = value["runs"][0]["cells"]
            value["runs"][0]["cells"] = [c for c in cells if c["labels"]["case"] != case]

        self.update(kind, apply, **options)

    def set_latency(self, repetition, before, after):
        for side, latency in (("main", before), ("pr", after)):
            self.update_cell(
                "timing",
                "example",
                lambda cell, latency=latency: cell.update(
                    shapes=example_cell(latency)["shapes"]
                ),
                sides=(side,),
                repetitions=[repetition],
            )

    def set_latencies(self, after, before=100):
        for repetition, value in enumerate(after, 1):
            self.set_latency(repetition, before, value)

    def report(self):
        return perf_report.render_report(self.root, "base", "candidate")

    def row(self, report, prefix):
        return next(r for r in report.splitlines() if r.startswith(prefix))

    def test_process_variation_prevents_a_regression_claim(self):
        # Every pair is slower, but variation between pairs leaves the
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

    def test_process_noise_control_retains_sensitivity_in_both_directions(self):
        # Paired log changes (%) from 32 unchanged warm-external-read runs.
        # Keep the observed process noise when adding known 3% effects.
        noise = (
            -0.413, -0.332, 0.237, -0.763, 0.440, -0.267, -0.573, -1.435,
            -0.066, 0.013, -0.254, 2.728, 0.252, -1.897, -0.230, 1.276,
            -1.344, -0.727, 1.832, -1.678, 1.344, -0.332, -1.084, 0.019,
            1.916, -1.892, 0.239, -0.314, 2.757, -1.035, 1.293, -1.925,
        )
        for effect in (0, -0.03, 0.03):
            with self.subTest(effect=effect):
                base = perf_report.Metric("ns", "time")
                candidate = perf_report.Metric("ns", "time")
                for count, change in enumerate(noise, 1):
                    base.add(100)
                    candidate.add(100 * (1 + effect) * math.exp(change / 100))
                    if count in (8, 16, 32):
                        comparison = perf_report.compare(base, candidate, family_size=60)
                        if not comparison.uncertain:
                            break
                self.assertFalse(comparison.uncertain)
                self.assertEqual(comparison.report, effect != 0)
                if effect:
                    assert comparison.relative is not None
                    self.assertGreater(comparison.relative * effect, 0)

    def test_paired_comparison_cancels_shared_host_drift(self):
        for repetition, before in enumerate((100, 200, 400, 800), 1):
            self.set_latency(repetition, before, before * 1.1)
        report = self.report()
        self.assertIn("+10.0%", report)
        self.assertIn("regressed", report)
        self.assertNotIn("inconclusive", report)

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
        self.remove_cell("timing", SHARED_LEAF, repetitions=[PAIRS])
        report = self.report()
        self.assertEqual(self.row(report, "| example: p50Ms"), before)
        self.assertIn("incomplete paired measurements", report)

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

    def test_planned_checks_protect_against_a_false_early_decision(self):
        self.extend(8)
        self.write_manifest(checkpoints=[8], completedPairs=8)
        self.set_latencies((101.6, 105.6) * 4)
        self.assertIn("regressed", self.report())
        # Even unused future checks consume confidence. Reusing a fixed-sample
        # interval here would let this first look stop the benchmark too early.
        self.write_manifest(checkpoints=[8, 16, 32])
        report = self.report()
        self.assertNotIn("regressed", report)
        self.assertIn("example: p50Ms: noisy or inconclusive", report)
        self.assertFalse(perf_report.analyze(self.root, self.manifest).resolved)

    def test_samples_between_checkpoints_cannot_change_the_verdict(self):
        self.write_manifest(checkpoints=[4, 8])
        before = perf_report.analyze(self.root, self.manifest)
        self.extend(6)
        for repetition in (5, 6):
            self.set_latency(repetition, 100, 1000)
        self.write_manifest(completedPairs=6)
        after = perf_report.analyze(self.root, self.manifest)
        self.assertEqual(after.metrics, before.metrics)
        self.assertEqual(after.pairs, 4)
        report = self.report()
        self.assertIn("using checkpoint at 4 pairs", report)
        self.assertNotIn("regressed", report)

    def test_missing_adaptive_measurements_do_not_reduce_the_correction(self):
        self.write_manifest(checkpoints=[4, 8])
        self.set_latencies((102, 102, 102, 102))
        before = self.row(self.report(), "| example: p50Ms")
        self.remove_cell("timing", SHARED_LEAF, repetitions=[PAIRS])
        report = self.report()
        self.assertEqual(self.row(report, "| example: p50Ms"), before)
        self.assertIn("incomplete paired measurements", report)

    def test_invalid_checkpoint_plans_are_rejected(self):
        self.write_manifest(checkpoints=[4, 8])
        for changes in (
            {"checkpoints": []},
            {"checkpoints": [3, 8]},
            {"checkpoints": [4, 4]},
            {"checkpoints": [8, 4]},
            {"checkpoints": [False, 8]},
            {"completedPairs": -1},
            {"completedPairs": 9},
            {"completedPairs": {"example": 4}},
            {"latencyJitter": "yes"},
            {"cases": ["example"]},
            {"cases": ["example", "example", SHARED_LEAF]},
            {"schemaVersion": 2},
        ):
            with self.subTest(changes=changes):
                self.write(self.root / "manifest.json", {**self.manifest, **changes})
                with self.assertRaises(perf_report.ReportError):
                    self.report()

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
        for repetition, p50 in enumerate((10.1, 11.2, 10.9, 10.4), 1):
            self.update_cell(
                "timing",
                SHARED_LEAF,
                lambda cell, p50=p50: cell["shapes"][0].update(p50Ms=p50),
                repetitions=[repetition],
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
        self.assertIn("incomplete paired measurements", report)
        self.assertNotIn("No meaningful changes detected", report)

    def test_every_case_is_compared_by_median_tail_and_throughput(self):
        self.set_latencies((130,) * PAIRS)
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

    def test_repeatable_cost_change_from_zero_is_visible(self):
        self.update_cell(
            "counters",
            "example",
            lambda cell: cell["costs"]["shutdown"].update(writeBodyBytes=0.125),
        )
        report = self.report()
        self.assertIn("example/shutdown: writeBodyBytes", report)
        self.assertIn("new from zero", report)

    def test_small_overlapping_cost_variation_is_not_a_warning(self):
        base = perf_report.Metric("bytes/tx", "cost")
        candidate = perf_report.Metric("bytes/tx", "cost")
        for before, after in zip((1854.5, 1854.5, 1855.5), (1854.5, 1855.5, 1855.5)):
            base.add(before)
            candidate.add(after)
        comparison = perf_report.compare(base, candidate)
        self.assertFalse(comparison.report)
        self.assertFalse(comparison.uncertain)

    def test_small_repeatable_cost_change_remains_visible(self):
        base = perf_report.Metric("bytes/tx", "cost")
        candidate = perf_report.Metric("bytes/tx", "cost")
        for _ in range(3):
            base.add(1854.5)
            candidate.add(1855.5)
        comparison = perf_report.compare(base, candidate)
        self.assertTrue(comparison.report)
        self.assertFalse(comparison.uncertain)

    def test_large_cost_variation_remains_inconclusive(self):
        base = perf_report.Metric("count/tx", "cost")
        candidate = perf_report.Metric("count/tx", "cost")
        for before, after in zip((1, 2, 1), (2, 1, 2)):
            base.add(before)
            candidate.add(after)
        comparison = perf_report.compare(base, candidate)
        self.assertFalse(comparison.report)
        self.assertTrue(comparison.uncertain)

    def test_moderate_overlapping_cost_increase_remains_inconclusive(self):
        base = perf_report.Metric("bytes/tx", "cost")
        candidate = perf_report.Metric("bytes/tx", "cost")
        for before, after in zip((100, 100, 109), (109, 109, 109)):
            base.add(before)
            candidate.add(after)
        comparison = perf_report.compare(base, candidate)
        self.assertFalse(comparison.report)
        self.assertTrue(comparison.uncertain)

    def test_missing_run_is_not_reported_as_unchanged(self):
        self.remove_cell("timing", SHARED_LEAF, repetitions=[PAIRS])
        report = self.report()
        self.assertIn("Measurement warnings", report)
        self.assertIn("incomplete paired measurements", report)

    def test_another_case_is_not_compared(self):
        self.update_cell(
            "timing", "example", lambda cell: cell.update(labels={"case": "other"})
        )
        self.assertIn("expected one example cell", self.report())

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
                (self.directory("pr", PAIRS) / "timing.json").write_text(contents)
                self.assertIn("invalid latency measurement", self.report())

    def test_models_must_match_before_latency_is_compared(self):
        directory = self.directory("pr", 1)
        cases = self.manifest["cases"]
        metrics, warnings = perf_report.read_measurement(
            directory, cases, latency_jitter=True
        )
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
            ("scenario", "mixed"),
        ):
            with self.subTest(key=key):
                self.update("timing", lambda v: v.update({key: value}), repetitions=[1])
                metrics, warnings = perf_report.read_measurement(
                    directory, cases, latency_jitter=True
                )
                # The counters come from another file, and stay valid.
                self.assertNotIn("example: p50Ms", metrics)
                self.assertIn("example/workload: reads", metrics)
                self.assertIn("invalid latency measurement", warnings[0])
                self.write(directory / "timing.json", result(True))
        metrics, warnings = perf_report.read_measurement(
            directory, cases, latency_jitter=False
        )
        self.assertNotIn("example: p50Ms", metrics)
        self.assertIn("latency profile", warnings[0])

    def test_counters_of_a_process_with_latency_jitter_are_not_compared(self):
        self.update(
            "counters", lambda v: v.update(latencyJitter=True), repetitions=[1]
        )
        metrics, warnings = perf_report.read_measurement(
            self.directory("pr", 1), self.manifest["cases"], latency_jitter=True
        )
        self.assertIn("example: p50Ms", metrics)
        self.assertNotIn("example/workload: reads", metrics)
        self.assertIn("invalid counter measurement", warnings[0])

    def test_model_mismatch_is_not_compared(self):
        self.update("timing", lambda value: value.update(modelTimeSpeedup=5))
        self.assertIn("unsupported suite schema or backend model", self.report())


if __name__ == "__main__":
    unittest.main()
