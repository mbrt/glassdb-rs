from __future__ import annotations

import json
import math
from pathlib import Path
import shutil
import tempfile
import unittest

from hack.ci import perf_report


class PerfReportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.write(
            self.root / "manifest.json",
            {"schemaVersion": 1, "repetitions": 3, "cases": ["example"]},
        )
        for side in ("main", "pr"):
            for repetition in range(1, 4):
                path = self.root / side / f"{repetition:02d}"
                self.write(
                    path / "criterion/diagnostic/example/new/estimates.json",
                    {
                        "mean": {
                            "point_estimate": 100,
                            "standard_error": 0.05,
                            "confidence_interval": {
                                "lower_bound": 99.9,
                                "upper_bound": 100.1,
                            },
                        }
                    },
                )
                counters = {
                    key: 0
                    for key in (
                        "reads",
                        "writes",
                        "lists",
                        "readBodyBytes",
                        "writeBodyBytes",
                        "coordinatorSubmissions",
                        "coordinatorRounds",
                    )
                }
                self.write(
                    path / "criterion.log",
                    {
                        "schemaVersion": 1,
                        "cases": [
                            {
                                "name": "example",
                                "transactions": 30,
                                **{
                                    window: counters.copy()
                                    for window in ("workload", "shutdown", "combined")
                                },
                            }
                        ],
                    },
                )
                self.write(
                    path / "mixed.json",
                    {
                        "schemaVersion": 1,
                        "scenario": "mixed",
                        "backend": "memory",
                        "modelTimeSpeedup": 5,
                        "runs": [
                            {
                                "cells": [
                                    {
                                        "mode": "lo",
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
                                                "p90Ms": 20,
                                                "txPerSec": 100,
                                            }
                                            for name in (
                                                "rwSingle",
                                                "rwMany",
                                                "roSingle",
                                                "roMulti",
                                            )
                                        ],
                                    }
                                ]
                            }
                        ],
                    },
                )

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.name == "criterion.log":
            path.write_text(
                "Benchmarking diagnostic/example\n\ndiagnostic-costs: "
                + json.dumps(value)
                + "\nFinal summary\n"
            )
        else:
            path.write_text(json.dumps(value))

    def edit(self, relative, change):
        for repetition in range(1, 4):
            path = self.root / "pr" / f"{repetition:02d}" / relative
            raw = path.read_text()
            if path.name == "criterion.log":
                raw = raw.split("diagnostic-costs: ", 1)[1].splitlines()[0]
            value = json.loads(raw)
            change(value)
            self.write(path, value)

    def report(self):
        return perf_report.render_report(self.root, "base", "candidate")

    def timing_pair(self, repetition, before, after):
        for side, value in (("main", before), ("pr", after)):
            self.write(
                self.root
                / side
                / f"{repetition:02d}"
                / "criterion/diagnostic/example/new/estimates.json",
                {
                    "mean": {
                        "point_estimate": value,
                        "standard_error": value * 0.0005,
                        "confidence_interval": {
                            "lower_bound": value * 0.999,
                            "upper_bound": value * 1.001,
                        },
                    }
                },
            )

    def test_process_variation_prevents_a_regression_claim(self):
        # Every process has a narrow interval and is slower, but variation
        # between pairs leaves the direction unresolved.
        for repetition, after in enumerate((101, 112, 109), 1):
            self.timing_pair(repetition, 100, after)
        report = self.report()
        self.assertNotIn("| Metric", report)
        self.assertRegex(
            report,
            r"example: mean group time: noisy or inconclusive\s*"
            r"\[-\d+\.\d+%, \+\d+\.\d+%\]",
        )
        self.assertIn("No conclusive changes", report)
        self.assertNotIn("No meaningful changes detected", report)

    def test_repeatable_regression_remains_visible(self):
        for repetition, after in enumerate((101.8, 102.0, 102.2), 1):
            self.timing_pair(repetition, 100, after)
        report = self.report()
        self.assertIn("example: mean group time |", report)
        self.assertIn("regressed", report)
        self.assertNotIn("inconclusive", report)

    def test_significance_is_tested_against_zero_not_the_size_threshold(self):
        for repetition, after in enumerate((101.18, 101.20, 101.22), 1):
            self.timing_pair(repetition, 100, after)
        report = self.report()
        self.assertIn("+1.2%", report)
        self.assertIn("regressed", report)

    def test_paired_comparison_cancels_shared_host_drift(self):
        for repetition, before in enumerate((100, 200, 400), 1):
            self.timing_pair(repetition, before, before * 1.1)
        report = self.report()
        self.assertIn("+10.0%", report)
        self.assertIn("regressed", report)
        self.assertNotIn("inconclusive", report)

    def test_small_estimate_with_large_uncertainty_is_not_unchanged(self):
        for repetition, after in enumerate((99, 103, 106), 1):
            self.timing_pair(repetition, 100, after)
        report = self.report()
        self.assertIn("inconclusive", report)
        self.assertNotIn("No meaningful changes detected", report)

    def test_matching_estimates_with_wide_process_intervals_remain_uncertain(self):
        self.edit(
            "criterion/diagnostic/example/new/estimates.json",
            lambda value: value["mean"].update(
                standard_error=15,
                confidence_interval={"lower_bound": 70, "upper_bound": 130},
            ),
        )
        report = self.report()
        self.assertIn("inconclusive", report)
        self.assertNotIn("No meaningful changes detected", report)

    def test_overlapping_process_intervals_can_establish_a_small_change(self):
        self.write(
            self.root / "manifest.json",
            {
                "schemaVersion": 1,
                "repetitions": 3,
                "diagnosticRepetitions": 8,
                "cases": ["example"],
            },
        )
        for side in ("main", "pr"):
            for repetition in range(4, 9):
                shutil.copytree(
                    self.root / side / "03", self.root / side / f"{repetition:02d}"
                )
        for repetition, after in enumerate((101.8, 102.0, 102.2, 102.0) * 2, 1):
            for side, mean in (("main", 100), ("pr", after)):
                self.write(
                    self.root
                    / side
                    / f"{repetition:02d}"
                    / "criterion/diagnostic/example/new/estimates.json",
                    {
                        "mean": {
                            "point_estimate": mean,
                            "standard_error": mean * 0.0055,
                            "confidence_interval": {
                                "lower_bound": mean * 0.989,
                                "upper_bound": mean * 1.011,
                            },
                        }
                    },
                )
        report = self.report()
        self.assertIn("+2.0%", report)
        self.assertIn("regressed", report)
        self.assertNotIn("inconclusive", report)

    def test_simultaneous_intervals_protect_against_multiple_false_alarms(self):
        base = perf_report.Metric("ns", "time")
        candidate = perf_report.Metric("ns", "time")
        for after in (100.72, 101.08, 101.33, 100.98):
            base.add(100, standard_error=0.05)
            candidate.add(after, standard_error=0.05)
        self.assertTrue(perf_report.compare(base, candidate).report)
        simultaneous = perf_report.compare(base, candidate, family_size=20)
        self.assertFalse(simultaneous.report)
        self.assertTrue(simultaneous.uncertain)

    def test_missing_metrics_do_not_reduce_multiple_comparison_correction(self):
        for repetition, after in enumerate((102.0, 102.1, 102.2), 1):
            self.timing_pair(repetition, 100, after)
        before = next(
            row for row in self.report().splitlines() if row.startswith("| example:")
        )
        (self.root / "pr/03/mixed.json").unlink()
        after = next(
            row for row in self.report().splitlines() if row.startswith("| example:")
        )
        self.assertEqual(before, after)

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

    def test_diagnostics_can_have_more_repetitions_than_mixed(self):
        self.write(
            self.root / "manifest.json",
            {
                "schemaVersion": 1,
                "repetitions": 3,
                "diagnosticRepetitions": 4,
                "cases": ["example"],
            },
        )
        for side in ("main", "pr"):
            shutil.copytree(self.root / side / "03", self.root / side / "04")
            (self.root / side / "04/mixed.json").unlink()
        self.assertNotIn("Measurement warnings", self.report())
        (self.root / "pr/04/criterion/diagnostic/example/new/estimates.json").unlink()
        self.assertIn("incomplete paired measurements", self.report())

    def test_unchanged_rows_are_hidden(self):
        report = self.report()
        self.assertIn("No meaningful changes detected", report)
        self.assertNotIn("| Metric", report)
        self.assertNotIn("Measurement warnings", report)

    def test_mean_and_p90_changes_have_correct_direction(self):
        def change(value):
            shapes = value["runs"][0]["cells"][0]["shapes"]
            shapes[0]["p90Ms"] = 30
            shapes[1]["txPerSec"] = 120
            shapes[2]["meanMs"] = 8

        self.edit("mixed.json", change)
        report = self.report()
        self.assertIn("mixed/rwSingle: p90Ms | 20.000 | 30.000", report)
        self.assertIn("mixed/rwMany: txPerSec | 100.000 | 120.000", report)
        self.assertIn("mixed/roSingle: meanMs | 10.000 | 8.000", report)
        self.assertIn("regressed", report)
        self.assertIn("improved", report)
        self.assertNotIn("mixed/roMulti:", report)

    def test_small_timing_change_is_hidden(self):
        self.edit(
            "mixed.json",
            lambda value: value["runs"][0]["cells"][0]["shapes"][0].update(
                meanMs=10.04
            ),
        )
        self.assertNotIn("| Metric", self.report())

    def test_criterion_interval_prevents_false_regression(self):
        self.edit(
            "criterion/diagnostic/example/new/estimates.json",
            lambda value: value.update(
                mean={
                    "point_estimate": 110,
                    "standard_error": 6,
                    "confidence_interval": {"lower_bound": 98, "upper_bound": 122},
                }
            ),
        )
        report = self.report()
        self.assertNotIn("| Metric", report)
        self.assertIn("inconclusive", report)

    def test_missing_or_invalid_process_error_is_not_zero_uncertainty(self):
        for error in (None, -1, float("nan")):
            with self.subTest(error=error):
                self.timing_pair(1, 100, 102)
                path = (
                    self.root / "pr/01/criterion/diagnostic/example/new/estimates.json"
                )
                value = json.loads(path.read_text())
                if error is None:
                    del value["mean"]["standard_error"]
                else:
                    value["mean"]["standard_error"] = error
                self.write(path, value)
                report = self.report()
                self.assertIn("missing or invalid Criterion measurement", report)
                self.assertIn("incomplete paired measurements", report)
                self.assertNotIn("No meaningful changes detected", report)

    def test_repeatable_cost_change_from_zero_is_visible(self):
        self.edit(
            "criterion.log",
            lambda value: value["cases"][0]["shutdown"].update(writeBodyBytes=0.125),
        )
        report = self.report()
        self.assertIn("example/shutdown: writeBodyBytes", report)
        self.assertIn("new from zero", report)

    def test_missing_run_is_not_reported_as_unchanged(self):
        (self.root / "pr/03/mixed.json").unlink()
        report = self.report()
        self.assertIn("Measurement warnings", report)
        self.assertIn("incomplete paired measurements", report)

    def test_missing_case_is_not_silently_intersected(self):
        self.edit("criterion.log", lambda value: value.update(cases=[]))
        self.assertIn("cost case set changed", self.report())

    def test_invalid_numbers_and_low_sample_counts_are_warnings(self):
        self.edit(
            "mixed.json",
            lambda value: value["runs"][0]["cells"][0]["shapes"][0].update(
                committed=20
            ),
        )
        self.assertIn("insufficient latency/throughput observations", self.report())
        self.edit(
            "criterion.log",
            lambda value: value["cases"][0]["workload"].update(reads=float("nan")),
        )
        self.assertIn("invalid cost measurements", self.report())

    def test_missing_malformed_or_duplicate_cost_records_are_warnings(self):
        for contents in (
            "Benchmark failed before costs\n",
            "diagnostic-costs: invalid JSON\n",
            "diagnostic-costs: {}\ndiagnostic-costs: {}\n",
        ):
            with self.subTest(contents=contents):
                (self.root / "pr/03/criterion.log").write_text(contents)
                self.assertIn("invalid cost measurements", self.report())

    def test_model_mismatch_is_not_compared(self):
        self.edit("mixed.json", lambda value: value.update(modelTimeSpeedup=1))
        self.assertIn("unsupported mixed schema or backend model", self.report())


if __name__ == "__main__":
    unittest.main()
