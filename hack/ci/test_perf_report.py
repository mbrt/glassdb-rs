from __future__ import annotations

import json
from pathlib import Path
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
                            "confidence_interval": {
                                "lower_bound": 99,
                                "upper_bound": 101,
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
            lambda value: value["runs"][0]["cells"][0]["shapes"][0].update(meanMs=10.4),
        )
        self.assertNotIn("| Metric", self.report())

    def test_criterion_interval_prevents_false_regression(self):
        self.edit(
            "criterion/diagnostic/example/new/estimates.json",
            lambda value: value.update(
                mean={
                    "point_estimate": 110,
                    "confidence_interval": {"lower_bound": 98, "upper_bound": 122},
                }
            ),
        )
        report = self.report()
        self.assertNotIn("| Metric", report)
        self.assertIn("inconclusive", report)

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
