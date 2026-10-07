#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
# ]
# ///

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import perfbench_results


def inline_report(run: int, total_tps: float) -> dict:
    return {
        "schemaVersion": 1,
        "scenario": "inline-pressure",
        "runs": [{"run": run, "phases": [{"phase": "total", "txPerSec": total_tps}]}],
    }


class PerfbenchResultsTest(unittest.TestCase):
    def write(self, directory: Path, name: str, value: dict) -> Path:
        path = directory / name
        path.write_text(json.dumps(value))
        return path

    def test_mixed_cells_carry_their_run_identity(self) -> None:
        report = {
            "schemaVersion": 1,
            "scenario": "mixed",
            "runs": [{"run": 2, "cells": [{"failures": 0}]}],
        }

        cells = perfbench_results.mixed_cells(report, "report")

        self.assertEqual(cells[0]["run"], 2)

    def test_cell_with_failures_is_rejected(self) -> None:
        report = {
            "schemaVersion": 1,
            "scenario": "mixed",
            "runs": [{"run": 1, "cells": [{"failures": 3}]}],
        }
        with self.assertRaisesRegex(perfbench_results.ReportError, "3 failures"):
            perfbench_results.mixed_cells(report, "report")

    def test_duplicate_run_is_rejected(self) -> None:
        report = {
            "schemaVersion": 1,
            "scenario": "mixed",
            "runs": [{"run": 1, "cells": []}, {"run": 1, "cells": []}],
        }
        with self.assertRaisesRegex(perfbench_results.ReportError, "duplicate run"):
            perfbench_results.mixed_cells(report, "report")

    def test_report_of_another_scenario_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "mixed.json", inline_report(1, 1.0))
            with self.assertRaisesRegex(perfbench_results.ReportError, "mixed report"):
                perfbench_results.read_mixed(path, require_converged=False)

    def test_legacy_retry_names_count_as_replays(self) -> None:
        self.assertEqual(perfbench_results.body_replays({"retries": 3}), 3)
        self.assertEqual(
            perfbench_results.body_replays_per_tx({"retriesPerTx": 0.5}), 0.5
        )

    def test_malformed_contention_cell_is_a_report_error(self) -> None:
        report = {
            "schemaVersion": 1,
            "scenario": "contention",
            "runs": [{"run": 1, "cells": [{"failures": 0}]}],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "contention.json", report)
            with self.assertRaisesRegex(perfbench_results.ReportError, "numKeys"):
                perfbench_results.read_contention([path])

    def test_failed_contention_cell_is_rejected(self) -> None:
        report = {
            "schemaVersion": 1,
            "scenario": "contention",
            "runs": [{"run": 1, "cells": [{"failures": 2}]}],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "contention.json", report)
            with self.assertRaisesRegex(perfbench_results.ReportError, "cleanly"):
                perfbench_results.read_contention([path])

    def test_duplicate_run_in_one_contention_file_is_rejected(self) -> None:
        report = {
            "schemaVersion": 1,
            "scenario": "contention",
            "runs": [{"run": 1, "cells": []}, {"run": 1, "cells": []}],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "contention.json", report)
            with self.assertRaisesRegex(perfbench_results.ReportError, "duplicate"):
                perfbench_results.read_contention([path])

    def test_report_without_worker_counts_still_loads(self) -> None:
        legacy_cell = {
            "mode": "lo",
            "affinityPct": 50,
            "databases": 4,
            "failures": 0,
            "shapes": [
                {"shape": shape, "txPerSec": 1, "p50Ms": 1, "p90Ms": 2}
                for shape in perfbench_results.SHAPES
            ],
        }
        report = {
            "schemaVersion": 1,
            "scenario": "mixed",
            "backend": "s3",
            "modelTimeSpeedup": 1.0,
            "runs": [{"run": 1, "cells": [legacy_cell]}],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "mixed.json", report)
            _, frame = perfbench_results.read_mixed(path, require_converged=False)

        self.assertEqual(frame["database_limit"].unique().tolist(), [4])
        self.assertTrue(frame["workers"].isna().all())

    def test_several_single_run_files_are_numbered_as_consecutive_runs(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            paths = [
                self.write(directory, "a.json", inline_report(1, 1.0)),
                self.write(directory, "b.json", inline_report(1, 2.0)),
            ]

            frame = perfbench_results.read_inline_pressure(paths)

        self.assertEqual(frame["run"].tolist(), [1, 2])
        self.assertEqual(frame["tx-per-sec"].tolist(), [1.0, 2.0])


if __name__ == "__main__":
    unittest.main()
