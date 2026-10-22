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
from perfbench_fixtures import contention_cell, mixed_cell, report as envelope


def inline_report(run: int, tps: float) -> dict:
    cell = {
        "labels": {"phase": "recovery"},
        "measure": {"wallMs": 10},
        "shapes": [{"shape": "pressure", "committed": 4, "txPerSec": tps}],
    }
    return envelope("inline-pressure", [{"run": run, "cells": [cell]}])


class PerfbenchResultsTest(unittest.TestCase):
    def write(self, directory: Path, name: str, value: dict) -> Path:
        path = directory / name
        path.write_text(json.dumps(value))
        return path

    def test_mixed_cells_carry_their_run_identity(self) -> None:
        report = envelope("mixed", [{"run": 2, "cells": [mixed_cell(mode="hi")]}])

        cells = perfbench_results.mixed_cells(report, "report")

        self.assertEqual(cells[0]["run"], 2)
        self.assertEqual(cells[0]["mode"], "hi")

    def test_cell_with_failures_is_rejected(self) -> None:
        report = envelope("mixed", [{"run": 1, "cells": [mixed_cell(failures=3)]}])
        with self.assertRaisesRegex(perfbench_results.ReportError, "3 failures"):
            perfbench_results.mixed_cells(report, "report")

    def test_duplicate_run_is_rejected(self) -> None:
        report = envelope(
            "mixed", [{"run": 1, "cells": []}, {"run": 1, "cells": []}]
        )
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
        report = envelope("contention", [{"run": 1, "cells": [{"failures": 0}]}])
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "contention.json", report)
            with self.assertRaisesRegex(perfbench_results.ReportError, "keys"):
                perfbench_results.read_contention([path])

    def test_failed_contention_cell_is_rejected(self) -> None:
        report = envelope("contention", [{"run": 1, "cells": [{"failures": 2}]}])
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "contention.json", report)
            with self.assertRaisesRegex(perfbench_results.ReportError, "cleanly"):
                perfbench_results.read_contention([path])

    def test_duplicate_run_in_one_contention_file_is_rejected(self) -> None:
        report = envelope(
            "contention", [{"run": 1, "cells": []}, {"run": 1, "cells": []}]
        )
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "contention.json", report)
            with self.assertRaisesRegex(perfbench_results.ReportError, "duplicate"):
                perfbench_results.read_contention([path])

    def test_contention_cell_gives_its_keys_and_overlap(self) -> None:
        cell = contention_cell(
            4, 2, committed=10, tx_per_sec=5.0, p50_ms=1.0, p90_ms=2.0, direct_landed=3
        )
        report = envelope("contention", [{"run": 1, "cells": [cell]}])
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "contention.json", report)
            stats = perfbench_results.read_contention([path])

        self.assertEqual(stats.loc[0, "overlap-pct"], 50)
        self.assertEqual(stats.loc[0, "direct-landed"], 3)
        self.assertEqual(stats.loc[0, "cell-duration-ms"], 2000)
        self.assertEqual(stats.loc[0, "p50-ms"], 1.0)
        self.assertEqual(stats.loc[0, "p90-ms"], 2.0)

    def test_throughput_band_comes_from_window_percentiles(self) -> None:
        shapes = [
            {"shape": shape, "txPerSec": 1, "p50Ms": 1, "p90Ms": 2}
            for shape in perfbench_results.SHAPES
        ]
        # Windows of 500 ms with 1 to 11 commits: 2 to 22 tx/s.
        shapes[0].update(windowMs=500, windowCounts=list(range(1, 12)))
        shapes[1].update(windowMs=0, windowCounts=[])
        report = envelope(
            "mixed",
            [{"run": 1, "cells": [mixed_cell(affinity=50, shapes=shapes)]}],
            backend="s3",
            modelTimeSpeedup=1.0,
        )
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write(Path(directory_name), "mixed.json", report)
            _, frame = perfbench_results.read_mixed(path, require_converged=False)

        by_shape = frame.set_index("shape")
        self.assertAlmostEqual(by_shape.loc["rwSingle", "throughput_p10"], 4.0)
        self.assertAlmostEqual(by_shape.loc["rwSingle", "throughput_p50"], 12.0)
        self.assertAlmostEqual(by_shape.loc["rwSingle", "throughput_p90"], 20.0)
        # Without windows, the line is the shape's throughput and there is no
        # band.
        self.assertTrue((by_shape.loc["rwMany":, "throughput_p50"] == 1).all())
        self.assertTrue(by_shape.loc["rwMany":, "throughput_p10"].isna().all())
        self.assertTrue(by_shape.loc["rwMany":, "throughput_p90"].isna().all())

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
