#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
#     "matplotlib>=3.8",
#     "seaborn>=0.13",
# ]
# ///

from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock


import perfbench_results
import plot_results as plotter


def read_canonical(path: Path):
    return perfbench_results.read_mixed(path, require_converged=True)


def shape_rows(run: int, affinity: int, databases: int, workers: int) -> list[dict]:
    rows = []
    for index, shape in enumerate(plotter.SHAPES):
        value = run * 10 + affinity + databases + workers + index
        rows.append(
            {
                "shape": shape,
                "committed": 500,
                "txPerSec": float(value),
                "p50Ms": float(value * 2),
                "p90Ms": float(value * 4),
                "relCi": 0.08,
                "converged": True,
            }
        )
    return rows


def cell(run: int, affinity: int, database_limit: int, workers: int) -> dict:
    databases = min(database_limit, workers)
    return {
        "mode": "lo",
        "affinityPct": affinity,
        "databaseLimit": database_limit,
        "databases": databases,
        "workersPerShape": workers,
        "setupSplits": 0,
        "splitSettleWallMs": 10,
        "failures": 0,
        "shapes": shape_rows(run, affinity, databases, workers),
        "aggregateOps": {},
        "aggregateProtocol": {},
    }


def report(runs: list[dict]) -> dict:
    return {
        "schemaVersion": 1,
        "scenario": "mixed",
        "backend": "memory",
        "modelTimeSpeedup": 5.0,
        "runs": runs,
    }


def worker_report() -> dict:
    return report(
        [
            {
                "run": run,
                "cells": [
                    cell(run, 100, plotter.WORKER_DATABASE_LIMIT, workers)
                    for workers in plotter.WORKER_POINTS
                ],
            }
            for run in plotter.EXPECTED_RUNS
        ]
    )


def affinity_report() -> dict:
    return report(
        [
            {
                "run": run,
                "cells": [
                    cell(
                        run,
                        affinity,
                        databases,
                        plotter.FIXED_AFFINITY_WORKERS,
                    )
                    for affinity in plotter.AFFINITY_POINTS
                    for databases in plotter.AFFINITY_DATABASES
                ],
            }
            for run in plotter.EXPECTED_RUNS
        ]
    )


class MixedSweepPlotterTest(unittest.TestCase):
    def write_report(self, directory: Path, name: str, value: dict) -> Path:
        path = directory / name
        path.write_text(json.dumps(value))
        return path

    def test_reports_form_complete_canonical_grids_and_medians(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            workers_path = self.write_report(directory, "workers.json", worker_report())
            affinity_path = self.write_report(
                directory, "affinity.json", affinity_report()
            )
            worker_metadata, workers = read_canonical(workers_path)
            affinity_metadata, affinities = read_canonical(affinity_path)

        self.assertEqual(worker_metadata, affinity_metadata)
        plotter.validate_worker_sweep(workers)
        plotter.validate_affinity_sweep(affinities)
        medians = plotter.median_rows(workers, ["workers"])
        row = medians[
            (medians["workers"] == 1) & (medians["shape"] == "rwSingle")
        ].iloc[0]
        self.assertEqual(row["throughput"], 122.0)
        self.assertEqual(row["p50_ms"], 244.0)
        self.assertEqual(row["p90_ms"], 488.0)
        self.assertEqual(plotter.WORKER_POINTS, (1, *range(10, 201, 10)))
        self.assertEqual(plotter.WORKER_TICKS, plotter.WORKER_POINTS)

    def test_database_count_must_match_limit_and_workers(self) -> None:
        invalid = worker_report()
        invalid["runs"][0]["cells"][0]["databases"] = 2
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "does not equal"):
                read_canonical(path)

    def test_unconverged_shape_is_rejected(self) -> None:
        invalid = affinity_report()
        invalid["runs"][0]["cells"][0]["shapes"][0]["converged"] = False
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "did not converge"):
                read_canonical(path)

    def test_p90_latency_must_not_be_below_p50(self) -> None:
        invalid = worker_report()
        shape = invalid["runs"][0]["cells"][0]["shapes"][0]
        shape["p90Ms"] = shape["p50Ms"] - 1
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "below p50Ms"):
                read_canonical(path)

    def test_series_use_plain_lines_and_latency_bands(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(
                Path(directory_name), "workers.json", worker_report()
            )
            _, workers = read_canonical(path)
        medians = plotter.median_rows(workers, ["workers"])
        colors = plotter._shape_colors()

        line_figure, line_axis = plotter.plt.subplots()
        plotter._plot_shape_lines(line_axis, medians, "workers", "throughput", colors)
        self.assertEqual(len(line_axis.lines), len(plotter.SHAPES))
        self.assertEqual(len(line_axis.collections), 0)
        self.assertTrue(all(line.get_marker() == "None" for line in line_axis.lines))
        plotter.plt.close(line_figure)

        band_figure, band_axis = plotter.plt.subplots()
        plotter._plot_shape_latency_bands(band_axis, medians, "workers", colors)
        self.assertEqual(len(band_axis.lines), len(plotter.SHAPES))
        self.assertEqual(len(band_axis.collections), len(plotter.SHAPES))
        self.assertTrue(all(line.get_marker() == "None" for line in band_axis.lines))
        plotter.plt.close(band_figure)

    def test_affinity_figures_are_faceted_by_database_count(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(
                Path(directory_name), "affinity.json", affinity_report()
            )
            _, affinities = read_canonical(path)

        figures = {}

        def capture(figure, out_dir, name):
            figures[name] = figure
            return out_dir / name

        with mock.patch.object(plotter, "_save", side_effect=capture):
            plotter.plot_affinity_throughput(affinities, Path("plots"))
            plotter.plot_affinity_latency(affinities, Path("plots"))

        expected_titles = [
            "1 DB instance",
            "3 DB instances",
            "5 DB instances",
            "7 DB instances",
        ]
        throughput = figures["affinity-throughput.png"]
        latency = figures["affinity-latency.png"]
        self.assertEqual(
            [axis.get_title() for axis in throughput.axes], expected_titles
        )
        self.assertEqual([axis.get_title() for axis in latency.axes], expected_titles)
        self.assertTrue(
            all(len(axis.lines) == len(plotter.SHAPES) for axis in throughput.axes)
        )
        self.assertTrue(
            all(len(axis.lines) == len(plotter.SHAPES) for axis in latency.axes)
        )
        self.assertTrue(
            all(len(axis.collections) == len(plotter.SHAPES) for axis in latency.axes)
        )
        self.assertEqual(len(latency.legends), 1)
        self.assertEqual(
            [text.get_text() for text in latency.legends[0].get_texts()],
            [plotter.SHAPE_LABELS[shape] for shape in plotter.SHAPES],
        )
        plotter.plt.close(throughput)
        plotter.plt.close(latency)

    def test_render_writes_all_four_figures(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            workers_path = self.write_report(directory, "workers.json", worker_report())
            affinity_path = self.write_report(
                directory, "affinity.json", affinity_report()
            )
            outputs = plotter.render(workers_path, affinity_path, directory / "plots")

            self.assertEqual(
                {path.name for path in outputs},
                {
                    "worker-throughput.png",
                    "worker-latency.png",
                    "affinity-throughput.png",
                    "affinity-latency.png",
                },
            )
            self.assertTrue(all(path.stat().st_size > 0 for path in outputs))

    def test_reports_must_use_the_same_backend_configuration(self) -> None:
        affinity = copy.deepcopy(affinity_report())
        affinity["modelTimeSpeedup"] = 1.0
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            workers_path = self.write_report(directory, "workers.json", worker_report())
            affinity_path = self.write_report(directory, "affinity.json", affinity)
            with self.assertRaisesRegex(plotter.ReportError, "same backend"):
                plotter.render(workers_path, affinity_path, directory / "plots")


class GenericPlotterTest(unittest.TestCase):
    def write_report(self, directory: Path, name: str, value: dict) -> Path:
        path = directory / name
        path.write_text(json.dumps(value))
        return path

    def real_mixed_report(self, cells: list[dict]) -> dict:
        value = report([{"run": 1, "cells": cells}])
        value.update(backend="s3", modelTimeSpeedup=1.0)
        return value

    def contention_report(self, overlap_pct: list[int]) -> dict:
        def contention_cell(num_keys: int, overlap_pct: int) -> dict:
            return {
                "numKeys": num_keys,
                "overlap": 1,
                "overlapPct": overlap_pct,
                "committed": 2,
                "durationMs": 1000,
                "txPerSec": 2.0,
                "samplesMs": [10.0, 20.0],
                "replays": 0,
                "directCandidates": 2,
                "directLanded": 2,
                "workerDrainMs": 1,
                "failures": 0,
            }

        return {
            "schemaVersion": 1,
            "scenario": "contention",
            "backend": "s3",
            "modelTimeSpeedup": 1.0,
            "runs": [
                {
                    "run": 1,
                    "cells": [
                        contention_cell(num_keys, pct)
                        for num_keys in (1, 2, 3)
                        for pct in overlap_pct
                    ],
                }
            ],
        }

    def test_mixed_report_is_plotted_without_canonical_grid(self) -> None:
        cells = []
        for mode in ("lo", "hi"):
            for affinity in (0, 50, 100):
                cells.append({**cell(1, affinity, 4, 8), "mode": mode})
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            path = self.write_report(
                directory, "mixed.json", self.real_mixed_report(cells)
            )

            outputs = plotter.plot_file(path, None)

            self.assertEqual(
                {output.name for output in outputs},
                {"mixed-throughput.png", "mixed-latency.png"},
            )
            self.assertTrue(all(output.parent == directory for output in outputs))

    def test_worker_sweep_and_single_run_median_are_plotted(self) -> None:
        cells = [cell(1, 100, 4, workers) for workers in (1, 4, 8)]
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            path = self.write_report(
                directory, "mixed.json", self.real_mixed_report(cells)
            )
            _, data = perfbench_results.read_mixed(path, require_converged=False)

            self.assertEqual(plotter._mixed_axis(data), "workers")
            medians = plotter.median_rows(data, ["mode", "workers"])
            self.assertEqual(len(medians), 3 * len(plotter.SHAPES))
            self.assertEqual(
                medians.iloc[0]["throughput"],
                data[
                    (data["workers"] == medians.iloc[0]["workers"])
                    & (data["shape"] == medians.iloc[0]["shape"])
                ].iloc[0]["throughput"],
            )

    def test_unconverged_shapes_are_plotted_with_a_warning(self) -> None:
        unconverged = cell(1, 0, 4, 8)
        unconverged["shapes"][0]["converged"] = False
        cells = [unconverged, cell(1, 100, 4, 8)]
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            path = self.write_report(
                directory, "mixed.json", self.real_mixed_report(cells)
            )
            with mock.patch("builtins.print") as printed:
                plotter.plot_file(path, directory)
            self.assertIn("did not converge", str(printed.call_args_list))

    def test_mixed_report_must_sweep_exactly_one_dimension(self) -> None:
        cells = [
            cell(1, affinity, 4, workers) for affinity in (0, 100) for workers in (4, 8)
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            path = self.write_report(
                directory, "mixed.json", self.real_mixed_report(cells)
            )
            with self.assertRaisesRegex(plotter.ReportError, "exactly one"):
                plotter.plot_file(path, directory)

    def test_contention_report_plots_full_overlap_cells(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            path = self.write_report(
                directory, "contention.json", self.contention_report([50, 100])
            )

            outputs = plotter.plot_file(path, directory / "plots")

            self.assertEqual(
                {output.name for output in outputs},
                {"contention-latency.png", "contention-throughput.png"},
            )

    def test_contention_report_without_full_overlap_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            path = self.write_report(
                directory, "contention.json", self.contention_report([50])
            )
            with self.assertRaisesRegex(plotter.ReportError, "100% overlap"):
                plotter.plot_file(path, directory)

    def test_bad_file_is_reported_as_one_line_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "bad.json", {"x": 1})
            with (
                mock.patch("sys.argv", ["plot_results.py", str(path)]),
                mock.patch("sys.stderr") as stderr,
            ):
                self.assertEqual(plotter.run(), 1)
            self.assertIn("bad.json", str(stderr.write.call_args_list))

    def test_scenario_without_plots_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            path = self.write_report(
                directory,
                "inline-pressure.json",
                {"schemaVersion": 1, "scenario": "inline-pressure", "runs": []},
            )
            with self.assertRaisesRegex(plotter.ReportError, "no plots"):
                plotter.plot_file(path, directory)


if __name__ == "__main__":
    unittest.main()
