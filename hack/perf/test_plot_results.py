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
from perfbench_fixtures import conflict_cell, contention_cell, mixed_cell


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
                # Ten windows of 100 ms with `value - 1`, `value`, and
                # `value + 1` commits per 100 ms.
                "windowMs": 100.0,
                "windowCounts": [value - 1] * 3 + [value] * 4 + [value + 1] * 3,
                "converged": True,
            }
        )
    return rows


def cell(
    run: int, affinity: int, database_limit: int, workers: int, mode: str = "lo"
) -> dict:
    databases = min(database_limit, workers)
    return mixed_cell(
        mode=mode,
        affinity=affinity,
        database_limit=database_limit,
        workers=workers,
        shapes=shape_rows(run, affinity, databases, workers),
        setupSplits=0,
        splitSettleWallMs=10,
        aggregateOps={},
        aggregateProtocol={},
    )


def report(runs: list[dict], name: str = "mixed") -> dict:
    return {
        "schemaVersion": 3,
        "backend": "memory",
        "modelTimeSpeedup": 1.0,
        "experiments": [{"name": name, "runs": runs}],
    }


def runs_of(value: dict) -> list[dict]:
    """Return the runs of the one experiment of a report."""
    return value["experiments"][0]["runs"]


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
        ],
        "workers",
    )


def conflict_report() -> dict:
    return report(
        [
            {
                "run": run,
                "cells": [
                    conflict_cell(
                        tx_keys,
                        pool_keys,
                        workers=plotter.CONFLICT_WORKERS,
                        databases=databases,
                        value=float(run * 100 + pool_keys + databases),
                    )
                    for tx_keys, pools in plotter.CONFLICT_POOLS.items()
                    for pool_keys in pools
                    for databases in plotter.CONFLICT_DATABASES
                ],
            }
            for run in plotter.EXPECTED_RUNS
        ],
        "conflict-sweep",
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
            conflict_path = self.write_report(
                directory, "conflict-sweep.json", conflict_report()
            )
            worker_metadata, workers = read_canonical(workers_path)
            conflict_metadata, conflicts = perfbench_results.read_conflict_sweep(
                conflict_path, require_converged=True
            )

        self.assertEqual(
            plotter._model(worker_metadata), plotter._model(conflict_metadata)
        )
        plotter.validate_worker_sweep(workers)
        plotter.validate_conflict_sweep(conflicts)
        medians = plotter.median_rows(workers, ["workers"])
        row = medians[
            (medians["workers"] == 1) & (medians["shape"] == "rwSingle")
        ].iloc[0]
        self.assertEqual(row["throughput"], 122.0)
        self.assertAlmostEqual(row["throughput_p10"], 1210.0)
        self.assertAlmostEqual(row["throughput_p50"], 1220.0)
        self.assertAlmostEqual(row["throughput_p90"], 1230.0)
        self.assertEqual(row["p50_ms"], 244.0)
        self.assertEqual(row["p90_ms"], 488.0)
        self.assertEqual(plotter.WORKER_POINTS, (1, *range(10, 201, 10)))
        self.assertEqual(plotter.WORKER_TICKS, plotter.WORKER_POINTS)

    def test_database_count_must_match_limit_and_workers(self) -> None:
        invalid = worker_report()
        runs_of(invalid)[0]["cells"][0]["databases"] = 2
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "does not equal"):
                read_canonical(path)

    def test_unconverged_shape_is_rejected(self) -> None:
        invalid = worker_report()
        runs_of(invalid)[0]["cells"][0]["shapes"][0]["converged"] = False
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "did not converge"):
                read_canonical(path)

    def test_p90_latency_must_not_be_below_p50(self) -> None:
        invalid = worker_report()
        shape = runs_of(invalid)[0]["cells"][0]["shapes"][0]
        shape["p90Ms"] = shape["p50Ms"] - 1
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "below p50Ms"):
                read_canonical(path)

    def test_series_use_plain_lines_and_metric_bands(self) -> None:
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

        for band in (plotter.THROUGHPUT_BAND, plotter.LATENCY_BAND):
            band_figure, band_axis = plotter.plt.subplots()
            plotter._plot_shape_bands(band_axis, medians, "workers", band, colors)
            self.assertEqual(len(band_axis.lines), len(plotter.SHAPES))
            self.assertEqual(len(band_axis.collections), len(plotter.SHAPES))
            self.assertTrue(
                all(line.get_marker() == "None" for line in band_axis.lines)
            )
            plotter.plt.close(band_figure)

    def test_reports_without_throughput_windows_have_no_throughput_band(
        self,
    ) -> None:
        legacy = worker_report()
        for run in runs_of(legacy):
            for value in run["cells"]:
                for shape in value["shapes"]:
                    del shape["windowMs"], shape["windowCounts"]
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "legacy.json", legacy)
            _, workers = read_canonical(path)
        medians = plotter.median_rows(workers, ["workers"])

        figure, axis = plotter.plt.subplots()
        plotter._plot_shape_bands(
            axis, medians, "workers", plotter.THROUGHPUT_BAND, plotter._shape_colors()
        )
        self.assertEqual(len(axis.lines), len(plotter.SHAPES))
        self.assertEqual(len(axis.collections), 0)
        plotter.plt.close(figure)

    def test_conflict_cells_have_probability_and_replays(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(
                Path(directory_name), "conflict-sweep.json", conflict_report()
            )
            _, conflicts = perfbench_results.read_conflict_sweep(
                path, require_converged=True
            )

        row = conflicts[
            (conflicts["run"] == 1)
            & (conflicts["databases"] == 5)
            & (conflicts["tx_keys"] == 1)
            & (conflicts["pool_keys"] == 5000)
        ].iloc[0]
        # 19 other transactions of one key each, in a pool of 5,000 keys.
        self.assertAlmostEqual(row["conflict_probability"], 1 - (4999 / 5000) ** 19)
        self.assertAlmostEqual(row["replays_per_tx"], 51.05)
        self.assertEqual(row["p50_ms"], 10210.0)
        self.assertAlmostEqual(row["throughput_p50"], 5100.0)

    def test_conflict_probability_covers_the_edges(self) -> None:
        probability = perfbench_results.conflict_probability
        self.assertEqual(probability(10, 1, 1), 0.0)
        self.assertEqual(probability(4, 4, 2), 1.0)
        self.assertEqual(probability(3, 4, 2), 1.0)
        # Two transactions of two keys in a pool of four share no key with
        # chance 1/6.
        self.assertAlmostEqual(probability(4, 2, 2), 5 / 6)

    def test_conflict_grid_must_be_complete(self) -> None:
        invalid = conflict_report()
        del runs_of(invalid)[0]["cells"][0]
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            _, conflicts = perfbench_results.read_conflict_sweep(
                path, require_converged=True
            )
        with self.assertRaisesRegex(plotter.ReportError, "complete cell grid"):
            plotter.validate_conflict_sweep(conflicts)

    def test_conflict_label_must_name_the_keys(self) -> None:
        invalid = conflict_report()
        runs_of(invalid)[0]["cells"][0]["labels"]["conflict"] = "wide"
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "keys<K>-pool<N>"):
                perfbench_results.read_conflict_sweep(path, require_converged=True)

    def test_unconverged_conflict_cell_is_rejected_when_required(self) -> None:
        invalid = conflict_report()
        runs_of(invalid)[0]["cells"][0]["shapes"][0]["converged"] = False
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "did not converge"):
                perfbench_results.read_conflict_sweep(path, require_converged=True)
            perfbench_results.read_conflict_sweep(path, require_converged=False)

    def test_conflict_instance_counts_are_separate_cells(self) -> None:
        invalid = conflict_report()
        cells = runs_of(invalid)[0]["cells"]
        cells.append(copy.deepcopy(cells[0]))
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "duplicate"):
                perfbench_results.read_conflict_sweep(path, require_converged=True)

    def test_conflict_figures_have_one_line_and_band_per_size(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(
                Path(directory_name), "conflict-sweep.json", conflict_report()
            )
            _, conflicts = perfbench_results.read_conflict_sweep(
                path, require_converged=True
            )

        figures = {}

        def capture(figure, out_dir, name):
            figures[name] = figure
            return out_dir / name

        with mock.patch.object(plotter, "_save", side_effect=capture):
            plotter.plot_conflict_throughput(conflicts, Path("plots"))
            plotter.plot_conflict_latency(conflicts, Path("plots"))

        for name in ("conflict-throughput.png", "conflict-latency.png"):
            axes = figures[name].axes
            self.assertEqual(
                [axis.get_title() for axis in axes], ["1 DB instance", "5 DB instances"]
            )
            title = figures[name].get_suptitle().split("\n")
            self.assertEqual(len(title), 2)
            self.assertTrue(title[1].endswith("(log scale)"))
            for axis in axes:
                self.assertEqual(axis.get_yscale(), "log")
                self.assertEqual(len(axis.lines), len(plotter.CONFLICT_POOLS))
                self.assertEqual(len(axis.collections), len(plotter.CONFLICT_POOLS))
                self.assertEqual(axis.get_xscale(), "linear")
                self.assertEqual(axis.get_xlim(), (0, 100))
            self.assertEqual(
                [text.get_text() for text in axes[0].get_legend().get_texts()],
                ["1 key", "4 keys"],
            )
            plotter.plt.close(figures[name])

    def test_render_writes_all_four_figures(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            workers_path = self.write_report(directory, "workers.json", worker_report())
            conflict_path = self.write_report(
                directory, "conflict-sweep.json", conflict_report()
            )
            outputs = plotter.render(workers_path, conflict_path, directory / "plots")

            self.assertEqual(
                {path.name for path in outputs},
                {
                    "worker-throughput.png",
                    "worker-latency.png",
                    "conflict-throughput.png",
                    "conflict-latency.png",
                },
            )
            self.assertTrue(all(path.stat().st_size > 0 for path in outputs))

    def test_reports_must_use_the_same_backend_configuration(self) -> None:
        conflict = copy.deepcopy(conflict_report())
        conflict["modelTimeSpeedup"] = 5.0
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            workers_path = self.write_report(directory, "workers.json", worker_report())
            conflict_path = self.write_report(directory, "conflict-sweep.json", conflict)
            with self.assertRaisesRegex(plotter.ReportError, "same backend"):
                plotter.render(workers_path, conflict_path, directory / "plots")

    def test_canonical_sweeps_need_real_time_at_the_speed_of_wall_time(self) -> None:
        # Virtual time shows no CPU saturation, and an older compressed run
        # makes CPU time count more than backend latency.
        for change in ({"timeMode": "virtual"}, {"modelTimeSpeedup": 5.0}):
            workers = copy.deepcopy(worker_report())
            conflict = copy.deepcopy(conflict_report())
            workers.update(change)
            conflict.update(change)
            with tempfile.TemporaryDirectory() as directory_name:
                directory = Path(directory_name)
                workers_path = self.write_report(directory, "workers.json", workers)
                conflict_path = self.write_report(
                    directory, "conflict-sweep.json", conflict
                )
                with self.assertRaisesRegex(plotter.ReportError, "in real time"):
                    plotter.render(workers_path, conflict_path, directory / "plots")


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
        def one(num_keys: int, overlap_pct: int) -> dict:
            return contention_cell(
                num_keys,
                max(1, num_keys * overlap_pct // 100),
                committed=2,
                tx_per_sec=2.0,
                direct_candidates=2,
                direct_completed=2,
                worker_drain_ms=1,
            )

        return {
            "schemaVersion": 3,
            "backend": "s3",
            "modelTimeSpeedup": 1.0,
            "experiments": [
                {
                    "name": "contention",
                    "runs": [
                        {
                            "run": 1,
                            "cells": [
                                one(num_keys, pct)
                                for num_keys in (2, 4, 6)
                                for pct in overlap_pct
                            ],
                        }
                    ],
                }
            ],
        }

    def test_mixed_report_is_plotted_without_canonical_grid(self) -> None:
        cells = []
        for mode in ("lo", "hi"):
            for affinity in (0, 50, 100):
                cells.append(cell(1, affinity, 4, 8, mode))
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

    def test_conflict_report_is_plotted_without_canonical_grid(self) -> None:
        value = report(
            [{"run": 1, "cells": [conflict_cell(1, 100), conflict_cell(1, 10)]}],
            "conflict-sweep",
        )
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            path = self.write_report(directory, "probe.json", value)
            outputs = plotter.plot_file(path, directory / "plots")

            self.assertEqual(
                {output.name for output in outputs},
                {"probe-throughput.png", "probe-latency.png"},
            )

    def test_conflict_reports_are_not_overlaid(self) -> None:
        value = report([{"run": 1, "cells": [conflict_cell(1, 100)]}], "conflict-sweep")
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            a = self.write_report(directory, "a.json", value)
            b = self.write_report(directory, "b.json", value)
            with self.assertRaisesRegex(plotter.ReportError, "cannot be overlaid"):
                plotter.plot_overlay({"a": a, "b": b}, directory / "plots")

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

    def test_overlay_writes_separate_figures_per_scenario(self) -> None:
        cells = [cell(1, affinity, 4, 8) for affinity in (0, 100)]
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            mixed = self.write_report(
                directory, "mixed.json", self.real_mixed_report(cells)
            )
            contention = self.write_report(
                directory, "contention.json", self.contention_report([100])
            )

            mixed_outputs = plotter.plot_overlay({"a": mixed, "b": mixed}, directory)
            contention_outputs = plotter.plot_overlay(
                {"a": contention, "b": contention}, directory
            )

            self.assertEqual(
                {output.name for output in mixed_outputs},
                {"cmp-mixed-throughput.png", "cmp-mixed-latency.png"},
            )
            self.assertEqual(
                {output.name for output in contention_outputs},
                {"cmp-contention-throughput.png", "cmp-contention-latency.png"},
            )

    def test_overlay_rejects_different_scenarios(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            mixed = self.write_report(
                directory,
                "mixed.json",
                self.real_mixed_report([cell(1, 0, 4, 8), cell(1, 100, 4, 8)]),
            )
            contention = self.write_report(
                directory, "contention.json", self.contention_report([100])
            )
            with self.assertRaisesRegex(plotter.ReportError, "different experiments"):
                plotter.plot_overlay({"a": mixed, "b": contention}, directory)

    def test_overlay_rejects_reports_sweeping_different_dimensions(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            by_affinity = self.write_report(
                directory,
                "affinity.json",
                self.real_mixed_report([cell(1, 0, 4, 8), cell(1, 100, 4, 8)]),
            )
            by_workers = self.write_report(
                directory,
                "workers.json",
                self.real_mixed_report([cell(1, 100, 4, 4), cell(1, 100, 4, 8)]),
            )
            with self.assertRaisesRegex(plotter.ReportError, "same dimension"):
                plotter.plot_overlay({"a": by_affinity, "b": by_workers}, directory)

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
                {
                    "schemaVersion": 3,
                    "experiments": [{"name": "inline-pressure", "runs": []}],
                },
            )
            with self.assertRaisesRegex(plotter.ReportError, "no plots"):
                plotter.plot_file(path, directory)


if __name__ == "__main__":
    unittest.main()
