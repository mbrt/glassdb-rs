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
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


def load_plotter():
    path = Path(__file__).with_name("plot-mixed-sweeps.py")
    spec = importlib.util.spec_from_file_location("mixed_sweep_plotter", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


plotter = load_plotter()


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
            worker_metadata, workers = plotter.read_report(workers_path)
            affinity_metadata, affinities = plotter.read_report(affinity_path)

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

    def test_database_count_must_match_limit_and_workers(self) -> None:
        invalid = worker_report()
        invalid["runs"][0]["cells"][0]["databases"] = 2
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "does not equal"):
                plotter.read_report(path)

    def test_unconverged_shape_is_rejected(self) -> None:
        invalid = affinity_report()
        invalid["runs"][0]["cells"][0]["shapes"][0]["converged"] = False
        with tempfile.TemporaryDirectory() as directory_name:
            path = self.write_report(Path(directory_name), "invalid.json", invalid)
            with self.assertRaisesRegex(plotter.ReportError, "did not converge"):
                plotter.read_report(path)

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


if __name__ == "__main__":
    unittest.main()
