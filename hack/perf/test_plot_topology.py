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

import importlib.util
import json
import math
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock


def load_plotter() -> Any:
    path = Path(__file__).with_name("plot-topology.py")
    spec = importlib.util.spec_from_file_location("topology_plotter", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


plotter = load_plotter()


def topology_cell(policy: str, tx_per_sec: float, workload: str = "hot") -> dict:
    return {
        "workload": workload,
        "leafMaxEntries": 16,
        "databases": 1,
        "policy": policy,
        "txPerSec": tx_per_sec,
        "p50Ms": 10.0,
        "adaptSplits": 2,
        "adaptMerges": 1,
        "finalLeaves": 8,
        "perTx": {"backendOps": 1.5},
    }


def mixed_cell(policy: str, affinity: int, databases: int, shapes: list[float]) -> dict:
    return {
        "mode": "lo",
        "affinityPct": affinity,
        "databases": databases,
        "policy": policy,
        "shapes": [
            {"shape": name, "txPerSec": tx_per_sec, "p50Ms": 10.0}
            for name, tx_per_sec in zip(plotter.SHAPES, shapes)
        ],
        "restructure": {
            "warmupSplits": 0,
            "warmupMerges": 0,
            "measuredSplits": 0,
            "measuredMerges": 0,
        },
        "aggregateOps": {"totalOpsPerTx": 2.0},
    }


def write(directory: Path, name: str, scenario: str, runs: list[list[dict]]) -> Path:
    path = directory / name
    data = {
        "scenario": scenario,
        "runs": [
            {"run": index + 1, "cells": cells} for index, cells in enumerate(runs)
        ],
    }
    path.write_text(json.dumps(data))
    return path


class PlotTopologyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_ratios_divide_by_the_baseline_of_the_same_file_and_run(self) -> None:
        path = write(
            self.root,
            "rep-s3-L16.json",
            "topology",
            [
                [topology_cell("fixed", 100.0), topology_cell("avoidable", 150.0)],
                [topology_cell("fixed", 50.0), topology_cell("avoidable", 100.0)],
            ],
        )
        topology, _ = plotter.load([str(path)], {"fixed"})

        ratios = plotter.relative_to_baseline(topology, plotter.TOPOLOGY_KEYS, "fixed")

        avoidable = ratios[ratios["policy"] == "avoidable"]
        self.assertEqual(avoidable["vs_baseline"].tolist(), [1.5, 2.0])

    def test_rows_without_a_baseline_are_dropped(self) -> None:
        with_baseline = write(
            self.root,
            "a-s3-L16.json",
            "topology",
            [[topology_cell("fixed", 100.0), topology_cell("avoidable", 150.0)]],
        )
        without_baseline = write(
            self.root,
            "b-s3-L16.json",
            "topology",
            [[topology_cell("avoidable", 150.0)]],
        )
        topology, _ = plotter.load(
            [str(with_baseline), str(without_baseline)], {"fixed"}
        )

        ratios = plotter.relative_to_baseline(topology, plotter.TOPOLOGY_KEYS, "fixed")

        self.assertEqual(ratios["source"].tolist(), ["a-s3-L16.json"] * 2)

    def test_selected_policies_keep_the_baselines(self) -> None:
        path = write(
            self.root,
            "v2-gcs-L16.json",
            "topology",
            [
                [
                    topology_cell("fixed", 100.0),
                    topology_cell("size", 90.0),
                    topology_cell("avoidable", 150.0),
                ]
            ],
        )

        topology, _ = plotter.load([f"{path}=avoidable"], {"fixed"})

        self.assertEqual(topology["policy"].tolist(), ["fixed", "avoidable"])
        self.assertEqual(topology["delays"].tolist(), ["gcs", "gcs"])

    def test_mixed_throughput_is_the_geometric_mean_of_the_shapes(self) -> None:
        path = write(
            self.root,
            "mixed-s3-db1.json",
            "mixed",
            [[mixed_cell("size", 0, 1, [1.0, 4.0, 16.0, 64.0])]],
        )

        _, mixed = plotter.load([str(path)], {"size"})

        self.assertTrue(math.isclose(mixed["tx_per_sec"].item(), 8.0))

    def test_mixed_cells_of_other_seeded_trees_have_their_own_baselines(self) -> None:
        seeded = [
            {**mixed_cell(policy, 0, 2, [scale] * 4), "seedLeafEntries": 7}
            for policy, scale in (("fixed", 2.0), ("avoidable", 3.0))
        ]
        path = write(
            self.root,
            "cf-s3-db2.json",
            "mixed",
            [
                [
                    mixed_cell("fixed", 0, 2, [1.0] * 4),
                    mixed_cell("avoidable", 0, 2, [3.0] * 4),
                    *seeded,
                ]
            ],
        )
        _, mixed = plotter.load([str(path)], {"fixed"})

        ratios = plotter.relative_to_baseline(mixed, plotter.MIXED_KEYS, "fixed")

        avoidable = ratios[ratios["policy"] == "avoidable"]
        self.assertEqual(avoidable["seed"].tolist(), ["default", "7"])
        for ratio, expected in zip(avoidable["vs_baseline"], [3.0, 1.5]):
            self.assertTrue(math.isclose(ratio, expected), (ratio, expected))

    def test_file_names_must_name_the_delay_model(self) -> None:
        path = write(self.root, "run.json", "topology", [[topology_cell("fixed", 1.0)]])

        with self.assertRaisesRegex(ValueError, "must contain -s3- or -gcs-"):
            plotter.load([str(path)], {"fixed"})

    def test_report_has_a_summary_and_writes_each_figure(self) -> None:
        topology = write(
            self.root,
            "final-s3-L16.json",
            "topology",
            [
                [
                    topology_cell("fixed", 100.0, workload),
                    topology_cell("avoidable", 120.0, workload),
                ]
                for workload in ("hot", "scan")
            ],
        )
        mixed = write(
            self.root,
            "final-mixed-s3-db1.json",
            "mixed",
            [
                [
                    mixed_cell(policy, affinity, databases, [scale] * 4)
                    for policy, scale in (("size", 1.0), ("avoidable", 2.0))
                    for affinity in (0, 100)
                    for databases in (1, 2)
                ]
            ],
        )
        output = self.root / "report.html"
        images = self.root / "images"
        argv = [
            "plot-topology.py",
            str(topology),
            str(mixed),
            "--output",
            str(output),
            "--image-dir",
            str(images),
        ]

        with mock.patch.object(sys, "argv", argv):
            plotter.main()

        report = output.read_text()
        self.assertIn("<h2>Summary</h2>", report)
        self.assertIn("<h2>Topology cells</h2>", report)
        self.assertIn("<h2>Mixed cells</h2>", report)
        pngs = sorted(images.glob("*.png"))
        self.assertEqual(report.count("<img "), len(pngs))
        self.assertTrue(all(png.stat().st_size > 0 for png in pngs))

    def test_inputs_without_cells_are_rejected(self) -> None:
        path = write(self.root, "empty-s3-L16.json", "topology", [[]])
        argv = ["plot-topology.py", str(path), "--output", str(self.root / "r.html")]

        with mock.patch.object(sys, "argv", argv), mock.patch("sys.stderr"):
            with self.assertRaises(SystemExit):
                plotter.main()


if __name__ == "__main__":
    unittest.main()
