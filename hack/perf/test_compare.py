#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pandas>=2.0",
#     "matplotlib>=3.8",
#     "seaborn>=0.13",
#     "numpy>=1.26",
# ]
# ///

from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


def load_compare():
    path = Path(__file__).with_name("compare.py")
    spec = importlib.util.spec_from_file_location("aws_bench_compare", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


compare = load_compare()
pd = compare.pd
from perfbench_fixtures import contention_cell, mixed_cell, report as envelope  # noqa: E402


class CompareTest(unittest.TestCase):
    def test_geomean_preserves_a_zero_ratio(self) -> None:
        self.assertEqual(compare._geomean(pd.Series([0.0, 1.0])), 0.0)

    def test_aggregate_throughput_uses_completions_and_common_clock(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "run": 1,
                    "num-db": 3,
                    "db": db,
                    "tx-type": "write",
                    "count": count,
                    "cell-duration-ms": 1000,
                    "tx-per-sec": count,
                }
                for db, count in enumerate([90, 5, 5])
            ]
        )

        aggregate = compare.aggregate_throughput(rows)

        self.assertEqual(aggregate.loc[0, "total-tps"], 100)
        self.assertNotEqual(
            aggregate.loc[0, "total-tps"],
            rows["tx-per-sec"].median() * 3,
        )

    def test_fairness_is_separate_from_system_throughput(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "run": 1,
                    "num-db": 3,
                    "db": db,
                    "tx-type": "write",
                    "count": count,
                    "cell-duration-ms": 1000,
                    "tx-per-sec": count,
                }
                for db, count in enumerate([90, 5, 5])
            ]
        )

        fairness = compare.throughput_fairness(rows)

        self.assertAlmostEqual(fairness.loc[0, "jain"], 10000 / 24450)
        self.assertEqual(fairness.loc[0, "db-tps-p50"], 5)

    def test_throughput_ratios_are_paired_by_run(self) -> None:
        def rows(counts: list[int]):
            return pd.DataFrame(
                [
                    {
                        "run": run,
                        "num-db": 1,
                        "db": 0,
                        "tx-type": "write",
                        "count": count,
                        "cell-duration-ms": 1000,
                        "tx-per-sec": count,
                    }
                    for run, count in enumerate(counts, start=1)
                ]
            )

        table = compare.throughput_table(rows([100, 200]), rows([120, 100]), 10)

        self.assertEqual(table["run"].tolist(), [1, 2])
        self.assertEqual(table["ratio"].tolist(), [1.2, 0.5])

    def test_legacy_repetitions_are_numbered_and_use_duration_fallback(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "num-db": 2,
                    "db": db,
                    "tx-type": "write",
                    "count": count,
                    "duration-ms": duration,
                    "tx-per-sec": count * 1000 / duration,
                }
                for count, duration in [(9, 900), (20, 1000)]
                for db in [0, 1]
            ]
        )

        aggregate = compare.aggregate_throughput(rows)

        self.assertEqual(aggregate["run"].tolist(), [1, 2])
        self.assertEqual(aggregate["total-tps"].tolist(), [20, 40])

    def test_legacy_compressed_time_is_normalized(self) -> None:
        throughput = pd.DataFrame(
            [
                {
                    "duration-ms": 1000.0,
                    "tx-per-sec": 20.0,
                }
            ]
        )
        samples = pd.DataFrame([{"latency": 2.0}])

        normalized_throughput = compare.normalize_rtbench_time(
            throughput, "throughput.csv", 50.0
        )
        normalized_samples = compare.normalize_rtbench_time(
            samples, "samples.csv", 50.0
        )

        self.assertEqual(normalized_throughput.loc[0, "duration-ms"], 50_000)
        self.assertEqual(normalized_throughput.loc[0, "tx-per-sec"], 0.4)
        self.assertEqual(normalized_samples.loc[0, "latency"], 100)
        self.assertEqual(throughput.loc[0, "duration-ms"], 1000)

    def test_invalid_legacy_time_factor_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "must be positive"):
            compare.normalize_rtbench_time(
                pd.DataFrame([{"latency": 1.0}]), "samples.csv", 0.0
            )

    def test_missing_paired_run_is_rejected(self) -> None:
        base = pd.DataFrame(
            [
                {
                    "run": 1,
                    "num-db": 1,
                    "db": 0,
                    "tx-type": "write",
                    "count": 10,
                    "cell-duration-ms": 1000,
                    "tx-per-sec": 10,
                }
            ]
        )
        candidate = pd.concat([base, base.assign(run=2)], ignore_index=True)

        with self.assertRaisesRegex(ValueError, "unpaired benchmark cells"):
            compare.throughput_table(base, candidate, 10)

    def test_mixed_affinity_cells_pair_by_mode_and_percentage(self) -> None:
        def cells(throughput: float, ops: float):
            return [
                {
                    "mode": "lo",
                    "affinityPct": 50,
                    "aggregateOps": {
                        "totalOpsPerTx": ops,
                        "replaysPerTx": 0.25,
                    },
                    "shapes": [
                        {
                            "shape": "rwSingle",
                            "committed": 1000,
                            "converged": True,
                            "relCi": 0.1,
                            "txPerSec": throughput,
                            "p50Ms": 10,
                            "p90Ms": 20,
                        }
                    ],
                }
            ]

        shapes = compare.mixed_shape_table(cells(10, 4), cells(15, 3))
        aggregate = compare.mixed_aggregate_table(cells(10, 4), cells(15, 3))

        self.assertEqual(shapes.loc[0, "layout"], "50%")
        self.assertEqual(shapes.loc[0, "tps-ratio"], 1.5)
        self.assertEqual(aggregate.loc[0, "ops-ratio"], 0.75)

        striped = [{**cells(15, 3)[0], "sharedPct": 1}]
        for table in (compare.mixed_shape_table, compare.mixed_aggregate_table):
            with self.assertRaisesRegex(ValueError, "sharedPct=None in a"):
                table(cells(10, 4), striped)
        self.assertEqual(
            compare.mixed_shape_table(striped, striped).loc[0, "tps-ratio"], 1.0
        )

    def test_perfbench_mixed_envelope_preserves_run_identity(self) -> None:
        cell = mixed_cell(
            mode="hi",
            aggregateOps={"totalOpsPerTx": 2, "replaysPerTx": 0},
            shapes=[
                {
                    "shape": "rwSingle",
                    "committed": 100,
                    "converged": True,
                    "txPerSec": 10,
                    "p50Ms": 1,
                    "p90Ms": 2,
                }
            ],
        )
        report = envelope("mixed", [{"run": 2, "cells": [cell]}])

        table = compare.mixed_shape_table(report, report)

        self.assertEqual(table.loc[0, "run"], 2)
        self.assertEqual(table.loc[0, "layout"], "100%")
        self.assertEqual(table.loc[0, "shape"], "rwSingle")

    def test_multidimensional_mixed_report_is_not_silently_collapsed(self) -> None:
        cells = [
            {
                "mode": "lo",
                "affinityPct": 100,
                "databaseLimit": database_limit,
                "databases": database_limit,
                "workersPerShape": 20,
                "shapes": [],
            }
            for database_limit in (1, 3)
        ]

        with self.assertRaisesRegex(ValueError, "plot_results.py"):
            compare.mixed_shape_table(cells, cells)

    def test_perfbench_contention_envelope_converts_to_legacy_frames(self) -> None:
        cell = contention_cell(
            1,
            1,
            committed=2,
            tx_per_sec=2,
            p50_ms=10,
            p90_ms=20,
            replays=1,
            direct_candidates=2,
            direct_landed=1,
            worker_drain_ms=3,
        )
        report = envelope("contention", [{"run": 1, "cells": [cell]}])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "contention.json").write_text(json.dumps(report))
            stats = compare.perfbench_contention_frames(path)
            table = compare.contention_table(stats, stats)

        self.assertEqual(table.loc[0, "p50_a"], 10)
        self.assertEqual(table.loc[0, "p90-ratio"], 1.0)
        self.assertEqual(stats.loc[0, "direct-landed"], 1)
        self.assertEqual(stats.loc[0, "num-replays"], 1)

    def test_an_experiment_is_found_in_a_result_of_several(self) -> None:
        cell = contention_cell(1, 1, committed=2, tx_per_sec=2)
        report = envelope("contention", [{"run": 1, "cells": [cell]}])
        report["experiments"].append({"name": "mixed", "runs": []})
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "all.json").write_text(json.dumps(report))
            (path / "notes.json").write_text("[]")
            stats = compare.perfbench_contention_frames(path)

        self.assertEqual(list(stats["count"]), [2])

    def test_mixed_aggregate_table_accepts_legacy_replays_per_tx(self) -> None:
        def cells(replays_per_tx: float):
            return [
                {
                    "mode": "lo",
                    "affinityPct": 50,
                    "aggregateOps": {
                        "totalOpsPerTx": 1,
                        "retriesPerTx": replays_per_tx,
                    },
                    "shapes": [],
                }
            ]

        table = compare.mixed_aggregate_table(cells(0.5), cells(1.0))
        self.assertEqual(table.loc[0, "replays-ratio"], 2.0)

    def test_mixed_overlay_is_skipped_without_current_format_reports(self) -> None:
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            compare.plot_mixed_overlay(directory, directory, "a", "b", directory)
            self.assertEqual(list(directory.glob("*.png")), [])

    def test_inconsistent_common_cell_clock_is_rejected(self) -> None:
        rows = pd.DataFrame(
            [
                {
                    "run": 1,
                    "num-db": 2,
                    "db": db,
                    "tx-type": "write",
                    "count": 10,
                    "cell-duration-ms": duration,
                    "tx-per-sec": 10,
                }
                for db, duration in enumerate([1000, 1100])
            ]
        )

        with self.assertRaisesRegex(ValueError, "common cell clock"):
            compare.aggregate_throughput(rows)

    def test_new_one_sided_diagnostic_metric_does_not_break_pairing(self) -> None:
        base = pd.DataFrame(
            [
                {
                    "run": 1,
                    "num-db": 1,
                    "component": "coordinator",
                    "metric": "rounds",
                    "value": 4,
                    "logical-tx": 2,
                }
            ]
        )
        candidate = pd.concat(
            [
                base.assign(value=6),
                base.assign(
                    component="direct_commit",
                    metric="candidates",
                    value=3,
                ),
            ],
            ignore_index=True,
        )

        table = compare.diagnostic_metrics_table(base, candidate, 10)

        self.assertEqual(len(table), 1)
        self.assertEqual(table.loc[0, "component"], "coordinator")
        self.assertEqual(table.loc[0, "ratio"], 1.5)

    def test_diagnostic_operation_totals_do_not_mix_in_bytes(self) -> None:
        table = pd.DataFrame(
            [
                {
                    "run": 1,
                    "concurrent": 10,
                    "component": "backend.node",
                    "metric": metric,
                    "per-tx_a": value_a,
                    "per-tx_b": value_b,
                }
                for metric, value_a, value_b in [
                    ("reads", 2, 3),
                    ("writes", 1, 1),
                    ("read-bytes", 2000, 3000),
                    ("write-bytes", 1000, 1000),
                ]
            ]
        )

        ops = compare.diagnostic_role_totals(table, ["reads", "writes", "lists"])
        byte_counts = compare.diagnostic_role_totals(
            table, ["read-bytes", "write-bytes"]
        )

        self.assertEqual(ops.loc[0, "per-tx_a"], 3)
        self.assertEqual(ops.loc[0, "per-tx_b"], 4)
        self.assertEqual(byte_counts.loc[0, "per-tx_a"], 3000)
        self.assertEqual(byte_counts.loc[0, "per-tx_b"], 4000)

    def test_inline_pressure_ratios_are_paired_by_phase_and_run(self) -> None:
        def rows(throughput: list[int], landed: list[float]):
            return pd.DataFrame(
                [
                    {
                        "run": run,
                        "phase": "recovery",
                        "logical-tx": 64,
                        "tx-per-sec": tps,
                        "p50-ms": 10,
                        "p90-ms": 20,
                        "lock-calls": 64,
                        "backend-ops": 192,
                        "write-bytes": 64_000,
                        "direct-candidates": 64,
                        "direct-landed": int(64 * land_rate),
                    }
                    for run, (tps, land_rate) in enumerate(
                        zip(throughput, landed, strict=True), start=1
                    )
                ]
            )

        old = rows([100, 200], [0, 0])
        old = pd.concat(
            [
                old,
                old.iloc[[0]].assign(
                    run=1,
                    phase="shutdown",
                    **{"logical-tx": 0},
                ),
            ],
            ignore_index=True,
        )
        table = compare.inline_pressure_table(old, rows([150, 100], [1, 1]))

        self.assertEqual(table["run"].tolist(), [1, 2])
        self.assertEqual(table["tx-per-sec-ratio"].tolist(), [1.5, 0.5])
        self.assertEqual(table["direct-land-rate_b"].tolist(), [1, 1])
        self.assertEqual(table["backend-ops-per-tx_a"].tolist(), [3, 3])


if __name__ == "__main__":
    unittest.main()
