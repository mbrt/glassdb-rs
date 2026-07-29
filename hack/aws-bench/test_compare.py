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


class CompareTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
