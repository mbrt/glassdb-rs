from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from hack.ci import perf_report


class PerfReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        for group in ("score", "mixed", "contention"):
            for side in ("main", "pr"):
                (self.root / group / side).mkdir(parents=True)
        for repetition in range(1, perf_report.SCORE_RUNS + 1):
            self._write_score("main", repetition, 99.0 + repetition)
            self._write_score("pr", repetition, 89.0 + repetition)
        for repetition in range(1, perf_report.MIXED_RUNS + 1):
            self._write_mix("main", repetition, 9.0 + repetition)
            self._write_mix("pr", repetition, 11.0 + repetition)
        for repetition in range(1, perf_report.CONTENTION_RUNS + 1):
            self._write_contention("main", repetition, 100.0 + repetition)
            self._write_contention(
                "pr", repetition, 80.0 + repetition, current=True
            )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_score(self, side: str, repetition: int, score: float) -> None:
        result = {
            "score": score,
            "secondary": {
                "allocBytesPerTx": score * 100,
                "allocsPerTx": score,
                "nsPerTx": score * 1000,
                "cpuNsPerTx": score * 900,
            },
            "workloads": [
                {"name": name, "costPerTx": score + index}
                for index, name in enumerate(perf_report.WORKLOADS)
            ],
        }
        path = self.root / "score" / side / f"{repetition:02d}.json"
        path.write_text(json.dumps(result))

    def _write_mix(
        self,
        side: str,
        repetition: int,
        throughput: float,
        *,
        converged: bool = True,
        failures: int = 0,
    ) -> None:
        result = {
            "schemaVersion": 1,
            "scenario": "mixed",
            "backend": "memory",
            "runs": [
                {
                    "run": 1,
                    "cells": [
                        {
                            "mode": "hi",
                            "affinityPct": 100,
                            "databases": 1,
                            "setupSplits": 0,
                            "splitSettleWallMs": 2000,
                            "failures": failures,
                            "aggregateOps": {
                                "totalOpsPerTx": throughput / 10,
                                "retriesPerTx": throughput / 100,
                            },
                            "shapes": [
                                {
                                    "shape": name,
                                    "txPerSec": throughput + index,
                                    "p50Ms": 100 - throughput + index,
                                    "p90Ms": 200 - throughput + index,
                                    "converged": converged,
                                    "committed": 1000 + index,
                                    "relCi": 0.1,
                                }
                                for index, name in enumerate(perf_report.SHAPES)
                            ],
                        }
                    ],
                }
            ],
        }
        path = self.root / "mixed" / side / f"{repetition:02d}.json"
        path.write_text(json.dumps(result))

    def _write_contention(
        self,
        side: str,
        repetition: int,
        latency: float,
        *,
        with_stats: bool = True,
        current: bool = False,
    ) -> None:
        if current:
            samples = [latency + sample for sample in range(10)]
            path = self.root / "contention" / side / f"{repetition:02d}.json"
            path.write_text(
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "scenario": "contention",
                        "backend": "memory",
                        "runs": [
                            {
                                "run": 1,
                                "cells": [
                                    {
                                        "numKeys": 1,
                                        "overlap": 1,
                                        "overlapPct": 100,
                                        "committed": len(samples),
                                        "durationMs": 1000,
                                        "txPerSec": 10,
                                        "p50Ms": latency + 4.5,
                                        "p90Ms": latency + 8.5,
                                        "samplesMs": samples,
                                        "retries": 2,
                                        "directCandidates": 12,
                                        "directLanded": 10,
                                        "workerDrainMs": 5,
                                        "failures": 0,
                                    }
                                ],
                            }
                        ],
                    }
                )
            )
            return
        path = self.root / "contention" / side / f"{repetition:02d}.csv"
        path.write_text(
            "run,num-keys,overlap,overlap-pct,latency-ms\n"
            + "".join(
                f"1,1,1,100,{latency + sample}\n" for sample in range(10)
            )
        )
        if not with_stats:
            return
        stats = self.root / "contention" / side / f"{repetition:02d}-stats.csv"
        stats.write_text(
            "run,num-keys,overlap,overlap-pct,count,cell-duration-ms,num-retries,"
            "direct-candidates,direct-landed,worker-drain-ms\n"
            "1,1,1,100,10,1000,2,12,10,5\n"
        )

    def test_render_report_aggregates_repetitions(self) -> None:
        report = perf_report.render_report(self.root, "main (aaa)", "PR (bbb)")

        self.assertIn("105.00 (100.00–110.00)", report)
        self.assertIn("95.00 (90.00–100.00)", report)
        self.assertIn("-9.52%", report)
        self.assertIn("11.00 (10.00–12.00)", report)
        self.assertIn("13.00 (12.00–14.00)", report)
        self.assertIn("+18.18%", report)
        self.assertIn("All shapes converged", report)
        self.assertIn("Focused one-key RMW contention", report)
        self.assertIn("| Completion tx/s | 10.00 | 10.00 | +0.00% |", report)
        self.assertIn("| Direct candidates/tx | 1.200 | 1.200 | +0.00% |", report)
        self.assertIn("| Direct land rate | 0.833 | 0.833 | +0.00% |", report)

    def test_render_report_marks_unconverged_shapes(self) -> None:
        self._write_mix("pr", 2, 13.0, converged=False)

        report = perf_report.render_report(self.root, "main", "PR")

        self.assertIn("PR run 2", report)
        self.assertIn("roMulti", report)
        self.assertIn("rwSingle", report)

    def test_missing_repetition_is_rejected(self) -> None:
        (self.root / "score" / "main" / "11.json").unlink()

        with self.assertRaisesRegex(perf_report.ReportError, "expected 11"):
            perf_report.render_report(self.root, "main", "PR")

    def test_transaction_failure_is_rejected(self) -> None:
        self._write_mix("main", 1, 10.0, failures=1)

        with self.assertRaisesRegex(perf_report.ReportError, "recorded 1 failures"):
            perf_report.render_report(self.root, "main", "PR")

    def test_affinity_result_requires_split_settlement(self) -> None:
        path = self.root / "mixed" / "pr" / "01.json"
        result = json.loads(path.read_text())
        del result["runs"][0]["cells"][0]["splitSettleWallMs"]
        path.write_text(json.dumps(result))

        with self.assertRaisesRegex(perf_report.ReportError, "splitSettleWallMs"):
            perf_report.render_report(self.root, "main", "PR")

    def test_legacy_baseline_without_deadlock_stats_is_supported(self) -> None:
        for repetition in range(1, perf_report.CONTENTION_RUNS + 1):
            stats = (
                self.root
                / "contention"
                / "main"
                / f"{repetition:02d}-stats.csv"
            )
            stats.unlink()

        report = perf_report.render_report(self.root, "main", "PR")

        self.assertIn("| Completion tx/s | n/a | 10.00 | n/a |", report)

    def test_candidate_requires_contention_metrics(self) -> None:
        path = self.root / "contention" / "pr" / "01.json"
        result = json.loads(path.read_text())
        del result["runs"][0]["cells"][0]["directCandidates"]
        path.write_text(json.dumps(result))

        with self.assertRaisesRegex(perf_report.ReportError, "directCandidates"):
            perf_report.render_report(self.root, "main", "PR")

    def test_contention_run_identity_must_be_paired(self) -> None:
        source = self.root / "contention" / "pr" / "03.json"
        source.rename(source.with_name("04.json"))

        with self.assertRaisesRegex(perf_report.ReportError, "paired contention runs"):
            perf_report.render_report(self.root, "main", "PR")


if __name__ == "__main__":
    unittest.main()
