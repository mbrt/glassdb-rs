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
        for group in ("score", "mix", "deadlock"):
            for side in ("main", "pr"):
                (self.root / group / side).mkdir(parents=True)
        for repetition in range(1, perf_report.SCORE_RUNS + 1):
            self._write_score("main", repetition, 99.0 + repetition)
            self._write_score("pr", repetition, 89.0 + repetition)
        for repetition in range(1, perf_report.MIX_RUNS + 1):
            self._write_mix("main", repetition, 9.0 + repetition)
            self._write_mix("pr", repetition, 11.0 + repetition)
        for repetition in range(1, perf_report.DEADLOCK_RUNS + 1):
            self._write_deadlock("main", repetition, 100.0 + repetition)
            self._write_deadlock("pr", repetition, 80.0 + repetition)

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
        result = [
            {
                "mode": "hi",
                "topology": "shared",
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
        ]
        path = self.root / "mix" / side / f"{repetition:02d}.json"
        path.write_text(json.dumps(result))

    def _write_deadlock(
        self,
        side: str,
        repetition: int,
        latency: float,
        *,
        with_stats: bool = True,
    ) -> None:
        path = self.root / "deadlock" / side / f"{repetition:02d}.csv"
        path.write_text(
            "run,num-keys,overlap,overlap-pct,latency-ms\n"
            + "".join(
                f"1,1,1,100,{latency + sample}\n" for sample in range(10)
            )
        )
        if not with_stats:
            return
        stats = self.root / "deadlock" / side / f"{repetition:02d}-stats.csv"
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

    def test_legacy_baseline_without_deadlock_stats_is_supported(self) -> None:
        for repetition in range(1, perf_report.DEADLOCK_RUNS + 1):
            stats = self.root / "deadlock" / "main" / f"{repetition:02d}-stats.csv"
            stats.unlink()

        report = perf_report.render_report(self.root, "main", "PR")

        self.assertIn("| Completion tx/s | n/a | 10.00 | n/a |", report)

    def test_candidate_requires_deadlock_stats(self) -> None:
        stats = self.root / "deadlock" / "pr" / "01-stats.csv"
        stats.unlink()

        with self.assertRaisesRegex(
            perf_report.ReportError, "focused deadlock stats are required"
        ):
            perf_report.render_report(self.root, "main", "PR")

    def test_deadlock_run_identity_must_be_paired(self) -> None:
        source = self.root / "deadlock" / "pr" / "03.csv"
        source.rename(source.with_name("04.csv"))

        with self.assertRaisesRegex(perf_report.ReportError, "paired deadlock runs"):
            perf_report.render_report(self.root, "main", "PR")


if __name__ == "__main__":
    unittest.main()
