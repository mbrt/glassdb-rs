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
        for side in ("main", "pr"):
            (self.root / "score" / side).mkdir(parents=True)
        for repetition in range(1, perf_report.SCORE_RUNS + 1):
            self._write_score("main", repetition, 99.0 + repetition)
            self._write_score("pr", repetition, 89.0 + repetition)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write_score(self, side: str, repetition: int, score: float) -> None:
        result = {
            "score": score,
            "backendLatencyMs": 1,
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

    def test_render_report_aggregates_score_and_secondary_metrics(self) -> None:
        report = perf_report.render_report(
            self.root, "main | aaa", "PR merge (bbb)"
        )

        self.assertIn("- Base: `main \\| aaa`", report)
        self.assertIn("105.00 (100.00–110.00)", report)
        self.assertIn("95.00 (90.00–100.00)", report)
        self.assertIn("-9.52%", report)
        self.assertIn("`batchWrite100` cost/tx", report)
        self.assertIn("fixed 1 ms operation latency over memory", report)
        self.assertIn(
            "Latency-stabilized in-memory secondary metrics (informational)", report
        )
        self.assertIn("10,500 (10,000–11,000)", report)
        self.assertIn("| Allocations/tx | 105.0 (100.0–110.0)", report)
        self.assertIn("| Wall ns/tx | 105,000 (100,000–110,000)", report)
        self.assertIn("| CPU ns/tx | 94,500 (90,000–99,000)", report)
        self.assertNotIn("Focused contention mix", report)
        self.assertNotIn("Focused one-key RMW contention", report)

    def test_missing_repetition_is_rejected(self) -> None:
        (self.root / "score" / "main" / "11.json").unlink()

        with self.assertRaisesRegex(perf_report.ReportError, "score runs"):
            perf_report.render_report(self.root, "main", "PR")

    def test_unexpected_workload_is_rejected(self) -> None:
        path = self.root / "score" / "pr" / "01.json"
        result = json.loads(path.read_text())
        result["workloads"][0]["name"] = "unexpected"
        path.write_text(json.dumps(result))

        with self.assertRaisesRegex(perf_report.ReportError, "workloads are"):
            perf_report.render_report(self.root, "main", "PR")

    def test_missing_secondary_metric_is_rejected(self) -> None:
        path = self.root / "score" / "pr" / "01.json"
        result = json.loads(path.read_text())
        del result["secondary"]["cpuNsPerTx"]
        path.write_text(json.dumps(result))

        with self.assertRaisesRegex(perf_report.ReportError, "cpuNsPerTx"):
            perf_report.render_report(self.root, "main", "PR")

    def test_mismatched_backend_latency_is_rejected(self) -> None:
        path = self.root / "score" / "pr" / "01.json"
        result = json.loads(path.read_text())
        result["backendLatencyMs"] = 2
        path.write_text(json.dumps(result))

        with self.assertRaisesRegex(perf_report.ReportError, "one backend latency"):
            perf_report.render_report(self.root, "main", "PR")


if __name__ == "__main__":
    unittest.main()
