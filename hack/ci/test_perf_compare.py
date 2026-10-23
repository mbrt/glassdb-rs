from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from hack.ci import perf_compare


class HarnessTest(unittest.TestCase):
    def test_same_harness_does_not_replace_engine_or_lockfile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("base", "candidate"):
                path = root / side / "crates/glassdb-bench-scale"
                path.mkdir(parents=True)
                (path / "example.rs").write_text(side)
                (root / side / "crates/glassdb").mkdir()
                (root / side / "crates/glassdb/lib.rs").write_text(side)
                (root / side / "Cargo.lock").write_text(side)
            digest = perf_compare.use_harness(root / "candidate", root / "base")
            self.assertEqual(len(digest), 64)
            self.assertEqual(
                (root / "base/crates/glassdb-bench-scale/example.rs").read_text(),
                "candidate",
            )
            self.assertEqual((root / "base/crates/glassdb/lib.rs").read_text(), "base")
            self.assertEqual((root / "base/Cargo.lock").read_text(), "base")

    def test_both_sides_use_the_base_harness_when_the_candidate_one_fails(self):
        built = []

        def fake_snapshot(repo, destination, revision):
            harness = destination / "crates/glassdb-bench-scale"
            harness.mkdir(parents=True)
            (harness / "main.rs").write_text(destination.name)

        def fake_build(source, target, output, side, log=None):
            harness = (source / "crates/glassdb-bench-scale/main.rs").read_text()
            built.append((side, harness))
            if side == "main" and harness == "pr":
                raise RuntimeError("unsupported comparison")
            return {"perfbench": f"{side}-perfbench"}

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            perf_compare, "capture", return_value="abc\n"
        ), mock.patch.object(
            perf_compare, "snapshot", fake_snapshot
        ), mock.patch.object(perf_compare, "build", fake_build):
            manifest = perf_compare.prepare(
                Path(directory), Path(directory) / "out", "main", None
            )

        self.assertEqual(
            built, [("main", "pr"), ("main", "main"), ("pr", "main")]
        )
        self.assertEqual(manifest["harness"], "base")
        self.assertEqual(len(manifest["warnings"]), 1)

    def test_current_snapshot_includes_edits_but_not_deleted_files(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "source"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / "kept").write_text("old")
            (repo / "deleted").write_text("old")
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            (repo / "kept").write_text("new")
            (repo / "deleted").unlink()
            (repo / "untracked").write_text("new file")
            perf_compare.snapshot(repo, root / "copy", None)
            self.assertEqual((root / "copy/kept").read_text(), "new")
            self.assertEqual((root / "copy/untracked").read_text(), "new file")
            self.assertFalse((root / "copy/deleted").exists())
            self.assertFalse((root / "copy/.git").exists())


class MeasurementTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fixture = self.root / "benchmark"
        # A real subprocess exercises filtering, artifacts, and pair order.
        self.fixture.write_text(f"#!{sys.executable}\n" + BENCHMARK_FIXTURE)
        self.fixture.chmod(0o755)
        self.configure()

    def configure(self, **settings):
        (self.root / "settings.json").write_text(json.dumps(settings))

    def manifest(self, cases=("a", "b"), checkpoints=(4, 8, 12)):
        return {
            "schemaVersion": 2,
            "base": "base",
            "candidate": "candidate",
            "cases": list(cases),
            "checkpoints": list(checkpoints),
            "completedPairs": {name: 0 for name in (*cases, "mixed")},
            "processTimeoutSeconds": 30,
            "warnings": [],
            "mixedArgs": ["--experiment=ci"],
            "diagnosticArgs": ["--experiment=diagnostics"],
            "counterArgs": ["--latency-jitter=false", "--experiment=diagnostics"],
            "mixedCounterArgs": ["--latency-jitter=false", "--experiment=ci"],
            **{side: {"perfbench": str(self.fixture)} for side in ("main", "pr")},
        }

    def order(self, kind="timing"):
        """Return the processes of one kind in the order they started."""
        path = self.root / "order.jsonl"
        if not path.exists():
            return []
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        return [row for row in rows if kind is None or row[3] == kind]

    def short_timeout_for(self, side, repetition):
        original = subprocess.run

        def run(command, **kwargs):
            self.assertEqual(kwargs["timeout"], 30)
            # Keep normal subprocesses independent of host speed. Only the
            # selected stalled process gets the short timeout used by the test.
            if kwargs["cwd"] == self.root / side / f"{repetition:02d}":
                kwargs["timeout"] = 0.05
            return original(command, **kwargs)

        return mock.patch.object(perf_compare.subprocess, "run", side_effect=run)

    def test_invalid_process_timeout_does_not_start_measurements(self):
        for timeout in (None, 0, -1, True, float("inf"), float("nan")):
            with self.subTest(timeout=timeout):
                manifest = self.manifest()
                manifest["processTimeoutSeconds"] = timeout
                with self.assertRaises(
                    (ValueError, perf_compare.perf_report.ReportError)
                ):
                    perf_compare.measure(self.root, manifest)
                self.assertFalse((self.root / "main").exists())
                self.assertFalse((self.root / "pr").exists())
                self.assertFalse(self.order())

    def test_fixed_latency_measurements_use_the_requested_profile(self):
        manifest = self.manifest(cases=("a",))
        manifest["mixedArgs"] = ["--latency-jitter=false", "--experiment=ci"]
        manifest["diagnosticArgs"] = ["--latency-jitter=false", "--experiment=diagnostics"]
        perf_compare.measure(self.root, manifest)
        self.assertFalse(manifest["warnings"])
        for side in ("main", "pr"):
            result = json.loads((self.root / side / "01/mixed.json").read_text())
            self.assertIs(result["latencyJitter"], False)

    def test_every_benchmark_measures_its_counters_without_latency_jitter(self):
        manifest = self.manifest(cases=("a",))
        perf_compare.measure(self.root, manifest)
        self.assertFalse(manifest["warnings"])
        for side in ("main", "pr"):
            for name in ("a", "mixed"):
                directory = self.root / side / "01"
                timing = json.loads((directory / f"{name}.json").read_text())
                counters = json.loads((directory / f"{name}.counters.json").read_text())
                self.assertIs(timing["latencyJitter"], True)
                self.assertIs(counters["latencyJitter"], False)
        started = self.order(kind=None)
        self.assertEqual([row[3] for row in started[:4]], ["timing", "counters"] * 2)

    def test_the_default_plan_measures_every_process_on_virtual_time(self):
        self.assertIn("--time=virtual", perf_compare.COMMON_ARGS)
        self.assertFalse(
            [arg for arg in perf_compare.COMMON_ARGS if arg.startswith("--delay-scale")]
        )

    def test_wrong_latency_profile_fails_before_counting_the_pair(self):
        self.configure(latencyJitter=True)
        manifest = self.manifest(cases=("a",))
        manifest["mixedArgs"] = ["--latency-jitter=false", "--experiment=ci"]
        manifest["diagnosticArgs"] = ["--latency-jitter=false", "--experiment=diagnostics"]
        with self.assertRaisesRegex(
            perf_compare.perf_report.ReportError, "latency profile"
        ):
            perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"]["mixed"], 0)

    def test_resolved_benchmarks_stop_and_uncertain_benchmarks_get_more_pairs(self):
        self.configure(factors={"b": [1.028, 1.052], "mixed": [1.028, 1.052]})
        manifest = self.manifest()
        perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], {"a": 4, "b": 8, "mixed": 8})
        order = self.order()
        self.assertEqual(order[24][1], "mixed")
        for index in range(0, len(order), 2):
            first, second = order[index : index + 2]
            self.assertEqual(first[:2], second[:2])
            self.assertNotEqual(first[2], second[2])
        for case, count in manifest["completedPairs"].items():
            pairs = [row for row in order[::2] if row[1] == case]
            self.assertEqual([row[0] for row in pairs], list(range(1, count + 1)))
            self.assertEqual([row[2] for row in pairs].count("main"), count // 2)
            self.assertEqual([row[2] for row in pairs].count("pr"), count // 2)
            for side in ("main", "pr"):
                for repetition in range(1, count + 1):
                    directory = self.root / side / f"{repetition:02d}"
                    if case == "mixed":
                        result = json.loads((directory / "mixed.json").read_text())
                        shapes = result["runs"][0]["cells"][0]["shapes"]
                        self.assertEqual(len(shapes), 4)
                    else:
                        result = json.loads((directory / f"{case}.json").read_text())
                        cell = result["runs"][0]["cells"][0]
                        self.assertEqual(cell["labels"], {"case": case})
        report = (self.root / "report.md").read_text()
        self.assertIn("regressed", report)
        self.assertNotIn("Measurement warnings", report)
        self.assertIn("cpuModel", manifest)
        with self.assertRaises(ValueError):
            perf_compare.measure(self.root, manifest)
        self.assertEqual(self.order(), order)
        with self.assertRaises(FileExistsError):
            perf_compare.measure(self.root, self.manifest())

    def test_clear_changes_and_small_effects_stop_at_the_first_checkpoint(self):
        self.configure(factors={"a": [1.024], "b": [0.970]})
        manifest = self.manifest()
        perf_compare.measure(self.root, manifest)
        self.assertEqual(set(manifest["completedPairs"].values()), {4})
        report = (self.root / "report.md").read_text()
        self.assertIn("regressed", report)
        self.assertIn("improved", report)
        self.assertNotIn("Measurement warnings", report)

    def test_unresolved_work_reaches_32_pairs_without_a_shared_deadline(self):
        self.configure(factors={"a": [0.8, 1.2], "mixed": [0.8, 1.2]})
        manifest = self.manifest(cases=("a",), checkpoints=(8, 16, 32))
        # Simulate a minute of total elapsed time per completed process. Each
        # real subprocess still completes well within its own timeout.
        with mock.patch.object(
            perf_compare.time, "monotonic", side_effect=lambda: 60 * len(self.order())
        ):
            perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], {"a": 32, "mixed": 32})
        self.assertGreater(manifest["runtimeSeconds"], 840)
        self.assertFalse(manifest["warnings"])
        report = (self.root / "report.md").read_text()
        self.assertIn("noisy or inconclusive", report)
        self.assertNotIn("using checkpoint", report)
        for name in manifest["completedPairs"]:
            result = perf_compare.perf_report.analyze_benchmark(
                self.root, manifest, name
            )
            self.assertEqual(result.pairs, 32)

    def test_stalled_process_fails_and_preserves_the_last_checkpoint(self):
        self.configure(factors={"a": [0.8, 1.2]}, hangAt=[6, "a", "main"])
        manifest = self.manifest(cases=("a",), checkpoints=(4, 8))
        with self.short_timeout_for("main", 6):
            with self.assertRaises(subprocess.TimeoutExpired):
                perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], {"a": 5, "mixed": 4})
        report = (self.root / "report.md").read_text()
        self.assertIn("Benchmark process timed out", report)
        self.assertIn("using checkpoint at 4 pairs", report)
        self.assertIn("noisy or inconclusive", report)
        self.assertNotIn("incomplete paired measurements", report)

    def test_stalled_first_process_does_not_claim_no_change(self):
        self.configure(hangAt=[1, "a", "main"])
        manifest = self.manifest(cases=("a",))
        with self.short_timeout_for("main", 1):
            with self.assertRaises(subprocess.TimeoutExpired):
                perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], {"a": 0, "mixed": 0})
        report = (self.root / "report.md").read_text()
        self.assertIn("no completed checkpoint", report)
        self.assertNotIn("No meaningful changes detected", report)

    def test_invalid_artifacts_between_checkpoints_fail_before_a_later_timeout(self):
        self.configure(
            factors={"a": [0.8, 1.2]},
            badCostAt=[5, "a", "pr"],
            hangAt=[6, "a", "main"],
        )
        manifest = self.manifest(cases=("a",), checkpoints=(4, 8))
        with self.short_timeout_for("main", 6):
            with self.assertRaises(perf_compare.perf_report.ReportError):
                perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], {"a": 4, "mixed": 4})
        report = (self.root / "report.md").read_text()
        self.assertIn("Measurement failed", report)
        self.assertIn("invalid cost measurements", report)
        self.assertNotIn("Benchmark process timed out", report)

    def test_malformed_cost_records_preserve_a_failure_report(self):
        self.configure(costRecord={})
        manifest = self.manifest(cases=("a",))
        with self.assertRaises(perf_compare.perf_report.ReportError):
            perf_compare.measure(self.root, manifest)
        saved = json.loads((self.root / "manifest.json").read_text())
        self.assertIn("Measurement failed", saved["warnings"][0])
        self.assertIn("Measurement warnings", (self.root / "report.md").read_text())

    def test_subprocess_failure_preserves_a_failure_report(self):
        self.configure(failAt=[1, "a", "pr"])
        manifest = self.manifest(cases=("a",))
        with self.assertRaises(subprocess.CalledProcessError):
            perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"]["a"], 0)
        self.assertIn("Measurement failed", (self.root / "report.md").read_text())
        self.assertNotIn(
            "Benchmark process timed out", (self.root / "report.md").read_text()
        )


BENCHMARK_FIXTURE = r"""
import json, os, sys, time
from pathlib import Path
root = Path(__file__).resolve().parent
settings = json.loads((root / 'settings.json').read_text())
side = Path.cwd().parent.name
repetition = int(Path.cwd().name)
only = [arg.split('=', 2)[2] for arg in sys.argv if arg.startswith('--only=case=')]
case = only[0] if only else 'mixed'
factors = settings.get('factors', {}).get(case, [1])
factor = factors[(repetition - 1) % len(factors)] if side == 'pr' else 1
with (root / 'order.jsonl').open('a') as log:
    kind = 'counters' if '.counters.json' in ' '.join(sys.argv) else 'timing'
    log.write(json.dumps([repetition, case, side, kind]) + '\n')
if settings.get('failAt') == [repetition, case, side]:
    sys.exit(7)
if settings.get('hangAt') == [repetition, case, side]:
    # Bound the fixture even if the driver's timeout is accidentally removed.
    time.sleep(5)
jitter = settings.get('latencyJitter', '--latency-jitter=false' not in sys.argv)
output = Path([arg for arg in sys.argv if arg.startswith('--output=')][0].split('=', 1)[1])
counters = {name: 0 for name in ('reads', 'writes', 'lists', 'readBodyBytes',
    'writeBodyBytes', 'coordinatorSubmissions', 'coordinatorRounds')}
costs = {'transactions': 30,
    **{window: counters for window in ('workload', 'shutdown', 'combined')}}
costs = settings.get('costRecord', costs)
if settings.get('badCostAt') == [repetition, case, side]:
    costs = {}
if case == 'mixed':
    shapes = [{'shape': name, 'committed': 200, 'converged': True,
        'p50Ms': 10, 'p90Ms': 20, 'txPerSec': 100}
        for name in ('rw-single', 'rw-many', 'ro-single', 'ro-multi')]
    shapes[0]['p50Ms'] *= factor
    cell = {'workload': 'low-conflict', 'affinityPct': 100, 'databases': 1,
        'workersPerShape': 1, 'failures': 0, 'shapes': shapes, 'costs': costs}
    experiment = 'ci'
else:
    mean = 100 * factor
    cell = {'labels': {'case': case}, 'failures': 0, 'costs': costs,
        'shapes': [{'shape': case, 'meanMs': mean, 'meanSeMs': 0.05}]}
    experiment = 'diagnostics'
result = {'schemaVersion': 2, 'scenario': experiment, 'backend': 'memory',
    'latencyJitter': jitter, 'timeMode': 'virtual', 'modelTimeSpeedup': 1,
    'runs': [{'run': 1, 'cells': [cell]}]}
output.write_text(json.dumps(result))
"""


if __name__ == "__main__":
    unittest.main()
