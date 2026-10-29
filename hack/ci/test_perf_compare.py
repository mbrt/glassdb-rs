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

    def prepare_with(self, fail_candidate_harness, base_has_experiments=True):
        """Prepare a comparison of fake snapshots, and return what the sides
        built, the manifest, and the experiment file that both sides read."""
        built = []

        def fake_snapshot(repo, destination, revision):
            harness = destination / "crates/glassdb-bench-scale"
            harness.mkdir(parents=True)
            (harness / "main.rs").write_text(destination.name)
            if destination.name == "main" and not base_has_experiments:
                return
            experiments = destination / perf_compare.EXPERIMENTS
            experiments.mkdir(parents=True)
            (experiments / "warm_read.toml").write_text(destination.name)

        def fake_build(source, target, output, side, log=None):
            harness = (source / "crates/glassdb-bench-scale/main.rs").read_text()
            built.append((side, harness))
            if fail_candidate_harness and side == "main" and harness == "pr":
                raise RuntimeError("unsupported comparison")
            return {"perfbench": f"{side}-perfbench"}

        with tempfile.TemporaryDirectory() as directory, mock.patch.object(
            perf_compare, "capture", return_value="abc\n"
        ), mock.patch.object(
            perf_compare, "snapshot", fake_snapshot
        ), mock.patch.object(perf_compare, "build", fake_build):
            output = Path(directory).resolve() / "out"
            manifest = perf_compare.prepare(Path(directory), output, "main", None)
            experiments = output / "harness" / perf_compare.EXPERIMENTS
            for arguments in (manifest["timingArgs"], manifest["counterArgs"]):
                self.assertEqual(arguments[-1], f"--experiment={experiments}")
            experiment = (experiments / "warm_read.toml").read_text()
        return built, manifest, experiment

    def test_both_sides_use_the_experiments_of_the_candidate_harness(self):
        built, manifest, experiment = self.prepare_with(False)

        self.assertEqual(built, [("main", "pr"), ("pr", "pr")])
        self.assertNotIn("harness", manifest)
        self.assertEqual(experiment, "pr")

    def test_both_sides_use_the_base_harness_when_the_candidate_one_fails(self):
        built, manifest, experiment = self.prepare_with(True)

        self.assertEqual(
            built, [("main", "pr"), ("main", "main"), ("pr", "main")]
        )
        self.assertEqual(manifest["harness"], "base")
        self.assertEqual(len(manifest["warnings"]), 1)
        self.assertEqual(experiment, "main")

    def test_a_base_harness_without_experiment_files_is_unsupported(self):
        with self.assertRaisesRegex(RuntimeError, "harness has no"):
            self.prepare_with(True, base_has_experiments=False)

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
        # A real subprocess exercises artifacts and pair order.
        self.fixture.write_text(f"#!{sys.executable}\n" + BENCHMARK_FIXTURE)
        self.fixture.chmod(0o755)
        self.configure()

    def configure(self, **settings):
        (self.root / "settings.json").write_text(json.dumps(settings))

    def manifest(self, cases=("a", "b"), checkpoints=(4, 8, 12)):
        (self.root / "cases.json").write_text(json.dumps(cases))
        return {
            "schemaVersion": perf_compare.perf_report.MANIFEST_VERSION,
            "base": "base",
            "candidate": "candidate",
            "cases": [*cases, "shapes_shared_leaf"],
            "checkpoints": list(checkpoints),
            "completedPairs": 0,
            "latencyJitter": True,
            "processTimeoutSeconds": 30,
            "warnings": [],
            "timingArgs": ["--experiment=default"],
            "counterArgs": ["--latency-jitter=false", "--experiment=default"],
            **{side: {"perfbench": str(self.fixture)} for side in ("main", "pr")},
        }

    def order(self, kind="timing"):
        """Return the processes of one kind in the order they started."""
        path = self.root / "order.jsonl"
        if not path.exists():
            return []
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        return [row for row in rows if kind is None or row[1] == kind]

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

    def read(self, side, repetition, kind):
        path = self.root / side / f"{repetition:02d}" / f"{kind}.json"
        return json.loads(path.read_text())

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
                self.assertFalse(self.order(kind=None))

    def test_fixed_latency_measurements_use_the_requested_profile(self):
        manifest = self.manifest(cases=("a",))
        manifest.update(
            timingArgs=["--latency-jitter=false", "--experiment=default"],
            latencyJitter=False,
        )
        perf_compare.measure(self.root, manifest)
        self.assertFalse(manifest["warnings"])
        for side in ("main", "pr"):
            self.assertIs(self.read(side, 1, "timing")["latencyJitter"], False)

    def test_each_side_measures_latency_and_then_counters_without_jitter(self):
        manifest = self.manifest(cases=("a",))
        perf_compare.measure(self.root, manifest)
        self.assertFalse(manifest["warnings"])
        for side in ("main", "pr"):
            timing, counters = self.read(side, 1, "timing"), self.read(side, 1, "counters")
            self.assertIs(timing["latencyJitter"], True)
            self.assertIs(counters["latencyJitter"], False)
            # One process measures every case, including the shapes_shared_leaf workload.
            for result in (timing, counters):
                names = [experiment["name"] for experiment in result["experiments"]]
                self.assertEqual(names, ["a", "shapes_shared_leaf"])
        kinds = [row[1] for row in self.order(kind=None)[:4]]
        self.assertEqual(kinds, ["timing", "counters"] * 2)

    def test_the_default_plan_measures_on_virtual_time(self):
        for arguments in (perf_compare.TIMING_ARGS, perf_compare.COUNTER_ARGS):
            self.assertIn("--time=virtual", arguments)
            self.assertFalse(
                [arg for arg in arguments if arg.startswith("--delay-scale")]
            )
        self.assertIn("--latency-jitter=true", perf_compare.TIMING_ARGS)
        self.assertIn("--latency-jitter=false", perf_compare.COUNTER_ARGS)
        self.assertEqual(
            perf_compare.CASES[-1], perf_compare.perf_report.SHARED_LEAF_CASE
        )

    def test_wrong_latency_profile_fails_before_counting_the_pair(self):
        self.configure(latencyJitter=True)
        manifest = self.manifest(cases=("a",))
        manifest.update(
            timingArgs=["--latency-jitter=false", "--experiment=default"],
            latencyJitter=False,
        )
        with self.assertRaisesRegex(
            perf_compare.perf_report.ReportError, "latency profile"
        ):
            perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], 0)

    def test_every_benchmark_gets_the_pairs_that_the_least_resolved_one_needs(self):
        self.configure(factors={"b": [1.028, 1.052], "shapes_shared_leaf": [1.028, 1.052]})
        manifest = self.manifest()
        perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], 8)
        order = self.order()
        self.assertEqual([row[0] for row in order[::2]], list(range(1, 9)))
        # The sides alternate which one starts a pair.
        for repetition in range(1, 9):
            pair = [row for row in order if row[0] == repetition]
            self.assertEqual(
                [row[2] for row in pair],
                ["main", "pr"] if repetition % 2 else ["pr", "main"],
            )
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
        self.assertEqual(manifest["completedPairs"], 4)
        report = (self.root / "report.md").read_text()
        self.assertIn("regressed", report)
        self.assertIn("improved", report)
        self.assertNotIn("Measurement warnings", report)

    def test_unresolved_work_reaches_32_pairs_without_a_shared_deadline(self):
        self.configure(factors={"a": [0.8, 1.2], "shapes_shared_leaf": [0.8, 1.2]})
        manifest = self.manifest(cases=("a",), checkpoints=(8, 16, 32))
        # Simulate a minute of total elapsed time per completed process. Each
        # real subprocess still completes well within its own timeout.
        with mock.patch.object(
            perf_compare.time,
            "monotonic",
            side_effect=lambda: 60 * len(self.order(kind=None)),
        ):
            perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], 32)
        self.assertGreater(manifest["runtimeSeconds"], 840)
        self.assertFalse(manifest["warnings"])
        report = (self.root / "report.md").read_text()
        self.assertIn("noisy or inconclusive", report)
        self.assertNotIn("using checkpoint", report)
        self.assertEqual(
            perf_compare.perf_report.analyze(self.root, manifest).pairs, 32
        )

    def test_stalled_process_fails_and_preserves_the_last_checkpoint(self):
        self.configure(factors={"a": [0.8, 1.2]}, hangAt=[6, "timing", "main"])
        manifest = self.manifest(cases=("a",), checkpoints=(4, 8))
        with self.short_timeout_for("main", 6):
            with self.assertRaises(subprocess.TimeoutExpired):
                perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], 5)
        report = (self.root / "report.md").read_text()
        self.assertIn("Benchmark process timed out", report)
        self.assertIn("using checkpoint at 4 pairs", report)
        self.assertIn("noisy or inconclusive", report)
        self.assertNotIn("incomplete paired measurements", report)

    def test_stalled_first_process_does_not_claim_no_change(self):
        self.configure(hangAt=[1, "timing", "main"])
        manifest = self.manifest(cases=("a",))
        with self.short_timeout_for("main", 1):
            with self.assertRaises(subprocess.TimeoutExpired):
                perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], 0)
        report = (self.root / "report.md").read_text()
        self.assertIn("no completed checkpoint", report)
        self.assertNotIn("No meaningful changes detected", report)

    def test_invalid_artifacts_between_checkpoints_fail_before_a_later_timeout(self):
        self.configure(
            factors={"a": [0.8, 1.2]},
            badCostAt=[5, "a", "pr"],
            hangAt=[6, "timing", "main"],
        )
        manifest = self.manifest(cases=("a",), checkpoints=(4, 8))
        with self.short_timeout_for("main", 6):
            with self.assertRaises(perf_compare.perf_report.ReportError):
                perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], 4)
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
        self.configure(failAt=[1, "timing", "pr"])
        manifest = self.manifest(cases=("a",))
        with self.assertRaises(subprocess.CalledProcessError):
            perf_compare.measure(self.root, manifest)
        self.assertEqual(manifest["completedPairs"], 0)
        self.assertIn("Measurement failed", (self.root / "report.md").read_text())
        self.assertNotIn(
            "Benchmark process timed out", (self.root / "report.md").read_text()
        )


BENCHMARK_FIXTURE = r"""
import json, sys, time
from pathlib import Path
root = Path(__file__).resolve().parent
settings = json.loads((root / 'settings.json').read_text())
cases = json.loads((root / 'cases.json').read_text())
side = Path.cwd().parent.name
repetition = int(Path.cwd().name)
output = Path([arg for arg in sys.argv if arg.startswith('--output=')][0].split('=', 1)[1])
kind = output.stem
with (root / 'order.jsonl').open('a') as log:
    log.write(json.dumps([repetition, kind, side]) + '\n')
if settings.get('failAt') == [repetition, kind, side]:
    sys.exit(7)
if settings.get('hangAt') == [repetition, kind, side]:
    # Bound the fixture even if the driver's timeout is accidentally removed.
    time.sleep(5)
jitter = settings.get('latencyJitter', '--latency-jitter=false' not in sys.argv)
counters = {name: 0 for name in ('reads', 'writes', 'lists', 'readBodyBytes',
    'writeBodyBytes', 'coordinatorSubmissions', 'coordinatorRounds')}
cells = []
for case in [*cases, 'shapes_shared_leaf']:
    factors = settings.get('factors', {}).get(case, [1])
    factor = factors[(repetition - 1) % len(factors)] if side == 'pr' else 1
    costs = {'transactions': 30,
        **{window: counters for window in ('workload', 'shutdown', 'combined')}}
    costs = settings.get('costRecord', costs)
    if settings.get('badCostAt') == [repetition, case, side] and kind == 'counters':
        costs = {}
    if case == 'shapes_shared_leaf':
        shapes = [{'shape': name, 'committed': 200, 'converged': True,
            'p50Ms': 10, 'p90Ms': 20, 'txPerSec': 100}
            for name in ('rw-single', 'rw-many', 'ro-single', 'ro-multi')]
        shapes[0]['p50Ms'] *= factor
        cell = {'workload': 'low-conflict', 'affinityPct': 100, 'databases': 1,
            'workersPerShape': 1, 'shapes': shapes}
    else:
        latency = 100 * factor
        cell = {'shapes': [{'shape': case, 'committed': 200, 'converged': True,
            'p50Ms': latency, 'p90Ms': 2 * latency, 'txPerSec': 1000 / latency}]}
    cells.append({'name': case, 'runs': [{'run': 1,
        'cells': [{**cell, 'failures': 0, 'costs': costs}]}]})
result = {'schemaVersion': 3, 'backend': 'memory',
    'latencyJitter': jitter, 'timeMode': 'virtual', 'modelTimeSpeedup': 1,
    'experiments': cells}
output.write_text(json.dumps(result))
"""


if __name__ == "__main__":
    unittest.main()
