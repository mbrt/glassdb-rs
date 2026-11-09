from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from hack.ci import perf_compare, perf_report


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
        self.events = []

        def fake_snapshot(repo, destination, revision):
            self.events.append(("snapshot", destination.name))
            harness = destination / "crates/glassdb-bench-scale"
            harness.mkdir(parents=True)
            (harness / "main.rs").write_text(destination.name)
            if destination.name == "main" and not base_has_experiments:
                return
            experiments = destination / perf_compare.EXPERIMENTS
            experiments.mkdir(parents=True)
            (experiments / "read-warm.toml").write_text(destination.name)

        def fake_build(source, target, output, side, log=None):
            harness = (source / "crates/glassdb-bench-scale/main.rs").read_text()
            built.append((side, harness))
            self.events.append(("build", side))
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
            self.assertEqual(manifest["experiments"], str(experiments))
            experiment = (experiments / "read-warm.toml").read_text()
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
        # The base snapshot of the second build must be newer than the
        # artifacts of the failed build in the same target directory.
        self.assertEqual(
            self.events,
            [
                ("snapshot", "main"),
                ("snapshot", "pr"),
                ("build", "main"),
                ("snapshot", "main"),
                ("build", "main"),
                ("build", "pr"),
            ],
        )

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

    def test_snapshots_are_newer_than_artifacts_built_before_them(self):
        # Cargo reuses an artifact that is newer than its sources, so a snapshot
        # must not keep the commit time or the time of the working-tree file.
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "source"
            repo.mkdir()
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / "file").write_text("old")
            past = "2001-01-01T00:00:00Z"
            os.utime(repo / "file", (978307200, 978307200))
            subprocess.run(["git", "add", "."], cwd=repo, check=True)
            subprocess.run(
                ["git", "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "x"],
                cwd=repo,
                check=True,
                env={**os.environ, "GIT_AUTHOR_DATE": past, "GIT_COMMITTER_DATE": past},
            )
            start = time.time() - 1
            perf_compare.snapshot(repo, root / "revision", "HEAD")
            perf_compare.snapshot(repo, root / "worktree", None)
            for copy in ("revision", "worktree"):
                with self.subTest(copy=copy):
                    self.assertEqual((root / copy / "file").read_text(), "old")
                    self.assertGreater((root / copy / "file").stat().st_mtime, start)


REPO = Path(__file__).resolve().parents[2]


class BenchTest(unittest.TestCase):
    """`make bench` runs one side of the comparison, with a fake perfbench."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.fixture = self.root / "benchmark"
        # A real subprocess exercises the artifacts and the order of the processes.
        self.fixture.write_text(f"#!{sys.executable}\n" + BENCHMARK_FIXTURE)
        self.fixture.chmod(0o755)
        self.configure()
        self.cases(("a", "b"))

    def configure(self, **settings):
        (self.root / "settings.json").write_text(json.dumps(settings))

    def cases(self, cases):
        (self.root / "cases.json").write_text(json.dumps(list(cases)))

    def bench(self, side, *variables, check=True):
        """Run `make bench` into the directory `side`, and return its output."""
        result = subprocess.run(
            [
                "make",
                "--no-print-directory",
                "bench",
                f"PERFBENCH={self.fixture}",
                f"EXPERIMENTS_DIR={self.root / 'experiments'}",
                f"OUT={self.root / side}",
                *variables,
            ],
            cwd=REPO,
            capture_output=True,
            text=True,
        )
        if check:
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result

    def order(self):
        """Return the processes, as `[kind, side]`, in the order they started."""
        path = self.root / "order.jsonl"
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def arguments(self):
        """Return the arguments of the processes, by kind."""
        return dict(
            json.loads(line)
            for line in (self.root / "arguments.jsonl").read_text().splitlines()
        )

    def read(self, side, kind):
        return json.loads((self.root / side / f"{kind}.json").read_text())

    def report(self):
        return perf_report.render_report(
            self.root / "main", self.root / "pr", "base", "candidate"
        )

    def test_the_counters_run_once_and_then_the_latency_in_32_runs(self):
        output = self.bench("main").stdout
        self.assertEqual(self.order(), [["counters", "main"], ["timing", "main"]])
        timing, counters = self.read("main", "timing"), self.read("main", "counters")
        self.assertIs(timing["latencyJitter"], True)
        self.assertIs(counters["latencyJitter"], False)
        self.assertEqual(len(timing["experiments"][0]["runs"]), 32)
        self.assertEqual(len(counters["experiments"][0]["runs"]), 1)
        # The summary of the main metrics prints at the end.
        self.assertIn("median of 32 runs", output)
        self.assertIn("| a | 100.00", output)
        self.assertIn(f"Results: {self.root / 'main'}", output)

    def test_both_processes_run_on_virtual_time_with_the_experiments_and_a_tag(self):
        self.bench("main")
        arguments = self.arguments()
        experiments = f"--experiment={self.root / 'experiments'}"
        for kind, tag, jitter, runs in (
            ("counters", "--tag=ci/counters", "--latency-jitter=false", "--runs=1"),
            ("timing", "--tag=ci/timing", "--latency-jitter=true", "--runs=32"),
        ):
            for argument in ("--time=virtual", "--backend=memory", experiments, tag, jitter, runs):
                self.assertIn(argument, arguments[kind], kind)
            # perfbench decides whether to draw its progress bar.
            self.assertFalse(any(a.startswith("--progress") for a in arguments[kind]))

    def test_the_extended_experiments_run_only_with_extended(self):
        exclude = "--exclude-tag=ci/extended"
        self.bench("main")
        self.assertIn(exclude, self.arguments()["timing"])
        self.assertNotIn(exclude, self.arguments()["counters"])
        self.bench("pr", "EXTENDED=0")
        self.assertIn(exclude, self.arguments()["timing"])
        self.bench("third", "EXTENDED=1")
        self.assertNotIn(exclude, self.arguments()["timing"])

    def test_clear_changes_are_reported_in_both_directions(self):
        self.bench("main")
        self.configure(factors={"a": [1.10, 1.12], "b": [0.88, 0.90]})
        self.bench("pr")
        report = self.report()
        self.assertIn("regressed", report)
        self.assertIn("improved", report)
        self.assertNotIn("Measurement warnings", report)

    def test_a_second_run_needs_a_new_directory(self):
        self.bench("main")
        order = self.order()
        result = self.bench("main", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.order(), order)

    def test_a_failed_process_fails_and_stops_the_side(self):
        self.configure(failAt=["counters", "main"])
        result = self.bench("main", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.order(), [["counters", "main"]])
        self.assertNotIn("Results:", result.stdout)

    def test_a_side_without_usable_results_fails(self):
        # The processes succeed, but every cell failed.
        self.cases(("a",))
        self.configure(failedCells=True)
        result = self.bench("main", check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cell failed", result.stdout + result.stderr)

    def test_counters_with_latency_jitter_are_not_compared(self):
        self.configure(latencyJitter=True)
        self.bench("main")
        self.bench("pr")
        report = self.report()
        self.assertIn("invalid counter measurement", report)
        self.assertIn("latency profile", report)

    def test_malformed_cost_records_are_warnings(self):
        self.configure(costRecord={})
        self.bench("main")
        self.bench("pr")
        report = self.report()
        self.assertIn("invalid cost measurements", report)
        self.assertNotIn("No meaningful changes detected", report)


BENCHMARK_FIXTURE = r"""
import json, sys
from pathlib import Path
root = Path(__file__).resolve().parent
settings = json.loads((root / 'settings.json').read_text())
cases = json.loads((root / 'cases.json').read_text())
output = Path([arg for arg in sys.argv if arg.startswith('--output=')][0].split('=', 1)[1])
side = output.parent.name
runs = int([arg for arg in sys.argv if arg.startswith('--runs=')][0].split('=', 1)[1])
kind = output.stem
with (root / 'order.jsonl').open('a') as log:
    log.write(json.dumps([kind, side]) + '\n')
with (root / 'arguments.jsonl').open('a') as log:
    log.write(json.dumps([kind, sys.argv[1:]]) + '\n')
if settings.get('failAt') == [kind, side]:
    sys.exit(7)
jitter = settings.get('latencyJitter', '--latency-jitter=false' not in sys.argv)
counters = {name: 0 for name in ('reads', 'writes', 'lists', 'readBodyBytes',
    'writeBodyBytes', 'coordinatorSubmissions', 'coordinatorRounds')}
experiments = []
for case in [*cases, 'shapes-shared-leaf']:
    factors = settings.get('factors', {}).get(case, [1])
    costs = {'transactions': 30,
        **{window: counters for window in ('workload', 'shutdown', 'combined')}}
    costs = settings.get('costRecord', costs)
    cells = []
    for run in range(1, runs + 1):
        factor = factors[(run - 1) % len(factors)] if side == 'pr' else 1
        if case == 'shapes-shared-leaf':
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
        failures = 1 if settings.get('failedCells') else 0
        cells.append({'run': run, 'cells': [{**cell, 'failures': failures, 'costs': costs}]})
    experiments.append({'name': case, 'runs': cells})
result = {'schemaVersion': 3, 'backend': 'memory',
    'latencyJitter': jitter, 'timeMode': 'virtual', 'modelTimeSpeedup': 1,
    'experiments': experiments}
output.write_text(json.dumps(result))
"""


if __name__ == "__main__":
    unittest.main()
