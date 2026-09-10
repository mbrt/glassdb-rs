from __future__ import annotations

from pathlib import Path
import subprocess
import json
import sys
import tempfile
import unittest

from hack.ci import perf_compare


class HarnessTest(unittest.TestCase):
    def test_same_harness_does_not_replace_engine_or_lockfile(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for side in ("base", "candidate"):
                for relative in (
                    "crates/glassdb/benches",
                    "crates/glassdb-bench-scale",
                ):
                    path = root / side / relative
                    path.mkdir(parents=True)
                    (path / "example.rs").write_text(side)
                (root / side / "crates/glassdb/Cargo.toml").write_text(
                    f'[package]\nname = "{side}"\n[dev-dependencies]\ncriterion = "{side}"\n[[bench]]\nname = "transactions"\n'
                    + (
                        '[[bench]]\nname = "diagnostics"\nharness = false\ntest = false\n'
                        if side == "candidate"
                        else ""
                    )
                    + "[features]\ndefault = []\n"
                )
                (root / side / "Cargo.lock").write_text(side)
            digest = perf_compare.use_harness(root / "candidate", root / "base")
            self.assertEqual(len(digest), 64)
            self.assertEqual(
                (root / "base/crates/glassdb/benches/example.rs").read_text(),
                "candidate",
            )
            self.assertEqual((root / "base/Cargo.lock").read_text(), "base")
            manifest = (root / "base/crates/glassdb/Cargo.toml").read_text()
            self.assertIn('name = "base"', manifest)
            self.assertIn('criterion = "candidate"', manifest)
            self.assertIn(
                '[[bench]]\nname = "diagnostics"\nharness = false\ntest = false',
                manifest,
            )
            self.assertIn("[features]\ndefault = []", manifest)
            self.assertEqual(manifest.count("[[bench]]"), 2)

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
    def test_malformed_cost_records_preserve_a_failure_report(self):
        for record in ({}, [], {"schemaVersion": 1, "cases": [{}]}):
            with (
                self.subTest(record=record),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                fixture = root / "benchmark"
                fixture.write_text(
                    f"#!{sys.executable}\n"
                    f"print('diagnostic-costs: ' + {json.dumps(record)!r})\n"
                )
                fixture.chmod(0o755)
                output = root / "output"
                output.mkdir()
                manifest = {
                    "schemaVersion": 1,
                    "base": "base",
                    "candidate": "candidate",
                    "cases": ["example"],
                    "repetitions": 3,
                    "runtimeLimitSeconds": 30,
                    "warnings": [],
                    "mixedArgs": [],
                    **{
                        side: {"diagnostics": str(fixture), "perfbench": str(fixture)}
                        for side in ("main", "pr")
                    },
                }
                with self.assertRaises(perf_compare.perf_report.ReportError):
                    perf_compare.measure(output, manifest)
                saved = json.loads((output / "manifest.json").read_text())
                self.assertIn("Measurement failed", saved["warnings"][0])
                self.assertIn(
                    "Measurement warnings", (output / "report.md").read_text()
                )

    def test_cases_are_paired_with_balanced_order_and_complete_costs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = root / "benchmark"
            # A real subprocess exercises filtering, paths, logs, and cost
            # aggregation without spending seconds measuring a test fixture.
            fixture.write_text(
                f"#!{sys.executable}\n"
                + """
import json, os, sys
from pathlib import Path
root = Path(__file__).resolve().parent
side = Path.cwd().parent.name
repetition = Path.cwd().name
if '--output' in sys.argv:
    case = 'mixed'
    Path(sys.argv[sys.argv.index('--output') + 1]).write_text('{}')
else:
    case = sys.argv[-1].removeprefix('^diagnostic/').removesuffix('$')
    path = Path(os.environ['CRITERION_HOME']) / 'diagnostic' / case / 'new'
    path.mkdir(parents=True)
    (path / 'estimates.json').write_text(json.dumps({'mean': {
        'point_estimate': 100, 'standard_error': 0.05,
        'confidence_interval': {'lower_bound': 99, 'upper_bound': 101}}}))
    counters = {name: 0 for name in ('reads', 'writes', 'lists', 'readBodyBytes',
        'writeBodyBytes', 'coordinatorSubmissions', 'coordinatorRounds')}
    row = {'name': case, 'transactions': 30,
        **{window: counters for window in ('workload', 'shutdown', 'combined')}}
    print('diagnostic-costs: ' + json.dumps({'schemaVersion': 1, 'cases': [row]}))
with (root / 'order.jsonl').open('a') as log:
    log.write(json.dumps([repetition, case, side]) + '\\n')
"""
            )
            fixture.chmod(0o755)
            manifest = {
                "schemaVersion": 1,
                "base": "base",
                "candidate": "candidate",
                "cases": ["a", "b"],
                "repetitions": 3,
                "diagnosticRepetitions": 4,
                "runtimeLimitSeconds": 30,
                "warnings": [],
                "mixedArgs": [],
                **{
                    side: {"diagnostics": str(fixture), "perfbench": str(fixture)}
                    for side in ("main", "pr")
                },
            }
            perf_compare.measure(root, manifest)
            order = [
                json.loads(line)
                for line in (root / "order.jsonl").read_text().splitlines()
            ]
            diagnostic = order[:16]
            for index in range(0, len(diagnostic), 2):
                first, second = diagnostic[index : index + 2]
                self.assertEqual(first[:2], second[:2])
                self.assertNotEqual(first[2], second[2])
            for case in ("a", "b"):
                first_sides = [
                    diagnostic[i][2]
                    for i in range(0, 16, 2)
                    if diagnostic[i][1] == case
                ]
                self.assertEqual(first_sides.count("main"), 2)
                self.assertEqual(first_sides.count("pr"), 2)
            self.assertEqual(len(order[16:]), 6)
            for side in ("main", "pr"):
                for repetition in range(1, 5):
                    costs = perf_compare.perf_report.read_costs(
                        root / side / f"{repetition:02d}" / "criterion.log"
                    )
                    self.assertEqual(
                        [row["name"] for row in costs["cases"]], ["a", "b"]
                    )
            self.assertIn("cpuModel", manifest)
            with self.assertRaises(FileExistsError):
                perf_compare.measure(root, manifest)
            self.assertEqual(
                len((root / "order.jsonl").read_text().splitlines()), len(order)
            )


if __name__ == "__main__":
    unittest.main()
