from __future__ import annotations

from pathlib import Path
import subprocess
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


if __name__ == "__main__":
    unittest.main()
