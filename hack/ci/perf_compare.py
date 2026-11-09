#!/usr/bin/env python3
"""Build perfbench for two revisions with the harness of the candidate.

The CI comparison builds both binaries with this script, runs `make bench`
with each binary, and compares the results with perf_report.py. Only the
engine differs between the two binaries.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

# The experiments, and the shared files that they include. They are part of
# the harness.
EXPERIMENTS = Path("hack/perf/experiments")
CONFIGS = Path("hack/perf/configs")
# The sources that the base revision takes from the harness. The crate must be
# in the base revision; the experiment files can be new.
HARNESS = (Path("crates/glassdb-bench-scale"), CONFIGS, EXPERIMENTS)
# The version of the manifest that the driver writes.
MANIFEST_VERSION = 6


def capture(command: list[str], cwd: Path) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True)


def snapshot(repo: Path, destination: Path, revision: str | None) -> None:
    """Copy the files of `revision`, or of the working tree, to `destination`.

    Each file gets the current time as its modification time. Cargo treats an
    artifact as fresh when it is newer than its sources, so a snapshot that
    kept older times could reuse artifacts that another snapshot built into the
    same target directory.
    """
    destination.mkdir()
    if revision:
        with subprocess.Popen(
            ["git", "archive", revision], cwd=repo, stdout=subprocess.PIPE
        ) as source:
            subprocess.run(
                ["tar", "-x", "-m", "-C", str(destination)],
                stdin=source.stdout,
                check=True,
            )
            if source.wait() != 0:
                raise RuntimeError(f"cannot archive revision {revision}")
    else:
        names = capture(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            repo,
        )
        for name in set(names.split("\0")) - {""}:
            source = repo / name
            if source.is_file() or source.is_symlink():
                target = destination / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(source, target, follow_symlinks=False)


def use_harness(candidate: Path, base: Path) -> str:
    """Copy the benchmark sources, retaining engine dependencies and locks."""
    digest = hashlib.sha256()
    for relative in HARNESS:
        source = candidate / relative
        target = base / relative
        if relative == HARNESS[0] and not target.is_dir():
            raise RuntimeError(f"unsupported comparison: baseline has no {relative}")
        # These directories are disposable snapshots owned by this invocation.
        if target.is_dir():
            shutil.rmtree(target)
        if not source.is_dir():
            continue
        shutil.copytree(source, target)
        for path in sorted(source.rglob("*")):
            if path.is_file():
                digest.update(str(path.relative_to(candidate)).encode())
                digest.update(path.read_bytes())
    return digest.hexdigest()


def build(
    source: Path, target: Path, output: Path, side: str, log: str | None = None
) -> dict:
    result = {}
    for package, selector, name in (("glassdb-bench-scale", "--bin", "perfbench"),):
        command = [
            "cargo",
            "build",
            "--release",
            "--target-dir",
            str(target),
            "-p",
            package,
            selector,
            name,
            "--message-format=json-render-diagnostics",
        ]
        try:
            messages = capture(command, source)
        except subprocess.CalledProcessError as error:
            (output / f"{log or side}-build.jsonl").write_text(error.output or "")
            raise RuntimeError(
                f"unsupported comparison: candidate harness does not build against {side}; see build log"
            ) from error
        executable = None
        for line in messages.splitlines():
            message = json.loads(line)
            if (
                message.get("reason") == "compiler-artifact"
                and message["target"]["name"] == name
            ):
                executable = message.get("executable") or executable
        if executable is None:
            raise RuntimeError(f"no executable for {side}/{name}")
        destination = output / "bin" / f"{side}-{name}"
        destination.parent.mkdir(exist_ok=True)
        shutil.copy2(executable, destination)
        result[name] = str(destination)
    result["lockSha256"] = hashlib.sha256(
        (source / "Cargo.lock").read_bytes()
    ).hexdigest()
    result["executableSha256"] = {
        "perfbench": hashlib.sha256(Path(result["perfbench"]).read_bytes()).hexdigest()
    }
    return result


def prepare(repo: Path, output: Path, base: str, candidate: str | None) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    manifest: dict[str, Any] = {
        "schemaVersion": MANIFEST_VERSION,
        "base": base,
        "candidate": candidate or "working tree",
        "rustc": capture(["rustc", "--version"], repo).strip(),
        "warnings": [],
    }
    manifest["baseCommit"] = capture(
        ["git", "rev-parse", f"{base}^{{commit}}"], repo
    ).strip()
    manifest["candidateCommit"] = capture(
        ["git", "rev-parse", f"{candidate or 'HEAD'}^{{commit}}"], repo
    ).strip()
    with tempfile.TemporaryDirectory(prefix="glassdb-comparison-") as directory:
        snapshots = Path(directory)
        snapshot(repo, snapshots / "main", manifest["baseCommit"])
        snapshot(
            repo, snapshots / "pr", manifest["candidateCommit"] if candidate else None
        )
        manifest["harnessSha256"] = use_harness(snapshots / "pr", snapshots / "main")
        targets = repo / "target/performance"
        try:
            manifest["main"] = build(
                snapshots / "main", targets / "main", output, "main", "main-shared"
            )
        except RuntimeError as error:
            # The candidate harness needs APIs that the base lacks. Measure both
            # engines with the base harness, so that the sides stay comparable.
            print(f"{error}; retrying with the base harness", file=sys.stderr)
            # use_harness replaced the harness of the base snapshot. A new
            # snapshot is also newer than the artifacts of the failed build, so
            # cargo does not reuse the candidate harness that they came from.
            shutil.rmtree(snapshots / "main")
            snapshot(repo, snapshots / "main", manifest["baseCommit"])
            manifest["harnessSha256"] = use_harness(snapshots / "main", snapshots / "pr")
            manifest["harness"] = "base"
            manifest["warnings"].append(
                f"The candidate harness does not build against {base}; "
                "both revisions were measured with the harness of the base."
            )
            manifest["main"] = build(snapshots / "main", targets / "main", output, "main")
        manifest["pr"] = build(snapshots / "pr", targets / "pr", output, "pr")
        # The candidate snapshot has the harness that both sides use, and the
        # measurement reads its experiment files after the snapshots are gone.
        harness = output.resolve() / "harness"
        if not (snapshots / "pr" / EXPERIMENTS).is_dir():
            raise RuntimeError(
                f"unsupported comparison: the harness has no {EXPERIMENTS}"
            )
        for relative in (CONFIGS, EXPERIMENTS):
            if (snapshots / "pr" / relative).is_dir():
                shutil.copytree(snapshots / "pr" / relative, harness / relative)
        manifest["experiments"] = str(harness / EXPERIMENTS)
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="main")
    parser.add_argument("--candidate")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    repo = Path(capture(["git", "rev-parse", "--show-toplevel"], Path.cwd()).strip())
    try:
        prepare(repo, args.output.resolve(), args.base, args.candidate)
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
