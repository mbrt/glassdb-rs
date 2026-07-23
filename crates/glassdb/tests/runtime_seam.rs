//! Source-level guard for the deterministic runtime seam.
//!
//! Production engine code that participates in simulation should route task
//! execution, time, and host I/O through a simulation-aware abstraction,
//! otherwise the deterministic executor can be bypassed without an obvious
//! test failure. Tokio synchronization and future-composition macros are
//! runtime-agnostic and remain usable directly.

use std::path::{Path, PathBuf};

// Module prefixes are intentional: importing or calling any API from these
// runtime-coupled surfaces needs an explicit simulation-aware design.
const FORBIDDEN: &[&str] = &[
    // Executor entry points, task scheduling, and runtime construction.
    "tokio::spawn",
    "tokio::task",
    "tokio::runtime",
    "tokio::main",
    // Clocks, timers, intervals, and deadlines.
    "tokio::time",
    // Reactor- or blocking-pool-backed host I/O.
    "tokio::fs",
    "tokio::io",
    "tokio::net",
    "tokio::process",
    "tokio::signal",
    // This lock is prohibited by the repository's concurrency policy.
    "tokio::sync::Mutex",
    // Wall-clock time must use the clock/runtime seam as well.
    "SystemTime::now(",
    "std::time::SystemTime::now(",
];

const ALLOWED_TOKIO: &[&str] = &[
    "tokio::sync",
    "tokio::select!",
    "tokio::join!",
    "tokio::try_join!",
    "tokio::pin!",
];

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| {
        panic!("read source dir {}: {e}", dir.display());
    }) {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "tests") {
                continue;
            }
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

fn is_allowed_runtime_use(path: &Path, pattern: &str) -> bool {
    path.ends_with("crates/glassdb-concurr/src/rt.rs")
        || (path.ends_with("crates/glassdb-concurr/src/clock.rs")
            && pattern.contains("SystemTime::now("))
        || (path.ends_with("crates/glassdb-concurr/src/exec.rs")
            && matches!(pattern, "tokio::task" | "tokio::runtime"))
}

fn unclassified_tokio_use(line: &str) -> Option<&str> {
    line.match_indices("tokio::")
        .map(|(index, _)| &line[index..])
        .find(|usage| {
            !FORBIDDEN.iter().any(|pattern| usage.starts_with(pattern))
                && !ALLOWED_TOKIO
                    .iter()
                    .any(|allowed| usage.starts_with(allowed))
        })
}

#[test]
fn unreviewed_tokio_apis_are_forbidden_by_default() {
    for allowed in ALLOWED_TOKIO {
        assert_eq!(unclassified_tokio_use(allowed), None);
    }
    assert_eq!(
        unclassified_tokio_use("use tokio::{spawn, sync::Notify};"),
        Some("tokio::{spawn, sync::Notify};")
    );
    assert_eq!(
        unclassified_tokio_use("tokio::future_runtime_api()"),
        Some("tokio::future_runtime_api()")
    );
}

#[test]
fn sim_controlled_code_uses_only_reviewed_runtime_apis() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let roots = [
        workspace.join("crates/glassdb/src"),
        workspace.join("crates/glassdb-trans/src"),
        workspace.join("crates/glassdb-storage/src"),
        workspace.join("crates/glassdb-concurr/src"),
    ];

    let mut files = Vec::new();
    for root in roots {
        collect_rs_files(&root, &mut files);
    }

    let mut violations = Vec::new();
    for path in files {
        let contents = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!("read source file {}: {e}", path.display());
        });
        let mut test_attribute = false;
        for (idx, line) in contents.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed == "#[cfg(test)]" {
                test_attribute = true;
                continue;
            }
            if test_attribute && trimmed.starts_with("#[") {
                continue;
            }
            if test_attribute && trimmed.starts_with("mod tests") {
                break;
            }
            test_attribute = false;
            if trimmed.starts_with("//") {
                continue;
            }
            if let Some(pattern) = FORBIDDEN.iter().find(|pattern| trimmed.contains(**pattern)) {
                if is_allowed_runtime_use(&path, pattern) {
                    continue;
                }
                let rel = path.strip_prefix(&workspace).unwrap_or(&path);
                violations.push(format!(
                    "{}:{} contains `{pattern}`",
                    rel.display(),
                    idx + 1
                ));
            } else if let Some(usage) = unclassified_tokio_use(trimmed)
                && !is_allowed_runtime_use(&path, usage)
            {
                let rel = path.strip_prefix(&workspace).unwrap_or(&path);
                violations.push(format!(
                    "{}:{} contains unclassified `{usage}`",
                    rel.display(),
                    idx + 1
                ));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "sim-controlled code must use simulation-aware runtime/I/O seams:\n{}",
        violations.join("\n")
    );
}
