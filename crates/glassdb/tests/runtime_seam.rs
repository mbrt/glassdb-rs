//! Source-level guard for the deterministic runtime seam.
//!
//! Production engine code that participates in simulation should route task
//! execution, time, and host I/O through a simulation-aware abstraction,
//! otherwise the deterministic executor can be bypassed without an obvious
//! test failure. Tokio synchronization and future-composition macros are
//! runtime-agnostic and remain usable directly.
//!
//! The two time seams are not interchangeable, and this file can only guard the
//! coarser half of that rule. `rt::Instant` is monotonic and always tracks the
//! active clock (virtual under the deterministic executor, tokio's possibly
//! paused clock otherwise), so it is the seam for *elapsed* time: timeouts,
//! deadlines, and retry budgets. `Clock` is a wall clock, needed only to compare
//! against *persisted* timestamps that may come from another node; it is real
//! time unless a caller anchors it, so measuring elapsed time with it silently
//! escapes simulated time. Only the latter misuse is greppable here — spending a
//! wall-clock budget while retries advance virtual time needs a behavioral test
//! that exercises the timeout under the default (real) clock.

use std::path::{Path, PathBuf};

// Module prefixes are intentional: importing or calling any API from these
// runtime-coupled surfaces needs an explicit simulation-aware design.
const FORBIDDEN: &[&str] = &[
    // Executor entry points, task scheduling, and runtime construction.
    "tokio::spawn",
    "tokio::task",
    "tokio::runtime",
    "tokio::main",
    "tokio::test",
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
    // Native execution and host I/O must sit behind an explicit seam too.
    "std::thread",
    "thread::spawn(",
    "thread::Builder",
    "thread::sleep(",
    "std::fs",
    "std::os::unix::fs",
    "rustix::fs",
    "std::net",
    "std::process",
    // Wall-clock time must use the clock/runtime seam as well.
    "SystemTime::now(",
    "std::time::SystemTime::now(",
    "std::time::Instant::now(",
    // An unanchored wall clock ignores simulated time, so engine code must take
    // the one its caller configured instead of minting a real clock in place.
    "Clock::real(",
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
        || (path.ends_with("crates/glassdb-concurr/src/exec.rs")
            && matches!(pattern, "tokio::task" | "tokio::runtime"))
        || (path.ends_with("crates/glassdb-storage/src/disk_cache/file_media.rs")
            && matches!(pattern, "std::fs" | "std::os::unix::fs" | "rustix::fs"))
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

    for forbidden in [
        "tokio::task::spawn_blocking",
        "tokio::task::block_in_place",
        "tokio::task::yield_now",
        "tokio::runtime::Handle",
        "tokio::time::sleep",
        "tokio::fs::read",
        "tokio::io::AsyncReadExt",
        "tokio::net::TcpStream",
        "tokio::process::Command",
        "tokio::signal::ctrl_c",
    ] {
        assert!(
            FORBIDDEN
                .iter()
                .any(|pattern| forbidden.starts_with(pattern)),
            "`{forbidden}` escaped the forbidden Tokio inventory"
        );
    }
}

#[test]
fn sim_controlled_code_uses_only_reviewed_runtime_apis() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let roots = [
        workspace.join("crates/glassdb/src"),
        workspace.join("crates/glassdb-backend/src"),
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

#[test]
fn synthetic_s3_time_uses_the_model_time_seam() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let files = [
        workspace.join("crates/glassdb-backend-s3/src/lib.rs"),
        workspace.join("crates/glassdb-backend-s3/src/fake_server.rs"),
    ];
    let timing = [
        "tokio::time",
        "SystemTime::now(",
        "std::time::SystemTime::now(",
        "std::time::Instant::now(",
    ];
    let mut violations = Vec::new();
    for path in files {
        let contents = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!("read source file {}: {e}", path.display());
        });
        for (idx, line) in contents.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            if let Some(pattern) = timing.iter().find(|pattern| trimmed.contains(**pattern)) {
                let rel = path.strip_prefix(&workspace).unwrap_or(&path);
                violations.push(format!(
                    "{}:{} contains `{pattern}`",
                    rel.display(),
                    idx + 1
                ));
            }
        }
    }
    assert!(
        violations.is_empty(),
        "synthetic S3 timing must use process-wide model time:\n{}",
        violations.join("\n")
    );
}
