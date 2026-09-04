//! Source-level guard for the deterministic runtime seam.
//!
//! Production engine code that participates in simulation should route task
//! execution, time, and host I/O through a simulation-aware abstraction,
//! otherwise the deterministic executor can be bypassed without an obvious
//! test failure. Tokio synchronization and future-composition macros are
//! runtime-agnostic and remain usable directly.
//!
//! Runtime time has two representations with distinct purposes. `rt::Instant`
//! is monotonic and measures elapsed time for deadlines and retry budgets;
//! `rt::system_now` supplies wall timestamps for comparisons with persisted
//! state. Both follow the runtime's active model-time domain. Raw clocks bypass
//! that domain and can silently escape simulated or accelerated time.

pub mod integration_support;

use std::path::{Path, PathBuf};

use glob::Pattern;
use integration_support::rust_sources;

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
    // This lock is prohibited as it has non-obvious correctness issues.
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
];

const ALLOWED_TOKIO: &[&str] = &[
    "tokio::sync",
    "tokio::select!",
    "tokio::join!",
    "tokio::try_join!",
    "tokio::pin!",
];

const EXEMPT_SEAM_GLOBS: &[&str] = &[
    "crates/glassdb-concurr/src/rt.rs",
    "crates/glassdb-concurr/src/rt/*.rs",
    "crates/glassdb-concurr/src/exec/executor.rs",
    "crates/glassdb-storage/src/disk_cache/file_media.rs",
];

// The source scanner recognizes inline `#[cfg(test)] mod tests` blocks, but an
// out-of-line test module has no distinguishing syntax in its own file. Keep
// this list exact so a production file named `tests.rs` cannot evade the guard.
const OUT_OF_LINE_TEST_MODULES: &[&str] = &["crates/glassdb-trans/src/algo/direct_commit/tests.rs"];

fn is_exempt_seam_file(path: &Path) -> bool {
    EXEMPT_SEAM_GLOBS.iter().any(|glob| {
        Pattern::new(glob)
            .expect("valid exempt seam glob")
            .matches_path(path)
    })
}

fn is_out_of_line_test_module(path: &Path) -> bool {
    OUT_OF_LINE_TEST_MODULES
        .iter()
        .any(|test_module| path == Path::new(test_module))
}

fn is_in_test_directory(path: &Path) -> bool {
    path.components()
        .any(|component| component.as_os_str() == "tests")
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

    let sources = rust_sources::collect(&workspace, &roots);

    let mut violations = Vec::new();
    for source in sources {
        if is_in_test_directory(&source.path)
            || is_exempt_seam_file(&source.path)
            || is_out_of_line_test_module(&source.path)
        {
            continue;
        }
        let lines: Vec<_> = source.text.lines().collect();
        let mut test_attribute = false;
        for (idx, line) in lines.iter().copied().enumerate() {
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
                violations.push(format!(
                    "{}:{} contains `{pattern}`",
                    source.path.display(),
                    idx + 1
                ));
            } else if let Some(usage) = unclassified_tokio_use(trimmed) {
                violations.push(format!(
                    "{}:{} contains unclassified `{usage}`",
                    source.path.display(),
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
    let roots = vec![
        workspace.join("crates/glassdb-backend-s3/src/lib.rs"),
        workspace.join("crates/glassdb-backend-s3/src/fake_server.rs"),
        workspace.join("crates/glassdb-backend-s3/src/fake_server"),
    ];
    let sources = rust_sources::collect(&workspace, &roots);
    let timing = [
        "tokio::time",
        "SystemTime::now(",
        "std::time::SystemTime::now(",
        "std::time::Instant::now(",
    ];
    let mut violations = Vec::new();
    for source in sources {
        let lines: Vec<_> = source.text.lines().collect();
        for (idx, line) in lines.iter().copied().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            if let Some(pattern) = timing.iter().find(|pattern| trimmed.contains(**pattern)) {
                violations.push(format!(
                    "{}:{} contains `{pattern}`",
                    source.path.display(),
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
