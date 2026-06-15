//! Source-level guard for the deterministic runtime seam.
//!
//! Production engine code that participates in simulation should route task
//! spawning and time through `glassdb_concurr::rt`, otherwise the deterministic
//! executor can be bypassed without an obvious test failure.

use std::path::{Path, PathBuf};

const FORBIDDEN: &[&str] = &[
    "tokio::spawn(",
    "tokio::time::sleep(",
    "tokio::time::Instant",
    "SystemTime::now(",
    "std::time::SystemTime::now(",
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

fn is_allowed_runtime_source(path: &Path) -> bool {
    path.ends_with("crates/glassdb-concurr/src/rt.rs")
        || path.ends_with("crates/glassdb-concurr/src/clock.rs")
}

#[test]
fn sim_controlled_code_uses_runtime_seam_for_spawn_and_time() {
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
        if is_allowed_runtime_source(&path) {
            continue;
        }
        let contents = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!("read source file {}: {e}", path.display());
        });
        for (idx, line) in contents.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("#[cfg(test)]") {
                break;
            }
            if trimmed.starts_with("//") {
                continue;
            }
            if let Some(pattern) = FORBIDDEN.iter().find(|pattern| trimmed.contains(**pattern)) {
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
        "sim-controlled code must use glassdb_concurr::rt for spawn/time:\n{}",
        violations.join("\n")
    );
}
