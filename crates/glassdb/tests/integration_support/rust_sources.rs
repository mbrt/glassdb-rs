use std::path::{Path, PathBuf};

/// A Rust source file loaded from the workspace.
pub struct RustSource {
    /// The path relative to the workspace root.
    pub path: PathBuf,
    /// The source text.
    pub text: String,
}

/// Loads the Rust source files below the supplied workspace paths.
pub fn collect(workspace: &Path, roots: &[PathBuf]) -> Vec<RustSource> {
    let mut paths = Vec::new();
    for root in roots {
        collect_paths(root, &mut paths);
    }
    paths.sort_unstable();
    paths.dedup();

    paths
        .into_iter()
        .map(|path| {
            let text = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read source file {}: {error}", path.display()));
            let relative = path
                .strip_prefix(workspace)
                .unwrap_or_else(|_| {
                    panic!(
                        "source file {} is outside workspace {}",
                        path.display(),
                        workspace.display()
                    )
                })
                .to_path_buf();
            RustSource {
                path: relative,
                text,
            }
        })
        .collect()
}

fn collect_paths(path: &Path, paths: &mut Vec<PathBuf>) {
    if path.is_dir() {
        if path
            .file_name()
            .is_some_and(|name| name == ".git" || name == "target")
        {
            return;
        }
        for entry in std::fs::read_dir(path)
            .unwrap_or_else(|error| panic!("read source directory {}: {error}", path.display()))
        {
            collect_paths(&entry.expect("read source entry").path(), paths);
        }
    } else if path.extension().is_some_and(|extension| extension == "rs") {
        paths.push(path.to_path_buf());
    }
}
