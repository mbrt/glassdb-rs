//! Source-level guard for the simulation-test selection rule.
//!
//! `make test-sim` filters full test names by `sim_test`. Tests in source roots
//! marked with `#![cfg(sim)]` must therefore be inside `mod sim_tests`.

pub mod integration_support;

use std::path::{Component, Path, PathBuf};

use integration_support::rust_sources::{self, RustSource};
use syn::parse::Parser;
use syn::punctuated::Punctuated;
use syn::{Attribute, File, Item, Meta, Token};

struct Source {
    path: PathBuf,
    syntax: File,
}

struct SimulationRoot {
    source: PathBuf,
    children: PathBuf,
}

impl SimulationRoot {
    fn from_source(path: &Path) -> Self {
        let stem = path.file_stem().and_then(|stem| stem.to_str());
        let children = if matches!(stem, Some("lib") | Some("main") | Some("mod")) {
            path.parent()
                .expect("source root has a parent")
                .to_path_buf()
        } else {
            path.with_extension("")
        };
        Self {
            source: path.to_path_buf(),
            children,
        }
    }

    fn contains(&self, path: &Path) -> bool {
        path == self.source || path.starts_with(&self.children)
    }
}

fn path_parts(path: &Path) -> Vec<&str> {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .collect()
}

fn selected_by_path(path: &Path) -> bool {
    path.file_stem().is_some_and(|stem| stem == "sim_tests")
        || path_parts(path).contains(&"sim_tests")
}

fn cfg_requires(meta: &Meta, name: &str) -> bool {
    match meta {
        Meta::Path(path) => path.is_ident(name),
        Meta::NameValue(_) => false,
        Meta::List(list) => {
            let nested = Punctuated::<Meta, Token![,]>::parse_terminated
                .parse2(list.tokens.clone())
                .unwrap_or_else(|error| panic!("parse cfg condition: {error}"));
            if list.path.is_ident("all") {
                nested.iter().any(|meta| cfg_requires(meta, name))
            } else if list.path.is_ident("any") {
                !nested.is_empty() && nested.iter().all(|meta| cfg_requires(meta, name))
            } else {
                false
            }
        }
    }
}

fn attributes_require(attributes: &[Attribute], name: &str) -> bool {
    attributes
        .iter()
        .filter(|attribute| attribute.path().is_ident("cfg"))
        .any(|attribute| {
            let Meta::List(list) = &attribute.meta else {
                return false;
            };
            let condition = syn::parse2::<Meta>(list.tokens.clone())
                .unwrap_or_else(|error| panic!("parse cfg attribute: {error}"));
            cfg_requires(&condition, name)
        })
}

fn is_test(attributes: &[Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        attribute
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "test")
    })
}

struct TestScanner<'a> {
    path: &'a Path,
    violations: &'a mut Vec<String>,
}

impl TestScanner<'_> {
    fn scan(&mut self, items: &[Item], sim_only: bool, selected: bool) {
        for item in items {
            match item {
                Item::Fn(function)
                    if is_test(&function.attrs)
                        && (sim_only || attributes_require(&function.attrs, "sim")) =>
                {
                    if !selected {
                        self.violations.push(format!(
                            "{}:{}: simulation test `{}` must be inside `mod sim_tests`",
                            self.path.display(),
                            function.sig.ident.span().start().line,
                            function.sig.ident,
                        ));
                    }
                }
                Item::Mod(module) => {
                    let module_sim_only = sim_only || attributes_require(&module.attrs, "sim");
                    let module_selected = selected || module.ident == "sim_tests";
                    if let Some((_, items)) = &module.content {
                        self.scan(items, module_sim_only, module_selected);
                    } else if module_sim_only
                        && attributes_require(&module.attrs, "test")
                        && !module_selected
                    {
                        self.violations.push(format!(
                            "{}:{}: simulation test module `{}` must be named `sim_tests`",
                            self.path.display(),
                            module.ident.span().start().line,
                            module.ident,
                        ));
                    }
                }
                _ => {}
            }
        }
    }
}

fn scan_source(source: &Source, roots: &[SimulationRoot]) -> Vec<String> {
    let sim_only = roots.iter().any(|root| root.contains(&source.path));
    let mut violations = Vec::new();
    TestScanner {
        path: &source.path,
        violations: &mut violations,
    }
    .scan(
        &source.syntax.items,
        sim_only,
        selected_by_path(&source.path),
    );
    violations
}

fn parse_source(source: RustSource) -> Source {
    let syntax = syn::parse_file(&source.text)
        .unwrap_or_else(|error| panic!("parse source file {}: {error}", source.path.display()));
    Source {
        path: source.path,
        syntax,
    }
}

#[test]
fn selection_policy_rejects_an_unmatched_simulation_test() {
    fn violations(source: &str, sim_only: bool) -> Vec<String> {
        let syntax = syn::parse_file(source).expect("parse test source");
        let mut violations = Vec::new();
        TestScanner {
            path: Path::new("test.rs"),
            violations: &mut violations,
        }
        .scan(&syntax.items, sim_only, false);
        violations
    }

    assert_eq!(violations("#[test] fn omitted() {}", true).len(), 1);
    assert_eq!(
        violations("#[cfg(sim)] #[test] fn omitted() {}", false).len(),
        1
    );
    assert!(violations("mod sim_tests { #[test] fn selected() {} }", true).is_empty());
}

#[test]
fn simulation_roots_follow_rust_module_paths() {
    let module = SimulationRoot::from_source(Path::new("src/exec.rs"));
    assert!(module.contains(Path::new("src/exec/executor.rs")));
    assert!(!module.contains(Path::new("src/executor.rs")));

    let directory_module = SimulationRoot::from_source(Path::new("src/sim/mod.rs"));
    assert!(directory_module.contains(Path::new("src/sim/history.rs")));

    let crate_root = SimulationRoot::from_source(Path::new("tests/sim/main.rs"));
    assert!(crate_root.contains(Path::new("tests/sim/sim_tests/api.rs")));
}

#[test]
fn simulation_tests_are_inside_sim_tests_modules() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source_roots = [workspace.join("crates")];
    let sources = rust_sources::collect(&workspace, &source_roots)
        .into_iter()
        .map(parse_source)
        .collect::<Vec<_>>();
    let simulation_roots = sources
        .iter()
        .filter(|source| attributes_require(&source.syntax.attrs, "sim"))
        .map(|source| SimulationRoot::from_source(&source.path))
        .collect::<Vec<_>>();
    let violations = sources
        .iter()
        .flat_map(|source| scan_source(source, &simulation_roots))
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "simulation tests must match the `make test-sim` name filter:\n{}",
        violations.join("\n")
    );
}
