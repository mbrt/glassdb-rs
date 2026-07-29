//! Source-level guard for user transaction bodies.
//!
//! A transaction body can observe reads that validation later finds
//! inconsistent. It must return an error for read-derived failures so GlassDB
//! can retry that attempt; panicking would bypass validation.

use std::path::{Component, Path, PathBuf};

use proc_macro2::Span;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Expr, ExprCall, ExprClosure, ExprMethodCall, Macro};

const FORBIDDEN_MACROS: &[&str] = &[
    "assert",
    "assert_eq",
    "assert_matches",
    "assert_ne",
    "debug_assert",
    "debug_assert_eq",
    "debug_assert_matches",
    "debug_assert_ne",
    "panic",
    "todo",
    "unimplemented",
    "unreachable",
];

const FORBIDDEN_METHODS: &[&str] = &["expect", "expect_err", "unwrap", "unwrap_err"];

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|error| {
        panic!("read source directory {}: {error}", dir.display());
    }) {
        let path = entry.expect("read source entry").path();
        if path.is_dir() {
            if path
                .file_name()
                .is_some_and(|name| matches!(name.to_str(), Some(".git" | "target")))
            {
                continue;
            }
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

fn is_user_workload_source(path: &Path) -> bool {
    let components: Vec<_> = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => value.to_str(),
            _ => None,
        })
        .collect();

    path.starts_with("fuzz")
        || path.starts_with("crates/glassdb/src/sim")
        || components
            .iter()
            .any(|component| matches!(*component, "tests" | "benches"))
        || components
            .windows(2)
            .any(|pair| matches!(pair, ["src", "bin"]))
}

struct ForbiddenConstructs<'a> {
    path: &'a Path,
    violations: &'a mut Vec<String>,
}

impl ForbiddenConstructs<'_> {
    fn record(&mut self, span: Span, construct: &str) {
        self.violations.push(format!(
            "{}:{} contains `{construct}` in a transaction body",
            self.path.display(),
            span.start().line
        ));
    }
}

impl<'ast> Visit<'ast> for ForbiddenConstructs<'_> {
    fn visit_macro(&mut self, node: &'ast Macro) {
        if let Some(name) = node
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            && FORBIDDEN_MACROS.contains(&name.as_str())
        {
            self.record(node.span(), &format!("{name}!"));
        }
        visit::visit_macro(self, node);
    }

    fn visit_expr_method_call(&mut self, node: &'ast ExprMethodCall) {
        let name = node.method.to_string();
        if FORBIDDEN_METHODS.contains(&name.as_str()) {
            self.record(node.span(), &format!(".{name}()"));
        }
        visit::visit_expr_method_call(self, node);
    }

    fn visit_expr_call(&mut self, node: &'ast ExprCall) {
        if let Expr::Path(path) = node.func.as_ref()
            && path
                .path
                .segments
                .last()
                .is_some_and(|segment| segment.ident == "panic_any")
        {
            self.record(node.span(), "panic_any()");
        }
        visit::visit_expr_call(self, node);
    }
}

struct TransactionClosure<'a> {
    path: &'a Path,
    found: bool,
    violations: &'a mut Vec<String>,
}

impl<'ast> Visit<'ast> for TransactionClosure<'_> {
    fn visit_expr_closure(&mut self, node: &'ast ExprClosure) {
        self.found = true;
        ForbiddenConstructs {
            path: self.path,
            violations: self.violations,
        }
        .visit_expr(&node.body);
    }
}

struct TransactionCalls<'a> {
    path: &'a Path,
    inspected: &'a mut usize,
    violations: &'a mut Vec<String>,
}

impl<'ast> Visit<'ast> for TransactionCalls<'_> {
    fn visit_expr_method_call(&mut self, node: &'ast ExprMethodCall) {
        if node.method == "tx"
            && let Some(body) = node.args.first()
        {
            let mut closure = TransactionClosure {
                path: self.path,
                found: false,
                violations: self.violations,
            };
            closure.visit_expr(body);
            if closure.found {
                *self.inspected += 1;
            } else {
                self.violations.push(format!(
                    "{}:{} passes a non-closure transaction body, which the policy cannot inspect",
                    self.path.display(),
                    node.span().start().line
                ));
            }
        }
        visit::visit_expr_method_call(self, node);
    }
}

fn inspect_source(path: &Path, contents: &str) -> (usize, Vec<String>) {
    let syntax = syn::parse_file(contents).unwrap_or_else(|error| {
        panic!("parse source file {}: {error}", path.display());
    });
    let mut inspected = 0;
    let mut violations = Vec::new();
    TransactionCalls {
        path,
        inspected: &mut inspected,
        violations: &mut violations,
    }
    .visit_file(&syntax);
    (inspected, violations)
}

#[test]
fn policy_recognizes_panicking_transaction_bodies() {
    let source = r#"
        async fn workload(db: &Database) {
            db.tx(|tx| async move {
                let value = tx.read().await?.unwrap();
                assert!(value);
                Ok(())
            }).await;
        }
    "#;

    let (inspected, violations) = inspect_source(Path::new("policy-self-test.rs"), source);
    assert_eq!(inspected, 1);
    assert_eq!(violations.len(), 2);
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains("assert!"))
    );
    assert!(
        violations
            .iter()
            .any(|violation| violation.contains(".unwrap()"))
    );
}

#[test]
fn user_transaction_bodies_return_errors_instead_of_panicking() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    collect_rs_files(&workspace.join("crates"), &mut files);
    collect_rs_files(&workspace.join("fuzz"), &mut files);
    files.sort();

    let mut inspected = 0;
    let mut violations = Vec::new();
    for path in files {
        let relative = path.strip_prefix(&workspace).unwrap_or(&path);
        if !is_user_workload_source(relative) {
            continue;
        }
        let contents = std::fs::read_to_string(&path).unwrap_or_else(|error| {
            panic!("read source file {}: {error}", path.display());
        });
        let (file_inspected, mut file_violations) = inspect_source(relative, &contents);
        inspected += file_inspected;
        violations.append(&mut file_violations);
    }

    assert!(
        inspected > 0,
        "transaction-body policy inspected no closures"
    );
    assert!(
        violations.is_empty(),
        "transaction bodies must return errors so their reads are validated:\n{}",
        violations.join("\n")
    );
}
