//! Source-level guard for user transaction bodies.
//!
//! A transaction body can observe reads that validation later finds
//! inconsistent. It must return an error for read-derived failures so GlassDB
//! can retry that attempt. Panics bypass validation and replay even though
//! framework-owned attempt resources are retired safely.

pub mod integration_support;

use std::path::{Component, Path, PathBuf};

use integration_support::rust_sources;
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
        || path.file_name().is_some_and(|name| name == "tests.rs")
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
    assert!(is_user_workload_source(Path::new(
        "crates/example/src/component/tests.rs"
    )));

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
    let roots = [workspace.join("crates"), workspace.join("fuzz")];
    let sources = rust_sources::collect(&workspace, &roots);

    let mut inspected = 0;
    let mut violations = Vec::new();
    for source in sources {
        if !is_user_workload_source(&source.path) {
            continue;
        }
        let (file_inspected, mut file_violations) = inspect_source(&source.path, &source.text);
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
