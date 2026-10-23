#![cfg(not(sim))]

use std::process::{Command, Output};

use serde_json::Value;

const MEMORY_MODEL: [&str; 5] = [
    "--backend=memory",
    "--delays=s3",
    "--latency-jitter=false",
    "--prefix-depth=3",
    "--progress=never",
];

fn perfbench(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_perfbench"))
        .args(arguments)
        .output()
        .unwrap()
}

fn virtual_suite_report() -> Value {
    let output =
        perfbench(&[&MEMORY_MODEL[..], &["--time=virtual", "--experiment=suite"]].concat());
    assert!(output.status.success(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

/// The cases whose persisted records carry a timestamp. The anchor of the wall
/// clock is the start time of the process, so the size of these records, and
/// with it the delay of their writes, moves by under 1% between processes.
const TIMESTAMPED_CASES: [&str; 2] = ["rmw_external_1025", "rmw_five_leaves"];

/// The numbers that a comparison of two revisions reads.
fn measured(report: &Value) -> Vec<Value> {
    report["runs"][0]["cells"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|cell| {
            let case = cell["labels"]["case"].as_str().unwrap();
            !TIMESTAMPED_CASES.contains(&case)
        })
        .flat_map(|cell| cell["shapes"].as_array().unwrap())
        .map(|shape| {
            ["shape", "committed", "meanMs", "p50Ms", "p90Ms", "txPerSec"]
                .map(|field| shape[field].clone())
                .into()
        })
        .collect()
}

// A comparison that needs repeated runs to average out noise would lose the
// reason to use virtual time.
#[test]
fn virtual_runs_repeat_exactly() {
    let first = virtual_suite_report();
    let second = virtual_suite_report();

    assert_eq!(first["runs"][0]["cells"].as_array().unwrap().len(), 9);
    assert_eq!(first["timeMode"], "virtual");
    assert_eq!(first["modelTimeSpeedup"], 1.0);
    assert_eq!(measured(&first), measured(&second));
}

#[test]
fn virtual_time_rejects_what_it_cannot_skip() {
    for (arguments, message) in [
        (
            ["--backend=fakes3", "--time=virtual"].as_slice(),
            "needs --backend=memory",
        ),
        (
            ["--time=virtual", "--delay-scale=0.2"].as_slice(),
            "does not combine with --delay-scale",
        ),
    ] {
        let output = perfbench(arguments);
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(message), "{stderr}");
    }
}
