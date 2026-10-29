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

/// The experiments that CI compares.
const DEFAULT_EXPERIMENTS: &str = concat!(
    "--experiment=",
    env!("CARGO_MANIFEST_DIR"),
    "/../../hack/perf/experiments/default"
);

fn perfbench(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_perfbench"))
        .args(arguments)
        .output()
        .unwrap()
}

fn virtual_default_report() -> Value {
    let output = perfbench(&[&MEMORY_MODEL[..], &["--time=virtual", DEFAULT_EXPERIMENTS]].concat());
    assert!(output.status.success(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

/// The experiments whose persisted records carry a timestamp. The anchor of the wall
/// clock is the start time of the process, so the size of these records, and
/// with it the delay of their writes, moves by under 1% between processes.
const TIMESTAMPED_EXPERIMENTS: [&str; 2] = ["rmw_external_1025", "rmw_five_leaves"];

/// The numbers that a comparison of two revisions reads.
fn measured(report: &Value) -> Vec<Value> {
    report["experiments"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|experiment| {
            let name = experiment["name"].as_str().unwrap();
            !TIMESTAMPED_EXPERIMENTS.contains(&name)
        })
        .flat_map(|experiment| experiment["runs"][0]["cells"].as_array().unwrap())
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
    let first = virtual_default_report();
    let second = virtual_default_report();

    assert_eq!(first["experiments"].as_array().unwrap().len(), 9);
    assert_eq!(first["timeMode"], "virtual");
    assert_eq!(first["modelTimeSpeedup"], 1.0);
    assert_eq!(measured(&first), measured(&second));
}

#[test]
fn virtual_time_rejects_what_it_cannot_skip() {
    for (arguments, message) in [
        (
            ["--backend=fakes3", "--time=virtual", DEFAULT_EXPERIMENTS].as_slice(),
            "needs --backend=memory",
        ),
        (
            ["--time=virtual", "--delay-scale=0.2", DEFAULT_EXPERIMENTS].as_slice(),
            "does not combine with --delay-scale",
        ),
    ] {
        let output = perfbench(arguments);
        assert!(!output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(message), "{stderr}");
    }
}

#[test]
fn a_failed_experiment_keeps_the_results_of_the_experiments_before_it() {
    let dir = std::env::temp_dir().join(format!("perfbench-partial-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let default = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../hack/perf/experiments/default"
    );
    let first = format!("--experiment={default}/warm_read.toml");
    // A pass condition needs a sequence source, and the runner finds out only
    // when the cell starts.
    let failing = dir.join("no_passes.toml");
    std::fs::write(
        &failing,
        format!(
            "include = [\"{default}/../../configs/default.toml\"]\n\
             [cell]\nworkload = \"diag-read\"\n[cell.measure]\nuntil = \"passes\"\n"
        ),
    )
    .unwrap();
    let output = dir.join("result.json");
    let result = perfbench(
        &[
            &MEMORY_MODEL[..],
            &[
                "--time=virtual",
                &first,
                &format!("--experiment={}", failing.display()),
                &format!("--output={}", output.display()),
            ],
        ]
        .concat(),
    );

    assert!(!result.status.success());
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("no_passes: the end condition needs passes"),
        "{stderr}"
    );
    let report: Value = serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
    let names: Vec<_> = report["experiments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|experiment| experiment["name"].clone())
        .collect();
    assert_eq!(names, ["warm_read"]);
    std::fs::remove_dir_all(&dir).unwrap();
}
