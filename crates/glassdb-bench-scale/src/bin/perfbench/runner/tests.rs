//! Runs of small experiments on the memory backend without delays.

use std::error::Error;
use std::time::Duration;

use glassdb::middleware::{DelayOptions, ProviderLatencyProfile, RateLimit, WriteRateLimits};

use super::config::Selection;
use super::result::RunResult;
use crate::Execution;
use crate::backend::Factory;

fn factory() -> Factory {
    Factory::Memory(DelayOptions {
        latency: ProviderLatencyProfile::zero(),
        rate_limits: WriteRateLimits {
            same_obj_write_ps: RateLimit::Unlimited,
            same_obj_write_retry_delay: Duration::ZERO,
            prefix_read_ps: RateLimit::Unlimited,
            prefix_write_ps: RateLimit::Unlimited,
            prefix_depth: 0,
        },
    })
}

/// Runs `experiment` with the values of `set`, and returns its cells as JSON.
fn run(experiment: &str, set: &[&str]) -> Result<Vec<serde_json::Value>, Box<dyn Error>> {
    let resolved = Selection {
        experiment: experiment.to_string(),
        set: set.iter().map(|s| s.to_string()).collect(),
        ..Selection::default()
    }
    .resolve()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let execution = Execution {
        runs: 1,
        run_cooldown: Duration::ZERO,
        drain_timeout: Duration::from_secs(10),
    };
    let runs: Vec<RunResult> = super::run(&runtime, &factory(), &resolved, execution)?;
    let json = serde_json::to_value(&runs)?;
    Ok(json[0]["cells"].as_array().cloned().unwrap_or_default())
}

const SHORT: &[&str] = &[
    "seed.quiet-period=\"0s\"",
    "cell.gc=false",
    "cell.warm-up.timeout=\"0s\"",
    "cell.measure.until=\"duration\"",
    "cell.measure.duration=\"200ms\"",
    "cell.measure.max-duration=\"1s\"",
];

#[test]
fn a_continued_cell_keeps_the_tree_and_the_instances_of_the_cell_before()
-> Result<(), Box<dyn Error>> {
    let mut set = SHORT.to_vec();
    set.extend([
        "pools.wide.keys=160",
        "cell.databases=1",
        "cell.workers=2",
        "cell.warm-up.timeout=\"0s\"",
    ]);
    let cells = run("load-shift", &set)?;
    let starts: Vec<_> = cells.iter().map(|cell| cell["start"].clone()).collect();
    assert_eq!(starts, ["seed", "previous", "previous"]);
    let workloads: Vec<_> = cells.iter().map(|cell| cell["workload"].clone()).collect();
    assert_eq!(workloads, ["low-conflict", "crossing", "low-conflict"]);
    for cell in &cells {
        assert!(cell["measure"]["met"].as_bool().unwrap());
        for shape in cell["shapes"].as_array().unwrap() {
            assert!(shape["committed"].as_u64().unwrap() > 0, "{shape}");
        }
    }
    Ok(())
}

#[test]
fn a_pass_condition_ends_the_measurement_of_a_sequence() -> Result<(), Box<dyn Error>> {
    let cells = run("inline-pressure", &[])?;
    let phases: Vec<_> = cells
        .iter()
        .map(|cell| {
            let shape = &cell["shapes"][0];
            (
                cell["labels"]["phase"].as_str().unwrap().to_string(),
                cell["measure"]["met"].as_bool().unwrap(),
                shape["committed"].as_u64().unwrap() % 64,
            )
        })
        .collect();
    // Each phase ends after whole passes of 64 keys.
    assert_eq!(
        phases,
        [
            ("saturation".to_string(), true, 0),
            ("pressure".to_string(), true, 0),
            ("recovery".to_string(), true, 0),
        ]
    );
    // The pass without locks ends the pressure phase outside it, and the
    // recovery phase repeats it.
    let pressure = &cells[1];
    if pressure["shapes"][0]["committed"].as_u64().unwrap() > 0 {
        assert!(pressure["aggregateProtocol"]["lockCalls"].as_u64().unwrap() > 0);
    }
    let recovery = &cells[2];
    assert_eq!(recovery["aggregateProtocol"]["lockCalls"], 0);
    assert_eq!(recovery["shapes"][0]["committed"], 64);
    Ok(())
}

#[test]
fn a_chain_of_cells_shares_the_seed_of_its_largest_cell() -> Result<(), Box<dyn Error>> {
    let mut set = SHORT.to_vec();
    set.extend(["pools.wide.keys=20000", "cell.databases=3"]);
    let resolved = Selection {
        experiment: "workers".to_string(),
        set: set.iter().map(|s| s.to_string()).collect(),
        ..Selection::default()
    }
    .resolve()?;
    let (points, specs) = super::plan(&resolved)?;
    assert_eq!(specs[0].collections, 1);
    assert!(specs.iter().all(|spec| spec.seed_collections == 3));
    assert!(
        points
            .iter()
            .skip(1)
            .all(|point| point.start != super::config::Start::Seed)
    );
    Ok(())
}

#[test]
fn groups_run_their_shapes_on_their_own_instances() -> Result<(), Box<dyn Error>> {
    let mut set = SHORT.to_vec();
    set.push("cell.warm-up.quiet=\"10s\"");
    let cells = run("membership-churn", &set)?;
    let groups: Vec<_> = cells[0]["groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|group| {
            (
                group["group"].clone(),
                group["instances"].as_array().unwrap().len(),
            )
        })
        .collect();
    assert_eq!(
        groups,
        [
            ("direct-churner".into(), 1),
            ("locked-churner".into(), 1),
            ("scanner".into(), 1)
        ]
    );
    let instances = &cells[0]["groups"];
    assert!(
        instances[0]["instances"][0]["directLanded"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(instances[1]["instances"][0]["directLanded"], 0);
    Ok(())
}

#[test]
fn a_negative_quiet_rate_is_an_error() {
    let mut set = SHORT.to_vec();
    set.push("cell.warm-up.quiet-rate=-1.0");
    let error = run("mixed", &set).unwrap_err();
    assert!(error.to_string().contains("quiet-rate"), "{error}");
}
