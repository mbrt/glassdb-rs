#![cfg(not(sim))]

use std::time::Duration;

use glassdb_bench_scale::bench::Bench;
use glassdb_concurr::rt;

#[tokio::test(start_paused = true)]
// A run must measure the same model time at each speedup, so that a change of
// the speedup does not change the experiment.
async fn samples_and_stopping_use_model_time() {
    rt::set_model_time_speedup(5.0).unwrap();
    let bench = Bench::new(Duration::from_secs(2));
    let wall_start = tokio::time::Instant::now();
    bench.start();

    bench
        .measure_once(|| async {
            rt::sleep(Duration::from_secs(5)).await;
            Ok::<_, ()>(())
        })
        .await
        .unwrap();
    for _ in 1..10 {
        bench
            .measure_once(|| async { Ok::<_, ()>(()) })
            .await
            .unwrap();
    }

    assert_eq!(wall_start.elapsed(), Duration::from_secs(1));
    assert_eq!(
        bench.results().samples[0],
        Duration::from_secs(5),
        "latency is reported in model time"
    );
    assert!(
        bench.is_finished(),
        "5 s of model time end a window of 2 s, after 1 s of wall time"
    );
    bench.end();
    assert_eq!(bench.results().tot_duration, Duration::from_secs(5));
}
