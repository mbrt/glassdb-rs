#![cfg(not(sim))]

//! Waits of `rt` when the process does not configure model time.

use std::time::Duration;

use glassdb_concurr::rt;

const WAITS: [Duration; 5] = [
    Duration::from_micros(1),
    Duration::from_millis(1),
    Duration::from_micros(1500),
    Duration::from_micros(4400),
    Duration::from_millis(20),
];

// The tests of other crates use a paused clock and expect the waits of Tokio.
#[tokio::test(start_paused = true)]
async fn paused_clock_waits_as_long_as_tokio() {
    for wait in WAITS {
        let start = tokio::time::Instant::now();
        tokio::time::sleep(wait).await;
        let tokio_wait = start.elapsed();

        let start = tokio::time::Instant::now();
        rt::sleep(wait).await;
        assert_eq!(start.elapsed(), tokio_wait, "sleep of {wait:?}");

        let start = tokio::time::Instant::now();
        assert_eq!(
            rt::timeout(wait, std::future::pending::<()>()).await,
            Err(rt::TimedOut)
        );
        assert_eq!(start.elapsed(), tokio_wait, "timeout of {wait:?}");
    }
}

#[tokio::test(start_paused = true)]
async fn timeout_prefers_a_future_that_is_ready_at_the_deadline() {
    for wait in WAITS {
        let ready = tokio::time::sleep(wait);
        assert_eq!(rt::timeout(wait, ready).await, Ok(()), "wait of {wait:?}");
    }
}

#[tokio::test]
async fn live_clock_never_ends_a_wait_early() {
    for wait in WAITS {
        for _ in 0..20 {
            let start = std::time::Instant::now();
            rt::sleep(wait).await;
            assert!(start.elapsed() >= wait, "sleep of {wait:?}");

            let start = std::time::Instant::now();
            assert_eq!(
                rt::timeout(wait, std::future::pending::<()>()).await,
                Err(rt::TimedOut)
            );
            assert!(start.elapsed() >= wait, "timeout of {wait:?}");
        }
    }
}
