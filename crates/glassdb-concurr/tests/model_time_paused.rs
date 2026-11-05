#![cfg(not(sim))]

use std::time::Duration;

use glassdb_concurr::rt;

// The paused clock of tokio rounds each timer up to a whole millisecond, so a
// model wait shorter than that must not round with it.
#[tokio::test(start_paused = true)]
async fn paused_model_time_resolves_waits_below_a_millisecond() {
    rt::set_paused_model_time().unwrap();

    for wait in [
        Duration::from_micros(1),
        Duration::from_micros(250),
        Duration::from_micros(1500),
        Duration::from_secs(5),
    ] {
        let start = rt::Instant::now();
        rt::sleep(wait).await;
        assert_eq!(start.elapsed(), wait);
    }

    let start = rt::Instant::now();
    assert_eq!(
        rt::timeout(Duration::from_micros(700), std::future::pending::<()>()).await,
        Err(rt::TimedOut)
    );
    assert_eq!(start.elapsed(), Duration::from_micros(700));
}
