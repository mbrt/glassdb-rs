//! Runtime indirection seam.
//!
//! Production builds delegate to real `tokio`. Under `--cfg sim`, task spawning
//! and time require the in-repo deterministic executor (`crate::exec`) to be
//! active on the current thread.
//!
//! `tokio::sync` and `tokio::select!` are runtime-agnostic and are used directly
//! elsewhere (non-`biased` selects stay deterministic under sim via the seeded
//! branch-poll RNG; see `crate::exec::block_on_with`).

#[cfg(not(sim))]
mod native;
#[cfg(sim)]
mod sim;

#[cfg(not(sim))]
pub use native::*;
#[cfg(sim)]
pub use sim::*;

/// Error returned when a runtime-seam deadline expires.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("operation timed out")]
pub struct TimedOut;

#[cfg(all(test, not(sim)))]
mod native_tests {
    use std::time::Duration;

    async fn runtime_facade_contract() {
        let sleep_started = super::Instant::now();
        let task = super::spawn(async {
            super::sleep(Duration::from_millis(5)).await;
            41
        });
        assert_eq!(task.await.unwrap(), 41);
        assert_eq!(sleep_started.elapsed(), Duration::from_millis(5));

        let timeout_started = super::Instant::now();
        assert_eq!(
            super::timeout(Duration::from_millis(7), std::future::pending::<()>(),).await,
            Err(super::TimedOut)
        );
        assert_eq!(timeout_started.elapsed(), Duration::from_millis(7));
    }

    #[tokio::test(start_paused = true)]
    async fn ambient_runtime_satisfies_the_facade_contract() {
        runtime_facade_contract().await;
    }
}

#[cfg(all(test, sim))]
mod sim_tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::time::Duration;

    async fn runtime_facade_contract() {
        let sleep_started = super::Instant::now();
        let task = super::spawn(async {
            super::sleep(Duration::from_millis(5)).await;
            41
        });
        assert_eq!(task.await.unwrap(), 41);
        assert_eq!(sleep_started.elapsed(), Duration::from_millis(5));

        let timeout_started = super::Instant::now();
        assert_eq!(
            super::timeout(Duration::from_millis(7), std::future::pending::<()>()).await,
            Err(super::TimedOut)
        );
        assert_eq!(timeout_started.elapsed(), Duration::from_millis(7));
    }

    #[test]
    fn active_simulation_preserves_the_runtime_contract() {
        crate::exec::block_on(runtime_facade_contract());
    }

    #[test]
    fn simulation_tasks_can_hold_thread_local_state() {
        let value = crate::exec::block_on(async {
            let value = Rc::new(RefCell::new(41));
            let task_value = value.clone();
            let task = super::spawn(async move {
                *task_value.borrow_mut() += 1;
            });
            task.await.unwrap();
            Rc::try_unwrap(value).unwrap().into_inner()
        });
        assert_eq!(value, 42);
    }

    #[test]
    #[should_panic(expected = "rt::spawn called outside the simulation executor")]
    fn simulation_spawn_requires_an_active_executor() {
        drop(super::spawn(async {}));
    }
}
