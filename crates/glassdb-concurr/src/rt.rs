//! Runtime indirection seam.
//!
//! Production builds delegate to real `tokio`. Under `--cfg sim`, task spawning
//! and time require the in-repo deterministic executor (`crate::exec`) to be
//! active on the current thread.
//!
//! `tokio::sync` and `tokio::select!` are runtime-agnostic and are used directly
//! elsewhere (non-`biased` selects stay deterministic under sim via the seeded
//! branch-poll RNG; see `crate::exec::block_on_with`).

mod dedicated;
#[cfg(not(sim))]
mod native;
#[cfg(sim)]
mod sim;

pub use dedicated::{DedicatedJoinError, DedicatedJoinHandle, SpawnError, spawn_dedicated};
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
    use std::sync::mpsc;
    use std::time::Duration;

    use super::{DedicatedJoinError, sleep, spawn_dedicated};

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

        let task = spawn_dedicated("glassdb-runtime-contract", async { 42 }).unwrap();
        assert_eq!(task.await.unwrap(), 42);
    }

    #[tokio::test(start_paused = true)]
    async fn ambient_runtime_satisfies_the_facade_contract() {
        assert!(!super::in_sim());
        runtime_facade_contract().await;
    }

    #[tokio::test]
    async fn dedicated_task_uses_a_named_native_thread() {
        let caller = std::thread::current().id();
        let task = spawn_dedicated("glassdb-dedicated-test", async move {
            sleep(Duration::from_millis(1)).await;
            (
                std::thread::current().id(),
                std::thread::current().name().map(str::to_owned),
            )
        })
        .unwrap();

        let (thread, name) = task.await.unwrap();
        assert_ne!(thread, caller);
        assert_eq!(name.as_deref(), Some("glassdb-dedicated-test"));
    }

    #[tokio::test]
    async fn dedicated_abort_waits_for_a_blocking_poll_to_return() {
        let (entered, entered_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let mut task = spawn_dedicated("glassdb-dedicated-blocked", async move {
            entered.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("dedicated task did not enter its blocking poll");

        task.abort();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut task)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        assert_eq!(task.await, Err(DedicatedJoinError::Cancelled));
    }

    #[tokio::test]
    async fn dedicated_panic_is_normalized() {
        let task = spawn_dedicated("glassdb-dedicated-panic", async move {
            panic!("dedicated test panic");
        })
        .unwrap();
        assert_eq!(task.await, Err(DedicatedJoinError::Panicked));
    }

    #[tokio::test]
    async fn dropping_dedicated_handle_detaches() {
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let (finished, finished_rx) = tokio::sync::oneshot::channel();
        let task = spawn_dedicated("glassdb-dedicated-detach", async move {
            let _ = release_rx.await;
            let _ = finished.send(());
        })
        .unwrap();
        drop(task);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), finished_rx)
            .await
            .expect("detached task did not finish")
            .unwrap();
    }
}

#[cfg(all(test, sim))]
mod sim_tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::{DedicatedJoinError, spawn_dedicated};

    struct DropNotice(Arc<AtomicBool>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

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

        let task = spawn_dedicated("glassdb-runtime-contract", async { 42 }).unwrap();
        assert_eq!(task.await.unwrap(), 42);
    }

    #[test]
    fn active_simulation_preserves_runtime_and_dedicated_task_contracts() {
        let caller = std::thread::current().id();
        let dropped_at_shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_notice = dropped_at_shutdown.clone();
        crate::exec::block_on(async move {
            runtime_facade_contract().await;

            let success = spawn_dedicated("not-a-native-thread", async move {
                super::yield_now().await;
                std::thread::current().id()
            })
            .unwrap();
            let panicked = spawn_dedicated("simulated-panic", async move {
                panic!("dedicated test panic");
            })
            .unwrap();
            let cancelled = spawn_dedicated("simulated-cancellation", async move {
                std::future::pending::<()>().await;
            })
            .unwrap();
            cancelled.abort();

            let notice = DropNotice(shutdown_notice);
            let detached = spawn_dedicated("simulated-shutdown", async move {
                let _notice = notice;
                std::future::pending::<()>().await;
            })
            .unwrap();
            drop(detached);

            assert_eq!(success.await.unwrap(), caller);
            assert!(matches!(panicked.await, Err(DedicatedJoinError::Panicked)));
            assert_eq!(cancelled.await, Err(DedicatedJoinError::Cancelled));
        });

        assert!(dropped_at_shutdown.load(Ordering::SeqCst));
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
