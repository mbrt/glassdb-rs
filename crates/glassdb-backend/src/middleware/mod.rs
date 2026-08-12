//! Decorators for [`Backend`](crate::Backend) implementations: simulated
//! latency, deterministic scheduling, and operation logging. Ported from the
//! Go `backend/middleware` package.

mod delay;
mod fault;
mod hook;
mod latency;
mod logger;
mod recording;
mod scheduled;

pub use delay::{
    DelayBackend, DelayOptions, DelayOptionsError, Latency, ProviderLatencyProfile, RateLimit,
    WriteRateLimits, gcs_delays, s3_delays,
};
pub use fault::{FaultBackend, FaultOptions};
pub use hook::{BackendOp, HookBackend, HookFuture, HookOutcome};
pub use latency::{Lognormal, LognormalError};
pub use logger::BackendLogger;
pub use recording::{OpLog, OpRecord, RecordingBackend, first_divergence};
pub use scheduled::{ScheduledBackend, Scheduler};

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::*;
    use crate::memory::MemoryBackend;
    use crate::{Backend, BackendError, ListCursor, ListRequest, StatsBackend};

    fn delay_options() -> DelayOptions {
        let zero = Latency::new(0, 0);
        DelayOptions {
            latency: ProviderLatencyProfile {
                meta_read: zero,
                meta_write: zero,
                obj_read: zero,
                obj_write: zero,
                list: Latency::new(2, 0),
            },
            rate_limits: WriteRateLimits {
                same_obj_write_ps: RateLimit::Unlimited,
                same_obj_write_retry_delay: Duration::ZERO,
                prefix_read_ps: RateLimit::Unlimited,
                prefix_write_ps: RateLimit::Unlimited,
                prefix_depth: 0,
            },
        }
    }

    struct ListingStack {
        backend: Arc<StatsBackend>,
        provider_stats: Arc<StatsBackend>,
        hook: Arc<HookBackend>,
        log: OpLog,
    }

    fn listing_stack(memory: Arc<MemoryBackend>, scheduled: Vec<u8>) -> ListingStack {
        let provider_stats = Arc::new(StatsBackend::new(memory));
        let delayed = DelayBackend::new(provider_stats.clone(), delay_options()).unwrap();
        let faulted = FaultBackend::new(Arc::new(delayed), 1, FaultOptions::from_intensity(0));
        faulted.set_active(true);
        let hook = HookBackend::new(faulted);
        let logger = BackendLogger::new(hook.clone(), "request-forwarding");
        let recording = RecordingBackend::new(Arc::new(logger));
        let log = recording.log();
        let scheduled = ScheduledBackend::new(
            Arc::new(recording),
            Arc::new(Scheduler::new(scheduled, Duration::from_millis(1))),
        );
        ListingStack {
            backend: Arc::new(StatsBackend::new(Arc::new(scheduled))),
            provider_stats,
            hook,
            log,
        }
    }

    fn encoded_list_args(cursor: Option<&ListCursor>, limit: usize) -> Vec<u8> {
        let mut encoded = Vec::new();
        match cursor {
            Some(cursor) => {
                encoded.push(1);
                encoded.extend_from_slice(&(cursor.as_str().len() as u64).to_le_bytes());
                encoded.extend_from_slice(cursor.as_str().as_bytes());
            }
            None => encoded.push(0),
        }
        encoded.extend_from_slice(&(limit as u64).to_le_bytes());
        encoded
    }

    #[tokio::test(start_paused = true)]
    async fn listing_request_flows_through_every_decorator() {
        let memory = Arc::new(MemoryBackend::new());
        memory
            .write_if_not_exists("a/one", Vec::new())
            .await
            .unwrap();
        memory
            .write_if_not_exists("a/two", Vec::new())
            .await
            .unwrap();
        let limit = NonZeroUsize::new(1).unwrap();
        let first = memory.list("a/", None, limit).await.unwrap();
        let cursor = first.next.unwrap();

        let stack = listing_stack(memory, vec![3, 3]);
        let seen = Arc::new(Mutex::new(Vec::new()));
        stack.hook.set_before({
            let seen = seen.clone();
            move |op| {
                if let BackendOp::List {
                    path,
                    cursor,
                    limit,
                } = op
                {
                    seen.lock().unwrap().push((
                        path.to_string(),
                        cursor.map(ListCursor::as_str).map(str::to_owned),
                        limit.get(),
                    ));
                }
                Box::pin(async { Ok(()) })
            }
        });

        let started = tokio::time::Instant::now();
        let page = stack
            .backend
            .list("a/", Some(&cursor), limit)
            .await
            .unwrap();
        let erased: Arc<dyn Backend> = stack.backend.clone();
        let direct = erased
            .list_request(ListRequest::new("a/", Some(&cursor), limit).unwrap())
            .await
            .unwrap();

        assert_eq!(page.objects, ["a/two"]);
        assert!(page.next.is_none());
        assert_eq!(direct, page);
        assert_eq!(started.elapsed(), Duration::from_millis(10));
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                ("a/".to_string(), Some(cursor.as_str().to_string()), 1),
                ("a/".to_string(), Some(cursor.as_str().to_string()), 1),
            ]
        );
        assert_eq!(stack.backend.stats_and_reset().obj_lists, 2);
        assert_eq!(stack.provider_stats.stats_and_reset().obj_lists, 2);
        let recorded = stack.log.lock().unwrap();
        assert_eq!(recorded.len(), 2);
        for record in recorded.iter() {
            assert_eq!(record.op, "list");
            assert_eq!(record.path, "a/");
            assert_eq!(record.args, encoded_list_args(Some(&cursor), 1));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn invalid_compatibility_call_preserves_decorator_effects_and_error_precedence() {
        let stack = listing_stack(Arc::new(MemoryBackend::new()), vec![3]);
        let hook_calls = Arc::new(Mutex::new(0));
        stack.hook.set_after({
            let hook_calls = hook_calls.clone();
            move |op, outcome| {
                if matches!(op, BackendOp::List { .. }) {
                    *hook_calls.lock().unwrap() += 1;
                    assert!(!outcome.is_success());
                    return Box::pin(async {
                        Err(BackendError::Unavailable("hook override".into()))
                    });
                }
                Box::pin(async { Ok(()) })
            }
        });

        let started = tokio::time::Instant::now();
        let error = stack
            .backend
            .list("invalid", None, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap_err();

        assert!(matches!(error, BackendError::Unavailable(ref msg) if msg == "hook override"));
        assert_eq!(started.elapsed(), Duration::from_millis(5));
        assert_eq!(*hook_calls.lock().unwrap(), 1);
        assert_eq!(stack.backend.stats_and_reset().obj_lists, 1);
        assert_eq!(stack.provider_stats.stats_and_reset().obj_lists, 1);
        let recorded = stack.log.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].op, "list");
        assert_eq!(recorded[0].path, "invalid");
        assert_eq!(recorded[0].args, encoded_list_args(None, 1));
    }
}
