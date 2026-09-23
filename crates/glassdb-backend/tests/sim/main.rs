#![cfg(sim)]

mod sim_tests {
    use std::future::{Future, poll_fn};
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::Poll;
    use std::time::Duration;

    use glassdb_backend::middleware::{
        DelayBackend, FaultBackend, FaultOptions, Latency, gcs_delays,
    };
    use glassdb_backend::{Backend, BackendError, Revision, memory::MemoryBackend};
    use glassdb_concurr::exec::{TapeScheduler, block_on_with};
    use glassdb_concurr::{entropy, rt};

    fn replay(seed: u64) -> (Duration, [u8; 8]) {
        block_on_with(TapeScheduler::new(Vec::new()), seed, async {
            let mut options = gcs_delays();
            options.latency.obj_read = Latency::new(57, 7);
            let backend = DelayBackend::new(Arc::new(MemoryBackend::new()), options).unwrap();
            let start = rt::Instant::now();
            assert!(matches!(
                backend.read("missing").await,
                Err(BackendError::NotFound)
            ));
            let elapsed = start.elapsed();
            let mut entropy_sentinel = [0; 8];
            entropy::fill_bytes(&mut entropy_sentinel);
            (elapsed, entropy_sentinel)
        })
    }

    #[test]
    fn delay_sampling_replays_from_the_executor_entropy_stream() {
        let first = replay(0xF2_5C);
        let repeated = replay(0xF2_5C);
        assert_eq!(first, repeated);
        assert_eq!(first.1, [213, 85, 126, 142, 115, 101, 39, 176]);

        let different_seed = replay(0xF2_5D);
        assert_ne!(first.0, different_seed.0);
        assert_ne!(first.1, different_seed.1);
    }

    fn delayed_reply_backend(memory: Arc<MemoryBackend>) -> Arc<FaultBackend> {
        let options = FaultOptions {
            delay_prob: 0,
            fault_prob: 0,
            max_delay: Duration::from_millis(10),
            ..FaultOptions::from_intensity(255)
        };
        // Delay the first reply by 5 ms; let the next reply pass immediately.
        let backend = FaultBackend::with_tape(memory, vec![0, 0x4c, 0x4b, 0x40, 255], 1, options);
        backend.set_active(true);
        backend
    }

    #[test]
    fn a_read_reply_can_arrive_after_a_newer_read_reply() {
        for conditional in [false, true] {
            block_on_with(TapeScheduler::new(Vec::new()), 1, async move {
                let memory = Arc::new(MemoryBackend::new());
                let original = memory
                    .write_if_not_exists("p", b"old".to_vec())
                    .await
                    .unwrap();
                let backend = delayed_reply_backend(memory.clone());
                let start = rt::Instant::now();
                let mut old_read = pin!(async {
                    if conditional {
                        backend
                            .read_if_modified("p", &Revision::new("different"))
                            .await
                    } else {
                        backend.read("p").await
                    }
                });
                assert!(
                    poll_fn(|cx| Poll::Ready(old_read.as_mut().poll(cx)))
                        .await
                        .is_pending()
                );

                let updated = memory
                    .write_if("p", b"new".to_vec(), &original)
                    .await
                    .unwrap();
                let newer_reply = backend.read("p").await.unwrap();
                assert_eq!(newer_reply.contents, b"new");
                assert_eq!(newer_reply.revision, updated);
                assert_eq!(start.elapsed(), Duration::ZERO);

                let older_reply = old_read.await.unwrap();
                assert_eq!(older_reply.contents, b"old");
                assert_eq!(older_reply.revision, original);
                assert_eq!(start.elapsed(), Duration::from_millis(5));
            });
        }
    }

    #[test]
    fn an_unchanged_read_reply_can_arrive_after_a_write() {
        block_on_with(TapeScheduler::new(Vec::new()), 1, async {
            let memory = Arc::new(MemoryBackend::new());
            let original = memory
                .write_if_not_exists("p", b"old".to_vec())
                .await
                .unwrap();
            let backend = delayed_reply_backend(memory.clone());
            let mut read = pin!(backend.read_if_modified("p", &original));
            assert!(
                poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );

            memory
                .write_if("p", b"new".to_vec(), &original)
                .await
                .unwrap();

            assert!(matches!(read.await, Err(BackendError::Precondition)));
        });
    }

    #[test]
    fn an_absent_read_reply_can_arrive_after_creation() {
        for conditional in [false, true] {
            block_on_with(TapeScheduler::new(Vec::new()), 1, async move {
                let memory = Arc::new(MemoryBackend::new());
                let backend = delayed_reply_backend(memory.clone());
                let mut read = pin!(async {
                    if conditional {
                        backend
                            .read_if_modified("p", &Revision::new("missing"))
                            .await
                    } else {
                        backend.read("p").await
                    }
                });
                assert!(
                    poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx)))
                        .await
                        .is_pending()
                );

                memory
                    .write_if_not_exists("p", b"new".to_vec())
                    .await
                    .unwrap();

                assert!(matches!(read.await, Err(BackendError::NotFound)));
            });
        }
    }

    #[test]
    fn request_and_reply_delays_allow_changes_on_both_sides_of_a_read() {
        block_on_with(TapeScheduler::new(Vec::new()), 1, async {
            let memory = Arc::new(MemoryBackend::new());
            let original = memory
                .write_if_not_exists("p", b"old".to_vec())
                .await
                .unwrap();
            let options = FaultOptions {
                delay_prob: 255,
                fault_prob: 0,
                max_delay: Duration::from_millis(10),
                ..FaultOptions::from_intensity(255)
            };
            // Delay the request by 5 ms and its reply by another 7 ms.
            let backend = FaultBackend::with_tape(
                memory.clone(),
                vec![0, 0x4c, 0x4b, 0x40, 0, 0x6a, 0xcf, 0xc0],
                1,
                options,
            );
            backend.set_active(true);
            let start = rt::Instant::now();
            let mut read = pin!(backend.read("p"));
            assert!(
                poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            let selected = memory
                .write_if("p", b"selected".to_vec(), &original)
                .await
                .unwrap();

            rt::sleep(Duration::from_millis(5)).await;
            assert!(
                poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            memory
                .write_if("p", b"new".to_vec(), &selected)
                .await
                .unwrap();

            let reply = read.await.unwrap();
            assert_eq!(reply.contents, b"selected");
            assert_eq!(reply.revision, selected);
            assert_eq!(start.elapsed(), Duration::from_millis(12));
        });
    }

    #[test]
    fn a_mutation_reply_can_arrive_after_its_installed_state_is_replaced() {
        block_on_with(TapeScheduler::new(Vec::new()), 1, async {
            let memory = Arc::new(MemoryBackend::new());
            let original = memory
                .write_if_not_exists("p", b"old".to_vec())
                .await
                .unwrap();
            let backend = delayed_reply_backend(memory.clone());
            let mut write = pin!(backend.write_if("p", b"installed".to_vec(), &original));
            assert!(
                poll_fn(|cx| Poll::Ready(write.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            let installed = memory.read("p").await.unwrap();
            assert_eq!(installed.contents, b"installed");
            memory
                .write_if("p", b"new".to_vec(), &installed.revision)
                .await
                .unwrap();

            assert_eq!(write.await.unwrap(), installed.revision);
            assert_eq!(memory.read("p").await.unwrap().contents, b"new");
        });
    }
}
