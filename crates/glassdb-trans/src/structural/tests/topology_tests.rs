use std::sync::atomic::{AtomicBool, Ordering};

use super::*;

#[tokio::test(start_paused = true)]
async fn departure_backs_off_after_a_rejected_cas() {
    let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let hooks = HookBackend::new(memory.clone());
    let store = store_with_backend(hooks.clone());
    let peer = store_with_backend(memory);
    let bg = Arc::new(Background::new());
    let mon = store.foundation.monitor_for(
        &bg,
        RetrySchedule::default(),
        crate::monitor::ProtocolTiming::default(),
    );
    let retry = RetryTiming::default().contention;
    let topology = TopologyMembership::new(store.records.clone(), mon, retry);
    let (departing, admitted) = (tx_id(b"departing"), tx_id(b"admitted"));
    let mut record = CollectionRecord::new();
    assert!(record.add_topology_participant(departing));
    assert!(
        store
            .records
            .create_record(&collection(), &record)
            .await
            .unwrap()
    );
    // A peer admits another participant just before the first removal CAS,
    // so that CAS is rejected and departure must retry.
    hooks.set_before({
        let path = ObjectPath::CollectionRecord {
            collection: collection(),
        }
        .to_string();
        let first = Arc::new(AtomicBool::new(true));
        move |op| {
            let conflict = matches!(op, BackendOp::WriteIf { .. })
                && op.path() == path
                && first.swap(false, Ordering::SeqCst);
            let records = peer.records.clone();
            Box::pin(async move {
                if conflict {
                    let (mut record, observed) = records
                        .load_record(&collection(), Requirement::ANY)
                        .await
                        .unwrap();
                    assert!(record.add_topology_participant(admitted));
                    assert!(records.store_record(&record, &observed).await.unwrap());
                }
                Ok(())
            })
        }
    });

    let start = tokio::time::Instant::now();
    let barrier = store.timeline.currentness_barrier();
    assert!(
        topology
            .leave(&collection(), &departing, Requirement::after(barrier))
            .await
            .unwrap()
    );

    // The first backoff delay is at least half of the initial interval.
    assert!(start.elapsed() >= retry.initial_interval / 2);
    let (record, _) = store
        .records
        .load_record(
            &collection(),
            Requirement::after(store.timeline.currentness_barrier()),
        )
        .await
        .unwrap();
    assert_eq!(
        record.topology_participants().copied().collect::<Vec<_>>(),
        vec![admitted]
    );
}
