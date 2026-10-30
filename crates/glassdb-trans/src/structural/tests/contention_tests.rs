use std::collections::BTreeSet;
use std::time::{Duration, SystemTime};

use super::*;
use crate::structural::contention::Contention;

// A writer that started `age` before now, told apart from the others by
// `name`.
fn writer(name: u8, age: Duration) -> TxId {
    let started = rt::system_now() - age;
    let nanos = started
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    TxId::with_priority(nanos, &[name])
}

fn recent(name: u8) -> TxId {
    writer(name, Duration::from_secs(1))
}

// A writer that is not recent, for the write spans and for this database
// instance.
fn old() -> TxId {
    writer(0, Duration::from_secs(600))
}

fn key(index: usize) -> Vec<u8> {
    format!("k{index:02}").into_bytes()
}

fn keys(indexes: impl IntoIterator<Item = usize>) -> BTreeSet<Vec<u8>> {
    indexes.into_iter().map(key).collect()
}

// The monitor of a database instance, which tells its local writers.
fn monitor(bg: &Arc<Background>) -> Monitor {
    store().foundation.monitor_for(
        bg,
        RetrySchedule::default(),
        crate::monitor::ProtocolTiming::default(),
    )
}

// The leaf of one key for each character of `writers`, from `k00`. Each key
// has its own writer: `L` is local to `mon`, `F` is foreign, `o` is not
// recent, and `T` is a tombstone of a foreign writer. All keys with `S` have
// one local writer, which wrote them together.
fn leaf(mon: &Monitor, writers: &str) -> Node {
    let span = recent(1);
    Node::leaf(LeafBody::from_entries(writers.chars().enumerate().map(
        |(index, kind)| {
            let name = u8::try_from(index).unwrap() + 2;
            let writer = match kind {
                'L' => recent(name),
                'F' | 'T' => recent(name + 100),
                'o' => old(),
                'S' => span,
                _ => panic!("unknown writer {kind}"),
            };
            if matches!(kind, 'L' | 'S') {
                mon.begin_owner_operation(&writer).unwrap().complete();
            }
            let current = if kind == 'T' {
                CurrentState::Tombstone { writer }
            } else {
                CurrentState::External { writer }
            };
            LeafEntry::new(key(index)).with_current(current)
        },
    )))
}

// Two adjacent leaves with the writers of `left` and of `right`, as in
// `leaf`. The keys of `right` follow the keys of `left`.
fn pair(mon: &Monitor, left: &str, right: &str) -> (Node, Node) {
    let both = leaf(mon, &format!("{left}{right}"));
    let entries: Vec<LeafEntry> = both.as_leaf().unwrap().entries().cloned().collect();
    let (left, right) = entries.split_at(left.len());
    (
        Node::leaf(LeafBody::from_entries(left.to_vec())),
        Node::leaf(LeafBody::from_entries(right.to_vec())),
    )
}

// The contention of a database instance and its monitor.
fn contention() -> (Contention, Monitor, Arc<Background>) {
    let bg = Arc::new(Background::new());
    let mon = monitor(&bg);
    (Contention::new(mon.clone()), mon, bg)
}

// Rounds of one key reach the threshold, because each of them has the whole
// loss.
#[tokio::test(start_paused = true)]
async fn rounds_of_one_key_make_the_key_contended() {
    let (contention, _mon, _bg) = contention();
    let round = keys([3]);

    for _ in 1..32 {
        assert_eq!(contention.lost_leaf_cas(&collection(), &round), None);
    }

    assert_eq!(
        contention.lost_leaf_cas(&collection(), &round),
        Some(key(3))
    );
}

// The keys of a round share its loss, so a busy database instance whose rounds
// batch many keys does not make each of them contended.
#[tokio::test(start_paused = true)]
async fn the_keys_of_a_round_share_its_loss() {
    let (contention, _mon, _bg) = contention();
    let round = keys(0..8);

    for _ in 0..200 {
        assert_eq!(contention.lost_leaf_cas(&collection(), &round), None);
    }
}

// Shares decay, so a key that lost leaf CASes long ago is not contended.
#[tokio::test(start_paused = true)]
async fn shares_decay() {
    let (contention, _mon, _bg) = contention();
    let round = keys([3]);
    for _ in 0..31 {
        contention.lost_leaf_cas(&collection(), &round);
    }

    tokio::time::advance(Duration::from_secs(60)).await;

    assert_eq!(contention.lost_leaf_cas(&collection(), &round), None);
}

// The contended key k03 has foreign neighbors on both sides. The split moves
// away the side with more keys first.
#[tokio::test]
async fn the_split_moves_away_the_side_with_more_keys() {
    let (contention, mon, _bg) = contention();
    let node = leaf(&mon, "FFFLFFFF");

    assert_eq!(
        contention.split_key(&node, &key(3), rt::system_now()),
        Some(key(4))
    );
}

// Keys without a live value of a recent writer stay with the contended key, so
// that the split moves only the keys of other database instances.
#[tokio::test]
async fn keys_without_a_recent_writer_stay_with_the_contended_key() {
    let (contention, mon, _bg) = contention();
    let node = leaf(&mon, "FLoTFF");

    assert_eq!(
        contention.split_key(&node, &key(1), rt::system_now()),
        Some(key(4))
    );
}

// The local writer of k03 wrote k05 too, so a split at k04 divides its write
// span. The split is at the other edge of the run.
#[tokio::test]
async fn the_split_divides_no_write_span() {
    let (contention, mon, _bg) = contention();
    let node = leaf(&mon, "FFFSFSFF");

    assert_eq!(
        contention.split_key(&node, &key(3), rt::system_now()),
        Some(key(3))
    );
}

// Regression: a database instance that loses every leaf CAS on its key from
// the start has not written the key yet, so another database instance wrote
// it last. The key still splits away.
#[tokio::test]
async fn a_contended_key_that_another_database_instance_wrote_last_splits_away() {
    let (contention, mon, _bg) = contention();
    let node = leaf(&mon, "FFFF");

    assert_eq!(
        contention.split_key(&node, &key(1), rt::system_now()),
        Some(key(2))
    );
}

// When no other database instance wrote the leaf recently, the key is already
// apart from their keys.
#[tokio::test]
async fn a_leaf_of_local_keys_does_not_split() {
    let (contention, mon, _bg) = contention();
    let node = leaf(&mon, "oLL");

    assert_eq!(contention.split_key(&node, &key(1), rt::system_now()), None);
}

// The leaf of a contended key, and its neighbor that another database instance
// writes, stay apart.
#[tokio::test]
async fn a_merge_into_a_leaf_of_another_database_instance_waits() {
    let (contention, mon, _bg) = contention();
    let (left, right) = pair(&mon, "L", "F");

    assert!(contention.keeps_apart(&left, &right, rt::system_now()));
}

// Regression: a merge request can be stale. This database instance asked for
// the merge of the left leaf while the leaf held its key, and a split of
// another database instance moved the key away. The leaf now holds only a key
// of the other database instance, so the merge waits.
#[tokio::test]
async fn a_merge_of_a_leaf_of_another_database_instance_waits() {
    let (contention, mon, _bg) = contention();
    let (left, right) = pair(&mon, "F", "L");

    assert!(contention.keeps_apart(&left, &right, rt::system_now()));
}

// A database instance that writes neither leaf leaves the merge to their
// writers.
#[tokio::test]
async fn a_database_instance_that_writes_neither_leaf_does_not_merge_them() {
    let (contention, mon, _bg) = contention();
    let (left, right) = pair(&mon, "F", "F");

    assert!(contention.keeps_apart(&left, &right, rt::system_now()));
}

// A database instance that writes both leaves merges them, also when other
// database instances write them too.
#[tokio::test]
async fn a_database_instance_that_writes_both_leaves_merges_them() {
    let (contention, mon, _bg) = contention();
    let (left, right) = pair(&mon, "L", "FL");

    assert!(!contention.keeps_apart(&left, &right, rt::system_now()));
}

// A merge with a leaf that has no live value of a recent writer cannot put the
// keys of two database instances together, so it goes ahead, also when the
// other leaf is foreign.
#[tokio::test]
async fn a_merge_with_a_leaf_without_recent_writers_goes_ahead() {
    let (contention, mon, _bg) = contention();
    let (left, right) = pair(&mon, "F", "o");

    assert!(!contention.keeps_apart(&left, &right, rt::system_now()));
}

// Tombstones hold no key apart, so the leaf of keys that another database
// instance deleted merges.
#[tokio::test]
async fn tombstones_do_not_keep_leaves_apart() {
    let (contention, mon, _bg) = contention();
    let (left, right) = pair(&mon, "F", "TT");

    assert!(!contention.keeps_apart(&left, &right, rt::system_now()));
}
