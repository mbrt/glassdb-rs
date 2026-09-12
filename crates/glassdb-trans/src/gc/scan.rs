use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use glassdb_backend::ListCursor;
use glassdb_concurr::rt;
use glassdb_data::{TxId, shuffle};
use glassdb_storage::{StorageError, transaction::TLogger};

use super::Counters;

// Matches MAX_LIST_PAGE_SIZE in both the S3 and GCS adapters, so each LIST can
// amortize its request cost over a full page. Backends can return shorter pages.
pub(super) const PAGE_SIZE: usize = 1000;
// Initial scheduling limit per generation: permits progress across several
// prefixes while bounding active cursors. Older generations retain their
// cursors separately; this is not a limit on concurrent LIST requests.
const MAX_TRAVERSALS: usize = 8;
// ADR-070 uses four distinct prefixes to reduce single-sample noise while
// reacting sooner than a full 64-prefix pass. Uniform transaction IDs let
// these samples estimate the whole namespace.
const SAMPLE_SIZE: usize = 4;
// ADR-070's initial traversal budget: narrow after 64 nonterminal pages rather
// than waiting for a large traversal to finish.
const NARROW_PAGES: usize = 64;
// Broaden only when a prefix is estimated to fit in 32 pages at the observed
// page capacity. Half the narrowing threshold leaves room for estimation noise
// and growth without immediately reversing the depth change (ADR-070).
const BROADEN_PAGES: f64 = 32.0;

struct Traversal {
    depth: u8,
    index: usize,
    generation: u64,
    cursor: Option<ListCursor>,
    pages: usize,
    objects: usize,
    sample: Option<u64>,
    retry_at: rt::Instant,
    failures: u32,
}

struct Sample {
    ticket: u64,
    index: usize,
    count: Option<usize>,
}

/// Traverses the transaction namespace with bounded, adaptive LIST work.
pub(super) struct GcScan {
    tl: TLogger,
    counters: Arc<Counters>,
    depth: u8,
    generation: u64,
    order: VecDeque<usize>,
    active: VecDeque<Traversal>,
    samples: VecDeque<Sample>,
    window: VecDeque<(usize, usize)>,
    next_ticket: u64,
    page_capacity: f64,
    prefer_new: bool,
    retry_delay: Duration,
}

impl GcScan {
    pub(super) fn new(tl: TLogger, counters: Arc<Counters>, retry_delay: Duration) -> Self {
        counters.prefix_count.store(1, Ordering::Relaxed);
        Self {
            tl,
            counters,
            depth: 0,
            generation: 0,
            order: VecDeque::from([0]),
            active: VecDeque::new(),
            samples: VecDeque::new(),
            window: VecDeque::new(),
            next_ticket: 0,
            page_capacity: PAGE_SIZE as f64,
            prefer_new: true,
            retry_delay,
        }
    }

    pub(super) fn has_continuation(&self) -> bool {
        self.active.iter().any(|scan| scan.cursor.is_some())
    }

    pub(super) fn retry_at(&self) -> Option<rt::Instant> {
        self.active.iter().map(|scan| scan.retry_at).min()
    }

    /// Performs at most one page request, preserving independent traversal progress.
    pub(super) async fn turn(&mut self) -> Result<Option<Vec<TxId>>, StorageError> {
        let now = rt::Instant::now();
        // Retained traversals must leave capacity for the new depth. Unique
        // physical prefixes bound all generations together to 1 + 64 + 4096.
        let new = if self
            .active
            .iter()
            .filter(|scan| scan.generation == self.generation)
            .count()
            < MAX_TRAVERSALS
            && (self.prefer_new || self.active.is_empty())
        {
            self.start_traversal(now)
        } else {
            None
        };
        self.prefer_new = !self.prefer_new;
        let mut scan = match new.or_else(|| {
            self.active
                .iter()
                .position(|scan| scan.retry_at <= now)
                .and_then(|position| self.active.remove(position))
        }) {
            Some(scan) => scan,
            None => return Ok(None),
        };
        self.counters.lists.fetch_add(1, Ordering::Relaxed);
        let response = self.tl.scan_transaction_ids(
            scan.depth,
            scan.index,
            scan.cursor.as_ref(),
            glassdb_backend::ListLimit::new(PAGE_SIZE).unwrap(),
        );
        // A stalled LIST must not stop every other prefix or candidate check.
        let page = match rt::timeout(self.retry_delay * 4, response).await {
            Ok(result) => result,
            Err(_) => Err(StorageError::other("GC LIST timed out")),
        };
        match page {
            Ok(page) => {
                scan.failures = 0;
                scan.pages += 1;
                scan.objects = scan.objects.saturating_add(page.ids.len());
                scan.cursor = page.next;
                if scan.cursor.is_some() {
                    if scan.generation == self.generation {
                        // Terminal pages are short even when provider pages pack fully.
                        self.page_capacity =
                            0.75 * self.page_capacity + 0.25 * page.ids.len() as f64;
                        if scan.pages >= NARROW_PAGES && self.depth < 2 {
                            self.change_depth(self.depth + 1);
                        }
                    }
                    self.active.push_back(scan);
                } else if scan.generation == self.generation {
                    self.complete_sample(&scan);
                }
                Ok(Some(page.ids))
            }
            Err(error) => {
                if matches!(error, StorageError::InvalidCursor) {
                    scan.cursor = None;
                    scan.pages = 0;
                    scan.objects = 0;
                }
                scan.failures = scan.failures.saturating_add(1);
                scan.retry_at = now + self.retry_delay * (1u32 << scan.failures.min(4));
                self.active.push_back(scan);
                Err(error)
            }
        }
    }

    fn start_traversal(&mut self, now: rt::Instant) -> Option<Traversal> {
        for _ in 0..64usize.pow(u32::from(self.depth)) {
            if self.order.is_empty() {
                self.reset_order();
            }
            let index = self.order.pop_front()?;
            if self
                .active
                .iter()
                .any(|scan| scan.depth == self.depth && scan.index == index)
            {
                continue;
            }
            let sample = if self.depth > 0
                && self.samples.len() < SAMPLE_SIZE
                && !self.samples.iter().any(|sample| sample.index == index)
                && !self.window.iter().any(|(previous, _)| *previous == index)
            {
                let ticket = self.next_ticket;
                self.next_ticket += 1;
                self.samples.push_back(Sample {
                    ticket,
                    index,
                    count: None,
                });
                Some(ticket)
            } else {
                None
            };
            return Some(Traversal {
                depth: self.depth,
                index,
                generation: self.generation,
                cursor: None,
                pages: 0,
                objects: 0,
                sample,
                retry_at: now,
                failures: 0,
            });
        }
        None
    }

    fn complete_sample(&mut self, scan: &Traversal) {
        let Some(ticket) = scan.sample else {
            return;
        };
        if let Some(sample) = self
            .samples
            .iter_mut()
            .find(|sample| sample.ticket == ticket)
        {
            sample.count = Some(scan.objects);
        }
        while self
            .samples
            .front()
            .is_some_and(|sample| sample.count.is_some())
        {
            let sample = self.samples.pop_front().unwrap();
            if self.window.len() == SAMPLE_SIZE {
                self.window.pop_front();
            }
            self.window.push_back((sample.index, sample.count.unwrap()));
            if self.window.len() < SAMPLE_SIZE {
                continue;
            }
            let total = self
                .window
                .iter()
                .map(|(_, count)| *count as f64)
                .sum::<f64>()
                / SAMPLE_SIZE as f64
                * 64usize.pow(u32::from(self.depth)) as f64;
            if let Some(depth) = (0..self.depth).find(|depth| {
                total / 64usize.pow(u32::from(*depth)) as f64 <= BROADEN_PAGES * self.page_capacity
            }) {
                self.change_depth(depth);
                break;
            }
        }
    }

    fn change_depth(&mut self, depth: u8) {
        self.depth = depth;
        self.generation += 1;
        self.samples.clear();
        self.window.clear();
        self.reset_order();
        self.counters
            .prefix_count
            .store(64u64.pow(u32::from(depth)), Ordering::Relaxed);
    }

    fn reset_order(&mut self) {
        let mut order: Vec<_> = (0..64usize.pow(u32::from(self.depth))).collect();
        shuffle(&mut order);
        self.order = order.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use glassdb_backend::{
        Backend, BackendError, ListLimit, ListPage, ReadReply, Version, memory::MemoryBackend,
    };
    use glassdb_data::{DbRoot, ObjectPath};
    use glassdb_storage::{CachedStore, Timeline};
    use std::collections::BTreeSet;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    struct ShortPages {
        inner: MemoryBackend,
        calls: Mutex<Vec<(String, bool)>>,
        completed: Mutex<BTreeSet<String>>,
        invalid_once: AtomicBool,
        hold_first_group: AtomicBool,
        held_group: Mutex<Option<String>>,
    }

    #[async_trait]
    impl Backend for ShortPages {
        async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
            self.inner.read(path).await
        }
        async fn read_if_modified(
            &self,
            path: &str,
            expected: &Version,
        ) -> Result<ReadReply, BackendError> {
            self.inner.read_if_modified(path, expected).await
        }
        async fn write_if(
            &self,
            path: &str,
            value: Vec<u8>,
            expected: &Version,
        ) -> Result<Version, BackendError> {
            self.inner.write_if(path, value, expected).await
        }
        async fn write_if_not_exists(
            &self,
            path: &str,
            value: Vec<u8>,
        ) -> Result<Version, BackendError> {
            self.inner.write_if_not_exists(path, value).await
        }
        async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
            self.inner.delete_if(path, expected).await
        }
        async fn list(
            &self,
            prefix: &str,
            cursor: Option<&ListCursor>,
            _limit: ListLimit,
        ) -> Result<ListPage, BackendError> {
            self.calls
                .lock()
                .unwrap()
                .push((prefix.to_owned(), cursor.is_some()));
            if self.hold_first_group.load(Ordering::Relaxed) && prefix.matches('/').count() == 3 {
                let mut held = self.held_group.lock().unwrap();
                if held.is_none() {
                    *held = Some(prefix.to_owned());
                }
                if held.as_deref() == Some(prefix) {
                    return Err(BackendError::Unavailable("sample held".into()));
                }
            }
            if cursor.is_some() && self.invalid_once.swap(false, Ordering::Relaxed) {
                return Err(BackendError::InvalidCursor);
            }
            let page = self
                .inner
                .list(prefix, cursor, ListLimit::new(1).unwrap())
                .await?;
            if page.next.is_none() {
                self.completed.lock().unwrap().insert(prefix.to_owned());
            }
            Ok(page)
        }
    }

    async fn fixture(
        count: usize,
    ) -> (
        GcScan,
        Arc<ShortPages>,
        Vec<(String, Version)>,
        Arc<Counters>,
    ) {
        let backend = Arc::new(ShortPages {
            inner: MemoryBackend::new(),
            calls: Mutex::new(Vec::new()),
            completed: Mutex::new(BTreeSet::new()),
            invalid_once: AtomicBool::new(false),
            hold_first_group: AtomicBool::new(false),
            held_group: Mutex::new(None),
        });
        let db_root = DbRoot::try_from("scan").unwrap();
        let mut objects = Vec::new();
        for i in 0..count {
            let prefix = ((i % 4096) as u16) << 4;
            let id = TxId::from_bytes(vec![(prefix >> 8) as u8, prefix as u8, (i / 4096) as u8]);
            let path = ObjectPath::Transaction {
                db_root: db_root.clone(),
                id,
            }
            .to_string();
            let version = backend
                .write_if_not_exists(&path, Vec::new())
                .await
                .unwrap();
            objects.push((path, version));
        }
        let tl = TLogger::new(
            CachedStore::new(backend.clone(), 1 << 20, Timeline::new(), None),
            db_root,
        );
        let counters = Arc::new(Counters::default());
        (
            GcScan::new(tl, counters.clone(), Duration::from_secs(1)),
            backend,
            objects,
            counters,
        )
    }

    #[tokio::test(start_paused = true)]
    async fn empty_scan_issues_one_request() {
        let (mut scan, backend, _, _) = fixture(0).await;
        assert_eq!(scan.turn().await.unwrap(), Some(Vec::new()));
        assert_eq!(
            *backend.calls.lock().unwrap(),
            vec![("scan/_t/".into(), false)]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn narrow_scans_start_while_broad_cursors_remain_and_sparse_samples_broaden() {
        let (mut scan, backend, objects, counters) = fixture(8192).await;
        let mut seen = BTreeSet::new();
        for _ in 0..1500 {
            seen.extend(scan.turn().await.unwrap().unwrap_or_default());
            if counters.prefix_count.load(Ordering::Relaxed) == 4096 {
                break;
            }
        }
        assert_eq!(counters.prefix_count.load(Ordering::Relaxed), 4096);
        let before = backend.calls.lock().unwrap().len();
        for _ in 0..20 {
            scan.turn().await.unwrap();
        }
        assert!(
            backend.calls.lock().unwrap()[before..]
                .iter()
                .any(|(prefix, _)| prefix.matches('/').count() == 4)
        );
        assert!(
            backend.calls.lock().unwrap()[before..]
                .iter()
                .any(|(prefix, continued)| prefix == "scan/_t/" && *continued)
        );
        assert!(!seen.is_empty());
        for (path, version) in objects {
            backend.delete_if(&path, &version).await.unwrap();
        }
        for _ in 0..100 {
            scan.turn().await.unwrap();
            if counters.prefix_count.load(Ordering::Relaxed) == 1 {
                break;
            }
        }
        assert_eq!(counters.prefix_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn cursor_errors_restart_only_the_affected_traversal_and_wait_without_a_list() {
        let (mut scan, backend, _, counters) = fixture(3).await;
        scan.turn().await.unwrap();
        backend.invalid_once.store(true, Ordering::Relaxed);
        assert!(matches!(
            scan.turn().await,
            Err(StorageError::InvalidCursor)
        ));
        let calls = counters.lists.load(Ordering::Relaxed);
        assert!(scan.turn().await.unwrap().is_none());
        assert_eq!(counters.lists.load(Ordering::Relaxed), calls);
        rt::sleep(Duration::from_secs(3)).await;
        scan.turn().await.unwrap();
        assert_eq!(
            backend.calls.lock().unwrap().last().unwrap(),
            &("scan/_t/".into(), false)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn failed_selected_sample_waits_for_its_complete_count_before_broadening() {
        let (mut scan, backend, objects, counters) = fixture(4096).await;
        for _ in 0..64 {
            scan.turn().await.unwrap();
        }
        assert_eq!(counters.prefix_count.load(Ordering::Relaxed), 64);
        backend.hold_first_group.store(true, Ordering::Relaxed);
        assert!(scan.turn().await.is_err());
        let held = backend.held_group.lock().unwrap().clone().unwrap();
        // Later empty scans must not replace the selected sample that failed.
        for (path, version) in &objects {
            if !path.starts_with(&held) {
                backend.delete_if(path, version).await.unwrap();
            }
        }
        for _ in 0..40 {
            scan.turn().await.unwrap();
            assert_eq!(counters.prefix_count.load(Ordering::Relaxed), 64);
        }
        backend.hold_first_group.store(false, Ordering::Relaxed);
        rt::sleep(Duration::from_secs(3)).await;
        let mut completed = false;
        for _ in 0..400 {
            scan.turn().await.unwrap();
            if backend.completed.lock().unwrap().contains(&held) {
                completed = true;
                break;
            }
            assert_eq!(counters.prefix_count.load(Ordering::Relaxed), 64);
        }
        assert!(completed, "the failed sample must resume and finish");
        // Its 64 objects keep the four-sample estimate above the root threshold.
        assert_eq!(counters.prefix_count.load(Ordering::Relaxed), 64);
        for (path, version) in &objects {
            if path.starts_with(&held) {
                backend.delete_if(path, version).await.unwrap();
            }
        }
        for _ in 0..40 {
            scan.turn().await.unwrap();
            if counters.prefix_count.load(Ordering::Relaxed) == 1 {
                break;
            }
        }
        assert_eq!(counters.prefix_count.load(Ordering::Relaxed), 1);
    }
}
