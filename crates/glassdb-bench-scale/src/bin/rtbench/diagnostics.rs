//! Opt-in diagnostic artifact collection for `rtbench`.

use std::error::Error;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use glassdb::{Database, Stats};
use glassdb_backend::Backend;
use glassdb_bench_scale::backend_breakdown::{
    BackendBreakdown, BackendBreakdownHandle, OperationCounts, wrap,
};

pub struct DiagnosticSession {
    backend: BackendBreakdownHandle,
    metrics: BufWriter<File>,
    failures: BufWriter<File>,
}

impl DiagnosticSession {
    pub fn new(
        dir: &Path,
        backend: Arc<dyn Backend>,
    ) -> Result<(Self, Arc<dyn Backend>), Box<dyn Error>> {
        fs::create_dir_all(dir)?;
        let mut metrics = BufWriter::new(File::create(dir.join("metrics.csv"))?);
        writeln!(metrics, "run,num-db,logical-tx,component,metric,value")?;
        let failures = BufWriter::new(File::create(dir.join("failure-state.txt"))?);
        let (backend, handle) = wrap(backend);
        Ok((
            Self {
                backend: handle,
                metrics,
                failures,
            },
            backend,
        ))
    }

    pub fn backend_snapshot(&self) -> BackendBreakdown {
        self.backend.snapshot()
    }

    pub fn record_cell(
        &mut self,
        run: usize,
        num_dbs: usize,
        logical_tx: u64,
        stats: &[Stats],
        backend_before: BackendBreakdown,
    ) -> Result<(), Box<dyn Error>> {
        let backend = self.backend.snapshot() - backend_before;
        let stats_backend_ops: u64 = stats
            .iter()
            .map(|s| s.obj_reads + s.obj_writes + s.obj_lists)
            .sum();
        if backend.total() != stats_backend_ops {
            return Err(format!(
                "diagnostic backend count mismatch: classified={} database-stats={stats_backend_ops}",
                backend.total()
            )
            .into());
        }

        for (component, counts) in backend.rows() {
            self.write_backend_rows(run, num_dbs, logical_tx, component, counts)?;
        }

        let sum = |field: fn(&Stats) -> u64| stats.iter().map(field).sum();
        for (component, metric, value) in [
            ("locker", "calls", sum(|s| s.lock_calls)),
            ("coordinator", "submissions", sum(|s| s.coord_submissions)),
            ("coordinator", "rounds", sum(|s| s.coord_rounds)),
            ("coordinator", "cas_retries", sum(|s| s.lock_retries)),
            ("splitter", "candidates", sum(|s| s.split_candidates)),
            ("splitter", "completed", sum(|s| s.split_completed)),
            ("splitter", "deferred", sum(|s| s.split_deferred)),
        ] {
            self.write_metric(run, num_dbs, logical_tx, component, metric, value)?;
        }
        Ok(())
    }

    pub fn record_failure(
        &mut self,
        run: usize,
        num_dbs: usize,
        phase: &str,
        error: &dyn std::fmt::Display,
        databases: &[Database],
    ) -> std::io::Result<()> {
        writeln!(
            self.failures,
            "=== run={run} num-db={num_dbs} phase={phase} error={error} ==="
        )?;
        for (index, database) in databases.iter().enumerate() {
            writeln!(self.failures, "database={index}")?;
            writeln!(self.failures, "{}", database.diagnostics())?;
        }
        self.failures.flush()
    }

    fn write_backend_rows(
        &mut self,
        run: usize,
        num_dbs: usize,
        logical_tx: u64,
        component: &str,
        counts: OperationCounts,
    ) -> std::io::Result<()> {
        for (metric, value) in [
            ("reads", counts.reads),
            ("writes", counts.writes),
            ("lists", counts.lists),
        ] {
            self.write_metric(run, num_dbs, logical_tx, component, metric, value)?;
        }
        Ok(())
    }

    fn write_metric(
        &mut self,
        run: usize,
        num_dbs: usize,
        logical_tx: u64,
        component: &str,
        metric: &str,
        value: u64,
    ) -> std::io::Result<()> {
        writeln!(
            self.metrics,
            "{run},{num_dbs},{logical_tx},{component},{metric},{value}"
        )
    }
}

#[cfg(test)]
mod tests {
    use glassdb::backend::memory::MemoryBackend;

    use super::*;

    #[tokio::test]
    async fn writes_tidy_metrics_and_failure_state() {
        let dir = tempfile::tempdir().unwrap();
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mut session, backend) = DiagnosticSession::new(dir.path(), inner).unwrap();
        let before = session.backend_snapshot();

        let db = Database::open("diag", backend).await.unwrap();
        db.shutdown().await;
        let stats = db.stats();
        session.record_cell(1, 1, 0, &[stats], before).unwrap();
        session
            .record_failure(
                1,
                1,
                "test",
                &std::io::Error::other("synthetic failure"),
                std::slice::from_ref(&db),
            )
            .unwrap();
        drop(session);

        let metrics = std::fs::read_to_string(dir.path().join("metrics.csv")).unwrap();
        assert!(metrics.starts_with("run,num-db,logical-tx,component,metric,value\n"));
        assert!(metrics.contains("1,1,0,backend.database_metadata,reads,"));
        assert!(metrics.contains("1,1,0,coordinator,rounds,0"));

        let failures = std::fs::read_to_string(dir.path().join("failure-state.txt")).unwrap();
        assert!(failures.contains("phase=test error=synthetic failure"));
        assert!(failures.contains("coordinator dedup"));
    }
}
