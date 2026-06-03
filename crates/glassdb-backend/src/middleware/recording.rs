//! A [`Backend`] decorator that records every operation it forwards into an
//! ordered, shared in-memory log.
//!
//! This underpins the deterministic-simulation self-check (see ADR-008): two
//! runs of the same madsim seed must issue a *byte-for-byte identical* sequence
//! of backend operations. Two different interleavings can reach the same final
//! state while issuing different operations, so comparing only the final result
//! is not enough — an identical operation stream is what proves the schedule
//! itself replayed deterministically.
//!
//! Each record captures the method tag and a canonical encoding of every
//! argument that crosses the boundary (path, value, tags, expected version, and
//! writer id). The recording order is the call-issue order; under a
//! deterministic madsim schedule that order is itself deterministic, which is
//! exactly the property under test.

use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use glassdb_concurr::Ctx;

use crate::{Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId};

/// A single recorded backend operation: the method tag, the primary path, and a
/// canonical encoding of the remaining arguments.
#[derive(Clone, PartialEq, Eq)]
pub struct OpRecord {
    /// The backend method name (e.g. `"write_if"`).
    pub op: &'static str,
    /// The object (or directory) path the call targeted.
    pub path: String,
    /// Canonical little-endian, length-prefixed encoding of the remaining
    /// arguments (value, tags, expected version, writer id), in the order they
    /// appear in the method signature.
    pub args: Vec<u8>,
}

impl fmt::Debug for OpRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(path={:?}", self.op, self.path)?;
        if !self.args.is_empty() {
            write!(f, ", args=0x")?;
            for b in &self.args {
                write!(f, "{b:02x}")?;
            }
        }
        write!(f, ")")
    }
}

/// An ordered, shared log of recorded operations.
pub type OpLog = Arc<Mutex<Vec<OpRecord>>>;

fn enc_bytes(buf: &mut Vec<u8>, b: &[u8]) {
    buf.extend_from_slice(&(b.len() as u64).to_le_bytes());
    buf.extend_from_slice(b);
}

fn enc_tags(buf: &mut Vec<u8>, tags: &Tags) {
    // `Tags` is a `BTreeMap`, so iteration is already in sorted key order.
    buf.extend_from_slice(&(tags.len() as u64).to_le_bytes());
    for (k, v) in tags {
        enc_bytes(buf, k.as_bytes());
        enc_bytes(buf, v.as_bytes());
    }
}

fn enc_version(buf: &mut Vec<u8>, v: &Version) {
    enc_bytes(buf, v.token.as_bytes());
}

/// A [`Backend`] decorator that appends every forwarded call to a shared
/// [`OpLog`] before delegating to the wrapped backend.
pub struct RecordingBackend {
    inner: Arc<dyn Backend>,
    log: OpLog,
}

impl RecordingBackend {
    /// Wraps `inner` with a fresh, empty log. Retrieve it with [`Self::log`].
    pub fn new(inner: Arc<dyn Backend>) -> Self {
        RecordingBackend {
            inner,
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Wraps `inner`, appending to the caller-provided shared log.
    pub fn with_log(inner: Arc<dyn Backend>, log: OpLog) -> Self {
        RecordingBackend { inner, log }
    }

    /// Returns a handle to the shared operation log.
    pub fn log(&self) -> OpLog {
        self.log.clone()
    }

    fn record(&self, op: &'static str, path: &str, args: Vec<u8>) {
        self.log.lock().unwrap().push(OpRecord {
            op,
            path: path.to_string(),
            args,
        });
    }
}

#[async_trait]
impl Backend for RecordingBackend {
    async fn read_if_modified(
        &self,
        ctx: &Ctx,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        let mut args = Vec::new();
        enc_bytes(&mut args, expected_writer.as_bytes());
        self.record("read_if_modified", path, args);
        self.inner
            .read_if_modified(ctx, path, expected_writer)
            .await
    }

    async fn read(&self, ctx: &Ctx, path: &str) -> Result<ReadReply, BackendError> {
        self.record("read", path, Vec::new());
        self.inner.read(ctx, path).await
    }

    async fn get_metadata(&self, ctx: &Ctx, path: &str) -> Result<Metadata, BackendError> {
        self.record("get_metadata", path, Vec::new());
        self.inner.get_metadata(ctx, path).await
    }

    async fn set_tags_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let mut args = Vec::new();
        enc_version(&mut args, expected);
        enc_tags(&mut args, &tags);
        self.record("set_tags_if", path, args);
        self.inner.set_tags_if(ctx, path, expected, tags).await
    }

    async fn write(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let mut args = Vec::new();
        enc_bytes(&mut args, &value);
        enc_tags(&mut args, &tags);
        self.record("write", path, args);
        self.inner.write(ctx, path, value, tags).await
    }

    async fn write_if(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let mut args = Vec::new();
        enc_bytes(&mut args, &value);
        enc_version(&mut args, expected);
        enc_tags(&mut args, &tags);
        self.record("write_if", path, args);
        self.inner.write_if(ctx, path, value, expected, tags).await
    }

    async fn write_if_not_exists(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let mut args = Vec::new();
        enc_bytes(&mut args, &value);
        enc_tags(&mut args, &tags);
        self.record("write_if_not_exists", path, args);
        self.inner.write_if_not_exists(ctx, path, value, tags).await
    }

    async fn delete(&self, ctx: &Ctx, path: &str) -> Result<(), BackendError> {
        self.record("delete", path, Vec::new());
        self.inner.delete(ctx, path).await
    }

    async fn delete_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
    ) -> Result<(), BackendError> {
        let mut args = Vec::new();
        enc_version(&mut args, expected);
        self.record("delete_if", path, args);
        self.inner.delete_if(ctx, path, expected).await
    }

    async fn list(&self, ctx: &Ctx, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.record("list", dir_path, Vec::new());
        self.inner.list(ctx, dir_path).await
    }
}

/// Returns the index of the first position at which two operation logs differ,
/// along with the records there (if any), or `None` if the logs are identical.
/// Useful for pinpointing where a schedule diverged.
pub fn first_divergence(
    a: &[OpRecord],
    b: &[OpRecord],
) -> Option<(usize, Option<OpRecord>, Option<OpRecord>)> {
    let n = a.len().max(b.len());
    for i in 0..n {
        let ai = a.get(i);
        let bi = b.get(i);
        if ai != bi {
            return Some((i, ai.cloned(), bi.cloned()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryBackend;

    #[tokio::test]
    async fn records_ops_in_call_order() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let rec = RecordingBackend::new(inner);
        let log = rec.log();
        let ctx = Ctx::background();

        rec.write(&ctx, "a/b", b"v".to_vec(), Tags::new())
            .await
            .unwrap();
        let _ = rec.read(&ctx, "a/b").await;
        let _ = rec.get_metadata(&ctx, "a/b").await;

        let recorded = log.lock().unwrap();
        let ops: Vec<&str> = recorded.iter().map(|r| r.op).collect();
        assert_eq!(ops, vec!["write", "read", "get_metadata"]);
        assert_eq!(recorded[0].path, "a/b");
        // The write encoded its value and (empty) tags into args.
        assert!(!recorded[0].args.is_empty());
        assert!(recorded[1].args.is_empty());
    }

    #[test]
    fn first_divergence_detects_mismatch() {
        let mk = |op: &'static str| OpRecord {
            op,
            path: "p".into(),
            args: Vec::new(),
        };
        let a = vec![mk("read"), mk("write"), mk("delete")];
        let b = vec![mk("read"), mk("get_metadata"), mk("delete")];
        let (i, ar, br) = first_divergence(&a, &b).unwrap();
        assert_eq!(i, 1);
        assert_eq!(ar.unwrap().op, "write");
        assert_eq!(br.unwrap().op, "get_metadata");

        assert!(first_divergence(&a, &a).is_none());
    }
}
