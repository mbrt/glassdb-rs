//! A [`Backend`] decorator that logs every operation at debug level. Ported
//! from the Go `middleware.BackendLogger` (using `tracing` instead of `slog`).

use std::sync::Arc;

use async_trait::async_trait;

use crate::{Backend, BackendError, ListCursor, ListLimit, ListPage, ReadReply, Version};

/// A [`Backend`] decorator that emits a `tracing` debug event for every
/// operation, tagged with the configured backend id.
pub struct BackendLogger {
    inner: Arc<dyn Backend>,
    id: String,
}

impl BackendLogger {
    /// Wraps `inner`, labelling each logged operation with `id`.
    pub fn new(inner: Arc<dyn Backend>, id: impl Into<String>) -> Self {
        BackendLogger {
            inner,
            id: id.into(),
        }
    }
}

fn read_reply_summary(r: &Result<ReadReply, BackendError>) -> String {
    match r {
        Ok(r) => format!("{{cont[size]={}, v={:?}}}", r.contents.len(), r.version),
        Err(e) => format!("err={e}"),
    }
}

fn version_summary(r: &Result<Version, BackendError>) -> String {
    match r {
        Ok(v) => format!("{v:?}"),
        Err(e) => format!("err={e}"),
    }
}

#[async_trait]
impl Backend for BackendLogger {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        let r = self.inner.read(path).await;
        tracing::debug!(backend_id = %self.id, path, res = %read_reply_summary(&r), "Read");
        r
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        let r = self.inner.read_if_modified(path, expected).await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            expv = %format!("{expected:?}"),
            res = %read_reply_summary(&r),
            "ReadIfModified"
        );
        r
    }

    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
        let size = value.len();
        let r = self.inner.write(path, value).await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            args = %format!("val[size]:{size}"),
            res = %version_summary(&r),
            "Write"
        );
        r
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        let size = value.len();
        let r = self.inner.write_if(path, value, expected).await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            args = %format!("val[size]:{size};expv:{expected:?}"),
            res = %version_summary(&r),
            "WriteIf"
        );
        r
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        let size = value.len();
        let r = self.inner.write_if_not_exists(path, value).await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            args = %format!("val[size]:{size}"),
            res = %version_summary(&r),
            "WriteIfNotExists"
        );
        r
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        let r = self.inner.delete(path).await;
        tracing::debug!(backend_id = %self.id, path, err = ?r.as_ref().err(), "Delete");
        r
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        let r = self.inner.list(prefix, cursor, limit).await;
        tracing::debug!(
            backend_id = %self.id,
            path = prefix,
            cursor = ?cursor,
            limit = limit.get(),
            err = ?r.as_ref().err(),
            "List"
        );
        r
    }
}
