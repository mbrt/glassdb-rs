//! A [`Backend`] decorator that logs every operation at debug level. Ported
//! from the Go `middleware.BackendLogger` (using `tracing` instead of `slog`).

use std::sync::Arc;

use async_trait::async_trait;

use crate::{
    Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId, encode_writer_tag,
};

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

fn meta_summary(r: &Result<Metadata, BackendError>) -> String {
    match r {
        Ok(m) => format!("{m:?}"),
        Err(e) => format!("err={e}"),
    }
}

#[async_trait]
impl Backend for BackendLogger {
    async fn read_if_modified(
        &self,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        let r = self.inner.read_if_modified(path, expected_writer).await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            writer = %encode_writer_tag(expected_writer),
            res = %read_reply_summary(&r),
            "ReadIfModified"
        );
        r
    }

    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        let r = self.inner.read(path).await;
        tracing::debug!(backend_id = %self.id, path, res = %read_reply_summary(&r), "Read");
        r
    }

    async fn get_metadata(&self, path: &str) -> Result<Metadata, BackendError> {
        let r = self.inner.get_metadata(path).await;
        tracing::debug!(backend_id = %self.id, path, res = %meta_summary(&r), "GetMetadata");
        r
    }

    async fn set_tags_if(
        &self,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let r = self.inner.set_tags_if(path, expected, tags.clone()).await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            args = %format!("expv:{expected:?};t:{tags:?}"),
            res = %meta_summary(&r),
            "SetTagsIf"
        );
        r
    }

    async fn write(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let size = value.len();
        let r = self.inner.write(path, value, tags.clone()).await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            args = %format!("val[size]:{size};t:{tags:?}"),
            res = %meta_summary(&r),
            "Write"
        );
        r
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let size = value.len();
        let r = self
            .inner
            .write_if(path, value, expected, tags.clone())
            .await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            args = %format!("val[size]:{size};expv:{expected:?};t:{tags:?}"),
            res = %meta_summary(&r),
            "WriteIf"
        );
        r
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        let size = value.len();
        let r = self
            .inner
            .write_if_not_exists(path, value, tags.clone())
            .await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            args = %format!("val[size]:{size};t:{tags:?}"),
            res = %meta_summary(&r),
            "WriteIfNotExists"
        );
        r
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        let r = self.inner.delete(path).await;
        tracing::debug!(backend_id = %self.id, path, err = ?r.as_ref().err(), "Delete");
        r
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        let r = self.inner.delete_if(path, expected).await;
        tracing::debug!(
            backend_id = %self.id,
            path,
            args = %format!("expv:{expected:?}"),
            err = ?r.as_ref().err(),
            "DeleteIf"
        );
        r
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        let r = self.inner.list(dir_path).await;
        tracing::debug!(backend_id = %self.id, path = dir_path, err = ?r.as_ref().err(), "List");
        r
    }
}
