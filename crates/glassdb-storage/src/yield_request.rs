//! Typed persistence for the yield requests of a collection (ADR-110).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use glassdb_data::{CollectionAddress, ObjectPath};
use glassdb_proto as pb;
use prost::Message;

use crate::cached_store::{CachedStore, CasResult, Codec, Observation, TypedCachedStore};
use crate::error::StorageError;
use crate::timeline::Requirement;
use crate::wire_time::{proto_ts_to_system, system_to_proto_ts};

/// The request of one database instance that the other instances yield the
/// leaves of a collection to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YieldRequest {
    /// A random token of the database instance.
    pub instance: u64,
    /// The request has no effect after this time.
    pub expires_at: SystemTime,
    /// The time between two coordinator rounds of one leaf that the instance
    /// asks for.
    pub gap: Duration,
}

/// The yield requests of one collection, at most one for each database
/// instance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct YieldRequests {
    /// Sorted by instance, so that the encoding is canonical.
    requests: Vec<YieldRequest>,
}

impl YieldRequests {
    /// Returns the requests, sorted by instance.
    pub fn requests(&self) -> &[YieldRequest] {
        &self.requests
    }

    /// Returns these requests with `request` in place of the earlier request
    /// of its instance, and without the requests that expired at `now`.
    pub fn with(&self, request: YieldRequest, now: SystemTime) -> Self {
        let mut requests: Vec<YieldRequest> = self
            .requests
            .iter()
            .filter(|old| old.instance != request.instance && old.expires_at > now)
            .cloned()
            .collect();
        let at = requests.partition_point(|old| old.instance < request.instance);
        requests.insert(at, request);
        Self { requests }
    }

    fn encode(&self) -> Vec<u8> {
        pb::YieldRequests {
            requests: self
                .requests
                .iter()
                .map(|request| pb::YieldRequest {
                    instance: request.instance,
                    expires_at: Some(system_to_proto_ts(request.expires_at)),
                    gap_micros: u64::try_from(request.gap.as_micros()).unwrap_or(u64::MAX),
                })
                .collect(),
        }
        .encode_to_vec()
    }

    fn decode(body: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::YieldRequests::decode(body)
            .map_err(|e| StorageError::with_source("unmarshalling yield requests", e))?;
        let mut requests = raw
            .requests
            .into_iter()
            .map(|request| {
                let expires_at = request
                    .expires_at
                    .ok_or_else(|| StorageError::other("yield request without an expiry"))?;
                Ok(YieldRequest {
                    instance: request.instance,
                    expires_at: proto_ts_to_system(expires_at),
                    gap: Duration::from_micros(request.gap_micros),
                })
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        requests.sort_by_key(|request| request.instance);
        requests.dedup_by_key(|request| request.instance);
        Ok(Self { requests })
    }
}

impl Codec for YieldRequests {
    type Value = YieldRequests;

    fn decode(_path: &ObjectPath, body: &[u8]) -> Result<Self::Value, StorageError> {
        YieldRequests::decode(body)
    }

    fn encode(_path: &ObjectPath, value: &Self::Value) -> Result<Vec<u8>, StorageError> {
        Ok(value.encode())
    }

    fn size(value: &Self::Value) -> usize {
        std::mem::size_of::<Self::Value>()
            + value.requests.len() * std::mem::size_of::<YieldRequest>()
    }

    fn accepts(path: &ObjectPath) -> bool {
        matches!(path, ObjectPath::YieldRequests { .. })
    }

    fn name() -> &'static str {
        "yield requests"
    }
}

/// Reads and conditionally writes the yield requests of collections.
#[derive(Clone)]
pub struct YieldRequestStore {
    requests: TypedCachedStore<YieldRequests>,
}

impl YieldRequestStore {
    /// Creates a yield-request store over `objects`.
    pub fn new(objects: CachedStore) -> Self {
        Self {
            requests: objects.typed(),
        }
    }

    /// Reads the current yield requests of `collection`.
    pub async fn read(
        &self,
        collection: &CollectionAddress,
    ) -> Result<Observation<YieldRequests>, StorageError> {
        // Proof: any instance can create or change the object after this
        // instance read it, so a cached state proves nothing. The read must
        // come after the current barrier.
        let requirement = Requirement::after(self.requests.currentness_barrier());
        let path = ObjectPath::YieldRequests {
            collection: collection.clone(),
        };
        self.requests.read(path, requirement).await
    }

    /// Deletes the yield requests of `collection`, and returns whether they
    /// existed. A drop of the collection reclaims them with its nodes.
    pub async fn delete(&self, collection: &CollectionAddress) -> Result<bool, StorageError> {
        loop {
            let observed = self.read(collection).await?;
            if observed.is_absent() {
                return Ok(false);
            }
            match self.requests.delete(&observed).await {
                Ok(_) => return Ok(true),
                // Another writer changed the object after this instance read
                // it, so read it again.
                Err(StorageError::Precondition) => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Replaces the observed requests with `requests`, and returns whether
    /// the write landed. It creates the object if it was absent.
    pub async fn write(
        &self,
        expected: &Observation<YieldRequests>,
        requests: YieldRequests,
    ) -> Result<bool, StorageError> {
        let requests = Arc::new(requests);
        let result = if expected.is_absent() {
            let key = expected.path().clone();
            self.requests.create(key, Some(expected), requests).await
        } else {
            self.requests.replace(expected, requests).await
        };
        match result {
            Ok(CasResult::Applied(_)) => Ok(true),
            Ok(CasResult::Rejected) | Err(StorageError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use glassdb_backend::memory::MemoryBackend;

    use super::*;
    use crate::Timeline;

    fn store() -> YieldRequestStore {
        let objects = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            Timeline::new(),
            None,
        );
        YieldRequestStore::new(objects)
    }

    fn collection() -> CollectionAddress {
        CollectionAddress::root("db")
    }

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn request(instance: u64, expires_secs: u64) -> YieldRequest {
        YieldRequest {
            instance,
            expires_at: at(expires_secs),
            gap: Duration::from_micros(1500),
        }
    }

    #[test]
    fn a_request_replaces_the_one_of_its_instance_and_drops_expired_ones() {
        let requests = YieldRequests::default()
            .with(request(3, 10), at(0))
            .with(request(1, 5), at(0))
            .with(request(2, 20), at(0));
        assert_eq!(
            requests.requests(),
            [request(1, 5), request(2, 20), request(3, 10)]
        );

        let requests = requests.with(request(3, 30), at(6));
        assert_eq!(requests.requests(), [request(2, 20), request(3, 30)]);
    }

    #[test]
    fn requests_round_trip() {
        let requests = YieldRequests::default()
            .with(request(7, 10), at(0))
            .with(request(u64::MAX, 11), at(0));
        assert_eq!(YieldRequests::decode(&requests.encode()).unwrap(), requests);
    }

    #[tokio::test]
    async fn a_write_lands_only_on_the_observed_state() {
        let store = store();
        let absent = store.read(&collection()).await.unwrap();
        assert!(absent.is_absent());
        let one = YieldRequests::default().with(request(1, 10), at(0));
        assert!(store.write(&absent, one.clone()).await.unwrap());
        // A write from the stale absence loses.
        assert!(!store.write(&absent, one.clone()).await.unwrap());

        let current = store.read(&collection()).await.unwrap();
        assert_eq!(current.value().map(|v| (**v).clone()), Some(one.clone()));
        let two = one.with(request(2, 10), at(0));
        assert!(store.write(&current, two.clone()).await.unwrap());
        // A write from the replaced state loses.
        assert!(!store.write(&current, one).await.unwrap());

        let current = store.read(&collection()).await.unwrap();
        assert_eq!(current.value().map(|v| (**v).clone()), Some(two));

        assert!(store.delete(&collection()).await.unwrap());
        assert!(!store.delete(&collection()).await.unwrap());
        let gone = store.read(&collection()).await.unwrap();
        assert!(gone.is_absent());
    }
}
