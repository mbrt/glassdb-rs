//! Timestamp field encoding shared by the storage codecs.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Encodes `timestamp`, which can be before the Unix epoch.
pub(crate) fn system_to_proto_ts(timestamp: SystemTime) -> prost_types::Timestamp {
    match timestamp.duration_since(UNIX_EPOCH) {
        Ok(duration) => prost_types::Timestamp {
            seconds: duration.as_secs() as i64,
            nanos: duration.subsec_nanos() as i32,
        },
        Err(error) => {
            let duration = error.duration();
            prost_types::Timestamp {
                seconds: -(duration.as_secs() as i64),
                nanos: -(duration.subsec_nanos() as i32),
            }
        }
    }
}

/// Decodes a timestamp that [`system_to_proto_ts`] encoded.
pub(crate) fn proto_ts_to_system(timestamp: prost_types::Timestamp) -> SystemTime {
    if timestamp.seconds >= 0 {
        UNIX_EPOCH + Duration::new(timestamp.seconds as u64, timestamp.nanos.max(0) as u32)
    } else {
        UNIX_EPOCH - Duration::new((-timestamp.seconds) as u64, timestamp.nanos.unsigned_abs())
    }
}
