//! Seeded per-operation latency for the fake S3 server.

use std::sync::Mutex;
use std::time::Duration;

use glassdb_backend::middleware::{Latency, Lognormal, ProviderLatencyProfile};
use glassdb_concurr::{Rng as DeterministicRng, rt};
use hyper::Method;

/// Per-operation lognormal latency, derived from a [`ProviderLatencyProfile`].
/// HTTP methods are mapped to the backend operation they implement so the
/// served latencies match the simulated `DelayBackend`.
pub(super) struct LatencyModel {
    get: Lognormal,
    head: Lognormal,
    put: Lognormal,
    delete: Lognormal,
    list: Lognormal,
    entropy: Mutex<DeterministicRng>,
}

impl LatencyModel {
    pub(super) fn from_profile(profile: ProviderLatencyProfile, entropy_seed: u64) -> Self {
        LatencyModel {
            get: latency_distribution(profile.obj_read),
            head: latency_distribution(profile.meta_read),
            put: latency_distribution(profile.obj_write),
            delete: latency_distribution(profile.obj_write),
            list: latency_distribution(profile.list),
            entropy: Mutex::new(DeterministicRng::new(entropy_seed)),
        }
    }

    pub(super) async fn sleep_for(&self, method: &Method, is_list: bool) {
        let Some(millis) = self.sample_millis(method, is_list) else {
            return;
        };
        let secs = millis / 1_000.0;
        if secs.is_finite() && secs > 0.0 {
            rt::sleep(Duration::from_secs_f64(secs)).await;
        }
    }

    fn sample_millis(&self, method: &Method, is_list: bool) -> Option<f64> {
        let distribution = if is_list {
            &self.list
        } else {
            match *method {
                Method::GET => &self.get,
                Method::HEAD => &self.head,
                Method::PUT => &self.put,
                Method::DELETE => &self.delete,
                _ => return None,
            }
        };
        Some({
            let mut entropy = self.entropy.lock().unwrap();
            distribution.sample(&mut *entropy)
        })
    }
}

fn latency_distribution(latency: Latency) -> Lognormal {
    let mean_ms = latency.mean.as_secs_f64() * 1_000.0;
    let standard_deviation_ms = if mean_ms == 0.0 {
        // The former fake-server distribution treated every zero-mean profile
        // as zero latency, even if its deviation was non-zero. Retain that
        // behavior while sharing the validated distribution implementation.
        0.0
    } else {
        latency.std_dev.as_secs_f64() * 1_000.0
    };
    Lognormal::new(mean_ms, standard_deviation_ms)
        .expect("Duration-based fake S3 latency is representable")
}

#[cfg(test)]
mod tests {
    use glassdb_backend::middleware::s3_delays;

    use super::*;

    fn delay_sequence(seed: u64) -> [u64; 6] {
        let model = LatencyModel::from_profile(s3_delays().latency, seed);
        let samples = [
            (&Method::GET, false),
            (&Method::HEAD, false),
            (&Method::PUT, false),
            (&Method::DELETE, false),
            (&Method::GET, true),
            (&Method::GET, false),
        ];
        samples.map(|(method, is_list)| model.sample_millis(method, is_list).unwrap().to_bits())
    }

    #[test]
    fn latency_entropy_replays_by_seed_and_diverges_across_seeds() {
        let first = delay_sequence(0xF2_5D);
        assert_eq!(first, delay_sequence(0xF2_5D));
        assert_ne!(first, delay_sequence(0xF2_5E));
    }
}
