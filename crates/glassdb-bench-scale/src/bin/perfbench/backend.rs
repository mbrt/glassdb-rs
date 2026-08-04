use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use aws_smithy_runtime_api::client::http::SharedHttpClient;
use clap::Args;
use glassdb::backend::memory::MemoryBackend;
use glassdb::middleware::{DelayBackend, DelayOptions, gcs_delays, s3_delays};
use glassdb::s3::{FakeS3, FakeS3Options, tuned_http_client};
use glassdb_backend::Backend;

#[derive(Clone, Args)]
pub(super) struct Options {
    /// Storage backend. The cloud backends use the bucket named by $BUCKET.
    #[arg(long, default_value = "memory", value_parser = ["memory", "gcs", "s3", "fakes3"], global = true)]
    backend: String,
    /// Cloud latency profile simulated by memory and fakes3.
    #[arg(long, default_value = "s3", value_parser = ["gcs", "s3"], global = true)]
    delays: String,
    /// Enable simulated per-object and per-prefix throttling.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set, global = true)]
    enable_throttling: bool,
    /// Override the simulated prefix partition depth; zero uses the profile.
    #[arg(long, default_value_t = 0, global = true)]
    prefix_depth: usize,
    /// Compress process-wide model time for synthetic backends.
    #[arg(long, default_value_t = 0.2, global = true)]
    delay_scale: f64,
    /// S3 connection-pool strategy. `churn` applies only to fakes3.
    #[arg(long, default_value = "tuned", value_parser = ["default", "tuned", "churn"], global = true)]
    http_pool: String,
}

#[derive(Clone)]
pub(super) enum Factory {
    Memory(DelayOptions),
    Gcs {
        bucket: String,
    },
    S3 {
        client: aws_sdk_s3::Client,
        bucket: String,
    },
}

impl Options {
    pub(super) async fn initialize(&self) -> Result<Factory, Box<dyn Error>> {
        match self.backend.as_str() {
            "memory" => Ok(Factory::Memory(self.delay_profile()?)),
            "gcs" => Ok(Factory::Gcs {
                bucket: required_env("BUCKET")?,
            }),
            "s3" => {
                let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
                if self.http_pool == "tuned" {
                    loader = loader.http_client(tuned_http_client());
                }
                let config = loader.load().await;
                let client = aws_sdk_s3::Client::from_conf(
                    aws_sdk_s3::config::Builder::from(&config).build(),
                );
                Ok(Factory::S3 {
                    client,
                    bucket: required_env("BUCKET")?,
                })
            }
            "fakes3" => self.initialize_fakes3().await,
            other => Err(format!("unknown backend {other:?}").into()),
        }
    }

    pub(super) fn label(&self) -> &str {
        &self.backend
    }

    /// Returns the model-time speedup for a synthetic backend. Real providers
    /// stay on live wall time because their clocks cannot share this process's
    /// model-time anchor.
    pub(super) fn model_time_speedup(&self) -> Result<Option<f64>, Box<dyn Error>> {
        match self.backend.as_str() {
            "memory" | "fakes3" => {
                if !(self.delay_scale > 0.0 && self.delay_scale.is_finite()) {
                    return Err(
                        format!("--delay-scale must be > 0, got {}", self.delay_scale).into(),
                    );
                }
                let speedup = 1.0 / self.delay_scale;
                if !speedup.is_finite() {
                    return Err(format!(
                        "--delay-scale is too small to represent a model-time speedup: {}",
                        self.delay_scale
                    )
                    .into());
                }
                Ok(Some(speedup))
            }
            _ => Ok(None),
        }
    }

    fn delay_profile(&self) -> Result<DelayOptions, Box<dyn Error>> {
        let mut delays = match self.delays.as_str() {
            "gcs" => gcs_delays(),
            "s3" => s3_delays(),
            other => return Err(format!("unknown delay profile {other:?}").into()),
        };
        if !self.enable_throttling {
            delays.same_obj_write_ps = 100_000;
            delays.prefix_read_ps = 0;
            delays.prefix_write_ps = 0;
        }
        if self.prefix_depth > 0 {
            delays.prefix_depth = self.prefix_depth;
        }
        Ok(delays)
    }

    async fn initialize_fakes3(&self) -> Result<Factory, Box<dyn Error>> {
        let fake = FakeS3::start_with(FakeS3Options {
            latency: Some(self.delay_profile()?),
            conn_counter: None,
        })
        .await;
        let mut config = fake.client_config();
        if let Some(client) = fake_http_client(&self.http_pool) {
            config = config.http_client(client);
        }
        Ok(Factory::S3 {
            client: aws_sdk_s3::Client::from_conf(config.build()),
            bucket: "bench".to_string(),
        })
    }
}

impl Factory {
    /// Creates an independent middleware instance over the selected storage.
    pub(super) fn backend(&self) -> Arc<dyn Backend> {
        match self {
            Factory::Memory(delays) => {
                Arc::new(DelayBackend::new(Arc::new(MemoryBackend::new()), *delays))
            }
            Factory::Gcs { bucket } => Arc::new(glassdb::gcs::GcsBackend::new(bucket.clone())),
            Factory::S3 { client, bucket } => {
                Arc::new(glassdb::s3::S3Backend::new(client.clone(), bucket.clone()))
            }
        }
    }
}

fn fake_http_client(pool: &str) -> Option<SharedHttpClient> {
    match pool {
        "default" => None,
        "churn" => Some(plaintext_http_client(Some(Duration::from_millis(1)))),
        _ => Some(plaintext_http_client(Some(Duration::from_secs(90)))),
    }
}

fn plaintext_http_client(pool_idle_timeout: Option<Duration>) -> SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .pool_idle_timeout(pool_idle_timeout)
        .build_http()
}

fn required_env(name: &str) -> Result<String, Box<dyn Error>> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Ok(value),
        _ => Err(format!("environment variable ${name} is required").into()),
    }
}
