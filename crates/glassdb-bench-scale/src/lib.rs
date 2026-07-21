//! Shared utilities for the GlassDB benchmark binaries (`rtbench`,
//! `backendbench`), ported from the Go `internal/testkit/bench` package.

use std::time::Duration;

pub mod backend_breakdown;
pub mod bench;
pub mod run;

/// Parses a duration like `20s`, `500ms`, `2m`, or `1h` (a small subset of
/// Go's `time.Duration` syntax) for the CLI flags. A bare number is seconds.
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = s
        .find(|c: char| c.is_alphabetic())
        .map(|i| s.split_at(i))
        .unwrap_or((s, "s"));
    let v: f64 = num.parse().map_err(|_| format!("invalid duration {s:?}"))?;
    let secs = match unit {
        "ms" => v / 1000.0,
        "s" | "" => v,
        "m" => v * 60.0,
        "h" => v * 3600.0,
        other => return Err(format!("unknown duration unit {other:?}")),
    };
    Ok(Duration::from_secs_f64(secs))
}
