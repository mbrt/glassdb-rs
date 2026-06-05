//! Process CPU-time accounting, ported from the Go `rtbench/cpu_linux.go`.
//!
//! Comparing the CPU-time delta over a step against the wall-clock time and the
//! core count tells us whether a throughput plateau is a client CPU bottleneck.

use std::time::Duration;

/// Returns the cumulative user and system CPU time consumed by the whole
/// process so far, via `getrusage(RUSAGE_SELF)`. Returns zeros on non-Unix
/// platforms or on error.
#[cfg(unix)]
pub fn process_cpu_time() -> (Duration, Duration) {
    // SAFETY: `getrusage` fills a caller-owned `rusage`; we zero it first and
    // only read scalar fields out of it afterward.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut ru) != 0 {
            return (Duration::ZERO, Duration::ZERO);
        }
        (timeval_to_dur(ru.ru_utime), timeval_to_dur(ru.ru_stime))
    }
}

#[cfg(unix)]
fn timeval_to_dur(tv: libc::timeval) -> Duration {
    let secs = tv.tv_sec.max(0) as u64;
    let usecs = tv.tv_usec.max(0) as u64;
    Duration::from_secs(secs) + Duration::from_micros(usecs)
}

#[cfg(not(unix))]
pub fn process_cpu_time() -> (Duration, Duration) {
    (Duration::ZERO, Duration::ZERO)
}
