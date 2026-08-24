# Choose concurrency limits and verification

Type: prototype
Status: open
Blocked by: 02, 05, 06, 07, 08

## Question

Which initial internal concurrency limit should each phase use, can one value serve all phases, and what deterministic tests and benchmarks prove the agreed latency-wave, one-leaf, active-work, serial-fallback, and throughput contracts? Use gated distinct-path tests and 1, 2, 8, and 32-leaf measurements with warm and cold caches.
