# Choose concurrency limits and verification

Type: prototype
Status: open
Blocked by: 02, 05, 06, 07, 08

## Question

Which initial internal concurrency limit should each bounded phase use, can one value serve all phases, and what deterministic tests and benchmarks prove the agreed latency-wave, one-leaf, maximum-incomplete-future, all-input, stable-output, retained-lock retry, serial-fallback, and throughput contracts? Include foreign-holder waits that occupy bounded positions, a cached complete same-identity hold that adds no CAS, uncertain-CAS reconciliation, and renewed-identity entry into sorted serial acquisition. Use gated distinct-path tests plus 1, 2, 8, and 32-leaf measurements with warm and cold caches.
