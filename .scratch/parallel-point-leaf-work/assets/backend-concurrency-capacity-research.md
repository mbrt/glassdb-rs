# Backend concurrency capacity research

## Question

If 500 large transactions can each admit 16 leaf operations, can GlassDB safely
have 8,000 operations in flight? Does this also mean 8,000 TCP/TLS connections?
What load target and limit structure should the design use?

## Result

Eight thousand incomplete leaf futures do **not** always mean 8,000 network
connections. The mapping depends on the HTTP version, the connection pool, and
the server's stream limits. However, an HTTP/1.1 burst can approach one active
connection for each active request. A pool reduces connection setup on later
requests, but it does not make the first concurrent burst small.

The provider documents show that the object stores can scale beyond their
initial request rates. They do not prove that this workload is safe. An
unbounded 8,000-request burst is not a suitable default for one GlassDB
backend. Host file descriptors, NAT ports, TLS and DNS work, network bandwidth,
provider ramp-up, and retries can all become the first limit.

Use 500 concurrent large transactions as a stress and long-duration target. Do not use
it as the active backend-request target. Keep the per-transaction limit of 16
for isolated transaction latency, but put it below a shared limit for active
backend operations:

```text
maximum active bounded-phase backend operations <= min(active_transactions * 16, shared_limit)
```

The sources do not give one correct value for `shared_limit`. Provider guidance
says to find the saturation point by measurement. Use 512 as the center
candidate, and sweep 64, 128, 256, 512, and 1,024 on each real transport. Also
keep the current unbounded behavior as a comparison cell. Select the smallest
value that is within 5% of the best stable throughput and that meets the
accepted p95 and p99 latency. A source-based choice of 8,000 is not justified.

Eight thousand connections are technically possible in a suitably configured
host and network. They are not a portable or demonstrated GlassDB target. In
particular, the current GCS transport uses HTTP/1.1, and the current S3 source
records a previous connection-ramp collapse at only a few hundred operations.
The S3 async DNS change removes that specific cause; it does not remove file
descriptor, NAT, TLS, or provider limits.

## Requests are not connections

The terms must stay separate:

- An **incomplete leaf future** is a leaf operation future that has not
  completed. It can still be waiting for admission, credentials, DNS, a stream,
  or a retry delay.
- A **provider request** is one HTTP API attempt. One leaf operation can cause
  more than one provider request, especially after a retry.
- A **connection** is a TCP/TLS channel for HTTP/1.1 or HTTP/2, or a QUIC
  channel for HTTP/3. A connection can serve a series of requests. HTTP/2 and
  HTTP/3 can also serve concurrent streams on one connection.

Hyper's pool makes the transport difference explicit: an HTTP/1 connection has
a unique reservation, while an HTTP/2 connection has a shared reservation and
can serve multiple requests. Its pool also prevents duplicate connection work
for one HTTP/2 host, but not for HTTP/1
([Hyper pool source](https://docs.rs/hyper-util/0.1.20/src/hyper_util/client/legacy/pool.rs.html#56-80),
[connection selection](https://docs.rs/hyper-util/0.1.20/src/hyper_util/client/legacy/pool.rs.html#173-198)).

The AWS Smithy Rust TLS connector enables both HTTP/1 and HTTP/2. It marks a
connection as HTTP/2 only when ALPN reports `h2`
([Smithy Rust TLS source](https://docs.rs/aws-smithy-http-client/1.1.13/src/aws_smithy_http_client/client/tls/rustls_provider.rs.html#203-210),
[ALPN detection](https://docs.rs/aws-smithy-http-client/1.1.13/src/aws_smithy_http_client/client/tls/rustls_provider.rs.html#405-433)).
Thus, support in the client does not prove that a given S3 request used HTTP/2.
The negotiated protocol must be measured.

The Smithy connector keeps idle sockets for 90 seconds by default
([`pool_idle_timeout`](https://docs.rs/aws-smithy-http-client/1.1.13/aws_smithy_http_client/struct.ConnectorBuilder.html#method.pool_idle_timeout)).
The Hyper client default permits an unlimited number of *idle* connections per
host
([Hyper client source](https://docs.rs/hyper-util/0.1.20/src/hyper_util/client/legacy/client.rs.html#1041-1046),
[`pool_max_idle_per_host`](https://docs.rs/hyper-util/0.1.20/src/hyper_util/client/legacy/client.rs.html#1090-1095)).
These are reuse settings. They are not a limit on active requests or active
connections. Therefore, a large HTTP/1 burst can also leave a large idle pool
for the idle timeout.

Google Cloud Storage supports HTTP/1.1, HTTP/2, and HTTP/3
([Cloud Storage request endpoints](https://cloud.google.com/storage/docs/request-endpoints)).
Google's HTTP guidance says to reuse HTTP/1.1 connections and avoid HTTP/1.1
pipelining. It also says that HTTP/2 and HTTP/3 multiplex concurrent requests
on one connection. A server can still limit concurrent streams
([Google Cloud HTTP guidelines](https://cloud.google.com/apis/docs/http#channels)).
The client configuration, and not provider support alone, determines which case
GlassDB uses.

## Current GlassDB transports

The current GCS backend creates one reusable `reqwest::Client`, but its Cargo
dependency disables reqwest default features and enables `json`, `query`, and
`rustls` only. In reqwest 0.13.4, `http2` is a separate feature, and `rustls`
does not enable it. The current dependency graph does not enable `http2` by
feature unification. Thus, GlassDB GCS must be treated as HTTP/1.1
([GCS dependency](../../../crates/glassdb-backend-gcs/Cargo.toml),
[reqwest features](https://docs.rs/crate/reqwest/0.13.4/source/Cargo.toml.orig#346-411),
[GCS client construction](../../../crates/glassdb-backend-gcs/src/lib.rs)). A
cold burst of 8,000 active GCS requests can therefore approach 8,000 active
TCP/TLS connections. Pool reuse helps after connections become idle; it does
not provide an active-connection cap.

One uncached GCS `Backend::read` normally makes two sequential provider
requests: one metadata GET and one media GET. `read_if_modified` normally makes
one conditional media GET. A conditional write normally makes one multipart
upload request. Concurrent rewrites and higher-level retries can add attempts
([GCS read and write paths](../../../crates/glassdb-backend-gcs/src/lib.rs)).
Thus, neither the leaf-operation count nor the `Backend` call count is the
provider request count.

The tuned S3 client uses one reusable Smithy HTTP client with a 90-second idle
timeout and an async DNS resolver. Its transport can negotiate HTTP/2, so many
requests can use fewer connections when the endpoint selects `h2`. It has no
configured hard cap for active requests or connections. The source explains
that the earlier default DNS path could saturate Tokio's 512-thread blocking
pool during a burst of a few hundred operations
([tuned S3 client](../../../crates/glassdb-backend-s3/src/tuned_http.rs),
[async DNS resolver](../../../crates/glassdb-backend-s3/src/dns.rs)). This is
direct local evidence that host-side behavior can reach a limit well below
8,000 even when the provider permits more connections.

The in-process fake S3 server uses HTTP/1.1 and can count accepted TCP
connections. It is suitable for the cold-pool and connection-churn comparison,
but not for proving real S3 capacity
([fake S3 transport](../../../crates/glassdb-backend-s3/src/fake_server/server.rs)).

## Provider request-rate guidance

### Amazon S3

For a general-purpose S3 bucket, AWS documents at least 3,500
PUT/COPY/POST/DELETE requests per second and 5,500 GET/HEAD requests per second
for each partitioned prefix. More prefixes can add capacity. Scaling to a new
rate is gradual, and temporary `503 Slow Down` responses can occur
([S3 performance](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html)).

AWS states that S3 has no connection-count limit for a bucket. AWS recommends
multiple concurrent requests over separate connections for throughput
([S3 performance guidelines](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-guidelines.html#optimizing-performance-guidelines-scale-horizontally)).
This is a provider-side statement. It is not a host or client guarantee.

### Google Cloud Storage

A bucket initially supports approximately 1,000 object writes per second and
5,000 object reads per second. Above these rates, Google says to ramp the rate
no faster than doubling it over 20 minutes. A faster increase can cause higher
latency and error rates
([Cloud Storage request-rate guidance](https://cloud.google.com/storage/docs/request-rate)).

Google lists the eventual per-bucket object read and write rates as unlimited,
after scaling, but separate bandwidth limits still apply
([Cloud Storage quotas](https://cloud.google.com/storage/quotas#objects)).
This means that 8,000 requests per second can be feasible. It does not mean
that a cold bucket should receive an immediate 8,000-request-per-second burst.

## Concurrency-to-rate arithmetic

Under steady saturation, an approximate relation is:

```text
request rate = active requests / mean request service time
```

This is an estimate, not a provider promise. Retries increase the actual request
rate.

| Active requests | Mean service time | Approximate completion rate |
| ---: | ---: | ---: |
| 8,000 | 100 ms | 80,000 requests/s |
| 8,000 | 200 ms | 40,000 requests/s |
| 8,000 | 1 s | 8,000 requests/s |
| 256 | 100 ms | 2,560 requests/s |
| 256 | 200 ms | 1,280 requests/s |

The result shows why the transaction count cannot select the backend limit.
The request latency, read/write mix, logical-key distribution, retry rate, and
number of application instances are necessary inputs.

The current simulator supplies a more specific starting estimate. Its mean
provider latencies are 22 ms for an S3 object read, 55 ms for an S3 object
write, 57 ms for a GCS object-data read, and 70 ms for a GCS object write. At
the documented initial or per-prefix request rates, the corresponding
concurrency products are:

| Model cell | Documented rate | Mean latency | Rate x latency |
| --- | ---: | ---: | ---: |
| S3 read, one partitioned prefix | 5,500/s | 22 ms | about 121 |
| S3 write, one partitioned prefix | 3,500/s | 55 ms | about 193 |
| GCS object-data read, initial bucket rate | 5,000/s | 57 ms | about 285 |
| GCS object write, initial bucket rate | 1,000/s | 70 ms | about 70 |

The latency values are a GlassDB simulation model, not provider guarantees
([delay profiles](../../../crates/glassdb-backend/src/middleware/delay.rs)). A
full uncached GCS read also has a metadata request before the object-data
request. The table only shows that a candidate near 512 is large enough to
cross the modeled initial knees. It does not select 512 without a real-backend
test.

For the proposed stress workload, 500 transactions times 32 distinct leaves is
16,000 leaf operations in one phase. The per-transaction limit admits at most
500 times 16, or 8,000, incomplete leaf futures at once. These are different
numbers. With equal operation duration and a fully synchronized phase, a
shared backend limit adds process-wide service waves as follows:

| Shared limit | Waves for 16,000 operations | Approximate phase drain at 100 ms/operation |
| ---: | ---: | ---: |
| 64 | 250 | 25 s |
| 128 | 125 | 12.5 s |
| 256 | 63 | 6.3 s |
| 512 | 32 | 3.2 s |
| 1,024 | 16 | 1.6 s |
| no shared limit | 2 transaction-local waves | 0.2 s |

This is an intentionally severe synchronized estimate. Cache hits, phase
staggering, unequal latency, and provider saturation change it. It makes the
trade-off explicit: a shared limit protects capacity by adding admission
latency. If all 500 transactions must finish this phase with near-isolated
latency, the deployment needs tens of thousands of provider requests per
second. A larger local limit cannot create that provider capacity.

## Host and network limits

### File descriptors and local ports

On Linux, `RLIMIT_NOFILE` bounds the file descriptors that one process can
open. Exceeding it gives `EMFILE`
([Linux `getrlimit(2)`](https://man7.org/linux/man-pages/man2/getrlimit.2.html)).
Each TCP socket uses a file descriptor, and sockets are not the process's only
file descriptors. The value depends on the deployment, so the benchmark must
record the actual soft and hard limits.

Linux also selects automatic TCP and UDP source ports from
`ip_local_port_range`. The documented default range is 32768 through 60999,
before reserved ports are removed
([Linux IP sysctl](https://docs.kernel.org/networking/ip-sysctl.html#ip-variables)).
Eight thousand connections fit numerically inside that default range, but
other connections and connection churn use the same host resource. The test
must record the configured range and socket states instead of treating the
default as guaranteed capacity.

### AWS NAT

One AWS NAT gateway IPv4 address supports up to 55,000 simultaneous connections
to each unique destination, where a destination is the destination IP, port,
and protocol. More gateway addresses add capacity
([AWS NAT gateway limits](https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-working-with.html#nat-gateway-edit-secondary-ip-addresses)).
Thus, 8,000 actual connections are below this specific limit. Other workloads,
connection churn, and destination distribution still consume the same network
resources.

The limit is irrelevant when NAT is not on the route. In the same AWS Region,
an S3 gateway endpoint lets a VPC access S3 without an internet gateway or NAT
device
([S3 gateway endpoints](https://docs.aws.amazon.com/vpc/latest/privatelink/vpc-endpoints-s3.html)).

### Google Cloud NAT

Cloud NAT reserves source IP and port tuples for each VM. The allocation limits
connections from that VM to one destination IP, port, and protocol. For
example, an allocation of 1,024 ports permits 1,024 simultaneous connections to
one such destination. Closed TCP mappings are not reusable for 120 seconds by
default
([Cloud NAT ports and connections](https://cloud.google.com/nat/docs/ports-and-addresses#ports-and-connections)).

Public NAT uses a default static allocation of 64 ports per VM. Dynamic
allocation starts at 32 ports per VM unless configured otherwise, and it can
grow to a configured maximum
([Cloud NAT port allocation](https://cloud.google.com/nat/docs/ports-and-addresses#port-reservation-procedure)).
Therefore, thousands of HTTP/1.1 connections from one VM are not feasible with
the default allocation. Cloud NAT must be configured from measured peak port
use, or it must not be on this path
([Cloud NAT tuning](https://cloud.google.com/nat/docs/tune-nat-configuration#choose-minimum-ports)).

Private Google Access lets a VM with only an internal IP address reach Google
APIs and services, including Cloud Storage
([Private Google Access](https://cloud.google.com/vpc/docs/private-google-access)).
This is one deployment where an ordinary Internet NAT path does not have to set
the Cloud Storage connection budget.

## Recommended load target and limit structure

Use two separate targets:

1. **Workload target:** 500 concurrent transactions with 32 distinct leaves.
   This is a stress and long-duration case. It verifies bounded resource use and stable
   throughput under a large queue. It is not a promise that all leaf operations
   start at the same time.
2. **Backend target:** a measured shared active-operation limit. The limit is
   shared by all transactions and `Database` instances in one process that use
   the same provider capacity domain. Do not create an independent limit for
   each `Database`, because those limits multiply again. Do not couple unrelated
   buckets or provider endpoints. The per-transaction limit of 16 remains below
   this shared limit.

The shared limit changes the 500-transaction case from a possible 8,000 active
backend operations to at most `shared_limit`. Waiting transactions pay queueing
latency, but they do not create more provider pressure. Above the provider's
saturation point, a larger active set cannot improve steady throughput. It can
only add queueing, memory use, connections, and retry pressure.

Use this calibration matrix:

- Transaction concurrency: 1, 8, 32, 128, and 500.
- Leaves per transaction: 1, 8, and 32.
- Shared active-operation limits: 64, 128, 256, 512, and 1,024.
- Control: the current behavior with no shared limit.
- Backend: real S3 and real Cloud Storage, in addition to simulations.
- Cache: warm and cold.
- Connection pool: cold burst and warm steady state.
- Client layout: one and multiple `Database` instances that share the same
  capacity domain.
- Mix: read-only, blind overwrite, and read-modify-write transactions.

Record these values for each cell:

- transaction throughput and p50, p95, and p99 latency;
- maximum active backend operations;
- provider requests per second and requests per transaction;
- negotiated HTTP version, open connections, new connections, and TLS
  handshakes;
- process file-descriptor count, CPU, memory, and network use;
- provider `429`, `503`, timeout, and retry counts;
- NAT port use when NAT is on the route.

Keep the smallest shared limit that is within 5% of the best stable throughput,
meets the accepted p95 and p99 latency objectives, has no sustained provider
rate-limit errors, and leaves explicit headroom in the file-descriptor and NAT
budgets. Run the 500-transaction case long enough to include provider ramp-up
and connection-pool steady state. Also run a cold burst, because that is where
connection creation and provider auto-scaling are most different from steady
state.

Treat 512 as a provisional center candidate only. If 256 is within 5% of its
throughput and has better p95 or p99 behavior under overload, select 256. If
1,024 gives more than 5% additional stable throughput and stays inside the
resource and error budgets, select 1,024. If the 500-transaction latency target
cannot be met at the stable throughput plateau, the answer is more provider or
deployment capacity, not an automatic increase toward 8,000.

## Decision implication

The earlier statement that a per-transaction limit does not need a shared
backend limit is incomplete. It controls the cost of one transaction only. The
product of transaction concurrency and per-transaction parallel work remains
unbounded.

The implementation-ready design should therefore specify a shared active
backend-operation bound, its provider-capacity ownership scope, and fair
admission between transactions. It should count active storage work, not leaf
futures that are asleep while they wait for a foreign transaction or a retry
delay. The value 16 can remain the initial per-transaction bound. The shared
value must come from the real-backend calibration above, not from the provider's
maximum connection count.
