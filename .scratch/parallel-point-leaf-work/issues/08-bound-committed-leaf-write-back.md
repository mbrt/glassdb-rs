# Bound committed leaf write-back

Type: grilling
Status: open
Blocked by: 01, 03, 04

## Question

How should committed leaf write-back use bounded parallel work for original and rerouted leaf groups while remaining best-effort, deterministic, safe to cancel and drain at shutdown, able to aggregate superseded transaction hints, and independent across leaves when one leaf is deferred by structural work?
