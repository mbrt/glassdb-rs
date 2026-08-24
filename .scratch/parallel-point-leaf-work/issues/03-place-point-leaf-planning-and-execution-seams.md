# Place the point-leaf planning and execution seams

Type: prototype
Status: open
Blocked by: 01, 02

## Question

Where should the interfaces for a routed point-leaf plan and bounded distinct-leaf execution live, and what is the smallest deep interface that gives routing, validation, locking, release, and write-back consistent scheduling without exposing a shallow generic concurrency wrapper? Use materially different interface prototypes and compare their depth, locality, and seam placement.
