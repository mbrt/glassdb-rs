# Bound normal leaf lock acquisition

Type: grilling
Status: open
Blocked by: 01, 03, 04

## Question

How should the normal lock-acquisition path apply the per-phase concurrency limit, stable leaf order, and finish-started rule to `Locked`, `Conflict`, `LeafFull`, waits, cancellation, and partial receipts while leaving the existing sorted serial fallback unchanged?
