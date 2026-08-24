# Bound normal leaf lock acquisition

Type: grilling
Status: open
Blocked by: 01, 03, 04

## Question

How should `KeyLocker` apply `join_all_bounded` to complete per-leaf lock operations, interpret `Locked`, `Conflict`, and `LeafFull` in stable leaf order after every input runs, account for foreign-holder waits that occupy bounded positions, preserve partial receipts and cancellation safety, and leave the existing sorted serial fallback unchanged?
