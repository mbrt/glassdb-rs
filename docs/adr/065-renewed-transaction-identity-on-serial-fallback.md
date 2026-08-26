# ADR-065: Renew transaction identity on serial fallback

## Status

Accepted.

This ADR supersedes [ADR-024](024-hold-and-wait-conflict-resolution.md)'s
same-identity foreground-release transition to sorted serial acquisition and
[ADR-025](025-dedup-shard-lock-acquisition.md)'s assumption that receipt-based
release always clears a late cancelled acquire. It narrows
[ADR-026](026-dedup-shard-release-write-back.md): release remains valid for
abort and recovery work, but serial fallback does not use it as foreground
control state.

## Context

A parallel lock future can be dropped after it dispatches a conditional write
but before it receives the result. A same-identity release can observe no lock
and finish before that late write lands. Serial acquisition under that identity
could then keep an out-of-order lock and invalidate its progress proof.

## Decision

Every transition from a running parallel acquisition episode to sorted serial
acquisition uses a renewed transaction identity. The attempt driver first ends
the old identity through the general transaction end path and waits until its
abort-side status is durable. An unresolved timed-out operation leaves the old
identity `Wounded`; a completed conflict episode can leave it `Aborted`. Only
then does the driver use the general rebegin path and force the replacement
identity to start in sorted serial mode.

The replacement keeps the wound-wait priority, samples a new validation lower
bound, reacquires collection directory locks, and rebuilds physical routing and
locks. Point and range transactions keep the transaction body's access set and
normal outcome, so this transition alone does not repeat the transaction body.
Collection create and drop keep their existing transaction-body replay rule.

Do not add special serial-retirement or serial-rebegin interfaces. Normal
parallel retries continue to keep their identity and physical locks. The
renewal rule applies only when parallel acquisition changes to sorted serial
acquisition.

## Consequences

A late lock still names a durable abort-side old identity. The replacement can
remove it through normal recovery while it acquires locks in sorted order. This
preserves the serial progress proof without a racy foreground release sweep.

Serial fallback creates a replacement transaction object and can add retirement
and recovery work. It avoids repeating point and range transaction bodies, but
collection create and drop still repeat their bodies because this decision does
not add collection-resource transfer rules.
