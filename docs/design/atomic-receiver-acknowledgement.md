# Atomic acknowledgement for marked queue operations

Status: accepted

## Problem

A marked scheduled task becomes pending during forwarding Apply. Settle and
Finalize currently use separate Redis calls. A concurrent dequeue can observe
that pending task with a valid intermediate reservation and reject it because
the finalized-source reader requires the completed acknowledgement shape.

A deterministic Redis test pauses after a successful forward Apply. The source
has 13 fields and the task is pending. Dequeue reports `invalid finalized source
shape`; after the original Finalize, the same dequeue succeeds.

## Selected implementation

The public RDB operation already returns only after acknowledgement. Execute its
existing Apply, Settle and Finalize sequence inside one Redis script, using a
local function containing the unchanged generated transaction. Preserve the
current replay branch: a replayed Apply first attempts Finalize, then Settle and
Finalize if necessary. Refusals retain their current result classification.

The executor retains its fixed key and argument vectors. Normal success invokes
the bounded kernel three times; replay invokes it at most four times. No keys,
fields, capacities, descriptor identities or receiver variants are added. The
finalized-source reader still rejects intermediate and malformed shapes. The
generated transaction remains byte-identical.

Before Apply, an existing acknowledgement envelope requires the receipt key to
be absent. The existing Finalize validates and completes that exact envelope;
it never falls through to Apply. Simultaneous receipt and envelope, malformed
state or mismatched identities refuse without a native mutation. If the
envelope is absent, Apply performs its existing receipt validation.

After successful Apply, Settle replaces a 23-field receipt with a smaller
21-field envelope. Finalize removes that envelope and adds only 77 logical
bytes to the source, or deletes the source on release. Neither step increases
rows or physical usage. They inspect the exact state created in the same
script without a concurrent writer.

This removes observation between the native mutation and acknowledgement.
Redis does not roll back unexpected script errors; this change makes no such
claim. Existing preflight and capacity protections remain required.

## Alternatives

Moving pending membership to Finalize changes the receipt accounting contract
and requires a recovery producer for interrupted forwarding. Reconstructing a
predecessor acknowledgement during every dequeue adds authority parsing and
reads. Accepting intermediate reservations or retrying their rejection weakens
custody or conceals the race. One script preserves the existing operation and
its exact checks with the smallest affected surface.

## Verification

- Pause the actual forward command after its successful Redis response and run
  concurrent dequeue. Require a finalized source, successful dequeue and exact
  task identity.
- Drop a completed operation's response and prove its existing retained recovery
  behavior without a duplicate enqueue or capacity charge.
- Refuse wrong-type, malformed, stale and simultaneous receipt/envelope state
  without writes; recover a real interrupted Settle with exact Finalize.
- Retain the complete receiver mutation inventory, kernel identity and existing
  real Redis corruption, capacity, initial, recovery and release tests.
- Run build, vet and race suites for the affected modules. Preserve unmarked task
  behavior and verify the original worker observation with the updated dependency.
