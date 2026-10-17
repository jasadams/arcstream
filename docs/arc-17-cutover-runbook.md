# ARC-17 candidate verification and cutover boundary

This procedure prepares a reviewable candidate. Production cutover is not
authorized, and the existing Flink deployment remains the rollback path.
The exact qualification receipt and remaining gates belong to
[the native MVP contract](arc-17-native-mvp.md) and ARC-17.

## Candidate verification

Use a fetched source checkout and the maintained shared queue. Run the finite
native Kafka graph supervisor with its pinned binary, compiler/plugin checkout,
compatible image, independent oracle and a new evidence directory. The exact
accepted command is recorded in that run's `host-commands.json` and
`container-command.json`; never silently substitute a rebuilt executable or
current mutable dependency. The supervisor checks native SQL compilation, real
Kafka read-committed outputs, application results, worker loss, persisted
checkpoint recovery and committed output continuity. Reusing a completed engine
capacity qualification does not imply application parity.

Each candidate namespace owns its broker topics, consumer groups, catalog,
checkpoint storage, live native state and compiled UDF artifacts. All data topics
have one partition and every sink has its own compacted recovery topic. Do not
reuse production groups, write into canonical application topics, or run a
second canonical writer. Keep the profile clock mode fixed for the persisted
state. Explicit progress controls are trusted internal inputs, never analytics
events. An idle stream needs explicit source progress to fire event-time timers;
wall-clock passage alone does not advance them.

The query API consumes full33 profile after-images with read_committed. Its live
subscription is a sampled UI stream, not the durable materialization boundary.
The independently committed native profile CDC topic provides complete state
transitions. Sessions are append summaries. Until backend replay handling is
qualified, do not assume the analytics backend deduplicates session inserts.

## Proposed switch sequence: requires separate authorization

1. Record the current deployment image/configuration, Kafka committed offsets,
   producer owner, consumer groups, canonical identity data, merge history and
   backend counts. Preserve their data and checkpoint locations.
2. Pause the sole producer and verify it has stopped. Record an explicit input
   end-offset boundary on every partition. Never run old and new canonical
   identity writers simultaneously.
3. Choose and document either a fresh isolated identity namespace or restoration
   of qualified native state. Replaying into new empty identity state generates
   new canonical IDs; existing analytics identities and merge history cannot be
   assumed reusable. A migration contract is required before reusing them.
4. Backfill the isolated candidate to the recorded boundary, verify exact logical
   event IDs, identity lineage, profile state, session summaries, CDC chains and
   backend materialization. Preserve physical delivery counts and any duplicates.
   Match dashboard queries and WebSocket payloads to the same reference data.
5. After all acceptance gates and explicit cutover approval, change consumer
   configuration to the reviewed candidate topics/groups and resume the sole
   producer from the recorded boundary. Observe lag, memory, checkpoint completion,
   restarts and application counts. Preserve the old deployment and data.

## Proposed rollback sequence: evaluation rehearsal required

Pause the sole producer; record the new boundary and stop candidate consumers.
Restore the saved old deployment configuration and its original state/groups.
Resume the old producer/consumers at a reviewed offset boundary. Reconcile the
candidate-only interval using logical event IDs and the original identity
namespace before replaying it. Do not reset offsets or append sessions blindly:
replaying already materialized summaries can inflate analytics without upsert.
Verify original canonical IDs, merge history and expected backend/profile counts,
then resume production input under one writer.

The isolated worker-loss checks exercise recovery, not this operational switch.
A tested evaluation switch/rollback, broker/container faults, full historical
backfill/live handoff, backend/dashboard qualification and sustained 24-hour
application run remain separately reportable ARC-17 acceptance gates. Release
image publishing and production deployment require a separately requested action.
