# Streamr reference preparation

Run the current production Flink functions against independent reference assertions before
using them to qualify Streamr. This is preparatory coverage for ARC-14/15/16 and STR-28/32.
It does not submit jobs, connect to Kafka, or change canonical output topics.

## Run

From the repository root, with Python 3 and Podman:

```sh
bash test/streamr-reference/run.sh
```

The runner uses a pinned Maven/JDK 17 image, the existing Flink 1.20.1 test dependencies,
and a public-dependency cache under ignored `target/reference/maven-cache`. Set
`CONTAINER_ENGINE=docker` to use Docker, or `REFERENCE_MAVEN_IMAGE` to choose another
JDK 17 Maven builder. The selected image, source file hashes, Git revision/status, captured
output hash and logs are recorded under `target/reference/`. Network access is used for
public Maven dependencies; these tests exercise local keyed operators.

## Identity fixture and comparison

`flink/identity-resolution/src/test/resources/reference/identity-input.json` contains
ordered raw-event steps and a checkpoint/reconstructed-operator step. The test runs the
production tenant-keyed `IdentityResolutionFunction` and captures its main and merge side
outputs. The independent `identity-expected.jsonl` oracle covers 13 unified events and
two directed merges, including anonymous repeats, null/empty user IDs, first login,
known-user/new-device lookup, conflict merge, tenant separation and linking after recovery.

Actual UUID-bearing records are exported to
`flink/identity-resolution/target/reference/identity.jsonl`. Each line has this envelope:

```json
{"stream":"unified-events","payload":{"event_id":"...","tenant_id":"...","canonical_id":"..."}}
```

Merge records use `stream: identity-merges` and the production merge payload. A future
Streamr capture adapter must emit these envelopes after executing the same input steps;
it must preserve fields and separate merge outputs, including checkpoint/replay boundaries.
For a Kafka-only replay, publish the `payload` of event steps, retaining order, and implement
the snapshot/restart step in the evaluation harness rather than publishing it as an event.

Compare a candidate capture:

```sh
python3 test/streamr-reference/compare_identity.py \
  flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl \
  /path/to/streamr-identity.jsonl
```

Canonical IDs are normalized by first appearance in the oracle's event order, separately
for each tenant. Candidate events are matched by `(tenant_id, event_id)` so output
interleaving does not change identity labels. Raw candidate IDs must remain distinct
across tenants, consistent with the current global UUID identity contract. The comparator
checks every payload field exactly, including explicit nulls and properties, and checks
merge direction and multiplicity. It rejects missing/duplicate events, lost fields,
wrong identity links and missing/reversed/duplicate merges. It does not silently coerce
types, remove timestamps or ignore extra business fields. Eleven comparator tests inject
these failures; comparison against only another engine is insufficient.
This comparison proves record contents and identity equivalence; it intentionally ignores
emission order, including merge-versus-unified ordering. Delivery/order guarantees require
separate replay and sink tests.

## Profile and session reference coverage

Production-function tests cover counter updates and last-nonempty metadata, event-time
session deadlines, checkpoint/recreated-operator recovery, the profile's five-second
processing-time debounce, and restored 1/7/30-day decay timers. Session tests distinguish
processing time from watermark advancement, ensure extended deadlines replace earlier
ones, and verify closed state does not emit again. Assertions check business results
rather than reproduce a separate implementation. Profile times are anchored to yesterday
at noon UTC to remain inside the production 91-day wall-clock clamp; volatile output
fields are not claimed deterministic. Profile/session JSONL comparison adapters are still
pending.

Some tests deliberately characterize existing behavior that needs a contract decision:

- Profile keys are `canonical_id` alone and session keys are `session_id` alone. Reusing
  either across tenants collides in the current jobs. The normal identity path generates
  unique canonical UUIDs; externally supplied/replayed keys still need explicit guarantees.
- Session start is the first arriving event, while end is the maximum event time. An older
  event arriving afterward can leave duration zero despite earlier historical activity.
- A late profile event preserves maximum `last_seen` but currently rearms the timeout from
  its incoming timestamp, moving the deadline earlier.

These assertions preserve evidence of the current reference; they do not approve those
behaviors as Streamr acceptance. STR-28 and ARC-15/16 must document fixes or supported
input assumptions explicitly. Set-like page outputs and tied top-K ordering need defined
comparison semantics before broader profile/session capture comparison.

## Remaining qualification

The snapshots here are Flink keyed-operator harness snapshots restored into new operator
instances. They do not establish Kafka offset/sink consistency, remote checkpoint durability,
RocksDB memory bounds, process crash recovery, partition idleness or a full replay.
Next, run the same identity fixture through actual Streamr SQL, add reusable profile/session
captures with explicit time controls, then execute the STR-32 backfill, hot-key,
larger-than-RAM and 24-hour live/fault qualification in isolated outputs. Milestone 1/2
review and STR-3/4 state semantics remain prerequisites for Streamr acceptance.
