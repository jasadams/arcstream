# Profile readiness handoff: ARC-15 / STR-28 / STR-29

The identity capture passes on milestone 2, but the historical profile query cannot yet
be treated as a disk-backed, production-compatible profile implementation. This audit
targets Streamr revision `b18dd98f3120a641887322dfb56c3574206f4469`, stacked on milestone
2 `fb01f5e9`. The current Flink production functions are the reference. Run
`bash test/streamr-reference/run.sh` for the independent six-emission, 33-field profile
oracle and three fresh-operator restores.

## Capabilities required before the Streamr profile capture can pass

| Requirement | Evidence in the milestone 2 candidate | STR-29 acceptance work |
| --- | --- | --- |
| Idle output and recovery | `StatefulProcessor` implements startup, batches and checkpoints, without its own timer or watermark callback. Framework tick/watermark hooks exist but do not implement profile deadlines. | Persist processing-time debounce and event-time session/decay deadlines, cancellation/replacement and the last emitted snapshot. Restore into a fresh worker and emit without another input event. |
| Native arrays and typed counters | Disk input/output validation accepts primitive and UTF-8 values, rejecting Arrow list types (`crates/arroyo-worker/src/arrow/stateful_processor.rs:131` and `:244`). | Return real `top_pages`, `top_features`, `changed_fields` arrays and integer counters. Serialized JSON strings and NULL counters fail the comparator. |
| Qualified expression allocations | The disk scalar-function allowlist (`stateful_processor.rs:527`) excludes `profile_step`, `extract_json` and `extract_json_string`. | Compile the actual replacement profile plan and qualify allocation bounds throughout it. Moving a whole-profile JSON UDF outside the state owner does not establish a bound. |
| Bounded hot-profile collections | State-map reads return complete values (`stateful_processor.rs:668`). A finite row-size cap cannot accommodate an unbounded JSON page/feature map. | Store growing page/feature entries separately, with bounded scans or a ranking index for exact top-5 pages/top-3 features. Bound intermediate allocations as well as RocksDB residency. |
| Tenant ownership | The current Flink profile job keys by canonical ID alone (`ProfileUpdaterJob.java:55`). | Define collision-safe `(tenant_id, canonical_id)` ownership. Characterize any baseline correction explicitly rather than accepting a cross-tenant leak as parity. |
| Output and time semantics | Flink emits immediately on creation, debounces later activity, closes sessions at 30 minutes, and decays idle windows at 1/7/30 days. It calculates active session duration from wall time and clamps old events. | Preserve or explicitly approve changes to all 33 fields, changed-field deltas, emission rate and both clocks. Qualify historical replay without silently claiming this recent-event fixture proves it. |

Linear CTE state-owner fusion and guarded conditional execution are already present in
milestone 2. They were exercised by the identity capture; they are not the remaining
profile blockers described here.

## Runnable first acceptance slice

Use `profile-input.json`, `profile-expected.jsonl`, and the exported resolved
`profile-fixture.json`. Capture the same steps in memory/controller, RocksDB/controller
and RocksDB/leader modes. The fixture restores before pending debounce, session and decay
timers, checks silence immediately before debounce/timeout, and then checks six outputs:
creation, debounce, timeout, 1-day decay, 7-day decay and 30-day decay. No synthetic events
may stand in for timer callbacks. The candidate harness must measure emission wall-clock
bounds independently and feed native payloads into `compare_profile.py`.

The oracle checks every field and its type. Only changed-field order is set-normalized;
top-K order, changed-field membership, counters, actions, triggers, metadata and event
times remain exact. Wall-clock fields and active duration are bounded by measured emission
times. Capture, resolved fixture and source hashes are recorded by the runner.

This slice uses one profile/session/page/feature and ordered input. Follow it with explicit
timer cancellation/reactivation, competing top-K ranks and ties, tenant collisions, late
events, historical backfill, high-cardinality hot profiles and process/remote checkpoint
recovery. Full larger-than-RAM, replay, sink and 24-hour fault qualification remains STR-32.
ARC-15 and STR-29 stay open until an actual Streamr implementation passes their remaining
acceptance work; a passing Flink reference is preparatory evidence only.
