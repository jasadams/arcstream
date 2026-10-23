# First native release contract

The user approved two scope decisions on 23 October 2026: “Ship core metrics and
closed-session average; defer live/start features”, then “Omit the unused
publication metadata for this release”. These decisions authorize the smaller
application contract below; they do not change Streamr's SQL semantics or resolve
STR-148/149's independently recorded product gaps.

The Kafka profile stream uses plain JSON snapshots emitted by native MERGE
RETURNING, with exactly 22 fields enumerated in
`deploy/streamr-native/profile-native/release-fields.json`: the existing core20,
`avg_session_duration_sec` and `action`. SQL projects the native MERGE action
(insert becomes `create`, update becomes `update`). The consumer validates and
preserves that actual producer action; it does not infer an action or compute
business state. Debezium/CDC envelopes are rejected. There is one current wire
contract, with no alternate-format fallback.

The decoder rejects incomplete or unknown snapshot fields, malformed arrays,
invalid timestamps, unsupported actions and negative/nonfinite completed-session
averages. Kafka tombstones contain no profile to publish. The ordinary JOIN/CDC
attempt and its failed intermediate publications remain recorded as failure
evidence; this native materialization route does not suppress delete/create
pairs in the client. Every emitted full snapshot must independently satisfy
application expectations, including closure updates and recovery continuity.

The completed-session average is a nullable floating-point number of seconds:
NULL means no completed session has contributed. A real zero-duration completed
session has average 0; millisecond fractions remain represented. Only completed
native SESSION durations contribute. The dashboard labels this “Avg completed
session” and displays an em dash for NULL. The average is neither a live-session
duration nor a count of session starts.

The release omits `total_sessions`, `sessions_1d`, `sessions_7d`, `sessions_30d`,
`sessions_90d`, `current_session_active`, `current_session_duration_sec` throughout
profile SQL output, strict decoding, GraphQL queries/subscriptions and dashboard
models/widgets. It also removes the separate wall-clock “active sessions”
DashboardStats/Tenant API and dashboard widgets. Profile session-start sorts,
start-count panels and active badges are absent. Unsupported fields are not
filled with zero/false or relabeled as closed counts.

The unused profile publication fields `updated_at`, `changed_fields`, `timestamp`
and `trigger` are omitted from the decoder, GraphQL update envelope and WebSocket
selection. Event timestamps `first_seen` and `last_seen` remain real source-event
times; event records retain their own `event_time`. Native `action` remains
as truthful native SQL output; complete snapshots drive profile-list upserts. Historical
profile-only signup and closed-count defaults are also absent; raw-event analytics
remain separate. No wall-clock profile business values are introduced.

STR-148 (open SESSION updates) and STR-149 (original-trigger clock through finalized
SESSION/calendar composition) are follow-up product work, not dependencies of this
approved 22-field release. STR-147's array_concat bug remains recorded separately;
removing unused changed-field metadata does not claim the engine bug is fixed.
Required readiness is still the composed native application graph, actual consumer
results, state lookups and Kafka/RocksDB checkpoint recovery. Scope approval is not
a deployment or production-cutover authorization.

Dashboard list rows use `(tenant_id, canonical_id)` keys. Every accepted full
profile snapshot replaces the retained row fields, including nullable completed
average and an earlier first_seen from an admitted out-of-order event. On the
first page an unknown complete snapshot is inserted even when its genuine native
action is update: the existing Kafka notification coalescer can deliver the latest
update without its initial create. This is a view upsert from actual profile
state, not a fabricated create event or a computation of aggregate values. The
view does not exclude historical snapshots by comparing first_seen with wall
clock. Detail profile and event subscriptions match both tenant and person keys.

Profile-list snapshots retain the fetched first-seen descending order before
trimming to page size, including historical upserts and earlier first-seen
corrections. Timestamp comparisons use parsed source times, with composite
identity as a deterministic tie-breaker.
