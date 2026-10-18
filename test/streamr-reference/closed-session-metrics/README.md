# Native completed-session profile metrics

Standalone, hand-counted fixtures for native SESSION results composed into native
per-profile aggregates. No UDF, transition implementation, opaque state, timer
feedback or observed engine output is used to produce expectations.

The same canonical ID occurs in two tenants. Tenant A reuses one session ID for
three disjoint native windows lasting 1,900ms, 2,900ms and 4,000ms. The third
window arrives two days later to test whether lifetime completed-session metrics
survive more than 24 hours of inactivity rather than being reset by aggregate
TTL. The query must configure native aggregate retention for lifetime metrics. Tenant B uses
the same canonical/session IDs for one 5,000ms window. An A event arriving 34s
behind progress must be rejected under the configured five-second watermark.
Ordinary page-view inputs with empty and null session IDs advance source event time;
normal business filtering excludes them from session aggregates. There is no
clock/control protocol or adjusted deadline. Inputs use the actual unified event
schema with event_time text. No precomputed event_ts, event_ms or arrival_seq is
provided; native SQL must derive timestamp and duration values from event_time.

| Source prefix | Closed profile | Completed milliseconds | Count | Integer average seconds |
| --- | --- | --- | --- | --- |
| 3 | tenant-a / shared-person | 1,900 | 1 | 1 |
| 6 | tenant-a / shared-person | 4,800 | 2 | 2 |
| 9 | tenant-b / shared-person | 5,000 | 1 | 5 |
| 13 | tenant-a / shared-person | 8,800 | 3 | 2 |

The API exposes avg_session_duration_sec as an integer (u64). Its declaration
alone does not choose a rounding policy. The existing application calculation
at flink/identity-resolution/src/main/java/com/pipeline/profile/ProfileFunction.java
uses total duration milliseconds / closed count / 1000, truncating positive
values. These fixtures explicitly preserve that numeric projection, while using
native SESSION completion and late admission rather than the old cursor policy.
No Java state-transition behavior is imported. Sum milliseconds before truncating:
averaging per-session integer seconds gives the wrong second A result (1 versus
2). SQL CAST rounding is not assumed; express the required floor explicitly.

Run python3 prepare.py /absolute/new/directory to prepare input.jsonl, four exact
metric images in expected.jsonl, two final profiles in expected-final.jsonl and
source/time-prefix expectations in expected-prefixes.json. Preparation refuses
overwrite and records hashes. Run python3 -m unittest discover -s
test/streamr-reference/closed-session-metrics -p 'test_*.py' for fixture checks.

The metric images specify logical aggregate values, not serialized CDC envelope
syntax or a promise of physical emission order. A runtime CDC consumer must
validate complete OLD/NEW chains, multiplicity and exact final keyed values;
it must not deduplicate away wrong intermediate records. Cross-key ordering is
unspecified. Native SQL runtime, checkpoint recovery, state lookups, full profile
schema/lifecycle and production Kafka behavior require separate evidence. These
fixtures claim only completed-session count and average, not 33-field parity.
