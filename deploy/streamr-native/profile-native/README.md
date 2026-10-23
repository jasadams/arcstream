# Native profile core candidate

`core.sql` prepares a 20-field component. Its planning attempt on merged Streamr
c5e06665 rejects the array/ranking aggregate-to-MERGE route; the prepared
composition assumed broader support than STR-64 delivered. See
[current validation](../../../docs/arc-18-merged-validation.md) for the exact
query errors and successful narrower application slices.
It is not the complete 33-field application publication and
must not feed the existing profile consumer, which requires all 33 fields.
`fields.json` records the exact partial output and remaining fields.

Render `{{SOURCE_OPTIONS}}` and `{{SINK_OPTIONS}}` as SQL connector options only.
The source receives ordinary unified application events, including `event_time`
text. Generated `event_ts` and `event_ms` are typed representations of that same
value; the latter uses TIMESTAMP(3) for milliseconds. The watermark expression
casts the physical payload field directly because the existing planner cannot
resolve the generated alias in that expression. Its delay is exactly five
seconds. No arrival sequence, application timer, external state or UDF is used.

## Field map

| Fields | Native calculation |
| --- | --- |
| tenant_id, canonical_id | Grouping identity |
| first_seen, last_seen | Earliest event-time FIRST_VALUE, maximum event milliseconds |
| total_events, page_views, clicks, logins, feature_uses | COUNT and declared event-type FILTER |
| user_id, last_page, last_country, last_device, last_browser | Event-time LAST_VALUE with explicit nonblank FILTER |
| events_1d, events_7d, events_30d, events_90d | Calendar COUNT FILTER, contribution date versus EVENT_DATE() |
| top_pages, top_features | Current per-item counts; descending count and ascending text tie-break; first five/three |

The first two rows through last metadata total 14 fields, calendar adds four,
and rankings add two. Source selectors deliberately have no arrival-order
substitute: STR-60 must provide the declared FOR timestamp as primary order.

Lifetime/core, page-count and feature-count relations are combined using tagged
UNION ALL and an outer current-result aggregate. Outer MAX copies the sole
current core row; it does not select a historical winner. Exact source-prefix
coherence and retract/add handling remain acceptance requirements. The previous
two-field composition experiment is evidence for the pattern, not this complete
query or this MERGE route.

The 18 primitive scalar fields enter a native snapshot state table through
STR-64 MERGE. RETURNING retains source/old/new/action for subsequent metadata
investigation. The output projects the actual 20-field source snapshot as append JSON diagnostic
snapshots after MERGE, not Debezium CDC and not the full consumer publication. Array
rankings pass through source and are not stored in state-table columns, so this
candidate does not require STR-63. It neither computes application changed_fields
nor pretends MERGE action alone constitutes the complete application metadata.

## Required integration checks

- STR-29 must emit quiet-key recent-count expiry with lifetime retention and
  exact CDC, without invented event-time progress. EVENT_DATE() is the declared
  event-relative clock, not wall time or the lateness-adjusted watermark.
- STR-60 must order source FIRST/LAST by event time, including out-of-order
  accepted events, ties and recovery on the actual generated-time lineage.
- STR-64 must support this shared-source multi-branch UNION/aggregate/MERGE
  graph, not just direct COUNT/MAX. Preserve each calculated event prefix,
  before/new snapshots and coherent branch completion. Reject ambiguous lineage
  rather than selecting an arbitrary source clock. Quiet-expiry maintenance also
  needs validation: it has no ordinary new event for the expiring profile.
- STR-146 must provide Kafka bounded ingestion/backpressure through this actual
  integration with configured RocksDB state. This file supplies no pacing hack.
- Native Kafka source and sink checkpoint/restart must preserve committed output
  and fresh application results. None of these checks is claimed passed here.

## Remaining 13 application fields

`total_sessions`, `sessions_1d`, `sessions_7d`, `sessions_30d`, `sessions_90d` need
an explicit native SESSION lifecycle route. Counting completed sessions is not
proof of a count that changes at session start. Do not import the old
A,A,B,A cursor transition policy or COUNT DISTINCT as a substitute.

`avg_session_duration_sec` needs native closed-session duration/denominator
composition. Sum milliseconds before integer conversion. This must be tested
with native closure, reuse and recovery.

`current_session_active` and `current_session_duration_sec` need demonstrated
native active-session/closure semantics. A completed session writing false alone
does not establish correct live state, late bridging, reuse, or protection from
an old closure clearing a newer session. No false/zero placeholders are emitted.

`updated_at`, `action`, `changed_fields`, `timestamp`, `trigger` need agreed
publication semantics and actual native context. Previous calculated state is
not automatically previous periodically emitted state. EVENT_TIMESTAMP() must
not be projected after aggregation without proven original-trigger lineage, nor
replaced with MAX(event_time), wall time or delayed watermark. Calendar expiry
and session closure metadata require explicit validation. Arrays are not in the
snapshot table; any required array comparison must first be expressed/tested
without silently assuming STR-63.

The four tickets are dependencies for preparation, not a guarantee that all
remaining application semantics are delivered. Any further native failure must
retain the runnable query, expected results, exact source and failure before a
new capability or semantic change is proposed. No deployment or cutover is
represented by these files.
