# Five-event mixed profile diagnostic

This is an independent literal twenty-field oracle for `profile-native/core.sql`.
No output or expected snapshot was generated from an engine execution. Each
`values` list is the cumulative complete committed append trace. Invoke the
isolated outer wrapper with this input and expected-prefixes pair, `--no-recovery`,
`--typed-owners 1 --window-owners 0`. It is a staged correctness diagnostic, not
recovery evidence or a complete thirty-three-field consumer fixture.

The five events are deliberately split across all four counted event types:
feature_used twice, page_view once, click once, login once. Final exact counts
are total_events=5, feature_uses=2, page_views=1, clicks=1, logins=1. Every calendar
window includes all five same-day contributions.

Event 2 arrives two seconds before event 1's event timestamp, within the configured
five-second allowance. It moves first_seen back to 10:00:02 but leaves last_seen
at 10:00:04 and last_device=desktop. Its user_id is retained because it is the
only nonempty user_id so far. Subsequent empty/null metadata must preserve the
most recent nonempty event-time value; event 5 changes user/device normally.

The input intentionally supplies nonempty feature_name on page/click/login
records too. This tests the actual core SQL contract: `feature_uses` filters
`event_type='feature_used'`, whereas feature ranking counts **every nonempty
feature_name**, independently of event_type. Thus final feature_uses is 2 but
search/export occurrence counts are 3/2. This is explicit fixture semantics,
not a claim that every application event normally populates feature_name.
Feature ranking is:

1. search (1).
2. export, search (1/1; lexical tie).
3. search, export (2/1).
4. export, search (2/2; lexical tie).
5. search, export (3/2).

Similarly top_pages counts every nonempty URL: /home on page_view and /login on
login, both one occurrence; page_views remains 1. The final URL tie is lexical.

This minimal diagnostic does not repeat late rejection, tenant isolation,
midnight expiry or recovery; those belong to the existing seven-event core
fixture and recovery experiment. Do not describe those properties as covered
by these five events.
