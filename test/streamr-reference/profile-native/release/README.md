# First-release 22-field application oracle

These eleven inputs and cumulative expected snapshots are independent literal
application expectations, not a trace copied from an engine run. Run
`profile-native/release.sql` through the owned Kafka wrapper with these explicit
input and expected-prefix files. The full query owns native updating aggregates,
a native SESSION operator and native updating JOIN; require
RocksDB and real transactional Kafka output. Select fault prefix 1 to kill the
worker with a nonempty committed output and an open session, then require fresh
restored application results through both subsequent closures.

The first five events reuse the established mixed-event literal expectations:
all four event types, event-time FIRST/LAST, admitted earlier input, feature
ranking ties/reversal and nonblank metadata. All 22 keys are present in every
snapshot; `avg_session_duration_sec` is NULL until a SESSION completes. NULL is
truthful absence of a completed sample, not a zero-duration session.

Input 6 is older than the exact five-second admission cutoff and emits nothing.
Input 7 is an ordinary event for the same canonical text under another tenant.
It advances stream progress past the first session's native closure. Tenant A's
session spans 10:00:02 through 10:00:07, so its completed average becomes 5.0
seconds. Tenant B's own profile has no completed session and retains NULL.

Inputs 8 and 9 open another tenant-A session, spanning 10:30:14.500 through
10:30:17.000. While open, the closed-session average remains 5.0. Input 10 is
another ordinary tenant-B event that closes that second native SESSION. The
exact average is (5.0 + 2.5) / 2 = 3.75 seconds; integer truncation is incorrect.
Input 11 crosses midnight on an ordinary tenant-B event. Tenant A is quiet:
its daily count becomes zero, lifetime metrics and completed average remain,
and tenant B has a daily count of one. No clock/control event or timer loop is
introduced.

Expected append order is per key. Rows for distinct tenants may interleave;
there is no invented cross-partition arrival order. `action` comes from standard native CDC operations in the transport adapter:
first insertion `create`, subsequent replacement `update`. The current JOIN
candidate must also validate every intermediate CDC before/after image; these
prepared complete-snapshot expectations do not authorize filtering other images. The session
average update must publish a complete profile without waiting for that person's
next event. There are no fabricated live-session or session-start fields.

The original seven-event midnight/raw-trigger-date fixture remains a separate
qualification of core calendar semantics. This fixture's same-day admitted
out-of-order event does not alone establish that distinct raw-date property.
