# Partial native profile fixture

Run `python3 prepare.py /fresh/evidence/directory` to prepare seven ordinary raw
application events and literal expected 20-field keyed snapshots. Preparation
and these unit tests are not engine execution or recovery passes.

`expected-prefixes.json` is a list of `{input_lines, name, rows}`. Each rows list
contains complete expected current profiles keyed by tenant_id/canonical_id.
Stages 1–6 contain tenant A; stage 7 contains A and B. The stage-6 late event
must not change or emit a fresh logical profile. Stage 7 requires both B's new
profile and quiet A's calendar-expiry replacement; their emission order is
unspecified. Exact duplicate/intermediate-output assertions need an explicit
runtime trace contract rather than deriving ordering from this list.

The fixture distinguishes these policies:

- Feb1 00:00:01 has a Jan31 watermark but a Feb1 EVENT_DATE; its today count is 1.
- Jan31 23:59:58 arriving next is within the five-second allowance. Its trigger
  date is Jan31, although latest observed time is Feb1. Today and recent counts
  are 1 because the future Feb1 contribution is outside the declared date range.
- Earlier mobile must not replace later desktop. Empty and null device values
  must not replace desktop; later tablet does. No arrival-order field exists.
- Page rankings tie /a before /b at 2 each, then reverse to /b before /a at 3/2.
- An event fourteen seconds behind observed progress is rejected.
- A different tenant with the same canonical text advances real event time to
  Feb2, zeroing quiet A's today count while preserving its lifetime 5 and other
  profile values. Tenant B independently has lifetime/today 1.

The quiet-key stage assumes STR-29's approved maintenance contract, not a
fabricated event for A. Native calendar scheduling and coherent STR-64 delivery
must actually produce that result. No wall-clock wait is treated as progress.
All events have the application event_time text; no normalized event_ts,
event_ms, arrival sequence or precomputed aggregate is supplied.
