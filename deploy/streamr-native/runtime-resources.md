# Native state resource envelope

The MVP uses existing bounded RocksDB state and native application SQL. Pages
already have sorted-set semantics in the final SESSION output. Applying the
same native array_sort/array_distinct projection before downstream grouping
removes repeated pages from grouping keys while preserving all twelve fields,
event-type multiplicity, SESSION membership and event-time behavior. It does
not use the unsupported DISTINCT aggregate or selector-companion alternatives.

The finite window key ceiling is16KiB. Stored index overhead leaves at most
16,355 serialized group bytes; this is not an individual-field-size promise.
The unchanged partial/output ceiling is32KiB. Collection preflight also reserves
the configured key ceiling and64 bytes per physical field: compact collection
payload, offsets, validity and element overhead must fit the remaining
16,384 minus64 times physical-field-count bytes. Actual Arrow output, selector,
SESSION state and IPC limits remain independently enforced. Larger values fail
admission; no values are clipped or discarded and sessions are not unbounded.

The shared scan-page pool is8MiB. At the eight-owner limit the existing exporter
provides83,740 bytes per checkpoint page, covering the conservatively bounded
66,051-byte encoded entry (including a512-byte namespace allowance). That entry
fits the unchanged128KiB window page; four entries fit the unchanged512KiB write
budget. A4MiB scan pool derives only25,486 bytes. The8KiB disk/typed row setting
is sizing validation, not a universal window-entry cap.

Window partial, page, write and resident settings are unchanged. These resource
settings are not a whole-process RSS bound. The deployment retains its separate
8GiB Streamr container cap. Correctness checks make no throughput/RSS regression
claim; reuse STR-126 performance evidence.

STR-152 records an attempted alternative: native collections with variable-width
FIRST_VALUE companions validate but fail worker construction. That diagnostic
shape is not deployed. Early scalar pages normalization uses existing supported
functions and does not require that feature. Full12-field fixtures, a near-bound
key and actual checkpoint/SIGKILL recovery must qualify this finite envelope.
