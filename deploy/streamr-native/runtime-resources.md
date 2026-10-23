# Native state resource envelope

The MVP uses existing bounded RocksDB state and the original application SQL.
Window grouping keys include finalized SESSION metadata, including its pages
array. The earlier512-byte key setting admitted only small fixture sessions.
The current finite key ceiling is32KiB, matching the existing32KiB partial
ceiling; stored index overhead leaves at most32,739 serialized group bytes.
This does not promise an unbounded session or a32KiB individual input field.
Larger rows still fail admission; values are never clipped or discarded.

The shared scan-page pool is16MiB so admitted window entries can also pass
checkpoint export. At the current eight-owner limit, the existing exporter
provides approximately200,248 bytes per checkpoint page. The window store's
worst zero-escaped encoded entry for32KiB keys/partials is about98.4kB,
including namespace overhead. It fits the existing128KiB window page and four
entries fit the existing512KiB write budget. A4MiB scan pool would derive only
25,486 bytes and cannot cover this envelope. The8KiB disk/typed row setting is
not a universal window-entry cap and must not be used as that guarantee.

Window partial, page, write and resident settings are unchanged. These are
accounted state-resource settings, not a whole-process RSS bound. The deployment
retains its separate8GiB Streamr container cap. No throughput/RSS regression
claim is made from these correctness checks; reuse STR-126 performance evidence.

The attempted alternative placing metadata in FIRST_VALUE aggregate values
exposed STR-152: collections with variable-width selector companions validate
but fail native worker construction. That unsupported alternative is retained
as diagnostic evidence and is not the deployed SQL. Existing original SQL does
not require that feature. Full12-field fixtures, large-key admission and actual
checkpoint/SIGKILL recovery must qualify this configured envelope.
