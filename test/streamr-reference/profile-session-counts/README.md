# Profile25 guarded rerun caller

Prepare a fresh run without executing the engine:

```sh
python3 test/streamr-reference/profile-session-counts/run.py \
  --run-dir /absolute/fresh/evidence-directory \
  --binary /absolute/pinned/arroyo-sql-testing \
  --executable-receipt /absolute/build-source-receipt.json \
  --expected-source-commit FULL_40_HEX_COMMIT \
  --query deploy/streamr-native/profile-session-counts/query-guarded.sql \
  --backend memory --checkpoint-mode controller --batch-rows 1
```

The coordinator adds `--execute` for serialized execution, selecting memory/RocksDB, controller/leader and batch rows 1/8. Both provenance arguments are required together. Without them, the historical preparation receipt's binary pin remains unchanged. The explicit receipt must contain a matching full source commit (`source_commit`, `commit` or `head`) and ELF hash (`binary_sha256`, `executable_sha256` or `sql_binary_sha256`). Every recognized alias must agree. If supplied, `build_exit` must be the integer zero. Duplicate JSON keys are rejected. Exact original receipt bytes and their SHA256 are preserved; binary and receipt are checked again immediately before launch.

The input, queries, independent25-field oracle, resource settings and existing assertions are unchanged. Checkpoint proof additionally requires the physical checkpoint file count to match the recovered capture receipt, its exact bytes to remain the recovered output prefix, a nonempty recovered suffix, and the complete typed checkpoint bag to match source prefix2. Initial and recovered finals still match source prefix9, with exact typed CDC before chains, monotonic independent per-key prefixes and no deletes.

This finite transition/calendar diagnostic does not qualify all33 application fields, timeout/reset behavior, every source prefix, capacity, or processing-clock/emission parity. The9-event fixture spans14 minutes. Previous receipts and the frozen deployment documentation remain historical evidence.

Host-only adversarial checks passed for matching provenance, wrong/short commit, Boolean build exit, duplicate JSON key, changed ELF, exact checkpoint count/byte-prefix/suffix, and Boolean-for-integer typed fields. No engine or build was executed during this preparation.

Current repaired-source qualification (2026-10-11): all8 memory/RocksDB × controller/leader × batch1/8 configurations passed, with independent source-derived typed-value, checkpoint byte-prefix and fresh-worker suffix audits. Executable source25d8b554c9396c6e49e4be61a6bfc1c5f35f8d39, ELF5254dd09df9b677d5d9e640fbbfe8425d20edca8e1ab4ce363c314213bf848ab. Exact build receipt and captures: /home/jason/qa-evidence/str32-post-merge-20261011/ordered-window-repair/; independent application-independent-audit.json/.md. This supersedes historical pending matrix statements below. Finite fixture qualification does not establish full profile lifecycle, clock/emission parity, capacity or live Kafka transaction recovery.
