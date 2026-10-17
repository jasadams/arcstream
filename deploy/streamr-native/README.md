# Streamr native SQL integration

The custom UDF/state-machine MVP graph has been removed. Its generator,
normalizer, timer feedback, synthetic clocks and nullable watermark bypasses
are no longer available. There is currently no complete demo deployment graph.

See [current integration status and gaps](../../docs/arc-18-native-status.md).
Native product semantics govern results; the old Flink implementation is not an
execution specification when its behavior conflicts with those semantics.

## Native candidates

- `identity.sql`, `unified.sql`, `merges.sql`: identity state tables and append
  publication; payload event_time drives the five-second disorder watermark.
- `session-native/query.sql`: native SESSION with twelve-field projection,
  arrival-ordered metadata and native array/map aggregation.
- Profile composition/session-count SQL directories: partial native candidates,
  with qualification boundaries recorded in their READMEs; not a complete
  profile lifecycle or deployable graph.

Prepare native session inputs and run fixture checks:

```sh
python3 test/streamr-reference/session-native/prepare.py /absolute/new/case --case five-second-cutoff
python3 -m unittest discover -s test/streamr-reference/session-native -p 'test_*.py' -v
python3 test/streamr-reference/native_sql_runtime.py --help
```

Actual native SQL recovery uses `native_sql_runtime.py` in the qualified runtime
image through the shared build/run queue. Supply a source-bound executable,
configuration, engine recovery helper and explicit expected results. It always
submits an empty UDF list. There is no compiler or Cargo-cache setup.

For the three-session fixture use `--typed-owners 0 --window-owners 3
--fault-prefix 3 --require-committed-output`. The runner verifies selected
checkpoint artifacts, actual worker SIGKILL, same-job fresh generation,
committed-output bytes and fresh complete application results.

`fixture-run.py` remains an isolated Kafka identity fixture tool; its existing
consumer test and historical receipts do not qualify the changed source-time
configuration or a full profile/session deployment. Run with `--help` for
arguments. Never infer production readiness from preparation or compilation.
