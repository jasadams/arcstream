# Native profile publication and recovery — 23 October 2026

The approved 22-field native SQL producer passes the real five-job application
Kafka/RocksDB fixture. This repairs complete publication, including the native
completed-session average, without suppressing committed records or substituting
application kernels, timers or serialized state machines.

## Exact sources and commands

Fresh Arcstream default base cbcb901be8482c01acce3471b4b6ba3a4242c3d0, incorporating
reviewed dependency6a13f634c538ae7688e766e8e8ea91220fc98cb6. Runtime manifest records
application8a7c78e. Engine source1314e702a955ad3f19c390b3f10c9d3ee1958d40, clean
compiled diff, normal maintained dev build and matching precompiled RocksDB.
Binary SHA2563e2594d62f6103501a84d17ff133032866d1929a0e84c42949ad3d7976b9d255.
Immutable runtime/build image bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e;
broker075dbe36397d3a1c0a8e52764f1b835f7d0a1c71767bb917d3bbb5f25ec7ab54.

Evidence root: `/home/jason/qa-evidence/str150-composed-publication-20261023/attempt-2/`.
`integration.json` records exact arguments, commands, exits and log paths. The
maintained driver runs under one outer shared `scripts/rust-build` reservation:
engine `scripts/build-pin`, compiled source/hash audit, then
`python3 test/streamr-reference/native_graph_fixture.py --case <root>/graph-2
--binary-pin <root>/dev-pin --engine-helper <engine>/scripts/test-native-process-recovery.py
--config-template /home/jason/qa-evidence/arc18-core-release-20261023/config-full-graph.json
--python-runtime /home/jason/qa-evidence/str32-20261010-52391450/python-runtime
--runtime-image <recorded digest> --broker-image <recorded digest> --scan-diagnostics`.
It finally executes maintained `cargo-dev test --locked --manifest-path
<application>/query-api/Cargo.toml --test native_profile_tests`.
All commands exit zero; all1,343 compiled inputs and executable hashes independently
rechecked. Maintained resource JSON overrides the template's old allocations.

## Results and independent review

All11 staged histories equal the unchanged literal expected results, grouped only
by composite profile identity; no committed rows are removed. There are13 complete
22-field profile rows. These cover four event types and changing ranks, admitted
out-of-order first/last values, rejected late events, tenant isolation and recovered
identity point lookups. Average is NULL before closure,5.0 after the first completed
session and3.75 after the next. Quiet-key daily count becomes zero.

The actual profile worker OS exit is signal9 and a fresh generation restores
checkpoint6. Identity worker independently exits signal9 and restores checkpoint16.
Committed profile prefixes of2 and5 rows remain unchanged, with11 and8 fresh suffix
rows after the respective kills. Retained checkpoint metadata has exactly6 native
disk owners plus1 typed profile owner, and2 typed identity owners. Seven retained
native artifact sizes/hashes independently verify. The strict runtime harness also
checks source offsets; the independent host audit did not repeat Parquet offset
decoding because host Python/pyarrow versions differ.

The finite capture, runtime result and outer receipts record complete owned cleanup.
All10 actual API consumer tests pass, including nullable fractional averages,
strict native actions/types and GraphQL delivery. All30 native Python tests and
three deployment preparation tests pass. Independent written source, contract,
deployment preparation and actual-evidence reviews approve their stated scopes.
Required CI checks pass implementation heads; documentation updates receive their
own current-head checks. Agent review signing is retired.

## Preserved failures and remaining boundaries

The ordinary JOIN/CDC c/d/c failure remains in lifecycle-candidates; ordinary JOIN
semantics are unchanged. Generic STR-150 extends existing accepted native MERGE
materialization to four aggregate branches including one finalized native SESSION.
The first new pin failed before input because SESSION's private response edge
shuffled. That failure is retained in graph-1; the repair preserves Forward for
proven response routes, ordinary SESSION Shuffle and strict worker guards.

Native test deployment configuration renders five jobs, existing raw Kafka input,
namespaced output/groups, persistent RocksDB/catalog/checkpoints and three schemas.
Actual published FlareDB95b58cb rejects native List table creation and returns
array display text over HTTP. Reproduction and literal failures are retained in
`/home/jason/qa-evidence/arc18-flaredb-native-list-20261023/` and FLA-178. The schema
correctly retains native lists; no SQL stringification or display parser hides this
gap. FlareDB repair, actual analytics ingestion, browser results and deployment
validation continue separately. Directed identity-merge behavior is not covered by
this fixture. Seven live/start fields and four publication metadata fields remain
explicitly deferred as approved. No whole-application parity or cutover is claimed.

Raw optional scan diagnostics are retained with a partial summary: multiprocess
log interleaving prevents complete counter accounting. These finite correctness
runs make no throughput/capacity/CPU/I/O/RSS regression claim. Reuse the completed
STR-126 qualification instead of repeating its full campaign.
