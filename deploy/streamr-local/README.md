# Local Streamr evaluation

This optional setup runs on the development machine before any production deployment.
It is a separate Compose project, `arcstream-streamr-eval`, with its own broker, API
database, checkpoint, working-state and compiler volumes. Existing Arcstream/Flink
services remain available. Only one instance of this evaluation project can run on a
machine; different worktrees share its project name and ports.

## Start and verify identity

Requires Python 3, Podman with podman-compose (or Docker Compose), and a locally packaged
Streamr milestone 2 candidate with source provenance labels. The earlier STR-1-only
image is insufficient: the helper requires exactly one state owner in the identity plan.
It pins the immutable image ID rather than repeatedly following a mutable tag.

```sh
python3 deploy/streamr-local/local.py pin --image localhost/streamr:arc-13-m2
python3 deploy/streamr-local/local.py up
python3 deploy/streamr-local/local.py submit
python3 deploy/streamr-local/local.py recovery
```

Set `CONTAINER_ENGINE=docker` for Docker. API/console: http://127.0.0.1:15115.
Host Kafka: `127.0.0.1:29092`; containers use `broker:9092`. Both published ports bind
only to loopback. `up` creates four single-partition evaluation topics and is repeatable.
`submit` validates the actual API graph, checks HTTP-success responses for SQL errors,
reuses matching pipelines and fails on changed queries/duplicate names. All jobs use
parallelism 1 and two-second checkpoints. Readiness and commands have bounded timeouts;
API error bodies and command errors produce nonzero exit status.

The three submitted SQL files are:

| Pipeline | Input | Output |
| --- | --- | --- |
| Identity owner | `arc-eval-raw-events` | `arc-eval-identity-capture` |
| Stateless unified projection | `arc-eval-identity-capture` | `arc-eval-unified-events` |
| Stateless merge projection | `arc-eval-identity-capture` | `arc-eval-identity-merges` |

Each source uses a distinct consumer group. The intermediate topic retains actual
`merge_old`/`user_binding` evidence alongside the 17 event fields. Two independent
stateless projections publish the full unified and four-field directed-merge payloads.
This avoids creating separate identity state owners for each output.

`recovery` requires fresh input topics. It publishes the first 10 fixture events, observes
committed output and completed checkpoints, recreates only the Streamr container, waits
for newer worker runs, then publishes the last three events. The broker and named volumes
survive. Jobs stop at completed checkpoints before finite committed reads. Every physical
record is compared against the independent 13-event/two-merge oracle; missing or duplicate
records fail. UUID normalization preserves tenant ownership and identity equivalence.

For a separate cold run on fresh evaluation storage:

```sh
python3 deploy/streamr-local/local.py reset
python3 deploy/streamr-local/local.py up
python3 deploy/streamr-local/local.py smoke
```

`reset` removes **only this evaluation project's volumes**, including its broker data,
after archiving prior successful evidence. `down` stops/removes its containers and network
but preserves volumes. `status` shows local jobs. `submit` resumes checkpoint-stopped jobs
without creating duplicate pipelines. A second fixture run on existing raw input is
rejected; it must use fresh evaluation storage.

## Persistence and evidence

SQLite metadata lives at `/state/config.sqlite`, full checkpoints at `/checkpoints`,
RocksDB working state at `/live-state`, compiler artifacts at `/compiler/artifacts`
and compiler build files at `/compiler/build`. The image contains the runtime toolchain,
console and Swagger assets; no Streamr source checkout is mounted. Its only bind mount
is the checked-in runtime configuration. The explicit small state budgets in
`streamr.toml` are for this correctness fixture, not measured production defaults.

Evidence is written under ignored `target/streamr-local`: candidate image/source labels,
API plans, pipeline IDs, prefix/checkpoint and recreation results, raw/intermediate/output
JSONL, strict comparison, logs and source/artifact hashes. Prior successful runs are
archived under `runs/` by `reset`. Helper failure/idempotence tests:

```sh
python3 -m unittest discover -s deploy/streamr-local -p test_local.py -v
```

## Readiness limits

This is local identity integration and container-recreation evidence. Live-state volumes
also persist, so it does not by itself prove recovery solely from a remote checkpoint.
The pipelines commit independently; unified and merge output visibility is not atomic
across the two topics. Sinks request transactional commits and consumers read committed
records, but the current Kafka sink has an unfinished commit-phase recovery path. This
test checks the observed completed-checkpoint case, not arbitrary crash-safe exactly-once
delivery. Kill-during-commit, broker faults and downstream duplicate handling remain
ARC-17/STR-32 qualification work.

Profiles and sessions are not submitted. Their strict Flink references are ready, but
Streamr still needs STR-29's bounded collections/durable profile timers and STR-20's
bounded session state and selected business contract. [Historical SQL/UDF assets](historical/README.md)
are byte-exact archived references, never automatically submitted or registered. Runtime
UDF compilation/persistence needs separate verification once the selected profile path
supports it. Larger-than-RAM workloads, hot keys, consumer/dashboard integration, backfill
and a 24-hour soak remain open. Passing this setup does not authorize production deployment.

## Local evidence on 2026-10-03

Both the cold Kafka run and the checkpoint/container-recreation run matched all 13
unified events and two directed merges, including every payload field. Repeat submission
reused the same three pipeline IDs. Recovery changed all worker run IDs from 1 to 2 and
logged restored published checkpoints for each job. The API plan contains one identity
state owner; the two fan-outs contain none. All 12 helper tests pass.

Candidate source: `b18dd98f3120a641887322dfb56c3574206f4469` (milestone 2 plus the capture
hook). Immutable local image ID:
`4a4676edbc6778e7cfd61be4b71b8fd4baf4b8cadcff336d42f14c3dd168ffc8`.
Manifest digest: `sha256:c6bfaa83fee541c98a2433613237a50591c8901183f8921231fa3952faeefd64`.
The candidate embeds `/app/streamr-provenance.json` with source, binary, builder and
runtime-asset hashes. This is an available local image, not a published registry release.
