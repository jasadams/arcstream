# Backfill Procedure

Generate historical event data and ingest it through the full pipeline (event-producer → Kafka → Flink → Pinot).

## ⚠️ Critical (hard-won, June 2026)

- **The live `event-producer` Deployment MUST stay at `replicas=0` for the ENTIRE backfill.** Step 1 scales it down — verify it actually stays at 0. If it runs concurrently with the backfill Job, it interleaves *current-dated* events through the historical block (~1 per 3000). Flink/sessionization's watermark (`max(event_ts)-5s`) then jumps to ~now on the first such event, and every later historical event is "late" → dropped (or, on Arroyo, sessions never close → OOM). Start the live producer ONLY in step 6, after the backfill Job is `Complete` — it then cleanly appends after the historical block (monotonic, safe).
- **redpanda data + Flink RocksDB local dir must be on `local-path`, NOT Longhorn.** Heavy reprocess IO through Longhorn's engine saturates it on a single node → EIO faults → cluster-wide melt. (redpanda → `redpanda-data-local`; Flink `state.backend.rocksdb.localdir` → a local-path PVC.)
- **Kafka tiers to the NAS MinIO** (`cloud_storage_*` cluster config → 192.168.1.100). Topics need `retention.ms=90d` (≥ backfill span, else old segments are deleted-then-stuck local) + a `retention.local.target.bytes` cap so local disk doesn't hoard the whole backfill.
- **Sessionization closes on EVENT-time** (watermark), not wall-clock — replay-safe. (`SessionFunction` uses event-time timers.)
- Backfill image: `event-producer:1.2.0` has `--backfill/--backfill-start/--backfill-end`. (`:paced` adds `--backfill-wall-hours` but is a local-only build.)

## Prerequisites

- `kubectl` access to the `data-pipeline` namespace
- All pipeline pods healthy: redpanda, pinot-controller, pinot-broker, pinot-server, flink-jobmanager, flink-taskmanager

## 1. Stop the pipeline

```bash
# Scale down live event producer
kubectl scale deployment event-producer -n data-pipeline --replicas=0

# Cancel all Flink jobs
JM_POD=$(kubectl get pod -n data-pipeline -l app=flink-jobmanager -o jsonpath='{.items[0].metadata.name}')
for jid in $(kubectl exec $JM_POD -n data-pipeline -- /opt/flink/bin/flink list -r 2>&1 | grep RUNNING | awk '{print $4}'); do
  kubectl exec $JM_POD -n data-pipeline -- /opt/flink/bin/flink cancel $jid
done
```

## 2. Wipe all data stores

### Kafka topics

```bash
RP_POD=$(kubectl get pod -n data-pipeline -l app=redpanda -o jsonpath='{.items[0].metadata.name}')

for topic in raw-events unified-events profile-updates session-events identity-merges; do
  kubectl exec $RP_POD -n data-pipeline -- rpk topic delete $topic
done

for topic in raw-events unified-events profile-updates session-events identity-merges; do
  kubectl exec $RP_POD -n data-pipeline -- rpk topic create $topic
  kubectl exec $RP_POD -n data-pipeline -- rpk topic alter-config $topic \
    --set redpanda.remote.write=true \
    --set redpanda.remote.read=true \
    --set retention.ms=7776000000 \
    --set retention.local.target.ms=259200000
done
```

### Pinot tables

Delete tables first, then schemas, wait a few seconds for external view cleanup, then recreate. The schema JSON files are in `pinot/schemas/` and table configs in `pinot/tables/`.

```bash
PC_POD=$(kubectl get pod -n data-pipeline -l app=pinot-controller -o jsonpath='{.items[0].metadata.name}')

for table in events profiles sessions; do
  kubectl exec $PC_POD -n data-pipeline -c controller -- \
    sh -c "curl -s -X DELETE 'http://localhost:9000/tables/${table}?type=realtime'"
  kubectl exec $PC_POD -n data-pipeline -c controller -- \
    sh -c "curl -s -X DELETE 'http://localhost:9000/schemas/${table}'"
done

sleep 5

for schema in events profiles sessions; do
  kubectl cp pinot/schemas/${schema}.json data-pipeline/$PC_POD:/tmp/${schema}-schema.json -c controller
  kubectl exec $PC_POD -n data-pipeline -c controller -- \
    sh -c "curl -s -X POST http://localhost:9000/schemas -H 'Content-Type: application/json' -d @/tmp/${schema}-schema.json"
done

for table in events profiles sessions; do
  kubectl cp pinot/tables/${table}.json data-pipeline/$PC_POD:/tmp/${table}-table.json -c controller
  kubectl exec $PC_POD -n data-pipeline -c controller -- \
    sh -c "sed 's/redpanda\.data-pipeline\.svc\.cluster\.local/redpanda/g' /tmp/${table}-table.json | curl -s -X POST http://localhost:9000/tables -H 'Content-Type: application/json' -d @-"
done

# Verify
kubectl exec $PC_POD -n data-pipeline -c controller -- sh -c 'curl -s http://localhost:9000/tables'
# Should show: {"tables":["events","profiles","sessions"]}
```

### MinIO (Flink checkpoints, Pinot segments, Redpanda tiered storage)

```bash
MINIO_POD=$(kubectl get pod -n data-pipeline -l app=minio -o jsonpath='{.items[0].metadata.name}')

kubectl exec $MINIO_POD -n data-pipeline -- sh -c '
  mc alias set local https://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD --insecure 2>/dev/null
  mc rm --recursive --force local/flink-checkpoints --insecure 2>/dev/null
  mc rm --recursive --force local/pinot-segments --insecure 2>/dev/null
  mc rm --recursive --force local/redpanda-tiered --insecure 2>/dev/null
'
```

### Flink HA state

```bash
kubectl get configmap -n data-pipeline | grep arcstream | grep -v flink | awk '{print $1}' | \
  xargs -r kubectl delete configmap -n data-pipeline
```

## 3. Restart Flink

The jobmanager needs a clean restart after HA state is cleared.

```bash
kubectl rollout restart deployment flink-jobmanager -n data-pipeline
kubectl rollout status deployment flink-jobmanager -n data-pipeline --timeout=120s
```

## 4. Submit Flink jobs

```bash
JM_POD=$(kubectl get pod -n data-pipeline -l app=flink-jobmanager -o jsonpath='{.items[0].metadata.name}')

kubectl exec $JM_POD -n data-pipeline -- /opt/flink/bin/flink run -d \
  -c com.pipeline.identity.IdentityResolutionJob /opt/flink/jobs/identity-resolution.jar

kubectl exec $JM_POD -n data-pipeline -- /opt/flink/bin/flink run -d \
  -c com.pipeline.profile.ProfileUpdaterJob /opt/flink/jobs/identity-resolution.jar

kubectl exec $JM_POD -n data-pipeline -- /opt/flink/bin/flink run -d \
  -c com.pipeline.session.SessionizationJob /opt/flink/jobs/identity-resolution.jar

# Verify all 3 are RUNNING
kubectl exec $JM_POD -n data-pipeline -- /opt/flink/bin/flink list -r
```

## 5. Run the backfill

The event-producer has a `--backfill` mode that simulates historical data at maximum throughput.

| Flag | Description | Default |
|------|-------------|---------|
| `--backfill` | Enable backfill mode | off |
| `--backfill-start` | Start date (YYYY-MM-DD) | 90 days ago |
| `--backfill-end` | End date (YYYY-MM-DD) | today |
| `--seed` | Deterministic RNG seed | random |
| `--target-daily-events` | Events per simulated day | 3200000 |
| `--tenants` | Number of tenants | 5 |
| `--daily-variance` | Diurnal traffic multiplier | 3.0 |

### Example: 7-day backfill

```bash
cat <<'EOF' | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: event-backfill
  namespace: data-pipeline
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: backfill
        image: ghcr.io/jasadams/arcstream/event-producer:latest
        imagePullPolicy: Always
        args:
        - "--broker=redpanda.data-pipeline.svc.cluster.local:9092"
        - "--topic=raw-events"
        - "--tenants=5"
        - "--target-daily-events=3200000"
        - "--daily-variance=3.0"
        - "--seed=42"
        - "--backfill"
        - "--backfill-start=2026-05-29"
        - "--backfill-end=2026-06-05"
        resources:
          limits:
            cpu: "2"
            memory: 1Gi
          requests:
            cpu: "1"
            memory: 512Mi
EOF
```

### Example: 90-day backfill

Change the start date:

```
- "--backfill-start=2026-03-07"
- "--backfill-end=2026-06-05"
```

### Monitor progress

```bash
kubectl logs -f -l job-name=event-backfill -n data-pipeline
```

Output looks like:
```
[BACKFILL Day 3/7 43%] sim=2026-06-01 15:52 | users=144056 sessions=573 events=7947379 sim=31/s real=23109/s
```

### Verify data is flowing

```bash
RP_POD=$(kubectl get pod -n data-pipeline -l app=redpanda -o jsonpath='{.items[0].metadata.name}')
for topic in raw-events unified-events profile-updates; do
  high=$(kubectl exec $RP_POD -n data-pipeline -- rpk topic describe $topic -p 2>&1 | grep -E "^[0-9]" | awk '{sum+=$6} END {print sum}')
  echo "$topic: $high msgs"
done
```

All three topics should show increasing message counts. If `profile-updates` stays at 0, check the Profile Updater Flink job status.

### Wait for completion

```bash
kubectl wait --for=condition=complete job/event-backfill -n data-pipeline --timeout=3600s
```

Typical throughput: ~15k-40k events/sec real. A 7-day backfill (~15M events) takes ~10-15 minutes. A 90-day backfill (~200M+ events) may take 2-3 hours.

## 6. Resume live mode

```bash
# Delete the backfill job
kubectl delete job event-backfill -n data-pipeline

# Scale up the live event producer
kubectl scale deployment event-producer -n data-pipeline --replicas=1
```

Flink jobs stay running — they seamlessly switch from consuming backfill data to consuming live events.

## 7. Verify

```bash
# Check dashboard counts are populated
curl -s https://cdp.alytic.com.au/graphql -H 'Content-Type: application/json' \
  -d '{"query":"{ dashboardStats { totalUsers totalEvents activeSessions } }"}'

# Check all Flink jobs are RUNNING
JM_POD=$(kubectl get pod -n data-pipeline -l app=flink-jobmanager -o jsonpath='{.items[0].metadata.name}')
kubectl exec $JM_POD -n data-pipeline -- /opt/flink/bin/flink list -r
```

## Troubleshooting

### Pinot server OOM during backfill
The server JVM heap (`-Xmx`) must be large enough for realtime segment indexing. Current config: `-Xmx2560m` with 4Gi container limit. If OOM occurs, patch the JVM heap:
```bash
kubectl get statefulset pinot-server -n data-pipeline -o jsonpath='{.spec.template.spec.containers[0].env[0].value}'
```

### Flink jobmanager CrashLoopBackOff after restart
Usually caused by stale HA state referencing classes from a previous image. Fix: delete the HA ConfigMaps (step 2) and restart.

### Query-api OOM during backfill
The WebSocket streaming consumers skip deserialization when no browsers are connected (`receiver_count() == 0`). They also have `queued.max.messages.kbytes=65536` to cap the internal rdkafka buffer. If still OOMing after both fixes, delete the consumer group offsets so `auto.offset.reset=latest` kicks in:
```bash
RP_POD=$(kubectl get pod -n data-pipeline -l app=redpanda -o jsonpath='{.items[0].metadata.name}')
kubectl exec $RP_POD -n data-pipeline -- rpk group delete query-api-subscriptions
kubectl exec $RP_POD -n data-pipeline -- rpk group delete query-api-events
kubectl delete pod -l app=query-api -n data-pipeline
```

### Pinot table creation fails with "External view still exists"
Wait 5-10 seconds after deleting a table before recreating it. Pinot needs time to clean up ZooKeeper state.

### Backfill produces events but Kafka topics show 0 messages
The event-producer uses `send_result()` with fire-and-forget into rdkafka's internal buffer. If the Kafka broker is unreachable, sends silently fail. Check that Redpanda is healthy and the broker address is correct.
