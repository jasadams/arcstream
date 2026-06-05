#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=== Cleaning up previous run ==="
docker compose down -v 2>/dev/null || true

echo ""
echo "=== Starting Redpanda + MinIO ==="
docker compose up -d

echo "Waiting for Redpanda..."
until docker compose exec redpanda rpk cluster health 2>/dev/null | grep -q "Healthy"; do sleep 2; done
echo "Redpanda healthy."

echo ""
echo "=== Verifying cloud storage config ==="
docker compose exec redpanda rpk cluster config get cloud_storage_enabled
docker compose exec redpanda rpk cluster config get cloud_storage_secret_key
docker compose exec redpanda rpk cluster config get cloud_storage_segment_max_upload_interval_sec

echo ""
echo "=== Creating topic with small segments (1MB) ==="
docker compose exec redpanda rpk topic create test-tiered \
  -c redpanda.remote.write=true \
  -c redpanda.remote.read=true \
  -c retention.ms=7776000000 \
  -c retention.local.target.ms=60000 \
  -c segment.bytes=1048576

echo ""
echo "=== Producing 50k messages (~5MB) ==="
docker compose exec redpanda bash -c '
seq 1 50000 | while read i; do
  echo "{\"key\":\"user-$((i % 100))\",\"value\":\"event-$i-$(date +%s%N)\"}"
done | rpk topic produce test-tiered --format "%v\n"
'

echo ""
echo "=== Topic status ==="
docker compose exec redpanda rpk topic describe test-tiered -p

echo ""
echo "=== Waiting 45s for segment upload interval ==="
sleep 45

echo ""
echo "=== MinIO bucket ==="
docker compose exec minio mc alias set local http://localhost:9000 minioadmin supersecretkey123 2>/dev/null
docker compose exec minio mc du local/redpanda-tiered

echo ""
echo "=== MinIO objects ==="
docker compose exec minio mc ls local/redpanda-tiered --recursive 2>/dev/null | head -20

echo ""
echo "=== Redpanda upload logs ==="
docker compose logs redpanda 2>&1 | grep -i "upload\|remote segment added\|s3.*PUT\|SignatureDoesNotMatch" | grep -v "purger\|housekeeping" | tail -10

echo ""
echo "========================================="
objects=$(docker compose exec minio mc ls local/redpanda-tiered --recursive 2>/dev/null | wc -l)
if [ "$objects" -gt 0 ]; then
  echo "SUCCESS: $objects objects in MinIO bucket"
else
  echo "FAILED: No objects in MinIO bucket"
  echo ""
  echo "=== Debug: all cloud storage errors ==="
  docker compose logs redpanda 2>&1 | grep -iE "error|warn" | grep -i "s3\|cloud\|upload\|signature\|forbidden\|denied" | tail -10
fi
echo "========================================="
