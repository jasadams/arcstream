#!/usr/bin/env bash
# Submit identity resolution pipeline to local Arroyo instance (ARC-2).
# Run from the arcstream repo root: ./deploy/submit-arroyo-pipelines.sh
set -euo pipefail

ARROYO_URL="${ARROYO_URL:-http://localhost:5115}"
SQL_FILE="$(dirname "$0")/../flink/sql/identity-resolution-arroyo.sql"

echo "Waiting for Arroyo to be ready..."
for i in $(seq 1 30); do
  if curl -sf "$ARROYO_URL/api/v1/pipelines" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "Submitting identity-resolution pipeline..."
QUERY=$(cat "$SQL_FILE")
RESPONSE=$(curl -sf -X POST "$ARROYO_URL/api/v1/pipelines" \
  -H "Content-Type: application/json" \
  -d "$(jq -n \
    --arg name "identity-resolution" \
    --arg query "$QUERY" \
    --argjson parallelism 1 \
    --argjson checkpoint_interval_micros 60000000 \
    '{name: $name, query: $query, parallelism: $parallelism, checkpoint_interval_micros: $checkpoint_interval_micros}'
  )")

echo "$RESPONSE" | jq '{id: .id, name: .name, state: .action}'
echo "Pipeline submitted. View at $ARROYO_URL"
