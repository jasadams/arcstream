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

# ---------------------------------------------------------------------------
# ARC-3: Profile Updater
# ---------------------------------------------------------------------------
# 1) Register the profile_step Rust UDF as a GLOBAL udf. The Arroyo fork's
#    create-udf endpoint (POST /api/v1/udfs) deserializes into UdfPost
#    { prefix: String, language: UdfLanguage(default rust), definition: String,
#      description: Option<String> }
#    (crates/arroyo-rpc/src/api_types/udfs.rs:48-56, route in
#     crates/arroyo-api/src/rest.rs:168). The function name is parsed out of the
#    compiled definition, so there is NO `name` field in the request body.
UDF_FILE="$(dirname "$0")/../flink/sql/udfs/profile_step.rs"

echo "Registering profile_step UDF..."
UDF_DEF=$(cat "$UDF_FILE")
# Not idempotent: a re-run when the UDF already exists returns non-2xx and (with set -e) halts. Delete the existing UDF first (DELETE /api/v1/udfs/{id}) when redeploying.
UDF_RESPONSE=$(curl -sf -X POST "$ARROYO_URL/api/v1/udfs" \
  -H "Content-Type: application/json" \
  -d "$(jq -n \
    --arg prefix "profile" \
    --arg language "rust" \
    --arg definition "$UDF_DEF" \
    --arg description "ARC-3 profile updater read-modify-write step" \
    '{prefix: $prefix, language: $language, definition: $definition, description: $description}'
  )")

echo "$UDF_RESPONSE" | jq '{id: .id, name: .name, prefix: .prefix}'
echo "UDF registered."

# 2) Submit the profile-updater pipeline (mirrors the identity-resolution
#    submit above).
PROFILE_SQL_FILE="$(dirname "$0")/../flink/sql/profile-updater-arroyo.sql"

echo "Submitting profile-updater pipeline..."
PROFILE_QUERY=$(cat "$PROFILE_SQL_FILE")
PROFILE_RESPONSE=$(curl -sf -X POST "$ARROYO_URL/api/v1/pipelines" \
  -H "Content-Type: application/json" \
  -d "$(jq -n \
    --arg name "profile-updater" \
    --arg query "$PROFILE_QUERY" \
    --argjson parallelism 1 \
    --argjson checkpoint_interval_micros 60000000 \
    '{name: $name, query: $query, parallelism: $parallelism, checkpoint_interval_micros: $checkpoint_interval_micros}'
  )")

echo "$PROFILE_RESPONSE" | jq '{id: .id, name: .name, state: .action}'
echo "Profile-updater pipeline submitted. View at $ARROYO_URL"
