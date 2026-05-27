#!/usr/bin/env bash
set -euo pipefail

CONTROLLER=${PINOT_CONTROLLER:-http://pinot-controller.data-pipeline.svc.cluster.local:9000}
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Applying Pinot schemas and tables to ${CONTROLLER}"

for schema in "${SCRIPT_DIR}/schemas/"*.json; do
  name=$(basename "$schema" .json)
  echo "  Creating schema: ${name}"
  curl -s -X POST "${CONTROLLER}/schemas" \
    -H "Content-Type: application/json" \
    -d @"${schema}" | jq .
done

for table in "${SCRIPT_DIR}/tables/"*.json; do
  name=$(basename "$table" .json)
  echo "  Creating table: ${name}"
  curl -s -X POST "${CONTROLLER}/tables" \
    -H "Content-Type: application/json" \
    -d @"${table}" | jq .
done

echo "Done. Pinot will begin ingesting from Kafka topics immediately."
