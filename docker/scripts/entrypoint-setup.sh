#!/usr/bin/env bash
set -euo pipefail

: "${PREFECT_API_URL:?Need PREFECT_API_URL to be set for setup scripts}"
: "${PREFECT_API_KEY:?Need PREFECT_API_KEY to be set for setup scripts if API is protected}"

echo "[SETUP] Waiting for Orion API at ${PREFECT_API_URL} to be healthy..."

HEALTH_TARGET_URL_BASE=$(echo "${PREFECT_API_URL}" | sed 's#/api$##')
HEALTH_TARGET_URL="${HEALTH_TARGET_URL_BASE}/health"

RETRY_COUNT=0
MAX_RETRIES=60

# Loop até que o endpoint /api/health do Orion retorne um status HTTP 200
until curl -sf "${HEALTH_TARGET_URL}"; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ "${RETRY_COUNT}" -gt "${MAX_RETRIES}" ]; then
        echo "[SETUP] ERROR: Orion API at ${HEALTH_TARGET_URL} did not become healthy after ${MAX_RETRIES} retries."
        exit 1
    fi
    echo "[SETUP] Orion API at ${HEALTH_TARGET_URL} not yet available (attempt ${RETRY_COUNT}/${MAX_RETRIES}). Retrying in 5s..."
    sleep 5
done

echo "[SETUP] Orion API at ${HEALTH_TARGET_URL} is healthy and reachable."

echo "[SETUP] Running create_or_update_core_blocks.py..."
python /app/docker/utils/create_or_update_core_blocks.py

echo "[SETUP] Running blocks_and_deployments.py (this may take a while if building/pushing images)..."
python /app/docker/utils/blocks_and_deployments.py

echo "[SETUP] Setup completed successfully."