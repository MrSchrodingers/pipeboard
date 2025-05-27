#!/usr/bin/env bash

set -euo pipefail
: "${PREFECT_API_URL:?Need PREFECT_API_URL to be set}"
: "${PREFECT_WORK_POOL_NAME:?Need PREFECT_WORK_POOL_NAME to be set}" 

# 1) Espera a API ficar UP
echo "⏳ Esperando Prefect API em ${PREFECT_API_URL}…" 

HEALTH_CHECK_URL="${PREFECT_API_URL%/api}/health"     
until curl -sf "${HEALTH_CHECK_URL}"; do              
    echo "  ainda não disponível (${HEALTH_CHECK_URL}), tentando de novo em 5s…"
    sleep 5                                         
done
echo "✅ Prefect API está pronta!"

# 2) Inicia o worker
echo "[$(date '+%F %T')] [INFO] Starting Prefect worker on pool '${PREFECT_WORK_POOL_NAME}'…"
exec prefect worker start --pool "${PREFECT_WORK_POOL_NAME}"