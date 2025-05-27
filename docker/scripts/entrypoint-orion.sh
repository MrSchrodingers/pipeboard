#!/usr/bin/env bash
set -euo pipefail

# Inicia o servidor Prefect em background
prefect server start --host "${PREFECT_SERVER_API_HOST}" --port "${PREFECT_SERVER_API_PORT}" &
SERVER_PID=$!

# Espera o servidor ficar saudável internamente
until curl -sf "http://localhost:${PREFECT_SERVER_API_PORT:-4200}/api/health" >/dev/null; do
    echo "[WAIT] Orion booting (localhost:${PREFECT_SERVER_API_PORT:-4200}/api/health)..."; sleep 3
done
echo "[OK] Orion healthy."

export PREFECT_API_URL="http://localhost:${PREFECT_SERVER_API_PORT:-4200}/api"
export PREFECT_API_KEY="${PREFECT_SERVER_API_ADMIN_API_KEY:-}" 

echo "[INFO] Ensuring Prefect work pool '${PREFECT_WORK_POOL_NAME:-docker-pool}' exists…"

if prefect work-pool inspect "${PREFECT_WORK_POOL_NAME:-docker-pool}" >/dev/null 2>&1; then
    echo "[INFO] Work pool '${PREFECT_WORK_POOL_NAME:-docker-pool}' already exists."
else
    echo "[INFO] Creating work pool '${PREFECT_WORK_POOL_NAME:-docker-pool}'..."
    prefect work-pool create "${PREFECT_WORK_POOL_NAME:-docker-pool}" --type docker
    echo "[INFO] Work pool '${PREFECT_WORK_POOL_NAME:-docker-pool}' created."
fi

# Limpar as variáveis de ambiente se não forem mais necessárias neste escopo.
unset PREFECT_API_URL
unset PREFECT_API_KEY

# Mantém o servidor Orion rodando em primeiro plano
wait $SERVER_PID