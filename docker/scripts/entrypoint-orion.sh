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

WORK_POOL_NAME="${PREFECT_WORK_POOL_NAME:-docker-pool}"

echo "[INFO] Garantindo que o work pool '${WORK_POOL_NAME}' existe..."

if prefect work-pool inspect "${WORK_POOL_NAME}" >/dev/null 2>&1; then
    echo "[INFO] Work pool '${WORK_POOL_NAME}' já existe."
else
    echo "[INFO] Criando work pool '${WORK_POOL_NAME}'..."
    prefect work-pool create "${WORK_POOL_NAME}" --type docker
    echo "[INFO] Work pool '${WORK_POOL_NAME}' criado."
fi

# Define o limite de concorrência
echo "[INFO] Definindo limite de concorrência para o work pool '${WORK_POOL_NAME}'..."
prefect work-pool set-concurrency-limit "${WORK_POOL_NAME}" 2
echo "[INFO] Limite de concorrência definido."

# Limpar as variáveis de ambiente se não forem mais necessárias neste escopo.
unset PREFECT_API_URL
unset PREFECT_API_KEY

# Mantém o servidor Orion rodando em primeiro plano
wait $SERVER_PID
