#!/usr/bin/env bash
host="$1"; port="$2"; shift 2
cmd="$@"

until curl -sf "http://${host}:${port}/api/health" >/dev/null; do
  echo "[WAIT] ${host}:${port} indisponível..."
  sleep 1
done
exec $cmd
