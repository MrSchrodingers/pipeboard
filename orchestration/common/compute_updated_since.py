# orchestration/common/compute_updated_since.py
from __future__ import annotations

from datetime import timedelta, timezone as dt_timezone
from typing import Dict, Any, Tuple

from infrastructure.db.postgres_adapter import get_postgres_conn
from orchestration.common.utils import get_last_successful_run_ts

def compute_updated_since(
    *,
    flow_id: str,
    deps_cfg: Dict[str, Any] | None = None,      # mantido para compatibilidade
    logger,
    skew_sec: int = 5,
) -> Tuple[str | None, bool]:
    """
    Retorna (updated_since_iso, is_full_sync).

    • FULL  → apenas se **nunca** houve execução bem-sucedida  
    • INCR. → sempre que já houver um `last_successful_run_ts`

    A pequena defasagem `skew_sec` garante que não falte nenhum
    registro que chegou exatamente no mesmo segundo do carimbo salvo.
    """
    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        last_ok = get_last_successful_run_ts(flow_id, cur)

    # 1ª execução → FULL
    if not last_ok:
        logger.info("first run – performing FULL sync")
        return None, True

    # Demais execuções → INCREMENTAL
    skewed = (last_ok - timedelta(seconds=skew_sec)).astimezone(dt_timezone.utc)
    updated_since_iso = skewed.strftime("%Y-%m-%dT%H:%M:%SZ")
    return updated_since_iso, False
