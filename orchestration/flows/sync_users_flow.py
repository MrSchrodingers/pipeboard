# ──────────────────────────────────────────────────────────────
#  flows/pipedrive_users.py
#  Prefect 2.0 flow – sincroniza /users  ➜  tabela “usuarios”
# ──────────────────────────────────────────────────────────────
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, Optional

from prefect import flow, get_run_logger
from prefect.runtime import flow_run

from core.schemas import User
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import RepositorioBase, SchemaConfig
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils

# ────────────────────────────────
# 1.  Configuração de tipos/índices
# ────────────────────────────────
_USERS_TYPES: Dict[str, str] = {
    k: TYPES_CLEAN[k]
    for k in (
        "role_id", "lang", "phone", "active_flag",
        "is_admin", "last_login", "created", "modified",
        "icon_url", "timezone_offset", "company_id", "company_name",
        "company_domain", "activated", "access", "default_currency",
        "has_created_company", "is_deleted", "is_you",
    )
}

_CORE_COLS = [
    "id", "name", "email", "lang", "locale", "timezone_name",
    "timezone_offset", "is_admin", "role_id", "phone",
    "last_login", "created", "modified", "icon_url",
    "default_currency", "has_created_company", "is_deleted", "is_you",
]

# ────────────────────────────────
# 2.  Flow
# ────────────────────────────────
@flow(name="Sync Pipedrive Users")
def sync_pipedrive_users_flow(full_refresh: bool = False) -> None:
    """
    • Executa sincronização incremental por default.  
    • Passar *full_refresh=True* força refetch completo.
    """
    log  = get_run_logger()
    tag  = "UsersSync"
    start_ts = time.time()

    # ––––– métricas inicial
    metrics.etl_counter.labels(flow_type=tag).inc()

    # ––––– controla “updated_since”
    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        last_ok: Optional[datetime] = utils.get_last_successful_run_ts(tag, cur)
        # se consumidor pediu refresh forçado, ignora timestamp salvo
        updated_since = None if full_refresh else last_ok

    try:
        # 2.1 Repositório
        repo = RepositorioBase(
            "usuarios",
            SchemaConfig(
                pk=["id"],
                types=_USERS_TYPES,
                indexes=["email", "name"],
                allow_column_dropping=True,
            ),
        )
        repo.ensure_table()

        # 2.2 Synchronizer
        syncer = PipedriveEntitySynchronizer(
            entity_name="User",
            pydantic_model_main=User,
            repository=repo,
            api_endpoint_main="/users",
            core_columns=_CORE_COLS,
            
        )

        log.info("▶ Syncing Users  (updated_since=%s)", updated_since)
        total_rows = syncer.run_sync(updated_since=updated_since)
        log.info("✔ Users finished – %s rows", total_rows)

        # 2.3  housekeeping pós-load
        if repo.schema_config.allow_column_dropping:
            repo.drop_fully_null_columns(protected=_CORE_COLS)

        # 2.4  persiste timestamp último sucesso
        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            utils.update_last_successful_run_ts(tag, datetime.utcnow(), cur)
            conn.commit()

        # métricas de sucesso
        metrics.etl_last_successful_run_timestamp.labels(flow_type=tag).set_to_current_time()

    except Exception as exc:          # ⇢ Prefect marcará run como Failed
        metrics.etl_failure_counter.labels(flow_type=tag).inc()
        log.exception("Users flow failed – %s", exc)
        raise

    finally:
        # métricas genéricas –  always run
        utils.finish_flow_metrics(tag, start_ts, log)

