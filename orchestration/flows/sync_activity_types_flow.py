"""Prefect 2 – Sync /activityTypes  ➜  atividade_tipos"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, Optional

from prefect import flow, get_run_logger

from core.schemas.activity_type_schema import ActivityType
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import RepositorioBase, SchemaConfig
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils


_TYPES: Dict[str, str] = {
    k: TYPES_CLEAN[k] for k in ("name", "key_string", "icon_key", "color")
}
_CORE_COLS = [
    "id",
    "name",
    "key_string",
    "icon_key",
    "active_flag",
    "color",
    "is_custom_flag",
    "order_nr",
    "add_time",
    "update_time",
]


@flow(name="Sync Pipedrive Activity Types")
def sync_pipedrive_activity_types_flow(full_refresh: bool = False) -> None:
    log   = get_run_logger()
    label = "ActivityTypesSync"
    start = time.time()

    metrics.etl_counter.labels(flow_type=label).inc()

    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        last_ok: Optional[datetime] = utils.get_last_successful_run_ts(label, cur)
        updated_since = None if full_refresh else last_ok

    try:
        repo = RepositorioBase(
            "atividade_tipos",
            SchemaConfig(
                pk=["id"],
                types=_TYPES,
                indexes=["key_string", "name"],
                allow_column_dropping=True,
            ),
        )
        repo.ensure_table()

        syncer = PipedriveEntitySynchronizer(
            entity_name="ActivityType",
            pydantic_model_main=ActivityType,
            repository=repo,
            api_endpoint_main="/activityTypes",
            core_columns=_CORE_COLS,
            
        )

        total = syncer.run_sync(updated_since=updated_since)
        log.info("✔ Activity-Types finished – %s registros", total)

        if repo.schema_config.allow_column_dropping:
            repo.drop_fully_null_columns(protected=_CORE_COLS)

        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            utils.update_last_successful_run_ts(label, datetime.utcnow(), cur)
            conn.commit()

        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()

    except Exception:
        metrics.etl_failure_counter.labels(flow_type=label).inc()
        log.exception("Activity-Types flow failed")
        raise

    finally:
        utils.finish_flow_metrics(label, start, log)
