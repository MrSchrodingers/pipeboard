"""Prefect 2 – Sync /pipelines  ➜  pipelines"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, Optional

from prefect import flow, get_run_logger

from core.schemas import Pipeline
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import RepositorioBase, SchemaConfig
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils


_PIPE_TYPES: Dict[str, str] = {
    k: TYPES_CLEAN[k] for k in ("active", "deal_probability", "url_title", "name")
}
_CORE_COLS = ["id", "name", "order_nr", "add_time", "update_time"]


@flow(name="Sync Pipedrive Pipelines")
def sync_pipedrive_pipelines_flow(full_refresh: bool = False) -> None:
    log   = get_run_logger()
    label = "PipelinesSync"
    start = time.time()

    metrics.etl_counter.labels(flow_type=label).inc()

    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        last_ok: Optional[datetime] = utils.get_last_successful_run_ts(label, cur)
        updated_since = None if full_refresh else last_ok

    try:
        repo = RepositorioBase(
            "pipelines",
            SchemaConfig(
                pk=["id"],
                types=_PIPE_TYPES,
                indexes=["name", "update_time"],
                allow_column_dropping=True,
            ),
        )

        syncer = PipedriveEntitySynchronizer(
            entity_name="Pipeline",
            pydantic_model_main=Pipeline,
            repository=repo,
            api_endpoint_main="/pipelines",
            core_columns=_CORE_COLS,
            
        )

        total = syncer.run_sync(updated_since=updated_since)
        log.info("✔ Pipelines finished – %s registros", total)

        if repo.schema_config.allow_column_dropping:
            repo.drop_fully_null_columns(protected=_CORE_COLS)

        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            utils.update_last_successful_run_ts(label, datetime.utcnow(), cur)
            conn.commit()

        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()

    except Exception:
        metrics.etl_failure_counter.labels(flow_type=label).inc()
        log.exception("Pipelines flow failed")
        raise

    finally:
        utils.finish_flow_metrics(label, start, log)
