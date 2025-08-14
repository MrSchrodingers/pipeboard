"""Prefect 2 – Sync /stages  ➜  etapas_do_funil"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, Optional

from prefect import flow, get_run_logger

from core.schemas.stage_schema import Stage
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import RepositorioBase, SchemaConfig
from infrastructure.repositories.enrich_with_lookups import enrich_with_lookups_sql
from infrastructure.repositories.lookups import STAGES_LOOKUP_MAPPINGS
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils


_TYPES: Dict[str, str] = {
    k: TYPES_CLEAN[k] for k in ("pipeline_id", "deal_probability", "rotten_flag", "order_nr")
}
_CORE_COLS = [
    "id",
    "name",
    "pipeline_id",
    "order_nr",
    "deal_probability",
    "rotten_flag",
    "add_time",
    "update_time",
]


@flow(name="Sync Pipedrive Stages")
def sync_pipedrive_stages_flow(full_refresh: bool = False) -> None:
    log   = get_run_logger()
    label = "StagesSync"
    start = time.time()

    metrics.etl_counter.labels(flow_type=label).inc()

    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        last_ok: Optional[datetime] = utils.get_last_successful_run_ts(label, cur)
        updated_since = None if full_refresh else last_ok

    try:
        repo = RepositorioBase(
            "etapas_funil",
            SchemaConfig(
                pk=["id"],
                types=_TYPES,
                indexes=["pipeline_id", "update_time", "order_nr"],
                allow_column_dropping=True,
            ),
        )
        repo.ensure_table()

        syncer = PipedriveEntitySynchronizer(
            entity_name="Stage",
            pydantic_model_main=Stage,
            repository=repo,
            api_endpoint_main="/stages",
            core_columns=_CORE_COLS,
            
        )

        total = syncer.run_sync(updated_since=updated_since)
        log.info("✔ Stages finished – %s registros", total)
        
        # with get_postgres_conn().connection() as conn:
        #     enrich_with_lookups_sql(
        #         table="etapas_funil",
        #         lookups_mapping=STAGES_LOOKUP_MAPPINGS,
        #         connection=conn,
        #         logger=log,
        #     )

        if repo.schema_config.allow_column_dropping:
            repo.drop_fully_null_columns(protected=_CORE_COLS)

        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            utils.update_last_successful_run_ts(label, datetime.utcnow(), cur)
            conn.commit()

        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()

    except Exception:
        metrics.etl_failure_counter.labels(flow_type=label).inc()
        log.exception("Stages flow failed")
        raise

    finally:
        utils.finish_flow_metrics(label, start, log)
