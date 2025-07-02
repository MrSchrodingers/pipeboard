"""Prefect 2 – Sync /deals  ➜  negocios"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, Optional

from prefect import flow, get_run_logger

from core.schemas.deal_fields_schema import DealField
from core.schemas.deal_schema import Deal
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import RepositorioBase, SchemaConfig
from infrastructure.repositories.dependent_entities import DEALS_DEPENDENT_ENTITIES_CONFIG
from infrastructure.repositories.enrich_with_lookups import enrich_with_lookups_sql
from infrastructure.repositories.lookups import DEALS_LOOKUP_MAPPINGS
from orchestration.common.compute_updated_since import compute_updated_since
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils


_TYPES: Dict[str, str] = {
    "title": TYPES_CLEAN["title"],
    "value": TYPES_CLEAN["value"],
    "currency": TYPES_CLEAN["currency"],
    "status": TYPES_CLEAN["status"],
    "pipeline_id": TYPES_CLEAN["pipeline_id"],
    "stage_id": TYPES_CLEAN["stage_id"],
    "person_id": TYPES_CLEAN["person_id"],
    "user_id": TYPES_CLEAN["user_id"],
    "add_time": TYPES_CLEAN["add_time"],
    "update_time": TYPES_CLEAN["update_time"],
    "custom_fields_overflow": TYPES_CLEAN["custom_fields_overflow"],
}
_CORE_COLS = [
    "id",
    "title",
    "status",
    "value",
    "currency",
    "add_time",
    "update_time",
    "close_time",
    "expected_close_date",
    "user_id",
    "pipeline_id",
    "stage_id",
    "custom_fields_overflow"
]


@flow(name="Sync Pipedrive Deals")
def sync_pipedrive_deals_flow() -> None:
    log   = get_run_logger()
    label = "DealsSync"
    start = time.time()

    metrics.etl_counter.labels(flow_type=label).inc()

    # Decide incremental/full a partir das dependências (persons, orgs, etc.)
    upd_since_iso, full = compute_updated_since(
        flow_id=label,
        deps_cfg=DEALS_DEPENDENT_ENTITIES_CONFIG,
        logger=log,
    )
    log.info("sync mode → %s (updated_since=%s)",
             "FULL" if full else "INCREMENTAL",
             upd_since_iso)

    try:
        repo = RepositorioBase(
            "negocios",
            SchemaConfig(
                pk=["id"],
                types=_TYPES,
                indexes=["user_id", "pipeline_id", "stage_id", "status", "update_time"],
                allow_column_dropping=True,
            ),
        )

        syncer = PipedriveEntitySynchronizer(
            entity_name="Deal",
            pydantic_model_main=Deal,
            repository=repo,
            api_endpoint_main="/deals",
            api_endpoint_fields="/dealFields",
            pydantic_model_field=DealField,
            core_columns=_CORE_COLS,
            
        )

        total = syncer.run_sync()
        log.info("✔ Deals finished – %s registros", total)
        
        with get_postgres_conn().connection() as conn:
            enrich_with_lookups_sql(
                table="negocios",
                lookups_mapping=DEALS_LOOKUP_MAPPINGS,
                connection=conn,
                logger=log,
            )

        if repo.schema_config.allow_column_dropping:
            repo.drop_fully_null_columns(protected=_CORE_COLS)

        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            utils.update_last_successful_run_ts(label, datetime.utcnow(), cur)
            conn.commit()

        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()

    except Exception:
        metrics.etl_failure_counter.labels(flow_type=label).inc()
        log.exception("Deals flow failed")
        raise

    finally:
        utils.finish_flow_metrics(label, start, log)
