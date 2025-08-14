"""Prefect 2 – Sync /persons  ➜  pessoas"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict

from prefect import flow, get_run_logger

from core.schemas import Person, PersonField
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import (
    RepositorioBase,
    SchemaConfig,
    enrich_with_lookups_sql,
)
from infrastructure.repositories.lookups import PERSONS_LOOKUP_MAPPINGS
from infrastructure.repositories.dependent_entities import PERSONS_DEPENDENT_ENTITIES_CONFIG
from orchestration.common.compute_updated_since import compute_updated_since
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils


_TYPES: Dict[str, str] = {
    k: TYPES_CLEAN[k] for k in ("label_ids", "picture_id", "org_id", "owner_id", "custom_fields_overflow")
}
_CORE_COLS = [
    "id",
    "owner_id",
    "name",
    "first_name",
    "last_name",
    "add_time",
    "update_time",
    "visible_to",
    "birthday",
    "job_title",
    "custom_fields_overflow"
]
_SPECIFIC = {
    "emails": {
        "function": utils.explode_list_column,
        "slug_name": "email_contatos",
        "params": {"value_key": "value", "label_key": "label", "primary_key": "primary"},
    },
    "phones": {
        "function": utils.explode_list_column,
        "slug_name": "telefone_contatos",
        "params": {"value_key": "value", "label_key": "label", "primary_key": "primary"},
    },
    "postal_address": {
        "function": utils.explode_address,
        "slug_name": "endereco_contato",
    },
}


@flow(name="Sync Pipedrive Persons")
def sync_pipedrive_persons_flow() -> None:
    log   = get_run_logger()
    label = "PersonsSync"
    start = time.time()

    metrics.etl_counter.labels(flow_type=label).inc()

    upd_since_iso, full = compute_updated_since(
        flow_id=label,
        deps_cfg=PERSONS_DEPENDENT_ENTITIES_CONFIG,
        logger=log,
    )
    log.info("sync mode → %s (updated_since=%s)",
             "FULL" if full else "INCREMENTAL",
             upd_since_iso)

    try:
        repo = RepositorioBase(
            "pessoas",
            SchemaConfig(
                pk=["id"],
                types=_TYPES,
                indexes=["owner_id", "update_time", "add_time", "name", "cpf_cnpj_text"],
                allow_column_dropping=True,
            ),
        )
        repo.ensure_table()

        syncer = PipedriveEntitySynchronizer(
            entity_name="Person",
            pydantic_model_main=Person,
            repository=repo,
            api_endpoint_main="/persons",
            api_endpoint_fields="/personFields",
            pydantic_model_field=PersonField,
            core_columns=_CORE_COLS,
            specific_field_handlers=_SPECIFIC,
            
        )

        total = syncer.run_sync()
        log.info("✔ Persons finished – %s registros", total)

        # with get_postgres_conn().connection() as conn:
        #     enrich_with_lookups_sql(
        #         table="pessoas",
        #         lookups_mapping=PERSONS_LOOKUP_MAPPINGS,
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
        log.exception("Persons flow failed")
        raise

    finally:
        utils.finish_flow_metrics(label, start, log)
