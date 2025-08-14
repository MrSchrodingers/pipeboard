"""Prefect 2 – Sync /organizations  ➜  organizacoes"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict

from prefect import flow, get_run_logger

from core.schemas import Organization, OrganizationField
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import (
    RepositorioBase,
    SchemaConfig,
    enrich_with_lookups_sql,
)
from infrastructure.repositories.lookups import ORGANIZATIONS_LOOKUP_MAPPINGS
from infrastructure.repositories.dependent_entities import (
    ORGANIZATIONS_DEPENDENT_ENTITIES_CONFIG,
)
from orchestration.common.compute_updated_since import compute_updated_since
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils


_TYPES: Dict[str, str] = {
    k: TYPES_CLEAN[k]
    for k in (
        "label_ids",
        "owner_id",
        "address_postal_code",
        "address_country",
        "address_admin_area_level_1",
        "custom_fields_overflow"
    )
}
_CORE_COLS = [
    "id",
    "owner_id",
    "name",
    "visible_to",
    "label_ids",
    "add_time",
    "update_time",
    "custom_fields_overflow"
]


@flow(name="Sync Pipedrive Organizations")
def sync_pipedrive_organizations_flow() -> None:
    log   = get_run_logger()
    label = "OrganizationsSync"
    start = time.time()

    metrics.etl_counter.labels(flow_type=label).inc()

    upd_since_iso, full = compute_updated_since(
        flow_id=label,
        deps_cfg=ORGANIZATIONS_DEPENDENT_ENTITIES_CONFIG,
        logger=log,
    )
    log.info("sync mode → %s (updated_since=%s)",
             "FULL" if full else "INCREMENTAL",
             upd_since_iso)

    # Escolhe campo de endereço (mudou entre versões da API)
    addr_field = (
        "postal_address"
        if "postal_address" in Organization.model_fields
        else "address"
    )
    specific_handlers = {
        addr_field: {
            "function": utils.explode_address,
            "slug_name": "endereco_principal",
        }
    }

    try:
        repo = RepositorioBase(
            "organizacoes",
            SchemaConfig(
                pk=["id"],
                types=_TYPES,
                indexes=["owner_id", "update_time", "add_time", "name"],
                allow_column_dropping=True,
            ),
        )
        repo.ensure_table()

        syncer = PipedriveEntitySynchronizer(
            entity_name="Organization",
            pydantic_model_main=Organization,
            repository=repo,
            api_endpoint_main="/organizations",
            api_endpoint_fields="/organizationFields",
            pydantic_model_field=OrganizationField,
            core_columns=_CORE_COLS,
            specific_field_handlers=specific_handlers,
            
        )

        total = syncer.run_sync()
        log.info("✔ Organizations finished – %s registros", total)

        # look-ups
        # with get_postgres_conn().connection() as conn:
        #     enrich_with_lookups_sql(
        #         table="organizacoes",
        #         lookups_mapping=ORGANIZATIONS_LOOKUP_MAPPINGS,
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
        log.exception("Organizations flow failed")
        raise

    finally:
        utils.finish_flow_metrics(label, start, log)
