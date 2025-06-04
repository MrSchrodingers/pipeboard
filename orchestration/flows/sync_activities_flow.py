"""Prefect 2 – Sync /activities  ➜  atividades"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, Optional

from prefect import flow, get_run_logger, task

from core.schemas.activity_schema import Activity
from core.schemas.activity_field_schema import ActivityField
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import (
    RepositorioBase,
    SchemaConfig,
    enrich_with_lookups_sql,
)
from infrastructure.repositories.lookups import ACTIVITIES_LOOKUP_MAPPINGS
from infrastructure.repositories.dependent_entities import (
    ACTIVITIES_DEPENDENT_ENTITIES_CONFIG,
)
from orchestration.common.compute_updated_since import compute_updated_since
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils


# ────────────────────────────  tipos / índices
_TYPES: Dict[str, str] = {
    k: TYPES_CLEAN[k]
    for k in (
        "user_id",
        "deal_id",
        "person_id",
        "org_id",
        "project_id",
        "created_by_user_id",
        "assigned_to_user_id",
        "update_user_id",
        "due_date",
        "note",
        "public_description",
        "location",
        "location_formatted_address",
        "participants",
        "attendees",
        "add_time",
        "update_time",
        "marked_as_done_time",
    )
}

_CORE_COLS = [
    "id",
    "user_id",
    "done",
    "type",
    "subject",
    "due_date",
    "due_time",
    "duration",
    "add_time",
    "update_time",
    "marked_as_done_time",
    "deal_id",
    "person_id",
    "org_id",
    "project_id",
    "note",
    "location",
    "location_formatted_address",
    "public_description",
    "update_user_id",
    "gcal_event_id",
    "google_calendar_id",
    "google_calendar_etag",
    "conference_meeting_client",
    "conference_meeting_url",
    "conference_meeting_id",
    "created_by_user_id",
    "assigned_to_user_id",
    "participants",
    "attendees",
    "busy_flag",
]

# ────────────────────────────  enrichment SQL (já existia)
@task
def _enrich_sql():
    log = get_run_logger()
    with get_postgres_conn().connection() as conn:
        enrich_with_lookups_sql(
            table="atividades",
            lookups_mapping=ACTIVITIES_LOOKUP_MAPPINGS,
            connection=conn,
            logger=log,
        )


# ────────────────────────────  Flow
@flow(name="Sync Pipedrive Activities")
def sync_pipedrive_activities_flow() -> None:
    log   = get_run_logger()
    label = "ActivitiesSync"
    start = time.time()

    metrics.etl_counter.labels(flow_type=label).inc()

    # ── decide modo full × incremental
    upd_since_iso, full_refresh = compute_updated_since(
        flow_id=label,
        deps_cfg=ACTIVITIES_DEPENDENT_ENTITIES_CONFIG,
        logger=log,
    )
    log.info("sync mode → %s (updated_since=%s)",
             "FULL" if full_refresh else "INCREMENTAL",
             upd_since_iso)

    try:
        repo = RepositorioBase(
            "atividades",
            SchemaConfig(
                pk=["id"],
                types=_TYPES,
                indexes=[
                    "user_id",
                    "deal_id",
                    "person_id",
                    "org_id",
                    "project_id",
                    "type",
                    "done",
                    "due_date",
                    "update_time",
                    "add_time",
                ],
                allow_column_dropping=True,
            ),
        )

        syncer = PipedriveEntitySynchronizer(
            entity_name="Activity",
            pydantic_model_main=Activity,
            repository=repo,
            api_endpoint_main="/activities",
            api_endpoint_fields="/activityFields",
            pydantic_model_field=ActivityField,
            core_columns=_CORE_COLS,
            
        )

        total = syncer.run_sync(updated_since=upd_since_iso)
        log.info("✔ Activities finished – %s registros", total)

        # housekeeping
        if repo.schema_config.allow_column_dropping:
            repo.drop_fully_null_columns(protected=_CORE_COLS)

        # carimbo OK
        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            utils.update_last_successful_run_ts(label, datetime.utcnow(), cur)
            conn.commit()

        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()

        # enrich look-ups
        _enrich_sql.submit()

    except Exception:
        metrics.etl_failure_counter.labels(flow_type=label).inc()
        log.exception("Activities flow failed")
        raise

    finally:
        utils.finish_flow_metrics(label, start, log)
