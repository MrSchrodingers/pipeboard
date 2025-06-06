# flows/pipedrive_activities.py
# Prefect 2 – Sync /activities  ➜  tabela “atividades”
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Dict, Optional, Any, Tuple, List

import pandas as pd
from prefect import flow, get_run_logger, task

from core.schemas.activity_schema import Activity
from core.schemas.activity_field_schema import ActivityField
from infrastructure.clients.pipedrive_api_client import PipedriveAPIClient
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import (
    RepositorioBase,
    SchemaConfig,
    enrich_with_lookups_sql,
)
from infrastructure.repositories.lookups import ACTIVITIES_LOOKUP_MAPPINGS
from orchestration.common import utils
from orchestration.common.compute_updated_since import compute_updated_since
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common.sync_state import get_sync_state, save_sync_state
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils as common_utils   # explode helpers etc.

# ─────────────────────────── Configurações internas
_ROWS_PER_RUN          = 100_000  # limite hard por execução
_PIPEDRIVE_PAGE_LIMIT  = PipedriveAPIClient.DEFAULT_PAGINATION_LIMIT  # 500
_ENTITY                = "Activity"

# ─────────────────────────── Tipos / índices
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

# ─────────────────────────── Task: enrichment SQL
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

# ─────────────────────────── Flow
@flow(name="Sync Pipedrive Activities")
def sync_pipedrive_activities_flow() -> None:
    """
    Estratégia dual:

    1) **Backfill paginado** – enquanto `etl_sync_state.backfill_done = false`,
       processa até 100 k atividades por run, retomando do último cursor (`start`).
    2) **Incremental** – após concluir o backfill, passa a usar
       `updated_since` via compute_updated_since (como os demais flows).
    """
    log   = get_run_logger()
    label = "ActivitiesSync"
    t0    = time.time()

    metrics.etl_counter.labels(flow_type=label).inc()

    # ──────────────────────── Estado do backfill
    last_cursor, backfill_done = get_sync_state(_ENTITY)  # (None, False) se não existir

    # ──────────────────────── Repositório
    repo = RepositorioBase(
        "atividades",
        SchemaConfig(
            pk=["id"],
            types=_TYPES,
            indexes=[
                "user_id", "deal_id", "person_id", "org_id", "project_id",
                "type", "done", "due_date", "update_time", "add_time",
            ],
            allow_column_dropping=True,
        ),
    )
    repo.save(pd.DataFrame(columns=repo.schema_config.pk), staging_threshold=0)

    # ──────────────────────── Sincronização
    try:
        if backfill_done:
            # ---------- modo incremental ----------
            upd_since_iso, _ = compute_updated_since(
                flow_id=label,
                deps_cfg=None,      # deps ignorados após mudança na compute_updated_since
                logger=log,
            )
            log.info("Incremental – updated_since=%s", upd_since_iso)

            syncer = PipedriveEntitySynchronizer(
                entity_name=_ENTITY,
                pydantic_model_main=Activity,
                repository=repo,
                api_endpoint_main="/activities",
                api_endpoint_fields="/activityFields",
                pydantic_model_field=ActivityField,
                core_columns=_CORE_COLS,
            )
            processed = syncer.run_sync(updated_since=upd_since_iso)
            save_sync_state(_ENTITY, None, True)   # mantém flag done=True

        else:
            # ---------- modo backfill paginado ----------
            client   = PipedriveAPIClient()
            syncer   = PipedriveEntitySynchronizer(
                entity_name=_ENTITY,
                pydantic_model_main=Activity,
                repository=repo,
                api_endpoint_main="/activities",
                api_endpoint_fields="/activityFields",
                pydantic_model_field=ActivityField,
                core_columns=_CORE_COLS,
                api_client=client,
            )

            params: Dict[str, Any] = {"limit": _PIPEDRIVE_PAGE_LIMIT}
            if last_cursor is not None:
                params["start"] = last_cursor

            log.info("Backfill – cursor=%s, limit/run=%s", last_cursor, _ROWS_PER_RUN)

            total_rows   = 0
            next_cursor: Optional[int] = None

            for batch in client.stream_all_entities("/activities", params=params):
                if total_rows >= _ROWS_PER_RUN:
                    break

                validated = common_utils.validate_parallel(
                    Activity, batch, workers=8
                )
                if not validated:
                    continue

                df = pd.DataFrame(validated)
                df = syncer._normalize_schema(
                    syncer._explode_custom_fields(
                        syncer._apply_specific_handlers(df)
                    )
                )
                repo.save(df)

                total_rows += len(df)

                # paginação atual
                pag = client.get_last_pagination()
                next_cursor = (
                    pag.get("next_start")
                    if pag and pag.get("more_items_in_collection")
                    else None
                )

                if total_rows >= _ROWS_PER_RUN or not next_cursor:
                    break

            processed = total_rows
            backfill_done_now = next_cursor is None
            save_sync_state(_ENTITY, next_cursor, backfill_done_now)

            log.info("Backfill batch %s rows – next_cursor=%s done=%s",
                     processed, next_cursor, backfill_done_now)

        # ─────────────────── housekeeping
        if repo.schema_config.allow_column_dropping:
            repo.drop_fully_null_columns(protected=_CORE_COLS)

        # carimbo timestamp “last_successful_run”
        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            utils.update_last_successful_run_ts(label, datetime.utcnow().astimezone(timezone.utc), cur)
            conn.commit()

        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()

        # enrichment (assíncrono)
        _enrich_sql.submit()

        log.info("✔ Activities finished – %s registros", processed)

    except Exception:
        metrics.etl_failure_counter.labels(flow_type=label).inc()
        log.exception("Activities flow failed")
        raise

    finally:
        utils.finish_flow_metrics(label, t0, log)
