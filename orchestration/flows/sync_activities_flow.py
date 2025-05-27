import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from core.schemas.activity_schema import Activity
from core.schemas.activity_field_schema import ActivityField
from infrastructure.repositories import RepositorioBase, SchemaConfig
from infrastructure.repositories.lookups import ACTIVITIES_LOOKUP_MAPPINGS
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common import utils
from infrastructure.observability import metrics
from infrastructure.db.postgres_adapter import get_postgres_conn
import time
import psycopg2
from psycopg2 import sql

def enrich_with_lookups_sql(
    table: str,
    lookups_mapping: dict,
    connection,
    logger=None,
):
    """
    Aplica enrichment (lookups) via SQL UPDATE ... FROM para os campos definidos em lookups_mapping.
    """
    mapping = lookups_mapping.get(table, {})
    with connection.cursor() as cur:
        for col, cfg in mapping.items():
            src_table = cfg['source']
            src_key = cfg['key']
            src_val = cfg['value_col']
            target_col = cfg['target_col']

            # Garante que a coluna existe
            cur.execute(sql.SQL(
                "ALTER TABLE {main} ADD COLUMN IF NOT EXISTS {target} TEXT"
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col)
            ))

            # UPDATE usando JOIN para enriquecer
            enrich_sql = sql.SQL(
                """
                UPDATE {main}
                SET {target} = src.{src_val}
                FROM {src_table} AS src
                WHERE {main}.{col} = src.{src_key}
                  AND {main}.{col} IS NOT NULL
                """
            ).format(
                main=sql.Identifier(table),
                target=sql.Identifier(target_col),
                src=sql.Identifier(src_table),
                src_val=sql.Identifier(src_val),
                src_table=sql.Identifier(src_table),
                col=sql.Identifier(col),
                src_key=sql.Identifier(src_key)
            )
            if logger:
                logger.info(f"Enriching {table}.{target_col} using {src_table} ({col} -> {src_val})...")
            cur.execute(enrich_sql)
        connection.commit()
    if logger:
        logger.info(f"SQL enrichment complete for {table}.")

@task(name="Enrich Activities Table (SQL)")
def enrich_activities_table_task_sql():
    logger = get_run_logger()
    logger.info("Enriching Activities table with SQL lookups...")
    try:
        with get_postgres_conn().connection() as conn:
            enrich_with_lookups_sql(
                table='atividades',
                lookups_mapping=ACTIVITIES_LOOKUP_MAPPINGS,
                connection=conn,
                logger=logger
            )
            logger.info("Activities enrichment (SQL) finished.")
            return True
    except Exception as e:
        logger.error(f"Error enriching Activities table: {e}", exc_info=True)
        return False

@flow(name="Sync Pipedrive Activities")
def sync_pipedrive_activities_flow():
    logger = get_run_logger()
    flow_name_label = "ActivitiesSync"

    metrics.etl_counter.labels(flow_type=flow_name_label).inc()
    start_time_flow = time.time()
    processed_count = 0
    flow_run_id_value = "unknown_flow_run" 

    try:
        activity_repo_config = SchemaConfig(
            pk=['id'],
            types={
                'user_id': 'BIGINT',
                'deal_id': 'BIGINT',
                'person_id': 'BIGINT',
                'org_id': 'BIGINT',
                'project_id': 'BIGINT',
                'created_by_user_id': 'BIGINT',
                'assigned_to_user_id': 'BIGINT',
                'update_user_id': 'BIGINT',
                'due_date': 'DATE',
                'note': 'TEXT',
                'public_description': 'TEXT',
                'location': 'TEXT',
                'location_formatted_address': 'TEXT',
                'participants': 'JSONB',
                'attendees': 'JSONB',
            },
            indexes=[
                'user_id', 'deal_id', 'person_id', 'org_id', 'project_id', 
                'type', 'done', 'due_date', 'update_time', 'add_time'
            ],
            allow_column_dropping=True
        )
        activity_repo = RepositorioBase("atividades", activity_repo_config)

        activity_core_columns = [
            'id', 'company_id', 'user_id', 'done', 'type', 'subject', 'due_date', 'due_time', 
            'duration', 'add_time', 'update_time', 'marked_as_done_time', 'deal_id', 
            'person_id', 'org_id', 'project_id', 'note', 'location', 'location_formatted_address',
            'public_description', 'active_flag', 'update_user_id', 'gcal_event_id', 
            'google_calendar_id', 'google_calendar_etag', 'conference_meeting_client', 
            'conference_meeting_url', 'conference_meeting_id', 'created_by_user_id', 
            'assigned_to_user_id', 'participants', 'attendees', 'busy_flag',
            'owner_name', 'person_name', 'org_name', 'deal_title'
        ]

        activity_synchronizer = PipedriveEntitySynchronizer(
            entity_name="Activity",
            pydantic_model_main=Activity,
            repository=activity_repo,
            api_endpoint_main="/activities",
            api_endpoint_fields="/activityFields",
            pydantic_model_field=ActivityField,
            core_columns=activity_core_columns,
            specific_field_handlers={},
            utils_module=utils
        )

        logger.info("Starting Activities synchronization...")
        processed_count = activity_synchronizer.run_sync()
        logger.info(f"Activities synchronization completed. Processed {processed_count} records.")
        metrics.etl_last_successful_run_timestamp.labels(flow_type=flow_name_label).set_to_current_time()

        if activity_repo.schema_config.allow_column_dropping:
            logger.info(f"Attempting to drop fully null columns from '{activity_repo.table_name}' table...")
            try:
                activity_repo.drop_fully_null_columns(protected=activity_core_columns)
                logger.info(f"Drop fully null columns for '{activity_repo.table_name}' completed.")
            except Exception as e_drop:
                logger.error(f"Failed to drop fully null columns for '{activity_repo.table_name}': {str(e_drop)}", exc_info=True)

        # ---- Enrichment após sync (full SQL) ----
        enrich_activities_table_task_sql()

    except Exception as e:
        metrics.etl_failure_counter.labels(flow_type=flow_name_label).inc()
        logger.error(f"Activities synchronization failed: {str(e)}", exc_info=True)
        raise
    finally:
        duration_flow = time.time() - start_time_flow
        metrics.etl_duration_hist.labels(flow_type=flow_name_label).observe(duration_flow)
        try:
            pg_pool = get_postgres_conn()
            if pg_pool:
                metrics.db_active_connections.set(pg_pool.active)
                metrics.db_idle_connections.set(pg_pool.idle)
        except Exception as db_e:
            logger.warning(f"Could not set DB pool metrics for {flow_name_label}: {db_e}")
        metrics.update_uptime()
        metrics.etl_heartbeat.labels(flow_type=flow_name_label).set_to_current_time()
        try:
            flow_run_id_value = flow_run.id
            if flow_run_id_value is None:
                logger.warning("flow_run.id retornou None. Usando 'unknown_flow_run_runtime_none'.")
                flow_run_id_value = "unknown_flow_run_runtime_none"
            else:
                flow_run_id_value = str(flow_run_id_value)
        except Exception as e_ctx:
            logger.error(f"Erro ao tentar obter flow_run_id via flow_run.id: {e_ctx}", exc_info=True)
            flow_run_id_value = "unknown_flow_run_exception"

        metrics.push_metrics_to_gateway(
            job_name="pipedrive_etl",
            grouping_key={'flow_name': flow_name_label, 'instance': flow_run_id_value}
        )
