import pandas as pd
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from prefect.exceptions import MissingContextError
from core.schemas import Lead
# from core.schemas import LeadField # Descomente e importe se você tiver um LeadField schema
from infrastructure.repositories import RepositorioBase, SchemaConfig, enrich_with_lookups_sql
from infrastructure.repositories.lookups import LEADS_LOOKUP_MAPPINGS
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common import utils
from infrastructure.observability import metrics
from infrastructure.db.postgres_adapter import get_postgres_conn
import time

@flow(name="Sync Pipedrive Leads")
def sync_pipedrive_leads_flow():
    logger = get_run_logger()
    flow_name_label = "LeadsSync"

    metrics.etl_counter.labels(flow_type=flow_name_label).inc()
    start_time_flow = time.time()
    processed_count = 0
    flow_run_id_value = "unknown_flow_run"

    try:
        lead_repo = RepositorioBase(
            "leads",
            SchemaConfig(
                pk=['id'],
                types={
                    'expected_close_date': 'DATE',
                    'value_amount': 'NUMERIC(18,4)',
                    'value_currency': 'VARCHAR(10)',
                    'owner_id': 'BIGINT',
                    'person_id': 'BIGINT',
                    'organization_id': 'BIGINT',
                    'label_ids': 'JSONB'
                },
                indexes=['owner_id', 'person_id', 'organization_id', 'is_archived', 'source_name', 'update_time'],
                allow_column_dropping=True
            )
        )

        lead_core_columns = [
            'id', 'title', 'owner_id', 'creator_id', 'label_ids', 'person_id', 'organization_id',
            'is_archived', 'value_amount', 'value_currency',
            'expected_close_date', 'add_time', 'update_time'
        ]
        lead_specific_handlers = {}
        pydantic_lead_field_model = None

        if pydantic_lead_field_model is None and "/leadFields" is not None:
            logger.warning("pydantic_model_field para Leads é None, mas api_endpoint_fields ('/leadFields') está configurado. O processamento de campos customizados pode ser limitado.")

        leads_synchronizer = PipedriveEntitySynchronizer(
            entity_name="Lead",
            pydantic_model_main=Lead,
            repository=lead_repo,
            api_endpoint_main="/leads",
            api_endpoint_fields="/leadFields",
            pydantic_model_field=pydantic_lead_field_model,
            core_columns=lead_core_columns,
            specific_field_handlers=lead_specific_handlers,
            utils_module=utils
        )

        logger.info("Starting Leads synchronization...")
        processed_count = leads_synchronizer.run_sync()
        logger.info(f"Leads synchronization completed. Processed {processed_count} records.")
        metrics.etl_last_successful_run_timestamp.labels(flow_type=flow_name_label).set_to_current_time()

        # Enrichment pós-sync (lookup automático)
        try:
            with get_postgres_conn().connection() as conn:
                enrich_with_lookups_sql(
                    table='leads',
                    lookups_mapping=LEADS_LOOKUP_MAPPINGS,
                    connection=conn,
                    logger=logger
                )
        except Exception as e_enrich:
            logger.warning(f"Leads lookup enrichment (SQL) failed: {e_enrich}")

        if lead_repo.schema_config.allow_column_dropping:
            logger.info(f"Attempting to drop fully null columns from '{lead_repo.table_name}' table...")
            try:
                lead_repo.drop_fully_null_columns(protected=lead_core_columns)
                logger.info(f"Drop fully null columns for '{lead_repo.table_name}' completed.")
            except Exception as e_drop:
                logger.error(f"Failed to drop fully null columns for '{lead_repo.table_name}': {str(e_drop)}", exc_info=True)

    except Exception as e:
        metrics.etl_failure_counter.labels(flow_type=flow_name_label).inc()
        logger.error(f"Leads synchronization failed: {str(e)}", exc_info=True)
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