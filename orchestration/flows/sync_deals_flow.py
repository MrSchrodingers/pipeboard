import pandas as pd
from prefect import flow, get_run_logger, task
from core.schemas import Deal, DealField
from prefect.runtime import flow_run
from infrastructure.clients import PipedriveAPIClient
from infrastructure.repositories import RepositorioBase, SchemaConfig, enrich_with_lookups_sql
from infrastructure.repositories.lookups import DEALS_LOOKUP_MAPPINGS
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common import utils
from infrastructure.observability import metrics
from infrastructure.db.postgres_adapter import get_postgres_conn
import time

@task(name="Get Pipedrive Currency Map", cache_policy=None)
def get_pipedrive_currency_map_task(api_client: PipedriveAPIClient) -> dict:
    logger = get_run_logger()
    logger.info("Fetching Pipedrive currency map...")
    currencies_data = api_client.call("/currencies").get('data', [])
    currency_map = {c['code']: c for c in currencies_data if 'code' in c} if currencies_data else {}
    logger.info(f"Fetched {len(currency_map)} currencies.")
    return currency_map


@task(name="Enrich Deals Table (SQL)")
def enrich_deals_table_task_sql():
    logger = get_run_logger()
    logger.info("Enriching Deals table with SQL lookups...")
    try:
        with get_postgres_conn().connection() as conn:
            enrich_with_lookups_sql(
                table='negocios',
                lookups_mapping=DEALS_LOOKUP_MAPPINGS,
                connection=conn,
                logger=logger
            )
        logger.info("Deals enrichment (SQL) finished.")
        return True
    except Exception as e:
        logger.error(f"Error enriching Deals table: {e}", exc_info=True)
        return False

@flow(name="Sync Pipedrive Deals")
def sync_pipedrive_deals_flow():
    logger = get_run_logger()
    flow_name_label = "DealsSync"
    metrics.etl_counter.labels(flow_type=flow_name_label).inc()
    start_time_flow = time.time()
    api_client = PipedriveAPIClient()
    processed_count = 0
    flow_run_id_value = "unknown_flow_run"

    try:
        currency_map_data = get_pipedrive_currency_map_task(api_client=api_client)
        deal_repo_config = SchemaConfig(
            pk=['id'],
            types={
                'value': 'NUMERIC(18,4)',
                'currency': 'VARCHAR(10)',
                'user_id': 'BIGINT',
                'person_id': 'BIGINT',
                'org_id': 'BIGINT',
                'stage_id': 'BIGINT',
                'pipeline_id': 'BIGINT',
                'label_ids': 'JSONB',
                'nº_aviso_de_sinistro_text': 'TEXT',
                'pasta_text': 'TEXT',
            },
            indexes=['stage_id', 'user_id', 'status', 'pipeline_id', 'update_time', 'add_time'],
            allow_column_dropping=True
        )
        deal_repo = RepositorioBase("negocios", deal_repo_config)
        deal_core_columns = [
            'id', 'creator_user_id', 'user_id', 'person_id', 'org_id',
            'stage_id', 'title', 'value', 'currency', 'add_time', 'update_time',
            'status', 'pipeline_id', 'expected_close_date', 'label_ids'
        ]
        deal_specific_handlers = {}
        if currency_map_data:
            deal_specific_handlers['_custom_currency_map_data'] = currency_map_data

        deals_synchronizer = PipedriveEntitySynchronizer(
            entity_name="Deal",
            pydantic_model_main=Deal,
            repository=deal_repo,
            api_endpoint_main="/deals",
            api_endpoint_fields="/dealFields",
            pydantic_model_field=DealField,
            core_columns=deal_core_columns,
            specific_field_handlers=deal_specific_handlers,
            utils_module=utils,
            api_client=api_client
        )

        logger.info("Starting Deals synchronization...")
        processed_count = deals_synchronizer.run_sync()
        logger.info(f"Deals synchronization completed. Processed {processed_count} records.")
        metrics.etl_last_successful_run_timestamp.labels(flow_type=flow_name_label).set_to_current_time()

        if deal_repo.schema_config.allow_column_dropping:
            logger.info(f"Attempting to drop fully null columns from '{deal_repo.table_name}' table...")
            try:
                deal_repo.drop_fully_null_columns(protected=deal_core_columns)
                logger.info(f"Drop fully null columns for '{deal_repo.table_name}' completed.")
            except Exception as e_drop:
                logger.error(f"Failed to drop fully null columns for '{deal_repo.table_name}': {str(e_drop)}", exc_info=True)

        enrich_deals_table_task_sql()

    except Exception as e:
        metrics.etl_failure_counter.labels(flow_type=flow_name_label).inc()
        logger.error(f"Deals synchronization failed: {str(e)}", exc_info=True)
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
