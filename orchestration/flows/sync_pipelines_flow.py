from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from prefect.exceptions import MissingContextError
from core.schemas import Pipeline 
from infrastructure.repositories import RepositorioBase, SchemaConfig
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common import utils
from infrastructure.observability import metrics
from infrastructure.db.postgres_adapter import get_postgres_conn
import time

@flow(name="Sync Pipedrive Pipelines")
def sync_pipedrive_pipelines_flow():
    logger = get_run_logger()
    flow_name_label = "PipelinesSync"
    
    metrics.etl_counter.labels(flow_type=flow_name_label).inc()
    start_time_flow = time.time()
    processed_count = 0
    flow_run_id_value = "unknown_flow_run"
    
    try:
        pipeline_repo = RepositorioBase(
            "pipelines",
            SchemaConfig(
                pk=['id'],
                types={
                    'active': 'BOOLEAN', 
                    'deal_probability': 'BOOLEAN',
                    'url_title': 'VARCHAR(255)',
                    'name': 'VARCHAR(255)'
                },
                indexes=['active', 'name', 'update_time'],
                allow_column_dropping=True
            )
        )

        pipeline_core_columns = [
            'id', 'name', 'url_title', 'order_nr', 'active', 'deal_probability',
            'add_time', 'update_time', 'selected'
        ]

        pipelines_synchronizer = PipedriveEntitySynchronizer(
            entity_name="Pipeline",
            pydantic_model_main=Pipeline,
            repository=pipeline_repo,
            api_endpoint_main="/pipelines",
            api_endpoint_fields=None, 
            pydantic_model_field=None,
            core_columns=pipeline_core_columns,
            specific_field_handlers={},
            utils_module=utils
        )

        logger.info("Starting Pipelines synchronization...")
        processed_count = pipelines_synchronizer.run_sync()
        logger.info(f"Pipelines synchronization completed. Processed {processed_count} records.")
        metrics.etl_last_successful_run_timestamp.labels(flow_type=flow_name_label).set_to_current_time()

        if pipeline_repo.schema_config.allow_column_dropping:
            logger.info(f"Attempting to drop fully null columns from '{pipeline_repo.table_name}' table...")
            try:
                pipeline_repo.drop_fully_null_columns(protected=pipeline_core_columns)
                logger.info(f"Drop fully null columns for '{pipeline_repo.table_name}' completed.")
            except Exception as e_drop:
                logger.error(f"Failed to drop fully null columns for '{pipeline_repo.table_name}': {str(e_drop)}", exc_info=True)

    except Exception as e:
        metrics.etl_failure_counter.labels(flow_type=flow_name_label).inc()
        logger.error(f"Pipelines synchronization failed: {str(e)}", exc_info=True)
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