from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from core.schemas import User
from infrastructure.repositories import RepositorioBase, SchemaConfig
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common import utils
from infrastructure.observability import metrics
from infrastructure.db.postgres_adapter import get_postgres_conn
import time

@flow(name="Sync Pipedrive Users")
def sync_pipedrive_users_flow():
    logger = get_run_logger()
    flow_name_label = "UsersSync"
    
    metrics.etl_counter.labels(flow_type=flow_name_label).inc()
    start_time_flow = time.time()
    processed_count = 0
    flow_run_id_value = "unknown_flow_run"
    
    try:
        user_repo = RepositorioBase(
            "usuarios",
            SchemaConfig(
                pk=['id'],
                types={
                    'role_id': 'BIGINT',
                    'lang': 'VARCHAR(10)',
                    'phone': 'VARCHAR(50)',
                    'active_flag': 'BOOLEAN', 
                    'is_admin': 'BOOLEAN',    
                    'last_login': 'TIMESTAMP WITH TIME ZONE',
                    'created': 'TIMESTAMP WITH TIME ZONE',
                    'modified': 'TIMESTAMP WITH TIME ZONE',
                    'icon_url': 'TEXT',
                    'timezone_offset': 'TEXT',
                    'company_id': 'TEXT', 
                    'company_name': 'TEXT',
                    'company_domain': 'TEXT',
                    'activated': 'BOOLEAN', 
                    'access': 'JSONB',      
                    'default_currency': 'TEXT',
                    'has_created_company': 'BOOLEAN', 
                    'is_deleted': 'BOOLEAN', 
                    'is_you': 'BOOLEAN'    
                },
                indexes=['email', 'active_flag', 'name', 'timezone_offset'],
                allow_column_dropping=True
            )
        )

        user_core_columns = [
            'id', 'name', 'email', 'lang', 'locale', 'timezone_name', 'timezone_offset',
            'active_flag', 'is_admin', 'role_id', 'phone', 'last_login',
            'created', 'modified', 'company_id', 'company_name', 'company_domain',
            'icon_url', 'activated', 'access', 'default_currency', 'has_created_company',
            'is_deleted', 'is_you'
        ]

        users_synchronizer = PipedriveEntitySynchronizer(
            entity_name="User",
            pydantic_model_main=User,
            repository=user_repo,
            api_endpoint_main="/users",
            api_endpoint_fields=None, 
            pydantic_model_field=None,
            core_columns=user_core_columns,
            specific_field_handlers={},
            utils_module=utils
        )

        logger.info("Starting Users synchronization...")
        processed_count = users_synchronizer.run_sync()
        logger.info(f"Users synchronization completed. Processed {processed_count} records.")
        
        if processed_count >= 0 : # Considera sucesso se run_sync não lançar exceção
             metrics.etl_last_successful_run_timestamp.labels(flow_type=flow_name_label).set_to_current_time()

        if user_repo.schema_config.allow_column_dropping:
            logger.info(f"Attempting to drop fully null columns from '{user_repo.table_name}' table...")
            try:
                user_repo.drop_fully_null_columns(protected=user_core_columns)
                logger.info(f"Drop fully null columns for '{user_repo.table_name}' completed.")
            except Exception as e_drop:
                logger.error(f"Failed to drop fully null columns for '{user_repo.table_name}': {str(e_drop)}", exc_info=True)

    except Exception as e:
        metrics.etl_failure_counter.labels(flow_type=flow_name_label).inc()
        logger.error(f"Users synchronization failed: {str(e)}", exc_info=True)
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