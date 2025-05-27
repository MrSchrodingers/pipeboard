import pandas as pd
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from prefect.exceptions import MissingContextError 
from core.schemas import Person, PersonField
from infrastructure.repositories import RepositorioBase, SchemaConfig, enrich_with_lookups_sql
from infrastructure.repositories.lookups import PERSONS_LOOKUP_MAPPINGS
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common import utils
from infrastructure.observability import metrics
from infrastructure.db.postgres_adapter import get_postgres_conn
import time

@flow(name="Sync Pipedrive Persons")
def sync_pipedrive_persons_flow():
    logger = get_run_logger()
    flow_name_label = "PersonsSync"
    
    metrics.etl_counter.labels(flow_type=flow_name_label).inc()
    start_time_flow = time.time()
    processed_count = 0

    try:
        person_repo = RepositorioBase(
            table_name="pessoas", 
            schema_config=SchemaConfig(
                pk=['id'], 
                types={ 
                    'label_ids': 'JSONB', 
                    'picture_id': 'BIGINT',
                    'org_id': 'BIGINT',
                    'owner_id': 'BIGINT',
                },
                indexes=['owner_id', 'org_id', 'update_time', 'add_time', 'name'],
                allow_column_dropping=True 
            )
        )
        
        person_core_columns = [
            'id', 'company_id', 'owner_id', 'org_id', 
            'name', 'first_name', 'last_name', 
            'add_time', 'update_time', 'active_flag', 'visible_to', 'cc_email',
            'birthday', 'job_title'
        ]

        person_specific_handlers = {
            'emails': {
                'function': utils.explode_list_column,
                'slug_name': 'email_contatos', 
                'params': {'value_key': 'value', 'label_key': 'label', 'primary_key': 'primary'}
            },
            'phones': { 
                'function': utils.explode_list_column,
                'slug_name': 'telefone_contatos',
                'params': {'value_key': 'value', 'label_key': 'label', 'primary_key': 'primary'}
            },
            'postal_address': {
                 'function': utils.explode_address,
                 'slug_name': 'endereco_contato'
            }
        }

        persons_synchronizer = PipedriveEntitySynchronizer(
            entity_name="Person",
            pydantic_model_main=Person, 
            repository=person_repo,
            api_endpoint_main="/persons", 
            api_endpoint_fields="/personFields",
            pydantic_model_field=PersonField,
            core_columns=person_core_columns,
            specific_field_handlers=person_specific_handlers,
            utils_module=utils 
        )

        logger.info("Starting Pipedrive Persons synchronization flow...")
        processed_count = persons_synchronizer.run_sync()
        logger.info(f"Pipedrive Persons synchronization flow completed. Processed {processed_count} records.")
        metrics.etl_last_successful_run_timestamp.labels(flow_type=flow_name_label).set_to_current_time()

        # Enrichment pós-sync
        try:
            with get_postgres_conn().connection() as conn:
                enrich_with_lookups_sql(
                    table='pessoas',
                    lookups_mapping=PERSONS_LOOKUP_MAPPINGS,
                    connection=conn,
                    logger=logger
                )
        except Exception as e_enrich:
            logger.warning(f"Persons lookup enrichment (SQL) failed: {e_enrich}")

        if person_repo.schema_config.allow_column_dropping:
            logger.info(f"Attempting to drop fully null columns from '{person_repo.table_name}' table...")
            try:
                person_repo.drop_fully_null_columns(protected=person_core_columns)
                logger.info(f"Drop fully null columns for '{person_repo.table_name}' completed.")
            except Exception as e_drop:
                logger.error(f"Failed to drop fully null columns for '{person_repo.table_name}': {str(e_drop)}", exc_info=True)

    except Exception as e:
        metrics.etl_failure_counter.labels(flow_type=flow_name_label).inc()
        logger.error(f"Pipedrive Persons synchronization flow failed: {str(e)}", exc_info=True)
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