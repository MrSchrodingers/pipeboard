import pandas as pd
from prefect import flow, get_run_logger
from prefect.runtime import flow_run
from prefect.exceptions import MissingContextError
from core.schemas import Organization, OrganizationField
from infrastructure.repositories import RepositorioBase, SchemaConfig, enrich_with_lookups_sql
from infrastructure.repositories.lookups import ORGANIZATIONS_LOOKUP_MAPPINGS
from orchestration.common.synchronizer import PipedriveEntitySynchronizer
from orchestration.common import utils
from infrastructure.observability import metrics
from infrastructure.db.postgres_adapter import get_postgres_conn
import time

@flow(name="Sync Pipedrive Organizations")
def sync_pipedrive_organizations_flow():
    logger = get_run_logger()
    flow_name_label = "OrganizationsSync"

    metrics.etl_counter.labels(flow_type=flow_name_label).inc()
    start_time_flow = time.time()
    processed_count = 0
    flow_run_id_value = "unknown_flow_run"

    try:
        org_repo = RepositorioBase(
            "organizacoes",
            SchemaConfig(
                pk=['id'],
                types={
                    'label_ids': 'JSONB',
                    'owner_id': 'BIGINT',
                    'address_postal_code': 'VARCHAR(30)',
                    'address_country': 'VARCHAR(100)',
                    'address_admin_area_level_1': 'VARCHAR(100)',
                },
                indexes=['owner_id', 'update_time', 'add_time', 'name'],
                allow_column_dropping=True
            )
        )

        org_core_columns = [
            'id', 'company_id', 'owner_id', 'name',
            'active_flag', 'visible_to', 'label_ids',
            'add_time', 'update_time', 'cc_email'
        ]

        org_specific_handlers = {}
        address_field_name_in_model = None
        if hasattr(Organization, 'postal_address') and 'postal_address' in Organization.model_fields:
            address_field_name_in_model = 'postal_address'
        elif hasattr(Organization, 'address') and 'address' in Organization.model_fields:
            address_field_name_in_model = 'address'

        if address_field_name_in_model:
            org_specific_handlers[address_field_name_in_model] = {
                'function': utils.explode_address,
                'slug_name': 'endereco_principal'
            }

        org_synchronizer = PipedriveEntitySynchronizer(
            entity_name="Organization",
            pydantic_model_main=Organization,
            repository=org_repo,
            api_endpoint_main="/organizations",
            api_endpoint_fields="/organizationFields",
            pydantic_model_field=OrganizationField,
            core_columns=org_core_columns,
            specific_field_handlers=org_specific_handlers,
            utils_module=utils
        )

        logger.info("Starting Organizations synchronization...")
        processed_count = org_synchronizer.run_sync()
        logger.info(f"Organizations synchronization completed. Processed {processed_count} records.")
        metrics.etl_last_successful_run_timestamp.labels(flow_type=flow_name_label).set_to_current_time()

        # Enrichment pós-sync
        try:
            with get_postgres_conn().connection() as conn:
                enrich_with_lookups_sql(
                    table='organizacoes',
                    lookups_mapping=ORGANIZATIONS_LOOKUP_MAPPINGS,
                    connection=conn,
                    logger=logger
                )
        except Exception as e_enrich:
            logger.warning(f"Organizations lookup enrichment (SQL) failed: {e_enrich}")

        if org_repo.schema_config.allow_column_dropping:
            logger.info(f"Attempting to drop fully null columns from '{org_repo.table_name}' table...")
            try:
                org_repo.drop_fully_null_columns(protected=org_core_columns)
                logger.info(f"Drop fully null columns for '{org_repo.table_name}' completed.")
            except Exception as e_drop:
                logger.error(f"Failed to drop fully null columns for '{org_repo.table_name}': {str(e_drop)}", exc_info=True)

    except Exception as e:
        metrics.etl_failure_counter.labels(flow_type=flow_name_label).inc()
        logger.error(f"Organizations synchronization failed: {str(e)}", exc_info=True)
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