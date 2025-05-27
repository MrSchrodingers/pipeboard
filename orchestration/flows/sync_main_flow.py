import numpy as np
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from typing import Dict
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.repositories import RepositorioBase, SchemaConfig
from infrastructure.observability import metrics
import time

# 1. Tabela alvo de BI
bi_target_table_name = "main_bi"
bi_target_pk = 'deal_id'

bi_target_repo = RepositorioBase(
    bi_target_table_name, 
    SchemaConfig(
        pk=[bi_target_pk], 
        types={
            bi_target_pk: 'BIGINT',
            'title': 'TEXT',
            'status': 'VARCHAR(50)',
            'value': 'NUMERIC(18,4)',
            'currency': 'VARCHAR(10)',
            'add_time': 'TIMESTAMP WITH TIME ZONE',
            'update_time': 'TIMESTAMP WITH TIME ZONE',
            'close_time': 'TIMESTAMP WITH TIME ZONE',
            'expected_close_date': 'DATE',
            'lost_time': 'TIMESTAMP WITH TIME ZONE',
            'won_time': 'TIMESTAMP WITH TIME ZONE',
            'stage_change_time': 'TIMESTAMP WITH TIME ZONE',
            'creator_user_id': 'BIGINT',
            'user_id': 'BIGINT',
            'pipeline_id': 'BIGINT',
            'stage_id': 'BIGINT',
            'label_ids': 'JSONB',
            'contact_name': 'TEXT',
            'contact_email': 'TEXT',
            'contact_phone': 'TEXT',
            'company_name': 'TEXT',
            'activity_count': 'SMALLINT',
            'last_activity_due_date': 'TIMESTAMP WITH TIME ZONE',
            'deal_duration_days': 'SMALLINT',
            'value_per_day': 'NUMERIC(18,4)',
        }, 
        indexes=['status', 'add_time', 'user_id', 'pipeline_id', 'stage_id', 'contact_name', 'company_name', 'update_time']
    )
)

@task(name="Load BI Sources")
def load_bi_sources() -> Dict[str, pd.DataFrame]:
    logger = get_run_logger()
    sources = {
        'deals': "SELECT * FROM negocios",
        'contacts': "SELECT * FROM pessoas",
        'companies': "SELECT * FROM organizacoes",
        'activities': "SELECT * FROM atividades",
    }
    data = {}
    try:
        with get_postgres_conn().connection() as conn:
            for name, query in sources.items():
                df = pd.read_sql(query, conn)
                data[name] = df
                logger.info(f"Loaded {len(df)} rows from '{name}'.")
    except Exception as e:
        logger.error(f"Error loading BI sources: {e}", exc_info=True)
        for k in sources: data[k] = pd.DataFrame()
    return data

@task(name="Transform & Aggregate BI")
def transform_and_aggregate_bi(
    deals_df: pd.DataFrame,
    contacts_df: pd.DataFrame,
    companies_df: pd.DataFrame,
    activities_df: pd.DataFrame
) -> pd.DataFrame:
    logger = get_run_logger()
    if deals_df.empty:
        logger.error("Deals DataFrame is empty. Returning empty BI.")
        return pd.DataFrame()
    bi_df = deals_df.copy()

    # -- Flatten contact fields (email, phone) se existirem --
    if 'person_id' in bi_df.columns and not contacts_df.empty:
        contacts_min = contacts_df.rename(columns={
            'id': 'person_id',
            'name': 'contact_name',
            'email_contatos_primary_value': 'contact_email',
            'telefone_contatos_primary_value': 'contact_phone'
        }, errors="ignore")
        bi_df = bi_df.merge(
            contacts_min[['person_id', 'contact_name', 'contact_email', 'contact_phone']].drop_duplicates('person_id'),
            on='person_id', how='left'
        )
    else:
        bi_df['contact_name'] = pd.NA
        bi_df['contact_email'] = pd.NA
        bi_df['contact_phone'] = pd.NA

    # -- Flatten company_name --
    if 'org_id' in bi_df.columns and not companies_df.empty:
        companies_min = companies_df.rename(columns={
            'id': 'org_id',
            'name': 'company_name'
        }, errors="ignore")
        bi_df = bi_df.merge(
            companies_min[['org_id', 'company_name']].drop_duplicates('org_id'),
            on='org_id', how='left'
        )
    else:
        bi_df['company_name'] = pd.NA

    # -- Aggregate activities (contagem + última due_date) --
    if not activities_df.empty and 'deal_id' in activities_df.columns:
        activities_df['due_date'] = pd.to_datetime(activities_df['due_date'], errors='coerce', utc=True)
        agg = activities_df.groupby('deal_id').agg(
            activity_count=('id', 'count'),
            last_activity_due_date=('due_date', 'max')
        ).reset_index()
        bi_df = bi_df.merge(agg, left_on='id', right_on='deal_id', how='left')
        bi_df['activity_count'] = bi_df['activity_count'].fillna(0).astype('Int16')
        bi_df['last_activity_due_date'] = pd.to_datetime(bi_df['last_activity_due_date'], errors='coerce', utc=True)
        bi_df.drop(columns=['deal_id'], inplace=True, errors='ignore')
    else:
        bi_df['activity_count'] = pd.NA
        bi_df['last_activity_due_date'] = pd.NaT

    # -- Calcula métricas adicionais --
    bi_df['close_time_dt'] = pd.to_datetime(bi_df.get('close_time'), errors='coerce', utc=True)
    bi_df['add_time_dt'] = pd.to_datetime(bi_df.get('add_time'), errors='coerce', utc=True)
    if 'close_time_dt' in bi_df and 'add_time_dt' in bi_df:
        duration = (bi_df['close_time_dt'] - bi_df['add_time_dt']).dt.days.astype('Int16')
        bi_df['deal_duration_days'] = duration
    else:
        bi_df['deal_duration_days'] = pd.NA
    if 'value' in bi_df:
        bi_df['value_num'] = pd.to_numeric(bi_df['value'], errors='coerce')
        bi_df['value_per_day'] = bi_df.apply(
            lambda row: row['value_num'] / row['deal_duration_days']
            if pd.notna(row['value_num']) and pd.notna(row['deal_duration_days']) and row['deal_duration_days'] > 0
            else np.nan, axis=1
        )
        bi_df['value_per_day'] = bi_df['value_per_day'].replace([np.inf, -np.inf], np.nan).astype('float64')
        bi_df.drop(columns=['value_num'], inplace=True, errors='ignore')
    else:
        bi_df['value_per_day'] = np.nan
    bi_df.drop(columns=['close_time_dt', 'add_time_dt'], inplace=True, errors='ignore')

    # -- Ajuste final PK --
    if 'id' in bi_df.columns:
        bi_df.rename(columns={'id': bi_target_pk}, inplace=True)

    # -- Limpa colunas intermediárias --
    drop_cols = ['person_id', 'org_id']
    bi_df.drop(columns=[c for c in drop_cols if c in bi_df.columns], inplace=True, errors='ignore')

    logger.info(f"BI DataFrame shape after all merges: {bi_df.shape}")
    return bi_df

@flow(name="Main BI Transformation Flow")
def main_bi_transformation_flow():
    logger = get_run_logger()
    flow_name_label = "BITransformationSync"
    metrics.etl_counter.labels(flow_type=flow_name_label).inc()
    start_time_flow = time.time()
    flow_run_id_value = "unknown_flow_run"

    logger.info("Starting Main BI Transformation Flow...")

    try:
        # 1. Carrega tabelas já enriquecidas
        data = load_bi_sources()

        # 2. Agrega/transforma dados para BI
        bi_df = transform_and_aggregate_bi(
            deals_df=data.get('deals', pd.DataFrame()),
            contacts_df=data.get('contacts', pd.DataFrame()),
            companies_df=data.get('companies', pd.DataFrame()),
            activities_df=data.get('activities', pd.DataFrame())
        )

        # 3. Salva resultado final
        if not bi_df.empty and bi_target_pk in bi_df.columns:
            logger.info(f"Saving {len(bi_df)} rows to BI target table: {bi_target_repo.table_name}")
            bi_target_repo.save(bi_df)
            metrics.records_processed_counter.labels(flow_type=flow_name_label).inc(len(bi_df))
        else:
            logger.warning("Final BI DataFrame is empty or missing primary key. Nothing to save.")

        metrics.etl_last_successful_run_timestamp.labels(flow_type=flow_name_label).set_to_current_time()
        logger.info("Main BI Transformation Flow completed successfully.")

    except Exception as e:
        metrics.etl_failure_counter.labels(flow_type=flow_name_label).inc()
        logger.error(f"Main BI Transformation Flow failed: {str(e)}", exc_info=True)
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
            job_name="pipedrive_bi_etl",
            grouping_key={'flow_name': flow_name_label, 'instance': flow_run_id_value}
        )
