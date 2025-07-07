from __future__ import annotations

import gc
import time
from typing import Dict, Iterable

import numpy as np
import pandas as pd
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import RepositorioBase, SchemaConfig

# ──────────────────────────────────────────────────────────
# Configurações gerais
# ──────────────────────────────────────────────────────────
BI_TABLE          = "main_bi"
PK                = "deal_id"
CHUNK             = 25_000   # linhas por lote na leitura paginada
STAGING_THRESHOLD = 1_000    # usa COPY se ≥ esse valor

bi_repo = RepositorioBase(
    BI_TABLE,
    SchemaConfig(
        pk=[PK],
        types={
            PK: "BIGINT",
            "title": "TEXT",
            "status": "VARCHAR(50)",
            "value": "NUMERIC(18,4)",
            "currency": "VARCHAR(10)",
            "add_time": "TIMESTAMP WITH TIME ZONE",
            "update_time": "TIMESTAMP WITH TIME ZONE",
            "close_time": "TIMESTAMP WITH TIME ZONE",
            "expected_close_date": "DATE",
            "lost_time": "TIMESTAMP WITH TIME ZONE",
            "won_time": "TIMESTAMP WITH TIME ZONE",
            "stage_change_time": "TIMESTAMP WITH TIME ZONE",
            "creator_user_id": "BIGINT",
            "user_id": "BIGINT",
            "pipeline_id": "BIGINT",
            "stage_id": "BIGINT",
            "label_ids": "JSONB",
            "contact_name": "TEXT",
            "contact_email": "TEXT",
            "contact_phone": "TEXT",
            "company_name": "TEXT",
            "activity_count": "SMALLINT",
            "last_activity_due_date": "TIMESTAMP WITH TIME ZONE",
            "deal_duration_days": "SMALLINT",
            "value_per_day": "NUMERIC(18,4)",
        },
        indexes=[
            "status",
            "add_time",
            "user_id",
            "pipeline_id",
            "stage_id",
            "contact_name",
            "company_name",
            "update_time",
        ],
    ),
)

# ──────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────
def _read_chunks(query: str) -> Iterable[pd.DataFrame]:
    """Lê um SELECT em pedaços para economizar RAM."""
    with get_postgres_conn().connection() as conn:
        for chunk in pd.read_sql_query(query, conn, chunksize=CHUNK):
            yield chunk


def _vectorized_value_per_day(df: pd.DataFrame) -> None:
    duration = (df["close_time"] - df["add_time"]).dt.days
    df["deal_duration_days"] = duration.astype("Int16")
    df["value_per_day"] = (
        pd.to_numeric(df["value"], errors="coerce") / duration.replace(0, np.nan)
    )

# ──────────────────────────────────────────────────────────
# Tasks
# ──────────────────────────────────────────────────────────
@task(name="Load BI Sources", retries=2, retry_delay_seconds=30)
def load_bi_sources() -> Dict[str, pd.DataFrame]:
    logger = get_run_logger()

    # apenas as colunas necessárias para o BI
    sources: dict[str, str] = {
        "deals": """
            SELECT id, person_id, org_id, title, status, value, currency,
                   add_time, update_time, close_time,
                   expected_close_date, lost_time, won_time,
                   stage_change_time, creator_user_id, user_id,
                   pipeline_id, stage_id, label_ids
            FROM negocios
        """,
        "contacts": """
            SELECT id, name, email_contatos_primary_value,
                   telefone_contatos_primary_value
            FROM pessoas
        """,
        "companies": "SELECT id, name FROM organizacoes",
    }

    data: Dict[str, pd.DataFrame] = {}
    try:
        for key, query in sources.items():
            df = pd.concat(_read_chunks(query), ignore_index=True)
            logger.info("Loaded %s rows from '%s'.", len(df), key)
            data[key] = df
    except Exception:
        logger.exception("Error loading BI sources – returning empty DataFrames.")
        return {k: pd.DataFrame() for k in sources}

    return data


@task(name="Transform & Aggregate BI")
def transform_and_aggregate_bi(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    logger = get_run_logger()
    deals = data["deals"]

    if deals.empty:
        logger.warning("No deals – nothing to transform.")
        return pd.DataFrame()

    # usa o próprio DataFrame (evita cópia)
    bi = deals

    # ── Contacts ─────────────────────────────
    if not data["contacts"].empty and "person_id" in bi:
        contacts = (
            data["contacts"]
            .rename(
                columns={
                    "id": "person_id",
                    "name": "contact_name",
                    "email_contatos_primary_value": "contact_email",
                    "telefone_contatos_primary_value": "contact_phone",
                }
            )
            .drop_duplicates("person_id")
        )
        bi["person_id"] = bi["person_id"].astype("string")
        contacts["person_id"] = contacts["person_id"].astype("string")
        bi = bi.merge(contacts, on="person_id", how="left")
    else:
        bi[["contact_name", "contact_email", "contact_phone"]] = pd.NA

    # ── Companies ────────────────────────────
    if not data["companies"].empty and "org_id" in bi:
        companies = (
            data["companies"]
            .rename(columns={"id": "org_id", "name": "company_name"})
            .drop_duplicates("org_id")
        )

        # --- Garantir dtype idêntico nas duas tabelas:
        bi["org_id"] = bi["org_id"].astype("string")
        companies["org_id"] = companies["org_id"].astype("string")


        bi = bi.merge(companies, on="org_id", how="left")
    else:
        bi["company_name"] = pd.NA

    # ── Métricas derivadas ───────────────────
    bi["add_time"] = pd.to_datetime(bi["add_time"], utc=True, errors="coerce")
    bi["close_time"] = pd.to_datetime(bi["close_time"], utc=True, errors="coerce")
    _vectorized_value_per_day(bi)

    # ── PK & limpeza ─────────────────────────
    # 1) descarta 'deal_id' redundante (gerado pelo merge)
    if "deal_id" in bi.columns:
        bi.drop(columns="deal_id", inplace=True)

    # 2) renomeia id → deal_id (PK)
    bi.rename(columns={"id": PK}, inplace=True)

    # 3) remove FKs que não precisamos manter no BI
    bi.drop(columns=["person_id", "org_id"], inplace=True, errors="ignore")

    logger.info("BI shape %s", bi.shape)
    return bi


# ──────────────────────────────────────────────────────────
# Flow principal
# ──────────────────────────────────────────────────────────
@flow(name="Main BI Transformation Flow")
def main_bi_transformation_flow() -> None:
    logger = get_run_logger()
    label = "BITransformationSync"

    metrics.etl_counter.labels(flow_type=label).inc()
    start = time.time()

    logger.info("=== Starting Main BI Transformation Flow ===")

    try:
        data   = load_bi_sources()
        bi_df  = transform_and_aggregate_bi(data)

        if not bi_df.empty and PK in bi_df.columns:
            logger.info("Persisting %s rows to %s", len(bi_df), BI_TABLE)
            bi_repo.save(bi_df)
            metrics.records_processed_counter.labels(flow_type=label).inc(len(bi_df))
        else:
            logger.warning("Nothing to save – empty DataFrame or PK missing.")

        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()
        logger.info("=== Flow completed OK ===")

    except Exception as exc:
        metrics.etl_failure_counter.labels(flow_type=label).inc()
        logger.exception("Flow failed: %s", exc)
        raise

    finally:
        # housekeeping
        duration = time.time() - start
        metrics.etl_duration_hist.labels(flow_type=label).observe(duration)

        try:
            pool = get_postgres_conn()
            if pool:
                metrics.db_active_connections.set(pool.active)
                metrics.db_idle_connections.set(pool.idle)
        except Exception:
            logger.debug("DB pool metrics unavailable.", exc_info=True)

        metrics.update_uptime()
        metrics.etl_heartbeat.labels(flow_type=label).set_to_current_time()

        run_id = getattr(flow_run, "id", "unknown") or "unknown"
        metrics.push_metrics_to_gateway(
            job_name="pipedrive_bi_etl",
            grouping_key={"flow_name": label, "instance": str(run_id)},
        )

        # libera RAM
        del data, bi_df
        gc.collect()
