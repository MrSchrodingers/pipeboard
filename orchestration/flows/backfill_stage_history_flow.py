"""
Prefect 2 – Backfill deal stage history
--------------------------------------
Recupera o histórico de mudanças de etapa dos negócios ainda não presentes em
``negocios_etapas_historico``. Quando o negócio jamais mudou de etapa, grava
uma **linha sentinela** com o *stage atual* (e change_time nulo) para não
reprocessar na próxima execução.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Optional, Dict, List

import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.futures import wait
from prefect.task_runners import ConcurrentTaskRunner

from infrastructure.clients.pipedrive_api_client import PipedriveAPIClient
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import RepositorioBase, SchemaConfig
from orchestration.common.types import TYPES_CLEAN
from orchestration.common import utils

MAX_DEALS_PER_RUN = 1_000

# ─────────────────────────── Helpers ────────────────────────────
def _extract_stage_changes(api_items: List[Dict]) -> List[Dict]:
    """Filtra apenas transições de stage, lidando com os dois formatos da API."""
    rows: List[Dict] = []
    for item in api_items:
        payload = item.get("data") or item 
        # Formato antigo  (field_key na raiz)
        if payload.get("field_key") == "stage_id":
            if new_val := payload.get("new_value"):
                rows.append(
                    dict(
                        stage_id=int(new_val),
                        change_time=item["timestamp"],
                        user_id=payload.get("user_id"), 
                    )
                )
            continue

        # Formato novo (field_key dentro de details)
        details = payload.get("details", {})
        if details.get("field_key") == "stage_id":
            if new_val := details.get("new_value"):
                rows.append(
                    dict(
                        stage_id=int(new_val),
                        change_time=item["timestamp"],
                        user_id=payload.get("user_id"),
                    )
                )
    return rows

@task(name="fetch_deal_flow", retries=2, retry_delay_seconds=30, log_prints=False)
def fetch_deal_flow(deal_id: int) -> List[Dict]:
    """Baixa todo o flow de um negócio (com paginação start/limit)."""
    client = PipedriveAPIClient()
    cursor: Optional[int] = 0
    items: List[Dict] = []

    while cursor is not None:
        resp = client.call(
            "/deals/detail/flow",
            deal_id=deal_id,
            params={"start": cursor},
        )
        batch = resp.get("data") or resp.get("data", {}).get("items") or []
        if not isinstance(batch, list):
            break
        items.extend(batch)

        pag = resp.get("additional_data", {}).get("pagination", {})
        cursor = pag.get("next_start") if pag.get("more_items_in_collection") else None

    return items

# ─────────────────────────── Flow ───────────────────────────────
@flow(
    name="Backfill Pipedrive Stage History",
    log_prints=True,
    task_runner=ConcurrentTaskRunner(max_workers=32),
)
def backfill_pipedrive_stage_history_flow() -> None:
    log = get_run_logger()
    label = "StageHistoryBackfill"
    t0 = time.time()
    metrics.etl_counter.labels(flow_type=label).inc()

    # Destino
    repo = RepositorioBase(
        "negocios_etapas_historico",
        SchemaConfig(
            pk=["deal_id", "stage_id", "change_time"],
            types={
                "deal_id": TYPES_CLEAN["deal_id"],
                "stage_id": TYPES_CLEAN["stage_id"],
                "change_time": TYPES_CLEAN["stage_change_time"],
                "user_id": TYPES_CLEAN["user_id"],
            },
            indexes=[["deal_id", "stage_id", "change_time"]],
            allow_column_dropping=False,
        ),
    )
    repo.ensure_table()

    # Negócios ainda não processados (id + stage atual)
    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT d.id, d.stage_id, d.add_time
            FROM negocios d
            WHERE d.id NOT IN (
                SELECT DISTINCT deal_id FROM negocios_etapas_historico
            )
            ORDER BY d.id
            LIMIT %s
            """,
            (MAX_DEALS_PER_RUN,),
        )
        pending: List[tuple[int, Optional[int], Optional[datetime]]] = cur.fetchall()

    if not pending:
        log.info("Sem negócios pendentes.")
        utils.finish_flow_metrics(label, t0, log)
        return

    deal_ids   = [d for d, _, _ in pending]
    stage_map  = {d: s for d, s, _ in pending}
    created_map = {d: ts for d, _, ts in pending} 
    
    metrics.backfill_deals_remaining_gauge.set(len(deal_ids))
    log.info("🔄 %s deals pendentes para back-fill", len(deal_ids))

    # Download concorrente
    futures = fetch_deal_flow.map(deal_ids)
    wait(futures)

    total_rows, deals_no_change = 0, 0
    for deal_id, fut in zip(deal_ids, futures, strict=False):
        try:
            items = fut.result()
        except Exception as exc:
            log.error("❌ Falha ao baixar flow do deal %s: %s", deal_id, exc)
            continue

        rows = _extract_stage_changes(items)
        if rows:
            # dedupe para não enviar duas vezes o mesmo (deal_id, stage_id)
            df = (
                pd.DataFrame(rows)
                .assign(deal_id=deal_id)
                .sort_values("change_time")
                .drop_duplicates(subset=["deal_id", "stage_id"], keep="last")
            )
            repo.save(df)
            total_rows += len(rows)
            metrics.backfill_stage_rows_saved.inc(len(rows))
        else:
            current_stage = stage_map.get(deal_id)
            if current_stage is not None: 
                repo.save(
                    pd.DataFrame(
                        [
                            dict(
                                deal_id     = deal_id,
                                stage_id    = current_stage,
                                change_time = created_map.get(deal_id)
                                            or datetime.utcnow(),
                                user_id     = None,
                            )
                        ]
                    ),
                    staging_threshold=0,
                )
                deals_no_change += 1
                metrics.backfill_deals_without_changes.inc()
            else:
                log.warning(
                    "Deal %s sem stage_id – não foi criada linha sentinela.",
                    deal_id,
                )

    log.info(
        "✔ Back-fill concluído – %s linhas gravadas; %s deals sem mudanças",
        total_rows,
        deals_no_change,
    )

    # Marca execução bem-sucedida
    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        utils.update_last_successful_run_ts(label, datetime.utcnow(), cur)
        conn.commit()
        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()

    utils.finish_flow_metrics(label, t0, log)
