from __future__ import annotations
import random, time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import requests
from prefect import flow, get_run_logger, task
from prefect.futures import wait
from prefect.task_runners import ConcurrentTaskRunner
from requests import HTTPError

from infrastructure.clients.pipedrive_api_client import PipedriveAPIClient
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import RepositorioBase, SchemaConfig
from orchestration.common import utils
from orchestration.common.types import TYPES_CLEAN

# ═════════════ Config – ajuste conforme necessidade ═════════════
MAX_DEALS_PER_RUN   = 10_000
MAX_WORKERS         = 16                    # concorrência ↓
REQ_DELAY_SECONDS   = 0.15                  # throttle client-side (≈6 req/s)
BACKOFF_BASE        = 2                     # exp. back-off
BACKOFF_MAX_SLEEP   = 30                    # máx. espera (s)
# Coluna-chave p/ ordenar (mais recentes primeiro).  
# Possíveis: "update_time", "add_time", "close_time"
ORDER_FIELD         = "update_time"

# ═════════════ Helpers ═════════════
def _extract_stage_changes(api_items: List[Dict]) -> List[Dict]:
    rows: List[Dict] = []
    for item in api_items:
        payload = item.get("data") or item
        if payload.get("field_key") == "stage_id":                    # formato antigo
            if new_val := payload.get("new_value"):
                rows.append(
                    dict(stage_id=int(new_val),
                         change_time=item["timestamp"],
                         user_id=payload.get("user_id"))
                )
            continue
        details = payload.get("details", {})                          # formato novo
        if details.get("field_key") == "stage_id":
            if new_val := details.get("new_value"):
                rows.append(
                    dict(stage_id=int(new_val),
                         change_time=item["timestamp"],
                         user_id=payload.get("user_id"))
                )
    return rows


@task(name="fetch_deal_flow", retries=4, retry_delay_seconds=0, log_prints=False)
def fetch_deal_flow(deal_id: int) -> List[Dict]:
    """Baixa o flow completo de um negócio, respeitando rate-limit."""
    log = get_run_logger()
    client = PipedriveAPIClient()
    items: List[Dict] = []
    cursor: Optional[int] = 0
    attempt = 0
    while cursor is not None:
        try:
            time.sleep(REQ_DELAY_SECONDS)          # throttle
            resp = client.call(
                "/deals/detail/flow",
                deal_id=deal_id,
                params={"start": cursor},
            )
        except HTTPError as err:
            if err.response.status_code in (429, 502, 503):
                attempt += 1
                delay = min(BACKOFF_BASE ** attempt, BACKOFF_MAX_SLEEP)
                delay += random.uniform(0, delay * .2)   # jitter
                log.warning("HTTP %s – dormindo %.1fs (deal %s)",
                           err.response.status_code, delay, deal_id)
                time.sleep(delay)
                continue
            raise
        except requests.RequestException as err:
            log.warning("Erro de rede %s – retry", err)
            time.sleep(5)
            continue
        attempt = 0                                  # reset back-off

        batch = resp.get("data") or resp.get("data", {}).get("items") or []
        if not isinstance(batch, list):
            break
        items.extend(batch)

        pag = resp.get("additional_data", {}).get("pagination", {})
        cursor = pag.get("next_start") if pag.get("more_items_in_collection") else None
    return items


# ═════════════ Flow principal ═════════════
@flow(
    name="Backfill Pipedrive Stage History",
    log_prints=True,
    task_runner=ConcurrentTaskRunner(max_workers=MAX_WORKERS),
)
def backfill_pipedrive_stage_history_flow() -> None:
    log   = get_run_logger()
    label = "StageHistoryBackfill"
    t0    = time.time()
    metrics.etl_counter.labels(flow_type=label).inc()

    # destino
    repo = RepositorioBase(
        "negocios_etapas_historico",
        SchemaConfig(
            pk=["deal_id", "stage_id", "change_time"],
            types={
                "deal_id":   TYPES_CLEAN["deal_id"],
                "stage_id":  TYPES_CLEAN["stage_id"],
                "change_time": TYPES_CLEAN["stage_change_time"],
                "user_id":   TYPES_CLEAN["user_id"],
            },
            indexes=[["deal_id", "stage_id", "change_time"]],
            allow_column_dropping=False,
        ),
    )
    repo.ensure_table()

    # negócios pendentes – NOVA ordenação
    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT d.id, d.stage_id, d.add_time
            FROM   negocios d
            WHERE  d.id NOT IN (
                     SELECT DISTINCT deal_id
                     FROM negocios_etapas_historico
                   )
            ORDER  BY COALESCE(d.{ORDER_FIELD},
                               d.update_time,
                               d.add_time,
                               d.close_time) DESC NULLS LAST
            LIMIT  %s
            """,
            (MAX_DEALS_PER_RUN,),
        )
        pending: List[tuple[int, Optional[int], Optional[datetime]]] = cur.fetchall()

    if not pending:
        log.info("Sem negócios pendentes.")
        utils.finish_flow_metrics(label, t0, log)
        return

    deal_ids     = [d for d, _, _ in pending]
    stage_map    = {d: s for d, s, _ in pending}
    created_map  = {d: ts for d, _, ts in pending}
    metrics.backfill_deals_remaining_gauge.set(len(deal_ids))
    log.info("🔄 %s deals pendentes para back-fill (ordenados por %s)",
             len(deal_ids), ORDER_FIELD)

    # download concorrente
    futures = fetch_deal_flow.map(deal_ids)
    wait(futures)

    total_rows, deals_no_change = 0, 0
    for deal_id, fut in zip(deal_ids, futures, strict=False):
        try:
            items = fut.result()
        except Exception as exc:        # noqa: BLE001
            log.error("❌ Falha ao baixar flow %s: %s", deal_id, exc)
            continue

        rows = _extract_stage_changes(items)
        if rows:
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
            if (current_stage := stage_map.get(deal_id)) is not None:
                repo.save(
                    pd.DataFrame(
                        [dict(
                            deal_id=deal_id,
                            stage_id=current_stage,
                            change_time=created_map.get(deal_id)
                                        or datetime.utcnow().astimezone(timezone.utc),
                            user_id=None,
                        )]
                    ),
                    staging_threshold=0,
                )
                deals_no_change += 1
                metrics.backfill_deals_without_changes.inc()

    log.info("✔ Back-fill concluído – %s linhas; %s deals sem mudanças",
             total_rows, deals_no_change)

    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
        utils.update_last_successful_run_ts(
            label, datetime.utcnow().astimezone(timezone.utc), cur
        )
        conn.commit()
        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()

    utils.finish_flow_metrics(label, t0, log)
