"""Prefect 2 – Backfill deal stage history.

The flow retrieves historical stage changes for deals that still lack
records in the ``negocios_etapas_historico`` table. It calls the
``/deals/{id}/flow`` endpoint to gather the stage movements.

At most :data:`MAX_DEALS_PER_RUN` deals are processed in a single run to
control API token usage. Once a deal has its entire history stored, it will
not be queried again.
"""
from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from prefect import flow, get_run_logger

from infrastructure.clients.pipedrive_api_client import PipedriveAPIClient
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from infrastructure.repositories import RepositorioBase, SchemaConfig
from orchestration.common import utils
from orchestration.common.types import TYPES_CLEAN

MAX_DEALS_PER_RUN = 10_000


@flow(name="Backfill Pipedrive Stage History")
def backfill_pipedrive_stage_history_flow() -> None:
    log = get_run_logger()
    label = "StageHistoryBackfill"
    start = time.time()

    metrics.etl_counter.labels(flow_type=label).inc()

    try:
        repo = RepositorioBase(
            "negocios_etapas_historico",
            SchemaConfig(
                pk=["deal_id", "stage_id", "change_time"],
                types={
                    "deal_id":  TYPES_CLEAN["deal_id"],
                    "stage_id": TYPES_CLEAN["stage_id"],
                    "change_time": TYPES_CLEAN["stage_change_time"],
                    "user_id":  TYPES_CLEAN["user_id"],
                },
                indexes=["deal_id", "stage_id", "change_time", "user_id"],
                allow_column_dropping=False,
            ),
        )

        # ── garante a existência da tabela
        repo.ensure_table()

        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            # consulta deals sem histórico
            cur.execute("""
                SELECT COUNT(*)
                FROM negocios n
                LEFT JOIN negocios_etapas_historico h ON h.deal_id = n.id
                WHERE h.deal_id IS NULL
            """)
            remaining_total = cur.fetchone()[0] 
            metrics.backfill_deals_remaining_gauge.set(remaining_total)

            cur.execute(
                """
                SELECT n.id
                FROM negocios n
                LEFT JOIN negocios_etapas_historico h ON h.deal_id = n.id
                WHERE h.deal_id IS NULL
                ORDER BY n.id
                LIMIT %s
                """,
                (MAX_DEALS_PER_RUN,),
            )
            remaining = [r[0] for r in cur.fetchall()]

        log.info(
            "%s deals remaining, processing %s in this run",
            remaining_total,
            len(remaining),
        )

        client = PipedriveAPIClient()
        total_rows = 0
        for deal_id in remaining:
            start_cursor: Optional[int] = 0
            rows: List[Dict] = []
            while start_cursor is not None:
                resp = client.call(
                    "/deals/detail/flow", deal_id=deal_id, params={"start": start_cursor}
                )
                if not isinstance(resp, dict):
                    break
                data = resp.get("data", [])
                for item in data:
                    if item.get("field_key") == "stage_id":
                        stage_id = item.get("new_value")
                        ts = item.get("timestamp")
                        if stage_id and ts:
                            rows.append(
                                {
                                    "deal_id": deal_id,
                                    "stage_id": int(stage_id),
                                    "change_time": ts,
                                    "user_id": item.get("user_id"),
                                }
                            )
                pag = resp.get("additional_data", {}).get("pagination", {})
                start_cursor = pag.get("next_start") if pag.get("more_items_in_collection") else None

            if rows:
                df = pd.DataFrame(rows)
                repo.save(df)
                total_rows += len(df)
                log.info("Deal %s: saved %s stage changes", deal_id, len(df))

        log.info("✔ Stage history finished – %s registros", total_rows)

        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            utils.update_last_successful_run_ts(label, datetime.utcnow(), cur)
            conn.commit()
        metrics.etl_last_successful_run_timestamp.labels(flow_type=label).set_to_current_time()

    except Exception:
        metrics.etl_failure_counter.labels(flow_type=label).inc()
        log.exception("Stage history flow failed")
        raise

    finally:
        utils.finish_flow_metrics(label, start, log)