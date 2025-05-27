from datetime import timedelta
import time
from typing import List, Dict, Any
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from infrastructure.repositories.repository_stage_history import RepositorioHistoricoEtapas
from infrastructure.repositories.repository_lookups import RepositorioAuxiliares

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fetch_pending_deal_ids(limit: int) -> List[str]:
    """Busca IDs de negócios que precisam de backfill de histórico."""
    logger = get_run_logger()
    repo_aux = RepositorioAuxiliares()
    ids = repo_aux.get_deals_needing_history_backfill(limit)
    logger.info(f"Fetched {len(ids)} deal IDs for backfill.")
    return ids

@task
def apply_backfill_batch(deal_ids: List[str]) -> int:
    """Aplica o backfill de histórico para um lote de IDs. Retorna quantidade processada."""
    logger = get_run_logger()
    if not deal_ids:
        logger.info("No deal IDs to backfill.")
        return 0
    repo_hist = RepositorioHistoricoEtapas()
    processed = repo_hist.run_historical_backfill(deal_ids)
    logger.info(f"Processed backfill for {processed} deals.")
    return processed

@flow(name="Pipedrive Stage History Backfill Flow", timeout_seconds=8*3600)
def backfill_stage_history_flow(
    daily_deal_limit: int = 2000,
    db_batch_size: int = 1000
) -> Dict[str, Any]:
    """
    Orquestra o backfill de histórico de fases em lotes,
    respeitando um limite diário.
    """
    logger = get_run_logger()
    total_processed = 0
    start_time = time.time()

    while total_processed < daily_deal_limit:
        batch_limit = min(db_batch_size, daily_deal_limit - total_processed)
        deal_ids = fetch_pending_deal_ids(batch_limit)
        count = apply_backfill_batch(deal_ids)
        total_processed += count
        logger.info(f"Total processed so far: {total_processed}/{daily_deal_limit}")
        if count == 0:
            break
    duration = time.time() - start_time
    logger.info(f"Backfill flow completed: {total_processed} deals in {duration:.2f}s")
    return {
        "status": "completed",
        "total_processed": total_processed,
        "duration_seconds": duration
    }

