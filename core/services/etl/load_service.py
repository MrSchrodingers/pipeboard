from typing import List, Dict

import structlog
from core.ports.data_repository_port import DataRepositoryPort
from infrastructure.observability.metrics import (
    db_operation_duration_hist,
    records_processed_counter,
    etl_loaded_records_per_batch,
    etl_failure_counter
)

log = structlog.get_logger()

class LoadService:
    def __init__(self, repository: DataRepositoryPort) -> None:
        self.repository = repository
        self.log = log.bind(service="LoadService")

    def load_batch(self, validated_batch: List[Dict], flow_type: str) -> int:
        """
        Carrega um batch validado para o repositório (banco de dados).

        Retorna o número de registros carregados.
        """
        if not validated_batch:
            self.log.warning("Attempted to load empty batch", flow_type=flow_type)
            return 0

        loaded_count = 0
        try:
            self.log.debug("Sending batch to repository", record_count=len(validated_batch))
            with db_operation_duration_hist.labels(operation='upsert').time():
                self.repository.save_data_upsert(validated_batch)
            loaded_count = len(validated_batch)
            records_processed_counter.labels(flow_type=flow_type).inc(loaded_count)
            etl_loaded_records_per_batch.labels(flow_type=flow_type).observe(loaded_count)
            self.log.info("Batch loaded successfully", loaded_count=loaded_count)
        except Exception as e:
            etl_failure_counter.labels(flow_type=flow_type).inc(len(validated_batch))
            self.log.error("Failed to load batch", error=str(e), exc_info=True)

        return loaded_count
