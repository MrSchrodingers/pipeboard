from datetime import datetime
from typing import List, Dict, Tuple
from uuid import uuid4
import pandas as pd
import numpy as np
import json
import structlog
from pydantic import ValidationError

from core.schemas.deal_schema import DealSchema
from core.utils.column_utils import flatten_custom_fields
from infrastructure.observability.metrics import (
    transform_duration_summary,
    etl_batch_validation_errors_total,
    etl_transformation_error_rate,
)
from infrastructure.repositories.board_repository import UNKNOWN_NAME

log = structlog.get_logger()

class TransformService:
    def __init__(self, repository) -> None:
        self.repository = repository
        self.log = log.bind(service="TransformService")

    def validate_and_transform_batch(
        self, batch: List[Dict], flow_type: str
    ) -> Tuple[List[Dict], int]:
        """
        Valida e transforma um batch de dados extraídos.

        Retorna:
        - Lista de registros transformados e validados.
        - Quantidade de registros que falharam.
        """
        start_time = pd.Timestamp.utcnow()
        original_count = len(batch)
        transform_log = self.log.bind(batch_original_size=original_count, flow_type=flow_type)

        if not batch:
            return [], 0

        valid_input_for_df = []
        pydantic_failed_count = 0

        for i, record in enumerate(batch):
            try:
                DealSchema.model_validate(record)
                valid_input_for_df.append(record)
            except ValidationError as e:
                pydantic_failed_count += 1
                errors_summary = [f"{err['loc']}: {err['msg']}" for err in e.errors()]
                transform_log.warning("Validation failed", errors=errors_summary)
            except Exception as e:
                pydantic_failed_count += 1
                transform_log.error("Unexpected validation error", error=str(e))

        if not valid_input_for_df:
            transform_log.warning("All records failed validation.")
            return [], pydantic_failed_count

        try:
            df = pd.DataFrame(valid_input_for_df)

            transformed_df = pd.DataFrame()
            transformed_df["id"] = df["id"].astype(str)
            transformed_df["titulo"] = df["title"].fillna("").astype(str)
            status_map = {"won": "Ganho", "lost": "Perdido", "open": "Em aberto", "deleted": "Deletado"}
            transformed_df["status"] = df["status"].fillna("").astype(str).map(status_map).fillna(df["status"])
            transformed_df["currency"] = df["currency"].fillna("USD").astype(str)
            transformed_df["value"] = pd.to_numeric(df["value"], errors='coerce').fillna(0.0)
            transformed_df["add_time"] = pd.to_datetime(df["add_time"], errors='coerce', utc=True)
            transformed_df["update_time"] = pd.to_datetime(df["update_time"], errors='coerce', utc=True)

            # Flatten custom fields
            if 'custom_fields' in df.columns:
                repo_custom_mapping = self.repository.custom_field_mapping
                df['custom_fields_parsed'] = df['custom_fields'].apply(
                    lambda x: json.loads(x) if isinstance(x, str) else (x if isinstance(x, dict) else {})
                )
                custom_fields_flattened_df = pd.json_normalize(
                    df['custom_fields_parsed'].apply(
                        lambda x: flatten_custom_fields(x, repo_custom_mapping)
                    ).tolist()
                )
                custom_fields_flattened_df.index = df.index
                transformed_df = pd.concat([transformed_df, custom_fields_flattened_df], axis=1)

            transformed_df = transformed_df.replace({pd.NA: None, np.nan: None, pd.NaT: None})
            batch_id = str(uuid4())
            sync_time = datetime.utcnow().isoformat()
            flow_run_id = None

            transformed_df["batch_id"] = batch_id
            transformed_df["sync_time"] = sync_time
            transformed_df["flow_run_id"] = flow_run_id
            
            validated_records = transformed_df.to_dict('records')
            transform_succeeded_count = len(validated_records)
            transform_failed_count = len(batch) - transform_succeeded_count

        except Exception as e:
            transform_log.error("Transformation failed", error=str(e), exc_info=True)
            transform_failed_count = len(batch)
            validated_records = []

        duration = (pd.Timestamp.utcnow() - start_time).total_seconds()
        transform_duration_summary.labels(flow_type=flow_type).observe(duration)
        total_failed_in_batch = pydantic_failed_count + transform_failed_count

        if original_count > 0:
            error_rate = total_failed_in_batch / original_count
            etl_transformation_error_rate.labels(flow_type=flow_type).set(error_rate)

        etl_batch_validation_errors_total.labels(flow_type=flow_type, error_type="pydantic").inc(pydantic_failed_count)
        etl_batch_validation_errors_total.labels(flow_type=flow_type, error_type="transform").inc(transform_failed_count)

        transform_log.info("Batch transformation complete", validated=len(validated_records), failed=total_failed_in_batch)
        return validated_records, total_failed_in_batch