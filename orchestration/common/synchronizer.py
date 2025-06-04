from __future__ import annotations

import itertools
import time
import threading
from typing import Any, Dict, Optional, Type, Callable

import numpy as np
import pandas as pd
import psutil
from prefect import get_run_logger
from pydantic import BaseModel

from infrastructure.clients import PipedriveAPIClient
from infrastructure.repositories import RepositorioBase
from . import utils
from infrastructure.observability import (
    etl_empty_batches_total,
    etl_batch_validation_errors_total,
    etl_transform_failures_total,
    etl_load_failures_total,
    records_processed_counter,
    etl_throughput_rows_per_second,
    etl_batch_duration_seconds,
    memory_usage_gauge,
    etl_cpu_usage_percent,
    etl_thread_count,
    batch_size_gauge,
)

# ──────────────────────────────────────────────────────────────
#  Configurações internas
# ──────────────────────────────────────────────────────────────
_MAX_API_LIMIT = PipedriveAPIClient.DEFAULT_PAGINATION_LIMIT  # 500 (Pipedrive fixo)
_BUFFER_ROWS    = 15_000    # grava no banco a cada 15 k linhas processadas
_N_WORKERS      = 8         # threads para validação pydantic

# mapping estático → evita recriar dicionário por linha
_FN_MAP: Dict[str, Callable] = {
    "text":         utils.explode_text,
    "numeric":      utils.explode_numeric,
    "monetary":     utils.explode_monetary,
    "enum":         utils.explode_enum,
    "address":      utils.explode_address,
    "phone_list":   utils.explode_list_column,
    "date":     lambda s, slug, **_: pd.to_datetime(s, errors="coerce", utc=True).rename(f"{slug}_date"),
    "time":     lambda s, slug, **_: s.astype(str).rename(f"{slug}_time"),
    "timerange":lambda s, slug, **_: s.astype(str).rename(f"{slug}_timerange"),
    "daterange":lambda s, slug, **_: s.astype(str).rename(f"{slug}_daterange"),
    "user_id":  lambda s, slug, **_: pd.to_numeric(s, errors="coerce").astype("Int64").rename(f"{slug}_user_id"),
    "person_id":lambda s, slug, **_: pd.to_numeric(s, errors="coerce").astype("Int64").rename(f"{slug}_person_id"),
}

# ──────────────────────────────────────────────────────────────
#  Classe principal
# ──────────────────────────────────────────────────────────────
class PipedriveEntitySynchronizer:
    """
    Sincroniza QUALQUER recurso do Pipedrive para Postgres.

    • Consome a API paginada (streaming) em lotes de 500 registros.  
    • Valida linhas via *pydantic* em paralelo (ThreadPool).  
    • “Explode” campos complexos & custom_fields → colunas atômicas.  
    • Usa buffer e `COPY` staging para escrita em lote, minimizando I/O.
    """

    # cache global de metadados por execução do processo
    _FIELD_META_CACHE: dict[str, list[Any]] = {}

    # ─────────── 1. ctor ───────────
    def __init__(
        self,
        *,
        entity_name: str,
        pydantic_model_main: Type[BaseModel],
        repository: RepositorioBase,
        api_endpoint_main: str,
        api_endpoint_fields: Optional[str] = None,
        pydantic_model_field: Optional[Type[BaseModel]] = None,
        specific_field_handlers: Optional[dict[str, dict[str, Any]]] = None,
        core_columns: Optional[list[str]] = None,
        api_client: Optional[PipedriveAPIClient] = None,
    ) -> None:
        self.entity_name           = entity_name
        self.pydantic_model_main   = pydantic_model_main
        self.repository            = repository
        self.api_endpoint_main     = api_endpoint_main
        self.api_endpoint_fields   = api_endpoint_fields
        self.pydantic_model_field  = pydantic_model_field
        self.core_columns          = core_columns or []
        self.specific_handlers     = specific_field_handlers or {}

        self.client = api_client or PipedriveAPIClient()
        self.log    = get_run_logger()

        self._field_meta: list[Any] = []
        self._slug_by_key: Dict[str, str] = {}
        self._pk: set[str] = set(self.repository.schema_config.pk or [])

        if self.api_endpoint_fields and self.pydantic_model_field:
            self._bootstrap_field_metadata()

    # ─────────── 2. bootstrap metadados ───────────
    def _bootstrap_field_metadata(self) -> None:
        cache_key = f"{self.entity_name}_fields"
        if cache_key in self._FIELD_META_CACHE:
            self._field_meta = self._FIELD_META_CACHE[cache_key]
            self._slug_by_key = {
                m.key: utils.slugify(getattr(m, "name", m.key))
                for m in self._field_meta if m.key
            }
            return

        raw = self.client.call(self.api_endpoint_fields).get("data", [])
        for item in raw if isinstance(raw, list) else []:
            try:
                meta = self.pydantic_model_field.model_validate(item)
                self._field_meta.append(meta)
            except Exception as exc:   # noqa: BLE001
                self.log.warning("invalid meta %s: %s", self.entity_name, exc)

        self._FIELD_META_CACHE[cache_key] = self._field_meta
        self._slug_by_key = {
            m.key: utils.slugify(getattr(m, "name", m.key))
            for m in self._field_meta if m.key
        }

    # ─────────── 3. validação pydantic ───────────
    def _validate_batch(self, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return utils.validate_parallel(self.pydantic_model_main, batch, workers=_N_WORKERS)

    # ─────────── 4. explode campos padronizados (emails, phones…) ───────────
    def _apply_specific_handlers(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.specific_handlers:
            return df
        res = df
        for col, cfg in self.specific_handlers.items():
            if col not in res or col in self._pk or res[col].isna().all():
                continue
            fn   = cfg["function"]
            slug = utils.slugify(cfg.get("slug_name", col))
            try:
                exploded = fn(res[col], slug=slug, **cfg.get("params", {}))
                res.drop(columns=[col], inplace=True, errors="ignore")
                res = pd.concat([res, exploded], axis=1)
            except Exception as exc:  # noqa: BLE001
                self.log.error("handler %s failed: %s", col, exc, exc_info=True)
        return res

    # ─────────── 5. explode custom_fields ───────────
    def _explode_custom_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self._field_meta:
            return df.drop(columns=["custom_fields"], errors="ignore")

        df = df.copy()
        df["custom_fields"] = df.get("custom_fields", pd.Series([{}] * len(df))).apply(
            lambda x: x if isinstance(x, dict) else {}
        )

        new_cols: Dict[str, pd.Series] = {}
        for meta in self._field_meta:
            key = meta.key
            if not key or key in self._pk:
                continue

            internal = utils.PIPEDRIVE_TO_INTERNAL_FIELD_TYPE_MAP.get(meta.field_type)
            fn = _FN_MAP.get(internal)
            if fn is None:
                continue

            slug  = self._slug_by_key[key]
            src   = df[key] if key in df.columns else df["custom_fields"].apply(lambda d: d.get(key))

            kwargs: dict[str, Any] = {}
            if internal == "enum":
                kwargs["options"] = getattr(meta, "options", None)
            if internal == "phone_list":
                cfg = self.specific_handlers.get(key, {}).get("params", {})
                kwargs |= {
                    "value_key":  cfg.get("value_key", "value"),
                    "label_key":  cfg.get("label_key", "label"),
                    "primary_key": cfg.get("primary_key", "primary"),
                }

            try:
                exploded = fn(src.copy(), slug=slug, **kwargs)
                if isinstance(exploded, pd.Series):
                    new_cols[exploded.name] = exploded
                else:  # DataFrame
                    for c in exploded.columns:
                        new_cols[c] = exploded[c]
            except Exception as exc:
                self.log.error("explode %s failed: %s", key, exc, exc_info=True)

        if new_cols:
            df = df.assign(**new_cols)

        return df.drop(columns=["custom_fields"], errors="ignore")

    # ─────────── 6. normaliza tipos e ordena colunas ───────────
    def _normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        res = df.copy()

        for pk in self._pk:
            if pk not in res.columns:
                res[pk] = np.nan

        for col in self.core_columns:
            if col not in res.columns:
                res[col] = np.nan

        # auto-datetime por sufixo
        for c in res.columns:
            if c.endswith(("_time", "_date")):
                res[c] = pd.to_datetime(res[c], errors="coerce", utc=True)

        res = res.convert_dtypes()

        ordered = [c for c in itertools.chain(self._pk, self.core_columns) if c in res]
        res = res[ordered + sorted([c for c in res if c not in ordered])]
        return res.loc[:, ~res.columns.duplicated()]

    # ─────────── 7. loop principal ───────────
    def run_sync(
        self,
        *,
        updated_since: Optional[pd.Timestamp] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Executa a sincronização.
        A cada flush de buffer, registra métricas de throughput, latência e uso de recursos.
        `updated_since` → timestamp UTC (pydatetime/pandas) para modo incremental.
        """
        flow_label = self.entity_name
        self.log.info("▶ %s – syncing…", self.entity_name)
        
        if isinstance(updated_since, str):
            try:
                updated_since = pd.to_datetime(updated_since, utc=True)
            except Exception:
                self.log.warning(f"Não foi possível converter '{updated_since}' para Timestamp. Usando None.")
                updated_since = None

        params = {"limit": _MAX_API_LIMIT}
        if extra_params:
            params.update(extra_params)
        if updated_since is not None:
            params["updated_since"] = updated_since.isoformat()

        buffer: list[pd.DataFrame] = []
        total_rows = 0

        for page_n, api_batch in enumerate(
            self.client.stream_all_entities(self.api_endpoint_main, params=params), 1
        ):
            # ── Atualiza métricas de sistema a cada página ──
            memory_mb = psutil.Process().memory_info().rss / (1024.0 ** 2)
            memory_usage_gauge.labels(flow_type=flow_label).set(memory_mb)
            etl_cpu_usage_percent.labels(flow_type=flow_label).set(psutil.cpu_percent(interval=None))
            etl_thread_count.labels(flow_type=flow_label).set(threading.active_count())

            if not api_batch:
                etl_empty_batches_total.labels(flow_type=flow_label).inc()
                continue

            # ❶ – validação Pydantic
            try:
                validated = utils.validate_parallel(
                    self.pydantic_model_main, api_batch, workers=_N_WORKERS
                )
            except Exception as e_val:
                etl_batch_validation_errors_total.labels(
                    flow_type=flow_label, error_type="validation_error"
                ).inc()
                self.log.error("Erro de validação no batch %s: %s", page_n, e_val)
                continue

            if not validated:
                etl_empty_batches_total.labels(flow_type=flow_label).inc()
                continue

            # ❷ – transformação em pandas
            try:
                df = pd.DataFrame(validated)
                df = self._normalize_schema(
                    self._explode_custom_fields(
                        self._apply_specific_handlers(df)
                    )
                )
            except Exception as e_trans:
                etl_transform_failures_total.labels(flow_type=flow_label).inc()
                self.log.error("Falha na transformação do batch %s: %s", page_n, e_trans, exc_info=True)
                continue

            if df.empty:
                etl_empty_batches_total.labels(flow_type=flow_label).inc()
                continue

            buffer.append(df)
            total_rows += len(df)

            # ❸ – se o buffer atingiu o tamanho configurado, faz flush
            sum_buffer = sum(len(b) for b in buffer)
            if sum_buffer >= _BUFFER_ROWS:
                combined = pd.concat(buffer, ignore_index=True)
                rows_this_flush = len(combined)
                batch_size_gauge.labels(flow_type=flow_label).set(rows_this_flush)

                t0 = time.time()
                try:
                    self.repository.save(combined)
                except Exception as e_load:
                    etl_load_failures_total.labels(flow_type=flow_label).inc()
                    self.log.error(
                        "Erro ao carregar batch (%s linhas) página %s: %s",
                        rows_this_flush, page_n, e_load, exc_info=True
                    )
                    buffer.clear()
                    continue
                t1 = time.time()

                elapsed = t1 - t0
                throughput = rows_this_flush / elapsed if elapsed > 0 else 0.0

                etl_throughput_rows_per_second.labels(flow_type=flow_label).set(throughput)
                etl_batch_duration_seconds.labels(flow_type=flow_label).observe(elapsed)
                records_processed_counter.labels(flow_type=flow_label).inc(rows_this_flush)

                self.log.info(
                    "… flushed %s rows (page %s) em %.2fs → %.1f linhas/s",
                    rows_this_flush, page_n, elapsed, throughput
                )
                buffer.clear()

        # ── flush final ──
        if buffer:
            combined = pd.concat(buffer, ignore_index=True)
            rows_this_flush = len(combined)
            batch_size_gauge.labels(flow_type=flow_label).set(rows_this_flush)

            t0 = time.time()
            try:
                self.repository.save(combined)
            except Exception as e_load2:
                etl_load_failures_total.labels(flow_type=flow_label).inc()
                self.log.error("Erro no flush final (%s linhas): %s", rows_this_flush, e_load2, exc_info=True)
            else:
                t1 = time.time()
                elapsed = t1 - t0
                throughput = rows_this_flush / elapsed if elapsed > 0 else 0.0

                etl_throughput_rows_per_second.labels(flow_type=flow_label).set(throughput)
                etl_batch_duration_seconds.labels(flow_type=flow_label).observe(elapsed)
                records_processed_counter.labels(flow_type=flow_label).inc(rows_this_flush)

                self.log.info(
                    "■ flushed final %s rows em %.2fs → %.1f linhas/s",
                    rows_this_flush, elapsed, throughput
                )
            buffer.clear()

        self.log.info("■ %s – done (%s rows)", self.entity_name, total_rows)
        return total_rows
