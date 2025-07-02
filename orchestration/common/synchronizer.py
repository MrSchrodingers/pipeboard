# ──────────────────────────────────────────────────────────────
#  orchestration/common/synchronizer.py
#  feat(sync): dynamic explode of *all* custom_fields with
#               safety-cap (1 600-N) – zero whitelist
#               + robust updated_since, error propagation,
#               system metrics and buffered COPY
# ──────────────────────────────────────────────────────────────
from __future__ import annotations

import itertools
import threading
import time
from datetime import timezone
from typing import Any, Callable, Dict, List, Optional, Type
from psycopg2 import sql

import numpy as np
import pandas as pd
import psutil
from prefect import get_run_logger
from pydantic import BaseModel

from infrastructure.clients import PipedriveAPIClient
from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import (
    etl_batch_duration_seconds,
    etl_batch_validation_errors_total,
    etl_cpu_usage_percent,
    etl_empty_batches_total,
    etl_load_failures_total,
    etl_thread_count,
    etl_throughput_rows_per_second,
    memory_usage_gauge,
    records_processed_counter,
)
from infrastructure.repositories import RepositorioBase
from orchestration.common import utils

# ═════════════════════ Config interna ═════════════════════
_MAX_API_LIMIT        = PipedriveAPIClient.DEFAULT_PAGINATION_LIMIT   # 500
_BUFFER_ROWS          = 15_000                                       # flush em lote
_N_WORKERS            = 8                                            # validação pydantic
_PG_HARD_LIMIT        = 1_600                                        # MaxTupleAttributeNumber
_SAFETY_BUFFER        = 100                                          # reserva p/ evoluções manuais
_MAX_AUTO_COLUMNS     = _PG_HARD_LIMIT - _SAFETY_BUFFER              # fusível dinâmico
_OVERFLOW_COL = "custom_fields_overflow"                             # JSONB

# evita recriar funções por linha
_FN_MAP: Dict[str, Callable] = {
    "text":        utils.explode_text,
    "numeric":     utils.explode_numeric,
    "monetary":    utils.explode_monetary,
    "enum":        utils.explode_enum,
    "address":     utils.explode_address,
    "phone_list":  utils.explode_list_column,
    "date":        lambda s, slug, **_: pd.to_datetime(s, errors="coerce", utc=True)
                                          .rename(f"{slug}_date"),
    "time":        lambda s, slug, **_: s.astype(str).rename(f"{slug}_time"),
    "timerange":   lambda s, slug, **_: s.astype(str).rename(f"{slug}_timerange"),
    "daterange":   lambda s, slug, **_: s.astype(str).rename(f"{slug}_daterange"),
    "user_id":     lambda s, slug, **_: pd.to_numeric(s, errors="coerce")
                                          .astype("Int64").rename(f"{slug}_user_id"),
    "person_id":   lambda s, slug, **_: pd.to_numeric(s, errors="coerce")
                                          .astype("Int64").rename(f"{slug}_person_id"),
}

# ═════════════════════ Classe principal ════════════════════
class PipedriveEntitySynchronizer:
    """
    Sincroniza qualquer recurso Pipedrive → Postgres.

    • Validação Pydantic paralela
    • Explode **todos** os custom_fields enquanto houver slots (< 1 500)
      ─ excedente vai para JSONB, sem abortar
    • Buffer + COPY p/ throughput alto
    • Propaga qualquer erro de flush → flow Failed
    """

    # cache de metadados por processo
    _FIELD_META_CACHE: Dict[str, list[Any]] = {}

    # ─────────── ctor ───────────
    def __init__(
        self,
        *,
        entity_name: str,
        pydantic_model_main: Type[BaseModel],
        repository: RepositorioBase,
        api_endpoint_main: str,
        api_endpoint_fields: Optional[str] = None,
        pydantic_model_field: Optional[Type[BaseModel]] = None,
        core_columns: Optional[List[str]] = None,
        specific_field_handlers: Optional[Dict[str, Dict[str, Any]]] = None,
        api_client: Optional[PipedriveAPIClient] = None,
    ) -> None:
        self.entity_name          = entity_name
        self.pydantic_model_main  = pydantic_model_main
        self.repository           = repository
        self.api_endpoint_main    = api_endpoint_main
        self.api_endpoint_fields  = api_endpoint_fields
        self.pydantic_model_field = pydantic_model_field
        self.core_columns         = core_columns or []
        self.specific_handlers    = specific_field_handlers or {}

        self.client = api_client or PipedriveAPIClient()
        self.log    = get_run_logger()

        self._field_meta: list[Any]        = []
        self._slug_by_key: Dict[str, str]  = {}
        self._pk: set[str]                 = set(self.repository.schema_config.pk or [])
        self._current_cols: Optional[int]  = None   # contado lazy

        if self.api_endpoint_fields and self.pydantic_model_field:
            self._bootstrap_field_metadata()

    # ─────────── 1. Metadata dos custom_fields ───────────
    def _bootstrap_field_metadata(self) -> None:
        cache_key = f"{self.entity_name}_fields"
        if cache_key in self._FIELD_META_CACHE:
            self._field_meta = self._FIELD_META_CACHE[cache_key]
        else:
            raw = self.client.call(self.api_endpoint_fields).get("data", [])
            for item in raw if isinstance(raw, list) else []:
                try:
                    self._field_meta.append(
                        self.pydantic_model_field.model_validate(item)
                    )
                except Exception as exc:  # noqa: BLE001
                    self.log.warning("invalid field meta %s", exc)
            self._FIELD_META_CACHE[cache_key] = self._field_meta

        self._slug_by_key = {
            m.key: utils.slugify(getattr(m, "name", m.key))
            for m in self._field_meta if m.key
        }

    # ─────────── 2. Helpers ───────────
    def _validate_batch(self, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return utils.validate_parallel(self.pydantic_model_main, batch, workers=_N_WORKERS)

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

    # ─────────── Explode ALL custom_fields with safety cap ───────────
    def _explode_custom_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self._field_meta:
            return df.drop(columns=["custom_fields"], errors="ignore")

        # garante contador de colunas existente uma única vez
        if self._current_cols is None:
            self._current_cols = self._get_current_column_count()

        df = df.copy()
        df["custom_fields"] = df.get("custom_fields", pd.Series([{}] * len(df))).apply(
            lambda x: x if isinstance(x, dict) else {}
        )

        new_cols: Dict[str, pd.Series] = {}
        cols_to_drop: list[str] = []
        for meta in self._field_meta:
            key = meta.key
            if not key or key in self._pk:
                continue

            slug = self._slug_by_key[key]
            # se coluna já existe na tabela → siga (será sobrescrita / migrada se preciso)
            col_already_exists = slug in df.columns or slug in self.repository.schema_config.types
            if not col_already_exists and self._current_cols + len(new_cols) >= _MAX_AUTO_COLUMNS:
                # atingiu fusível: campo fica no JSONB
                continue

            internal = utils.PIPEDRIVE_TO_INTERNAL_FIELD_TYPE_MAP.get(meta.field_type)
            fn = _FN_MAP.get(internal)
            if fn is None:
                continue

            if key in df.columns:
                src = df[key]
                cols_to_drop.append(key)
            else:
                src = df["custom_fields"].apply(lambda d: d.get(key))
            kwargs: Dict[str, Any] = {}
            if internal == "enum":
                kwargs["options"] = getattr(meta, "options", None)
            if internal == "phone_list":
                cfg = self.specific_handlers.get(key, {}).get("params", {})
                kwargs |= {
                    "value_key":   cfg.get("value_key", "value"),
                    "label_key":   cfg.get("label_key", "label"),
                    "primary_key": cfg.get("primary_key", "primary"),
                }

            try:
                exploded = fn(src.copy(), slug=slug, **kwargs)
                if isinstance(exploded, pd.Series):
                    new_cols[exploded.name] = exploded
                else:
                    for c in exploded.columns:
                        new_cols[c] = exploded[c]
            except Exception as exc:
                self.log.error("explode %s failed: %s", key, exc, exc_info=True)

        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True, errors="ignore")
            
        if new_cols:
            df = df.assign(**new_cols)
            self._current_cols += len(new_cols)

        return df.drop(columns=["custom_fields"], errors="ignore")

    # util p/ contar colunas já existentes
    def _get_current_column_count(self) -> int:
        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name   = %s
                """,
                (self.repository.table_name,),
            )
            (cnt,) = cur.fetchone()
            return cnt or 0
        
    def _prune_to_column_limit(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Garante que não ultrapassaremos o hard-limit de 1 600 colunas físicas.
        Qualquer coluna "sobrando" vai para JSONB `custom_fields_overflow`.
        Agora o teste de nulidade é seguro para valores array-like, evitando
        `ValueError: truth value of an array is ambiguous`.
        """

        # 1) slots já usados na tabela alvo
        with get_postgres_conn().connection() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(attnum),0) FROM pg_attribute WHERE attrelid = %s::regclass",
                (self.repository.table_name,),
            )
            (attr_used,) = cur.fetchone()

            cur.execute(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name   = %s
                  AND column_name  = %s
                """,
                (self.repository.table_name, _OVERFLOW_COL),
            )
            overflow_exists = bool(cur.fetchone())

        remaining = _MAX_AUTO_COLUMNS - int(attr_used)
        if not overflow_exists and remaining > 0:
            remaining -= 1   # deixa espaço p/ criar a coluna overflow
            need_overflow_creation = True
        else:
            need_overflow_creation = False

        # 2) mapeia colunas novas nesta batch
        known = (
            set(self.repository.schema_config.pk)
            | set(self.repository.schema_config.types)
            | set(self.repository.schema_config.indexes)
        )
        new_cols = [c for c in df.columns if c not in known]

        keep_new, drop_new = new_cols[:remaining], new_cols[remaining:]

        # helper seguro de nulidade -------------------------------------
        def _nonnull(v: Any) -> bool:  # noqa: ANN401
            if v is None or (isinstance(v, float) and np.isnan(v)):
                return False
            if isinstance(v, (list, tuple, set, dict, np.ndarray)):
                return len(v) > 0
            return True

        if drop_new:
            if overflow_exists or need_overflow_creation:
                if need_overflow_creation:
                    with get_postgres_conn().connection() as conn, conn.cursor() as cur:
                        cur.execute(
                            sql.SQL("ALTER TABLE {} ADD COLUMN {} JSONB").format(
                                sql.Identifier(self.repository.table_name),
                                sql.Identifier(_OVERFLOW_COL),
                            )
                        )
                    self.log.info("overflow-column-added", tbl=self.repository.table_name)

                df[_OVERFLOW_COL] = (
                    df[drop_new]
                    .apply(lambda r: {k: r[k] for k in drop_new if _nonnull(r[k])}, axis=1)
                    .where(lambda s: s.astype(bool), None)
                )
            else:
                self.log.warning("overflow-discarded")

            df.drop(columns=drop_new, inplace=True, errors="ignore")

        return df

    # ─────────── Normalize + ordering ───────────
    def _normalize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        res = df.copy()
        for pk in self._pk:
            if pk not in res.columns:
                res[pk] = np.nan
        for col in self.core_columns:
            if col not in res.columns:
                res[col] = np.nan

        # parse datetime por sufixo
        for c in res.columns:
            if c.endswith(("_time", "_date")):
                res[c] = pd.to_datetime(res[c], errors="coerce", utc=True)

        res = res.convert_dtypes()
        ordered = [c for c in itertools.chain(self._pk, self.core_columns) if c in res]
        res = res[ordered + sorted([c for c in res.columns if c not in ordered])]
        return res.loc[:, ~res.columns.duplicated()]

    # ─────────── 3. Loop de sync ───────────
    def run_sync(
        self,
        *,
        updated_since: Optional[pd.Timestamp | str] = None,
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> int:
        label = self.entity_name
        self.log.info("▶ %s – syncing…", label)

        # robust updated_since
        if isinstance(updated_since, str):
            try:
                updated_since = pd.to_datetime(updated_since).tz_convert(timezone.utc).floor("S")
            except Exception:
                self.log.warning("updated_since inválido: %s", updated_since)
                updated_since = None
        elif isinstance(updated_since, pd.Timestamp):
            updated_since = updated_since.tz_convert(timezone.utc).floor("S")

        params = {"limit": _MAX_API_LIMIT} | (extra_params or {})
        if updated_since is not None:
            params["updated_since"] = updated_since.isoformat()

        buffer: List[pd.DataFrame] = []
        total_rows = 0

        for page_n, batch in enumerate(
            self.client.stream_all_entities(self.api_endpoint_main, params=params), 1
        ):
            # métricas de sistema
            memory_usage_gauge.labels(flow_type=label).set(
                psutil.Process().memory_info().rss / (1024**2)
            )
            etl_cpu_usage_percent.labels(flow_type=label).set(psutil.cpu_percent(interval=None))
            etl_thread_count.labels(flow_type=label).set(threading.active_count())

            if not batch:
                etl_empty_batches_total.labels(flow_type=label).inc()
                continue

            # 1. valida
            try:
                validated = self._validate_batch(batch)
            except Exception as exc:
                etl_batch_validation_errors_total.labels(flow_type=label, error_type="validation").inc()
                self.log.error("Validação falhou no batch %s: %s", page_n, exc)
                continue
            if not validated:
                etl_empty_batches_total.labels(flow_type=label).inc()
                continue

            # 2. transforma
            try:
                df = pd.DataFrame(validated)
                df = self._normalize_schema(
                    self._explode_custom_fields(
                        self._apply_specific_handlers(df)
                    )
                )
                df = self._prune_to_column_limit(df)
            except Exception as exc:
                self.log.error("Transform falhou no batch %s: %s", page_n, exc, exc_info=True)
                continue

            if df.empty:
                etl_empty_batches_total.labels(flow_type=label).inc()
                continue

            buffer.append(df)
            total_rows += len(df)

            if sum(map(len, buffer)) >= _BUFFER_ROWS:
                self._flush_buffer(buffer, label, page_n)
                buffer.clear()

        if buffer:
            self._flush_buffer(buffer, label, "final")
            buffer.clear()

        self.log.info("■ %s – done (%s rows)", label, total_rows)
        return total_rows

    # ─────────── Flush buffer ───────────
    def _flush_buffer(
        self,
        buffer: List[pd.DataFrame],
        label: str,
        tag: str,
    ) -> None:
        combined = pd.concat(buffer, ignore_index=True)
        n_rows   = len(combined)

        t0 = time.time()
        try:
            self.repository.save(combined)
        except Exception as exc:
            etl_load_failures_total.labels(flow_type=label).inc()
            self.log.exception("flush %s falhou – abortando run: %s", tag, exc)
            raise
        dt = time.time() - t0

        throughput = n_rows / dt if dt else 0.0
        etl_throughput_rows_per_second.labels(flow_type=label).set(throughput)
        etl_batch_duration_seconds.labels(flow_type=label).observe(dt)
        records_processed_counter.labels(flow_type=label).inc(n_rows)
        self._current_cols = self._get_current_column_count()

        self.log.info("… flushed %s rows (%s) em %.2fs → %.1f rows/s", n_rows, tag, dt, throughput)
