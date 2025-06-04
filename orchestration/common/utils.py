# -*- coding: utf-8 -*-
"""
Funções utilitárias compartilhadas pelos fluxos Pipedrive → Postgres.

Principais blocos:
• slugify (estável, sem acentos)  
• *explode_* helpers – convertem colunas compostas em colunas atômicas  
• rotinas de validação paralela (pydantic)  
• helpers de “last run” e checagem de dependências  
• métricas / finish_flow wrapper  
• TYPES_CLEAN → subset_types
"""
from __future__ import annotations

# ───────────────────────── Imports
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from prefect import get_run_logger
from prefect.runtime import flow_run
from psycopg2 import sql

from infrastructure.db.postgres_adapter import get_postgres_conn
from infrastructure.observability import metrics
from orchestration.common.types import TYPES_CLEAN as T

# ───────────────────────── Constantes & regex
_N_CPU          = os.cpu_count() or 4
_VALIDATE_WORKERS_DEFAULT = min(32, _N_CPU * 4)

_NON_WORD_RE    = re.compile(r"[^\w]+")
_MULTI_US_RE    = re.compile(r"_{2,}")

ADDRESS_COMPONENTS: tuple[str, ...] = (
    "formatted_address",
    "street_number",
    "route",
    "sublocality",
    "subpremise",
    "locality",
    "admin_area_level_1",
    "admin_area_level_2",
    "country",
    "postal_code",
)

# Mapeamento de tipos Pipedrive → interno (mais simples que o “puro” Pipedrive)
PIPEDRIVE_TO_INTERNAL_FIELD_TYPE_MAP: dict[str, str] = {
    # texto
    "varchar":  "text",
    "text":     "text",
    # números
    "int":      "numeric",
    "double":   "numeric",
    # dinheiro
    "monetary": "monetary",
    # enum / multi-select
    "enum":     "enum",
    "set":      "enum",
    # listas de contato
    "phone":        "phone_list",
    "phone_list":   "phone_list",
    # IDs relacionais
    "user":     "user_id",
    "person":   "person_id",
    # datas / horários
    "date":     "date",
    "time":     "time",
    "timerange":"timerange",
    "daterange":"daterange",
    # endereços
    "address":  "address",
    # outros
    "picture":  "text",
}

# ───────────────────────── Slugify
@lru_cache(maxsize=1_000)
def slugify(label: str | None) -> str:
    """
    Converte *label* em um slug:
    • sem acentos / pontuação
    • minúsculo
    • “_” como separador único
    • valor determinístico
    """
    s = _NON_WORD_RE.sub("_", str(label or "").lower()).strip("_")
    s = _MULTI_US_RE.sub("_", s)
    return s or f"field_{hash(label) & 0xFFFF:X}"


# ───────────────────────── Explode helpers
# Cada função recebe uma *Series* e devolve Series/DataFrame com coluna(s) normalizadas.
def _series(obj: Any) -> pd.Series:
    return obj if isinstance(obj, pd.Series) else pd.Series(obj)


def explode_text(s: pd.Series, slug: str, **__) -> pd.Series:
    return _series(s).astype("string").rename(f"{slug}_text")


def explode_numeric(s: pd.Series, slug: str, **__) -> pd.Series:
    return (
        pd.to_numeric(_series(s), errors="coerce")
        .rename(f"{slug}_value")
        .astype("Float64")
    )


def explode_monetary(s: pd.Series, slug: str, **__) -> pd.DataFrame:
    s = _series(s)

    def _val(v: Any) -> float | None:
        if isinstance(v, dict):
            return v.get("value")
        return pd.to_numeric(v, errors="coerce")

    def _cur(v: Any) -> str | None:
        if isinstance(v, dict):
            return v.get("currency")
        return None

    return pd.DataFrame(
        {
            f"{slug}_valor": s.apply(_val).astype("Float64"),
            f"{slug}_moeda": s.apply(_cur).astype("string"),
        }
    )


def explode_enum(
    s: pd.Series,
    slug: str,
    *,
    options: Optional[list[dict[str, Any]]] = None,
    **__,
) -> pd.Series:
    s = _series(s)
    if not options:
        return s.astype("object").rename(f"{slug}_label")

    id_to_label = {opt["id"]: opt["label"] for opt in options if isinstance(opt, dict)}

    def _map(v: Any):
        if v is None or (np.isscalar(v) and pd.isna(v)):
            return None
        if isinstance(v, (list, tuple, np.ndarray)):
            return [id_to_label.get(int(x)) for x in v if str(x).isdigit()]
        if isinstance(v, str) and "," in v:
            return [id_to_label.get(int(x)) for x in v.split(",") if x.strip().isdigit()]
        if str(v).isdigit():
            return id_to_label.get(int(v))
        return None

    return s.apply(_map).rename(f"{slug}_label").astype("object")


def explode_address(s: pd.Series, slug: str, **__) -> pd.DataFrame:
    s = _series(s)

    def _norm(v: Any):
        if isinstance(v, dict):
            return v
        if isinstance(v, str) and v.strip():
            try:
                parsed = json.loads(v)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass
            return {"formatted_address": v}
        return {}

    s = s.apply(_norm)
    cols: dict[str, Any] = {}
    for comp in ADDRESS_COMPONENTS:
        col = f"{slug}_{comp.replace('admin_area_level_', 'aal')}"
        cols[col] = s.apply(lambda d: d.get(comp) if isinstance(d, dict) else None)
    return pd.DataFrame(cols).convert_dtypes()


def explode_list_column(
    s: pd.Series,
    slug: str,
    *,
    value_key: str = "value",
    label_key: str = "label",
    primary_key: str = "primary",
) -> pd.DataFrame:
    s = _series(s)

    def _list(lst: Any):
        if not isinstance(lst, list):
            return []
        return [
            d.get(value_key)
            for d in lst
            if isinstance(d, dict) and d.get(value_key) not in (None, "")
        ]

    def _primary(lst: Any, key: str):
        if not isinstance(lst, list):
            return None
        for d in lst:
            if isinstance(d, dict) and d.get(primary_key):
                return d.get(key)
        return None

    return (
        pd.DataFrame(
            {
                f"{slug}_list": s.apply(_list),
                f"{slug}_primary_{value_key}": s.apply(lambda x: _primary(x, value_key)),
                f"{slug}_primary_{label_key}": s.apply(lambda x: _primary(x, label_key)),
            }
        )
        .convert_dtypes()
    )


# Mantemos compatibilidade de import * pelas etapas externas
__all__ = [
    "slugify",
    "explode_text",
    "explode_numeric",
    "explode_monetary",
    "explode_enum",
    "explode_address",
    "explode_list_column",
    "PIPEDRIVE_TO_INTERNAL_FIELD_TYPE_MAP",
]

# Garantimos que o __module__ aponta para este arquivo (requisito do Prefect 1.x)
for _fn in (
    explode_list_column,
    explode_address,
    explode_text,
    explode_numeric,
    explode_monetary,
    explode_enum,
    slugify,
):
    _fn.__module__ = __name__

# ───────────────────────── Validação paralela
def validate_parallel(
    model_cls,
    batch: list[dict[str, Any]],
    workers: int = _VALIDATE_WORKERS_DEFAULT,
) -> list[dict[str, Any]]:
    """
    Valida uma lista de dicts com Pydantic em paralelo.
    Retorna apenas os itens válidos (já como dict pronto p/ DataFrame).
    """
    if not batch:
        return []

    workers = min(workers, len(batch)) or 1

    def _safe_validate(d):
        try:
            return model_cls.model_validate(d).model_dump(exclude_none=False)
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return [r for r in pool.map(_safe_validate, batch) if r is not None]


__all__.append("validate_parallel")

# ───────────────────────── AUX – última execução + dependências
def create_etl_flow_meta_if_not_exists(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS etl_flow_meta (
            flow_id TEXT PRIMARY KEY,
            last_success_ts TIMESTAMP WITH TIME ZONE NOT NULL
        );
        """
    )


def get_last_successful_run_ts(flow_id: str, cur) -> Optional[datetime]:
    create_etl_flow_meta_if_not_exists(cur)
    cur.execute(
        "SELECT last_success_ts FROM etl_flow_meta WHERE flow_id = %s", (flow_id,)
    )
    res = cur.fetchone()
    return res[0] if res else None


def update_last_successful_run_ts(flow_id: str, ts: datetime, cur) -> None:
    create_etl_flow_meta_if_not_exists(cur)
    cur.execute(
        """
        INSERT INTO etl_flow_meta (flow_id, last_success_ts)
        VALUES (%s, %s)
        ON CONFLICT (flow_id)
        DO UPDATE SET last_success_ts = EXCLUDED.last_success_ts
        """,
        (flow_id, ts),
    )


def has_recent_updates_in_dependencies(
    dependent_config: Dict[str, Dict[str, Any]],
    since_timestamp: datetime,
    conn,
    logger,
) -> bool:
    """
    Testa se ALGUMA tabela/coluna indicada em *dependent_config*
    tem registros mais recentes que *since_timestamp*.
    """
    if not since_timestamp:
        # nunca rodou: força full
        return True

    with conn.cursor() as cur:
        for dep, info in dependent_config.items():
            tbl = info.get("table_name")
            ts_cols = info.get("timestamp_columns", [])
            if not tbl or not ts_cols:
                logger.debug("dep %s sem config válida", dep)
                continue

            # filtra somente colunas existentes
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                  AND column_name = ANY(%s)
                  AND table_schema = current_schema()
                """,
                (tbl, ts_cols),
            )
            cols_exist = [r[0] for r in cur.fetchall()]
            if not cols_exist:
                continue

            cond = sql.SQL(" OR ").join(
                sql.SQL("{} >= %s").format(sql.Identifier(c)) for c in cols_exist
            )
            q = sql.SQL("SELECT 1 FROM {} WHERE {} LIMIT 1").format(
                sql.Identifier(tbl), cond
            )
            cur.execute(q, (since_timestamp,) * len(cols_exist))
            if cur.fetchone():
                logger.info("dep %s alterada", dep)
                return True
    return False


# ───────────────────────── Métricas / finish_flow
def finish_flow_metrics(flow_name: str, start_ts: float, logger):
    duration = time.time() - start_ts
    metrics.etl_duration_hist.labels(flow_type=flow_name).observe(duration)

    try:
        pool = get_postgres_conn()
        if pool:
            metrics.db_active_connections.set(pool.active)
            metrics.db_idle_connections.set(pool.idle)
    except Exception:
        logger.debug("DB pool metrics unavailable", exc_info=True)

    metrics.update_uptime()
    metrics.etl_heartbeat.labels(flow_type=flow_name).set_to_current_time()

    run_id = getattr(flow_run, "id", "unknown") or "unknown"
    metrics.push_metrics_to_gateway(
        job_name="pipedrive_etl",
        grouping_key={"flow_name": flow_name, "instance": str(run_id)},
    )


def finish_flow(label: str, start_ts: float) -> None:
    """
    Wrapper simplificado (evita necessidade de logger explícito no chamador).
    """
    finish_flow_metrics(label, start_ts, get_run_logger())


# ───────────────────────── TIPOS util -> repos
def subset_types(keys: List[str]) -> Dict[str, str]:
    """Retorna TYPES_CLEAN filtrado pelas *keys* desejadas."""
    return {k: T[k] for k in keys}
