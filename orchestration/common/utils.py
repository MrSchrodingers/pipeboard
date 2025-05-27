from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
#  utils – slugify + explode helpers
# ──────────────────────────────────────────────────────────────────────────────
import json
import re
from functools import lru_cache
from typing import Any, Optional

import numpy as np
import pandas as pd

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

PIPEDRIVE_TO_INTERNAL_FIELD_TYPE_MAP: dict[str, str] = {
    # texto
    "varchar": "text",
    "text": "text",
    # números
    "int": "numeric",
    "double": "numeric",
    # dinheiro
    "monetary": "monetary",
    # enum / multi‑select
    "enum": "enum",
    "set": "enum",
    # listas de contato
    "phone": "phone_list",
    "phone_list": "phone_list",
    # IDs relacionais
    "user": "user_id",
    "person": "person_id",
    # datas e horários
    "date": "date",
    "time": "time",
    "timerange": "timerange",
    "daterange": "daterange",
    # endereços
    "address": "address",
    # outros
    "picture": "text",
}


@lru_cache(maxsize=1_000)
def slugify(label: str | None) -> str:
    """Slug simples, estável e livre de acentos/caracteres estranhos."""
    s = re.sub(r"[^\w]+", "_", str(label or "").lower()).strip("_")
    s = re.sub(r"_{2,}", "_", s)  # colapsa múltiplos _
    return s or f"field_{hash(label) & 0xFFFF:X}"


# ── explode helpers ───────────────────────────────────────────────────────────

def _series(obj: Any) -> pd.Series:
    return obj if isinstance(obj, pd.Series) else pd.Series(obj)

def explode_text(s: pd.Series, slug: str, **__) -> pd.Series:
    return _series(s).astype("string").rename(f"{slug}_text")

def explode_numeric(s: pd.Series, slug: str, **__) -> pd.Series:
    return pd.to_numeric(_series(s), errors="coerce").rename(f"{slug}_value").astype("Float64")

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
    return pd.DataFrame({
        f"{slug}_valor": s.apply(_val).astype("Float64"),
        f"{slug}_moeda": s.apply(_cur).astype("string"),
    })

def explode_enum(s: pd.Series, slug: str, *, options: Optional[list[dict[str, Any]]] = None, **__) -> pd.Series:
    s = _series(s)
    if not options:
        return s.astype("object").rename(f"{slug}_label")
    id_to_label = {opt["id"]: opt["label"] for opt in options if isinstance(opt, dict)}
    def _map(v: Any):
        if v is None or (np.isscalar(v) and pd.isna(v)):  # noqa: PD011
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
        return [d.get(value_key) for d in lst if isinstance(d, dict) and d.get(value_key) not in (None, "")] if isinstance(lst, list) else []
    def _primary(lst: Any, key: str):
        if isinstance(lst, list):
            for d in lst:
                if isinstance(d, dict) and d.get(primary_key):
                    return d.get(key)
        return None
    return pd.DataFrame({
        f"{slug}_list": s.apply(_list),
        f"{slug}_primary_{value_key}": s.apply(lambda x: _primary(x, value_key)),
        f"{slug}_primary_{label_key}": s.apply(lambda x: _primary(x, label_key)),
    }).convert_dtypes()

# Public export list for utils module compatibility
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

# Aliases expected by synchronizer
explode_list_column.__module__ = __name__
explode_address.__module__ = __name__
explode_text.__module__ = __name__
explode_numeric.__module__ = __name__
explode_monetary.__module__ = __name__
explode_enum.__module__ = __name__
slugify.__module__ = __name__

# ──────────────────────────────────────────────────────────────────────────────
#  PipedriveEntitySynchronizer
# ──────────────────────────────────────────────────────────────────────────────
from typing import Callable, Optional, Type
from prefect import get_run_logger
from pydantic import BaseModel

from infrastructure.clients import PipedriveAPIClient  # noqa: E402
from infrastructure.repositories import RepositorioBase  # noqa: E402

class PipedriveEntitySynchronizer:
    """Sincroniza qualquer entidade do Pipedrive → Postgres."""

    _global_field_metadata_cache: dict[str, list[Any]] = {}

    # ── 1. ctor ──────────────────────────────────────────────────────────
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
        utils_module=__import__(__name__),  # este próprio arquivo
        api_client: Optional[PipedriveAPIClient] = None,
        core_columns: Optional[list[str]] = None,
    ) -> None:
        self.entity_name = entity_name
        self.pydantic_model_main = pydantic_model_main
        self.repository = repository
        self.api_endpoint_main = api_endpoint_main
        self.api_endpoint_fields = api_endpoint_fields
        self.pydantic_model_field = pydantic_model_field
        self.core_columns = core_columns or []
        self.specific_field_handlers = specific_field_handlers or {}
        self.utils = utils_module
        self.logger = get_run_logger()
        self.api_client = api_client or PipedriveAPIClient()
        # helpers
        self.pipedrive_field_metadata: list[Any] = []
        self.custom_field_slug_map: dict[str, str] = {}
        self.primary_keys: set[str] = set(self.repository.schema_config.pk or [])

        if self.api_endpoint_fields and self.pydantic_model_field:
            self._fetch_and_cache_field_metadata()
            self._precompute_custom_field_slugs()
            self._build_field_helper_maps() 

    # ── 2. metadata ──────────────────────────────────────────────────────
    def _fetch_and_cache_field_metadata(self) -> None:
        cache_key = f"{self.entity_name}_fields_metadata"
        if cache_key in self._global_field_metadata_cache:
            self.pipedrive_field_metadata = self._global_field_metadata_cache[cache_key]
            return

        raw = self.api_client.call(self.api_endpoint_fields).get("data", [])
        for item in raw if isinstance(raw, list) else []:
            try:
                meta = self.pydantic_model_field.model_validate(item)
                self.pipedrive_field_metadata.append(meta)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Invalid field meta: %s", exc)
        self._global_field_metadata_cache[cache_key] = self.pipedrive_field_metadata

    def _precompute_custom_field_slugs(self) -> None:
        for meta in self.pipedrive_field_metadata:
            k = getattr(meta, "key", None)
            nm = getattr(meta, "name", k)
            if k:
                self.custom_field_slug_map[k] = slugify(nm)

    # ── 3. validação ─────────────────────────────────────────────────────
    def _validate_batch(self, batch: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for i, d in enumerate(batch):
            try:
                out.append(self.pydantic_model_main.model_validate(d).model_dump(exclude_none=False))
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Validation failed idx=%s: %s", i, exc)
        return out

    # ── 4. campos padrões com handlers específicos ──────────────────────
    def _process_specific_standard_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.specific_field_handlers:
            return df
        res = df.copy()
        for col, cfg in self.specific_field_handlers.items():
            if col not in res.columns or res[col].isnull().all() or col in self.primary_keys:
                continue
            fn: Callable = cfg["function"]
            slug = slugify(cfg.get("slug_name", col))
            params = cfg.get("params", {})
            try:
                exploded = fn(res[col], slug=slug, **params)
                res.drop(columns=[col], inplace=True, errors="ignore")
                res = pd.concat([res, exploded], axis=1)
            except Exception as exc:  # noqa: BLE001
                self.logger.error("Specific handler error (%s): %s", col, exc, exc_info=True)
        return res

    # ── 5. explode custom_fields ────────────────────────────────────────
    def _explode_custom_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.pipedrive_field_metadata:
            return df.drop(columns=["custom_fields"], errors="ignore")

        proc = df.copy()
        proc["custom_fields"] = proc.get("custom_fields", pd.Series([{}] * len(proc))).apply(
            lambda x: x if isinstance(x, dict) else {}
        )

        # mapa de funções
        fn_map = {
            "text": explode_text,
            "numeric": explode_numeric,
            "monetary": explode_monetary,
            "enum": explode_enum,
            "address": explode_address,
            "phone_list": explode_list_column,
            "date": lambda s, slug, **_: pd.to_datetime(s, errors="coerce", utc=True).rename(f"{slug}_date"),
            "time": lambda s, slug, **_: s.astype(str).rename(f"{slug}_time"),
            "timerange": lambda s, slug, **_: s.astype(str).rename(f"{slug}_timerange"),
            "daterange": lambda s, slug, **_: s.astype(str).rename(f"{slug}_daterange"),
            "user_id": lambda s, slug, **_: pd.to_numeric(s, errors="coerce").astype("Int64").rename(f"{slug}_user_id"),
            "person_id": lambda s, slug, **_: pd.to_numeric(s, errors="coerce").astype("Int64").rename(f"{slug}_person_id"),
        }

        new_cols: dict[str, pd.Series] = {}
        for meta in self.pipedrive_field_metadata:
            k = getattr(meta, "key", None)
            t = getattr(meta, "field_type", None)
            if not k or k in self.primary_keys:
                continue
            internal = PIPEDRIVE_TO_INTERNAL_FIELD_TYPE_MAP.get(t)
            fn = fn_map.get(internal)
            if fn is None:
                continue
            slug = self.custom_field_slug_map.get(k, slugify(k))
            # busca valor: primeiro coluna padrão, depois custom_fields
            s = proc[k] if k in proc.columns else proc["custom_fields"].apply(lambda d: d.get(k))
            # se todo nulo, ainda cria a coluna (evita desaparecimento) — mantém esquema fixo
            opts: dict[str, Any] = {}
            if internal == "enum":
                opts["options"] = getattr(meta, "options", None)
            if internal == "phone_list":
                cfg = self.specific_field_handlers.get(k, {}).get("params", {})
                opts.update(value_key=cfg.get("value_key", "value"), label_key=cfg.get("label_key", "label"), primary_key=cfg.get("primary_key", "primary"))
            try:
                exploded = fn(s.copy(), slug, **opts)
                if isinstance(exploded, pd.Series):
                    new_cols[exploded.name] = exploded
                else:
                    new_cols.update(exploded.to_dict(orient="series"))
            except Exception as exc:  # noqa: BLE001
                self.logger.error("explode %s failed: %s", k, exc, exc_info=True)

        # extras sem metadata
        known = {getattr(m, "key", None) for m in self.pipedrive_field_metadata}
        for k in {k for d in proc["custom_fields"] for k in d if k not in known}:
            new_cols[f"{slugify(k)}_raw"] = proc["custom_fields"].apply(lambda d: d.get(k))

        if new_cols:
            proc = proc.assign(**new_cols)
        return proc.drop(columns=["custom_fields"], errors="ignore")

    # ── 6. padroniza tipos + ordem de colunas ───────────────────────────
    def _ensure_final_schema_and_types(self, df: pd.DataFrame) -> pd.DataFrame:
        res = df.copy()
        for pk in self.primary_keys:
            if pk not in res.columns:
                res[pk] = np.nan
        for c in self.core_columns:
            if c not in res.columns:
                res[c] = np.nan
        # datetime por sufixo
        for c in res.columns:
            if c.endswith(("_time", "_date")):
                res[c] = pd.to_datetime(res[c], errors="coerce", utc=True)
        res = res.convert_dtypes()
        ordered = [c for c in (*self.primary_keys, *self.core_columns) if c in res.columns]
        res = res[ordered + sorted([c for c in res.columns if c not in ordered])]
        return res.loc[:, ~res.columns.duplicated()]

    # ── 7. loop principal ───────────────────────────────────────────────
    def run_sync(self) -> int:
        self.logger.info("▶ Starting sync for %s", self.entity_name)
        total = 0
        for n, batch in enumerate(self.api_client.stream_all_entities(self.api_endpoint_main), 1):
            if not batch:
                continue
            self.logger.info("Batch %s (%s records)", n, len(batch))
            records = self._validate_batch(batch)
            if not records:
                continue
            df = pd.DataFrame(records)
            df = self._process_specific_standard_fields(df)
            df = self._explode_custom_fields(df)
            df = self._ensure_final_schema_and_types(df)
            if df.empty:
                continue
            try:
                self.repository.save(df)
                total += len(df)
                self.logger.info("✔ Saved batch %s (%s rows)", n, len(df))
            except Exception as exc:  # noqa: BLE001
                self.logger.error("Save failed batch %s: %s", n, exc, exc_info=True)
        self.logger.info("■ Sync finished — %s records", total)
        return total
        
