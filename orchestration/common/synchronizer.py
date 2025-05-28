from __future__ import annotations
import structlog

from typing import Any, Callable, Dict, List, Optional, Type

import numpy as np
import pandas as pd
from pydantic import BaseModel
from prefect import get_run_logger

from infrastructure.clients import PipedriveAPIClient
from infrastructure.repositories import RepositorioBase
from . import utils

log = structlog.get_logger(__name__)

class PipedriveEntitySynchronizer:
    """
    Sincronizador genérico reutilizável para qualquer recurso suportado pela
    API do Pipedrive. Trabalha em lotes (streaming), valida, explode campos
    customizados e salva no repositório.
    """

    _global_field_metadata_cache: Dict[str, List[Any]] = {}

    # ───────────────────────────────
    # 1. CONSTRUTOR
    # ───────────────────────────────
    def __init__(
        self,
        *,
        entity_name: str,
        pydantic_model_main: Type[BaseModel],
        repository: RepositorioBase,
        api_endpoint_main: str,
        api_endpoint_fields: Optional[str] = None,
        pydantic_model_field: Optional[Type[BaseModel]] = None,
        specific_field_handlers: Optional[Dict[str, Dict[str, Any]]] = None,
        utils_module = utils,
        api_client: Optional[PipedriveAPIClient] = None,
        core_columns: Optional[List[str]] = None,
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

        # cache & helpers
        self.pipedrive_field_metadata: List[Any] = []
        self.custom_field_slug_map: Dict[str, str] = {}
        self.standard_fields_processed_via_metadata: set[str] = set()
        self.primary_keys: set[str] = set(self.repository.schema_config.pk or [])

        if self.api_endpoint_fields and self.pydantic_model_field:
            self._fetch_and_cache_field_metadata()
            self._precompute_custom_field_slugs()
            self._build_field_helper_maps()   

    # ───────────────────────────────
    # 2. METADADOS
    # ───────────────────────────────
    def _fetch_and_cache_field_metadata(self) -> None:
        cache_key = f"{self.entity_name}_fields_metadata"
        cache_hit = self._global_field_metadata_cache.get(cache_key)

        if cache_hit:
            self.pipedrive_field_metadata = cache_hit
            self.logger.info("Loaded field metadata for %s from cache.", self.entity_name)
            return

        self.logger.info("Fetching field metadata for %s…", self.entity_name)
        raw = self.api_client.call(self.api_endpoint_fields).get("data", [])

        self.pipedrive_field_metadata = []
        for item in raw if isinstance(raw, list) else []:
            try:
                meta = self.pydantic_model_field.model_validate(item)
                self.pipedrive_field_metadata.append(meta)
            except Exception as exc:
                self.logger.warning(
                    "Invalid field metadata in %s: %s", self.entity_name, exc, extra={"item": str(item)[:200]}
                )

        self._global_field_metadata_cache[cache_key] = self.pipedrive_field_metadata
        self.logger.info("Cached %d field metadata items.", len(self.pipedrive_field_metadata))

    def _build_field_helper_maps(self) -> None:
        self._slug_map = {m.key: self.custom_field_slug_map.get(m.key, utils.slugify(m.key))
                          for m in self.pipedrive_field_metadata if m.key}

        self._internal_type_map = {
            m.key: utils.PIPEDRIVE_TO_INTERNAL_FIELD_TYPE_MAP.get(m.field_type)
            for m in self.pipedrive_field_metadata if m.key
        }

        self._field_meta_map = {m.key: m for m in self.pipedrive_field_metadata if m.key}
    
    def _precompute_custom_field_slugs(self) -> None:
        for field in self.pipedrive_field_metadata:
            key = getattr(field, "key", None)
            label = getattr(field, "name", key)
            if key:
                self.custom_field_slug_map[key] = self.utils.slugify(label)

    # ───────────────────────────────
    # 3. VALIDAÇÃO DO LOTE
    # ───────────────────────────────
    def _validate_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        • Valida cada item com o Pydantic principal.
        • Mantém campos extras (`extra="allow"`) preservados.
        """
        out: List[Dict[str, Any]] = []
        for idx, data in enumerate(batch):
            try:
                model = self.pydantic_model_main.model_validate(data)
                out.append(model.model_dump(exclude_none=False))
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "Validation failed (%s idx=%s): %s", self.entity_name, idx, exc, extra={"preview": str(data)[:200]}
                )
        return out

    # ───────────────────────────────
    # 4. HANDLERS ESPECÍFICOS
    # ───────────────────────────────
    def _process_specific_standard_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.specific_field_handlers:
            return df

        result = df.copy()
        for col, cfg in self.specific_field_handlers.items():
            if col not in result.columns or result[col].isnull().all():
                continue
            if col in self.primary_keys:
                continue

            fn: Callable = cfg["function"]
            slug = self.utils.slugify(cfg.get("slug_name", col))
            params = cfg.get("params", {})

            try:
                exploded = fn(result[col], slug=slug, **params)
                result.drop(columns=[col], inplace=True, errors="ignore")
                result = pd.concat([result, exploded], axis=1)
            except Exception as exc:  # noqa: BLE001
                self.logger.error("Specific handler error (%s.%s): %s", self.entity_name, col, exc, exc_info=True)
        return result

    # ───────────────────────────────
    # 5. EXPLODIR CUSTOM_FIELDS
    # ───────────────────────────────
    def _get_source_series(
        self, df: pd.DataFrame, key: str, is_custom: bool
    ) -> Optional[pd.Series]:
        """Localiza a série correta (coluna padrão ou dentro de custom_fields)."""
        if key in df.columns:
            return df[key]
        if is_custom:
            return df["custom_fields"].apply(
                lambda cf: cf.get(key) if isinstance(cf, dict) else None
            )
        return None

    def _explode_custom_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        if "custom_fields" in df.columns:
            original_custom_fields_data = df["custom_fields"].copy()
        else:
            original_custom_fields_data = pd.Series([None] * len(df), index=df.index, dtype=object)

        if not self.pipedrive_field_metadata:
            proc = df.drop(columns=["custom_fields"], errors="ignore")
            proc["custom_fields_raw"] = original_custom_fields_data
            return proc

        proc = df.copy()
        def ensure_dict_or_none(item: Any) -> Optional[Dict[str, Any]]:
            if isinstance(item, dict):
                return item
            if item is None:
                return None
            return {} 

        if "custom_fields" in proc.columns:
            proc["custom_fields"] = proc["custom_fields"].apply(ensure_dict_or_none)
        else:
            final_df_no_explode = proc.drop(columns=["custom_fields"], errors="ignore")
            final_df_no_explode["custom_fields_raw"] = original_custom_fields_data
            return final_df_no_explode


        fn_map = {
            "text": self.utils.explode_text,
            "numeric": self.utils.explode_numeric,
            "monetary": self.utils.explode_monetary,
            "enum": self.utils.explode_enum,
            "address": self.utils.explode_address,
            "phone_list": self.utils.explode_list_column,
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
            
            internal_type = self.utils.PIPEDRIVE_TO_INTERNAL_FIELD_TYPE_MAP.get(t)
            explode_function = fn_map.get(internal_type)
            
            if explode_function is None:
                self.logger.debug(f"No explode function for Pipedrive type '{t}' (internal: {internal_type}), key: {k}")
                continue

            slug = self.custom_field_slug_map.get(k, self.utils.slugify(getattr(meta, "name", k)))

            # Busca o valor:
            # 1. Diretamente de uma coluna com a chave hash (improvável para dados brutos da API)
            # 2. Do dicionário dentro da coluna "custom_fields"
            # Garante que 'd' é um dicionário antes de chamar d.get(k)
            source_series = proc[k] if k in proc.columns else proc["custom_fields"].apply(
                lambda d: d.get(k) if isinstance(d, dict) else None
            )
            
            if source_series.isnull().all() and internal_type not in ["enum", "phone_list", "address"]:
                 pass


            kwargs: dict[str, Any] = {}
            if internal_type == "enum":
                kwargs["options"] = getattr(meta, "options", None)
            elif internal_type == "phone_list":
                cfg_params = self.specific_field_handlers.get(k, {}).get("params", {})
                kwargs.update(
                    value_key=cfg_params.get("value_key", "value"),
                    label_key=cfg_params.get("label_key", "label"),
                    primary_key=cfg_params.get("primary_key", "primary")
                )

            try:
                exploded_data = explode_function(source_series.copy(), slug=slug, **kwargs)
                if isinstance(exploded_data, pd.Series):
                    new_cols[exploded_data.name] = exploded_data
                elif isinstance(exploded_data, pd.DataFrame):
                    for col_name in exploded_data.columns:
                        new_cols[col_name] = exploded_data[col_name]
            except Exception as e:
                self.logger.error(f"Error exploding custom field {k} (slug: {slug}): {e}", exc_info=True)

        if "custom_fields" in proc.columns:
            known_metadata_keys = {getattr(m, "key", None) for m in self.pipedrive_field_metadata}
            all_data_custom_field_keys = set()
            for item_dict in proc["custom_fields"]:
                if isinstance(item_dict, dict):
                    all_data_custom_field_keys.update(item_dict.keys())
            
            extra_keys = all_data_custom_field_keys - known_metadata_keys
            for extra_key in extra_keys:
                extra_slug = self.utils.slugify(extra_key) 
                new_cols[f"{extra_slug}_raw_extra"] = proc["custom_fields"].apply(
                    lambda d: d.get(extra_key) if isinstance(d, dict) else None
                )

        if new_cols:
            proc = proc.assign(**new_cols)

        final_df = proc.drop(columns=["custom_fields"], errors="ignore")
        
        return final_df

    # ───────────────────────────────
    # 6. AJUSTE FINAL DE TIPO & ESQUEMA
    # ───────────────────────────────
    def _ensure_final_schema_and_types(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()

        for pk in self.primary_keys:
            if pk not in result.columns:
                result[pk] = np.nan

        for col in self.core_columns:
            if col not in result.columns:
                result[col] = np.nan 

        # conversão de datetime automática por sufixo
        sufixos_dt = ("_time", "_date")
        for col in result.columns:
            if col.endswith(sufixos_dt):
                result[col] = pd.to_datetime(result[col], errors="coerce", utc=True)

        result = result.convert_dtypes()

        ordered: list[str] = [c for c in (*self.primary_keys, *self.core_columns) if c in result.columns]
        remaining = [c for c in result.columns if c not in ordered]
        result = result[ordered + sorted(remaining)]
        result = result.loc[:, ~result.columns.duplicated()]
        return result

    # ───────────────────────────────
    # 7. LOOP PRINCIPAL
    # ───────────────────────────────
    def run_sync(self,
                 updated_since_val: Optional[str] = None,
                 additional_params: Optional[Dict[str, Any]] = None) -> int:
        """
        Sincroniza entidades do Pipedrive.
        Pode realizar uma sincronização completa ou incremental baseada no parâmetro updated_since_val.

        Args:
            updated_since_val: Se fornecido (formato YYYY-MM-DDTHH:MM:SSZ),
                               busca apenas entidades atualizadas desde este timestamp.
            additional_params: Outros parâmetros a serem passados para a API.

        Returns:
            O número total de registros processados.
        """
        try:
            self.logger = get_run_logger()
        except Exception:
            self.logger = structlog.get_logger(__name__).bind(entity_name=self.entity_name if hasattr(self, 'entity_name') else "UnknownEntity")

        self.logger.info(f"Iniciando run_sync para entidade: {self.entity_name}")

        all_fields = self._fetch_and_prepare_entity_fields()
        all_records_processed = []
        total_count = 0

        # Prepara os parâmetros para a chamada da API
        api_call_params = {'limit': PipedriveAPIClient.DEFAULT_PAGINATION_LIMIT} 
        if additional_params:
            api_call_params.update(additional_params)

        if updated_since_val:
            # Assume-se que o endpoint principal (V2) suporta 'updated_since'
            api_call_params['updated_since'] = updated_since_val
            self.logger.info(f"Sincronização incremental para {self.entity_name}: usando updated_since='{updated_since_val}'")
        else:
            self.logger.info(f"Sincronização completa para {self.entity_name}: não usando updated_since.")

        page_num = 1
        for page_data_list in self.api_client.stream_all_entities(
            endpoint=self.api_endpoint_main,
            params=api_call_params # Passa os parâmetros combinados
        ):
            self.logger.info(f"Processando página {page_num} para {self.entity_name} com {len(page_data_list)} registros.")
            page_num += 1
            if not page_data_list:
                self.logger.info(f"Página de dados vazia recebida para {self.entity_name}, continuando para o caso de haver mais páginas (raro).")
                continue

            df_page = pd.DataFrame(page_data_list)
            if df_page.empty:
                self.logger.info(f"DataFrame vazio após converter página de dados para {self.entity_name}.")
                continue

            df_processed = self._process_page_data(df_page, all_fields)
            if not df_processed.empty:
                all_records_processed.append(df_processed)
                total_count += len(df_processed)
            else:
                self.logger.info(f"DataFrame vazio após processamento da página para {self.entity_name}.")

        if not all_records_processed:
            self.logger.info(f"Nenhum registro para salvar para {self.entity_name} após processar todas as páginas.")
            return 0

        final_df = pd.concat(all_records_processed, ignore_index=True)
        self.logger.info(f"Total de {total_count} registros processados para {self.entity_name} antes de salvar.")

        if not final_df.empty:
            self.repository.save(final_df)
            self.logger.info(f"Dados salvos com sucesso para {self.entity_name}. Total de registros: {len(final_df)}.")
        else:
            self.logger.info(f"DataFrame final vazio para {self.entity_name}. Nada foi salvo.")

        return total_count
