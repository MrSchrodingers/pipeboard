import structlog
from typing import List, Dict, Any, Union, Iterator # Iterator adicionado

from infrastructure.config.settings import settings
from infrastructure.observability import metrics
from infrastructure.redis.redis_cache import RedisCache
from infrastructure.clients.api.base_client import BasePipedriveAPIClient
from infrastructure.clients.api.route_registry import route_registry, RouteInfo

log = structlog.get_logger(__name__)

class PipedriveAPIClient:
    DEFAULT_PAGINATION_LIMIT = 500

    def __init__(self, enable_compression: bool = False):
        self.api_key = settings.PIPEDRIVE_API_KEY
        self.redis_url = settings.REDIS_URL
        self._last_pagination: dict[str, Any] | None = None
        self.cache = RedisCache(connection_string=self.redis_url, enable_compression=enable_compression)
        if not self.api_key:
            log.error("PIPEDRIVE_API_KEY is missing from settings.")
            raise ValueError("Missing Pipedrive API Key.")
        
        self.http_client = BasePipedriveAPIClient(api_key=self.api_key)
        self.http_client.cache = self.cache 
        self.http_client.log = log.bind(client_type="BasePipedriveAPIClient_Instance")
        self.routes = route_registry

        log.info("PipedriveAPIClient initialized", compression_enabled=enable_compression)



    # ───────────────────── helper interno
    def _store_pagination(self, pagination: dict[str, Any] | None) -> None:
        """Guarda a última paginação observada (V1 ou V2)."""
        if pagination is not None:
            self._last_pagination = pagination


    # ───────────────────── getter público
    def get_last_pagination(self) -> dict[str, Any] | None:
        """
        Retorna o dicionário de paginação da última chamada feita
        por stream_all_entities(). Pode ser None se nenhuma chamada foi feita.
        """
        return self._last_pagination                        

    def _register_api_usage(self, route_info: 'RouteInfo', calls_made: int = 1):
        logger = getattr(self, "log", structlog.get_logger(__name__))
        
        try:
            from infrastructure.observability.metrics import pipedrive_api_token_cost_total

            if route_info is None:
                logger.error("Tentativa de registrar uso de API com route_info None.")
                return

            normalized_endpoint = self._normalize_endpoint_for_metrics(route_info.endpoint)
            
            cost_value = getattr(route_info, 'cost', 0)
            if not isinstance(cost_value, (int, float)):
                logger.warning(
                    "Custo inválido em RouteInfo, não é numérico.",
                    endpoint=route_info.endpoint,
                    cost_received=cost_value,
                    type_received=type(cost_value)
                )
                cost_value = 0 

            actual_cost = cost_value * calls_made
            
            logger.info( 
                "Tentando registrar métrica de uso de API (custo de token)", 
                raw_endpoint=route_info.endpoint, 
                normalized_endpoint=normalized_endpoint,
                api_version=route_info.version,
                base_cost_per_call=cost_value, 
                calls_made_for_this_op=calls_made,
                calculated_total_cost=actual_cost
            )

            if actual_cost > 0:
                pipedrive_api_token_cost_total.labels(endpoint=normalized_endpoint).inc(actual_cost)
                logger.info(
                    "Métrica 'pipedrive_api_token_cost_total' incrementada.", 
                    endpoint=normalized_endpoint, 
                    incremented_by=actual_cost
                )
            else:
                logger.warning(
                    "Custo de API calculado é zero ou negativo. Métrica 'pipedrive_api_token_cost_total' não incrementada.",
                    endpoint=normalized_endpoint,
                    calculated_cost=actual_cost,
                    base_cost=cost_value,
                    calls_made=calls_made
                )

        except ImportError:
            logger.error(
                "FALHA DE IMPORTAÇÃO: Métrica 'pipedrive_api_token_cost_total' não encontrada. "
                "Verifique infrastructure.observability.metrics."
            )
        except Exception as e:
            raw_endpoint_for_error = getattr(route_info, 'endpoint', 'unknown_endpoint_due_to_error_in_route_info')
            logger.error(
                "Falha ao registrar métrica de uso de API (custo de token).", 
                endpoint=raw_endpoint_for_error, 
                exc_info=str(e) 
            )

    def _normalize_endpoint_for_metrics(self, endpoint: str) -> str:
        parts = endpoint.strip('/').split('/')
        return f"/{parts[0]}" if parts and parts[0] else "/"

    def stream_all_entities(self, endpoint: str, **kwargs) -> Iterator[List[Dict[str, Any]]]:
        """
        Busca todas as entidades de um endpoint paginado e as produz (yields) página por página.
        Cada item produzido é uma lista de dicionários (uma página de dados).
        """
        route_info = self.routes.get_route_info(endpoint)
        log.info(f"Streaming all entities for endpoint: {endpoint} (API version: {route_info.version})")
        
        current_call_params = kwargs.pop('params', {}).copy()
        current_call_params['limit'] = current_call_params.get('limit', self.DEFAULT_PAGINATION_LIMIT)
        
        calls_count = 0
        total_entities_yielded = 0

        if route_info.version == 'v1':
            log.debug(f"Using start/limit pagination for V1 endpoint {endpoint} (streaming)")
            current_call_params['start'] = current_call_params.get('start', 0)
            more_items = True

            while more_items:
                fetch_kwargs = {'params': current_call_params.copy(), **kwargs}
                try:
                    response_json = route_info.fetch_fn(self.http_client, **fetch_kwargs)
                    self._register_api_usage(route_info) # Registra custo por chamada de página
                    calls_count += 1
                except Exception as e:
                    log.error(f"API call failed during V1 streaming for {endpoint}", params=current_call_params, exc_info=e)
                    break 

                page_data = response_json.get("data")
                if page_data is None or not isinstance(page_data, list):
                    log.warn(f"No valid 'data' list in V1 response for {endpoint}. Stopping stream.", params=current_call_params, data_type=type(page_data))
                    break
                
                if page_data: # Só faz yield se houver dados na página
                    yield page_data
                    total_entities_yielded += len(page_data)
                
                pagination_info = response_json.get("additional_data", {}).get("pagination", {})
                self._store_pagination(pagination_info)
                more_items = pagination_info.get("more_items_in_collection", False)
                
                if more_items:
                    next_start = pagination_info.get("next_start")
                    if next_start is not None:
                        current_call_params['start'] = next_start
                    else: 
                        current_call_params['start'] += len(page_data)
                        if not page_data: 
                            log.warn(f"V1 Pagination for {endpoint} reported more items but no data. Stopping stream.", params=current_call_params)
                            more_items = False
                if not page_data and more_items:
                     log.warn(f"V1 Pagination for {endpoint} reported more items but empty list. Stopping stream.", params=current_call_params)
                     more_items = False
        
        elif route_info.version == 'v2':
            log.debug(f"Using cursor-based pagination for V2 endpoint {endpoint} (streaming)")
            current_call_params.pop('start', None) 
            next_cursor_val: str | None = current_call_params.pop('cursor', None) 
            first_call = True

            while first_call or next_cursor_val:
                if first_call:
                    first_call = False
                
                if next_cursor_val: 
                    current_call_params['cursor'] = next_cursor_val
                    metrics.pipedrive_api_cursor_advance_total.labels(endpoint=self._normalize_endpoint_for_metrics(endpoint)).inc()
                elif 'cursor' in current_call_params and not first_call:
                    current_call_params.pop('cursor', None)


                fetch_kwargs = {'params': current_call_params.copy(), **kwargs}
                try:
                    response_json = route_info.fetch_fn(self.http_client, **fetch_kwargs)
                    self._register_api_usage(route_info) # Registra custo por chamada de página
                    calls_count += 1
                except Exception as e:
                    log.error(f"API call failed during V2 streaming for {endpoint}", params=current_call_params, exc_info=e)
                    break 

                page_data = response_json.get("data")
                if page_data is None or not isinstance(page_data, list):
                    log.warn(f"No valid 'data' list in V2 response for {endpoint}. Stopping stream.", params=current_call_params, data_type=type(page_data))
                    break

                if page_data:
                    yield page_data
                    total_entities_yielded += len(page_data)

                additional_data_info = response_json.get("additional_data", {})
                self._store_pagination(additional_data_info)
                next_cursor_val = additional_data_info.get("next_cursor") 
                
                if not next_cursor_val: 
                    log.debug(f"No 'next_cursor' in additional_data for {endpoint}. Ending V2 stream.")
                    break
        else:
            log.error(f"Unsupported API version '{route_info.version}' for streaming for {endpoint}.")
            # Poderia fazer yield de uma única chamada se desejado, ou levantar erro

        log.info(f"Finished streaming for {endpoint}. Yielded {total_entities_yielded} entities in {calls_count} API call(s).")


    def call(self, endpoint: str, fetch_all: bool = False, **kwargs) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """
        Mantém o método 'call' para chamadas únicas ou para buscar tudo e agregar na memória (legado).
        Para processamento eficiente de grandes volumes, use 'stream_all_entities'.
        """
        if fetch_all:
            log.warn(f"Using fetch_all=True for {endpoint}. This loads all data into memory. For large datasets, consider using stream_all_entities().")
            all_data_pages = list(self.stream_all_entities(endpoint, **kwargs)) # Consome o gerador
            aggregated_entities = [item for page in all_data_pages for item in page]
            return aggregated_entities
        else:
            route_info = self.routes.get_route_info(endpoint)
            self._register_api_usage(route_info, calls_made=1)
            return route_info.fetch_fn(self.http_client, **kwargs)

    def available_routes(self) -> List[str]:
        return list(self.routes.all_routes().keys())