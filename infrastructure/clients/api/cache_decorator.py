import json
import hashlib
import structlog
from functools import wraps
from inspect import signature
from typing import Callable
from infrastructure.observability import metrics

log = structlog.get_logger(__name__)

def _hash_dict(d: dict) -> str:
    """
    Produz um hash estável a partir de qualquer dicionário,
    garantindo ordem determinística das chaves.
    """
    encoded = json.dumps(d, sort_keys=True, default=str)
    return hashlib.md5(encoded.encode()).hexdigest()

def with_cache(ttl: int = 300, key_prefix: str | None = None):
    """
    Cacheia a resposta levando em conta todos os argumentos posicionais +
    nomeados (depois de serializar para JSON ordenado).
    """
    def decorator(func: Callable):
        sig = signature(func)

        @wraps(func)
        def wrapper(client, *args, **kwargs):
            cache = getattr(client, "cache", None)
            if cache is None:
                return func(client, *args, **kwargs)

            cache = getattr(client, "cache", None)
            log_context = getattr(client, "log", structlog.get_logger())
    
            # Empacota args + kwargs em dict
            bound = sig.bind_partial(client, *args, **kwargs)
            bound.apply_defaults()
            # Remove o próprio `client`
            bound_arguments = dict(bound.arguments)
            bound_arguments.pop("client", None)

            hash_suffix = _hash_dict(bound_arguments)
            cache_key = f"{key_prefix or func.__name__}:{hash_suffix}"
            
            entity_label_for_metric = key_prefix or func.__name__
            cached = cache.get(cache_key)
            if cached is not None:
                metrics.pipedrive_api_cache_hit_total.labels(entity=entity_label_for_metric, source="redis").inc()
                log_context.debug("Cache hit", cache_key=cache_key)
                return cached

            metrics.pipedrive_api_cache_miss_total.labels(entity=entity_label_for_metric, source="redis").inc()
            log_context.debug("Cache miss", cache_key=cache_key)
            result = func(client, *args, **kwargs)
            if result is not None:
                cache.set(cache_key, result, ex_seconds=ttl)
            return result

        return wrapper
    return decorator
