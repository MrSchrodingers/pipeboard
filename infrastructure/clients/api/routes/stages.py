from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/stages", version="v2", cost=5)
@with_cache(ttl=3600, key_prefix="pipedrive:stages")
def fetch_stages(client, params=None):
    """
    Busca todos os stages (etapas do pipeline) no Pipedrive (V2).
    """
    url = f"{client.BASE_URL_V2}/stages"
    return client.get(url, params=params).json()
