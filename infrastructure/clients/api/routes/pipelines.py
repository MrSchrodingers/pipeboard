from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/pipelines", version="v2", cost=5)
@with_cache(ttl=3600, key_prefix="pipedrive:pipelines")
def fetch_pipelines(client, params=None):
    """
    Busca todos os pipelines no Pipedrive (V2).
    """
    url = f"{client.BASE_URL_V2}/pipelines"
    return client.get(url, params=params).json()
