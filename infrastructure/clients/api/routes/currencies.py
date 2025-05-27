from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/currencies", version="v1", cost=20)
@with_cache(ttl=3600, key_prefix="pipedrive:currencies")
def fetch_currencies(client, params=None):
    """Busca todas as moedas disponíveis no Pipedrive."""
    url = f"{client.BASE_URL_V1}/currencies"
    return client.get(url, params=params).json()
