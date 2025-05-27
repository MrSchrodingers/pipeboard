from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/filters", version="v1", cost=20)
@with_cache(ttl=3600, key_prefix="pipedrive:filters")
def fetch_filters(client, params=None):
    """Busca filtros salvos do usuário (Filters)."""
    url = f"{client.BASE_URL_V1}/filters"
    return client.get(url, params=params).json()
