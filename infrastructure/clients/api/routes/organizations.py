from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/organizations", version="v2", cost=10)
@with_cache(ttl=3600, key_prefix="pipedrive:organizations")
def fetch_organizations(client, params=None):
    """Busca todas as organizações cadastradas."""
    url = f"{client.BASE_URL_V1}/organizations"
    return client.get(url, params=params).json()
