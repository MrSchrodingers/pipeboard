from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/users", version="v1", cost=20)
@with_cache(ttl=3600, key_prefix="pipedrive:users")
def fetch_users(client, params=None):
    """
    Busca todos os usuários no Pipedrive (V1).
    """
    url = f"{client.BASE_URL_V1}/users"
    return client.get(url, params=params).json()
