from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/personFields", version="v1", cost=20)
@with_cache(ttl=3600, key_prefix="pipedrive:person_fields")
def fetch_person_fields(client, params=None):
    """Busca os campos customizados de pessoas (contacts)."""
    url = f"{client.BASE_URL_V1}/personFields"
    return client.get(url, params=params).json()
