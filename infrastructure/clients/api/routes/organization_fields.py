from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/organizationFields", version="v1", cost=20)
@with_cache(ttl=3600, key_prefix="pipedrive:organization_fields")
def fetch_organization_fields(client, params=None):
    """Busca os campos customizados de organizações."""
    url = f"{client.BASE_URL_V1}/organizationFields"
    return client.get(url, params=params).json()
