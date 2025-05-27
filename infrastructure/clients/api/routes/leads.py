from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/leads", version="v1", cost=20)
@with_cache(ttl=3600, key_prefix="pipedrive:leads")
def fetch_leads(client, params=None):
    """Busca todos os leads (v1)."""
    url = f"{client.BASE_URL_V1}/leads"
    return client.get(url, params=params).json()
