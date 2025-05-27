from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/leadLabels", version="v1", cost=10)
@with_cache(ttl=3600, key_prefix="pipedrive:lead_labels")
def fetch_lead_labels(client, params=None):
    """Busca labels associadas a leads."""
    url = f"{client.BASE_URL_V1}/leadLabels"
    return client.get(url, params=params).json()
