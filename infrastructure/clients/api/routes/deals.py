from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry

@route_registry.register(endpoint="/deals", version="v2", cost=10)
@with_cache(ttl=3600, key_prefix="pipedrive:deals")
def fetch_deals(client, params=None):
    """Busca negócios (deals) com paginação via V2."""
    url = f"{client.BASE_URL_V2}/deals"
    return client.get(url, params=params).json()

@route_registry.register(endpoint="/deals/detail/changelog", version="v1", cost=20)
@with_cache(ttl=3600, key_prefix="pipedrive:deal_changelog")
def fetch_deal_changelog(client, deal_id: int):
    """Busca o changelog de um negócio específico."""
    url = f"{client.BASE_URL_V1}/deals/{deal_id}/changelog"
    return client.get(url).json()
