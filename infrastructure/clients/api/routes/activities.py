from infrastructure.clients.api.cache_decorator import with_cache
from infrastructure.clients.api.route_registry import route_registry
from infrastructure.clients.api.base_client import BasePipedriveAPIClient

@route_registry.register(endpoint="/activities", version="v2", cost=10) 
@with_cache(ttl=300, key_prefix="pipedrive:activities_page") 
def fetch_activities_page(client: BasePipedriveAPIClient, params: dict | None = None):
    url = f"{client.BASE_URL_V2}/activities"
    return client.get(url, params=params).json()